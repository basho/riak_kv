%% -------------------------------------------------------------------
%%
%% riak_kv_hints: Using a golomb coded set for approximate presence checking
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-define(CURRENT_VERSION, 1).

-define(KEYS_PER_SLOT_FACTOR, 6).
    %% This means 2 ^ 6 will be the target number of keys in a slot
    %% The count of slots is limited to 256 - so the maximum size at the
    %% expected fpr = 64 * 256
    %% Decreasing this factor will reduce the time to check the filter, but
    %% reduce the capacity of the filter before the fpr deteriorates
-define(HELPER_BAND, 127).
-define(HELPER_BITS, 7).
    %% There are some bits of the hash used in a tiny bloom to reduce the
    %% number of slot checks required
-define(HASH_BITLIMIT, 32).
    %% It is assumed we're dealing with 32-bit hashes

-define(FNV32_PRIME, 16777619).
-define(FNV32_INIT, 2166136261).
-define(FNV32_MASK, 16#FFFFFFFF).
    %% Constants required for Fowler-Noll-Vo hash function

-module(riak_kv_hints).

-export([create_gcs_metavalue/3,
            check_key/2, check_key/3, check_all_keys/3,
            check_all_hashes/4, check_hashes/3,
            hash/2]).

-type gcs() :: binary().
-type hash_algo() :: fnva|md5.

-export_type([gcs/0, hash_algo/0]).

-include_lib("eunit/include/eunit.hrl").

%% @doc
%% Create a serialised (rice-encoded) golomb coded set (partitioned) to
%% represent the keys in the keylist.
%% BitsPerKey is a target number of bits per key to be used in the filter,
%% setting a higher number of BitsPerKey will lead to a lower FPR.
%%
%% e.g. Target BitsPerKey = 8 (minimum):
%% KeyCount | FPR | BitsPerKey
%% 1000 | 8.30% | 6.34
%% 2000 | 8.37% | 6.34
%% 3000 | 5.00% | 7.76
%% 5000 | 3.44% | 8.86
%% 8000 | 8.58% | 6.36
%% 13000 | 5.80% | 7.33
%%
%% e.g. Target BitsPerKey = 11:
%% KeyCount | FPR | BitsPerKey
%% 1000 | 1.29% | 9.59
%% 2000 | 1.31% | 9.58
%% 3000 | 0.66% | 10.90
%% 5000 | 0.49% | 11.97
%% 8000 | 1.29% | 9.56
%% 13000 | 0.90% | 10.48
%%
%% e.g. Target BitsPerKey = 15:
%% KeyCount | FPR | BitsPerKey
%% 1000 | 0.04% | 13.69
%% 2000 | 0.07% | 13.67
%% 3000 | 0.04% | 14.98
%% 5000 | < 0.01% | 16.02
%% 8000 | 0.10% | 13.64
%% 13000 | 1.74% | 14.55 
%%     - note that this is too big a number of keys for this FPR
%%
%% Where the length of a KeyList is juts bigger than a factor of 2 the 
%% actual BitsPerKey will be lower, and the fasl-positive rate higher.  As
%% the length gets closer to the next factor of 2 the BitsPerKey increases and
%% the false positive rate drops.
%% The relationship between FPR and bits per key is close to that found with
%% optimised bloom filters
%% - http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
%% The advantage of this probabilistic data format is that to check a key over
%% many such sets, where all sets may be of different sizes - a single hash can
%% be used for all checks.  Differing numbers of hashes is not required
%% depending on the size of each individual set.
%% With %KEYS_PER_SLOT_FACTOR at the default (6), then the capacity of the data
%% structure is 16K keys.  Beyond this number, the size of the structure will
%% grow in line with the number of keys, but the FPR will decrease as it grows
%% and the itme to check will increase.
-spec create_gcs_metavalue(list(term()), pos_integer(), hash_algo()) -> gcs().
create_gcs_metavalue(KeyList, BitsPerKey, HA)
                                    when BitsPerKey < 20, BitsPerKey >= 8 ->
    %% Slot count set to try and keep the counts to less than 256 keys per slot
    %% without increasing the number of slots (and hence associated storage
    %% inefficiency) needlessly
    FPRFactor = BitsPerKey - 6,
    %% This is an approximate - there will be fluctuations by +/- 1.5 bits per
    %% key depending on the precise size of the data structure
    Length = length(KeyList),
    SlotFactor = min(8, bitshift(Length bsr ?KEYS_PER_SLOT_FACTOR, 0)),
    HashBitsRequired =
        max(16, SlotFactor + FPRFactor + ?KEYS_PER_SLOT_FACTOR + ?HELPER_BITS),
        %% How many bits of the hash will be required.  This needs to be < 32
        %% as the hash will only be 32-bit integer.  It must be at least 16, a
        %% limit imposed by the format in which this value will be stored
    case HashBitsRequired of
        HashBitsRequired when HashBitsRequired < ?HASH_BITLIMIT ->
            SlotCount = 1 bsl SlotFactor,
            InitAcc = array:new(SlotCount, [{default, {{0, 0, 0, 0}, []}}]),
            HashLists =
                create_hashlist(KeyList,
                                SlotCount,
                                {HA, HashBitsRequired},
                                InitAcc),
            serialise_bloom(HashLists, {HA, HashBitsRequired}, FPRFactor);
        _ ->
            create_gcs_metavalue(KeyList, FPRFactor  - 1, HA)
    end.

-spec check_key(binary(), gcs()) -> boolean().
check_key(Key, GCS) ->
    check_key(Key, GCS, false).

%% @doc
%% Check a key against a riak_kv_hints:gcs(), with the option to confirm no
%% matches by checking the entire slot is not corrupted.
-spec check_key(binary(), gcs(), boolean()) -> boolean().
check_key(Key,
            <<Version:4/integer, HA:4/integer, GCSBloomBin/binary>>,
            CrcCheckOnMiss) when Version == 1 ->
    HAlgo = case HA of 1 -> fnva; 2 -> md5 end,
    check_hashes([hash(Key, HAlgo)], GCSBloomBin, CrcCheckOnMiss).


%% @doc
%% check to confirm all keys are present within a GCS
-spec check_all_keys(list(binary()), gcs(), boolean()) -> boolean().
check_all_keys(KeyList,
                <<Version:4/integer, HA:4/integer, GCSBloomBin/binary>>,
                CrcCheckOnMiss) when Version == 1 ->
    HAlgo = case HA of 1 -> fnva; 2 -> md5 end,
    check_hashes(lists:map(fun(Key) -> hash(Key, HAlgo) end, KeyList),
                    GCSBloomBin,
                    CrcCheckOnMiss).

%% @doc
%% Check all pre-calculated hashes
-spec check_all_hashes(hash_algo(), list(pos_integer()), binary(), boolean())
                                                                -> boolean().
check_all_hashes(fnva,
                    HashList,
                    <<Version:4/integer, 1:4/integer, GCSBloomBin/binary>>,
                    CrcCheckOnMiss) when Version == 1 ->
    check_hashes(HashList, GCSBloomBin, CrcCheckOnMiss);
check_all_hashes(md5,
                    HashList,
                    <<Version:4/integer, 2:4/integer, GCSBloomBin/binary>>,
                    CrcCheckOnMiss) when Version == 1 ->
    check_hashes(HashList, GCSBloomBin, CrcCheckOnMiss).

%% @doc
%% Check the hash of a key against a riak_kv_hints:gcs(), with the option to
%% confirm no matches by checking the entire slot is not corrupted.
-spec check_hashes(list(pos_integer()), binary(), boolean()) -> boolean().
check_hashes(HashList,
            <<HBR:4/integer, FPRFactor:4/integer, SlotCountM1:8/integer,
                RestGCSBin/binary>>,
            CrcCheckOnMiss) ->
    SlotCount = SlotCountM1 + 1,
    HashBitsRequired = HBR + 16,
    check_hashes(HashList,
                    RestGCSBin, CrcCheckOnMiss,
                    SlotCount, HashBitsRequired, FPRFactor,
                    true, []).
    

%%%============================================================================
%%% Internal Functions - GCS
%%%============================================================================

-spec check_hashes(list(pos_integer()),
                    binary(), boolean(),
                    pos_integer(), pos_integer(), pos_integer(),
                    boolean(), list(fun())) -> boolean().
check_hashes(_HL, _GCSBin, _CrcCheck, _SlotCnt, _HBR, _FPR, false, _SlotChk) ->
    false;
check_hashes([], _GCSBin, _CrcCheck, _SlotC, _HBR, _FPR, true, SlotChecks) ->
    CheckFun =
        fun(F, Acc) ->
            case Acc of
                false ->
                    false;
                true ->
                    F()
            end
        end,
    lists:foldl(CheckFun, true, SlotChecks);
check_hashes([Hash|T], GCSBin, CrcCheck, SlotC, HBR, FPR, true, SlotChecks) ->
    {Slot, SlotHash, FH} = get_hashslot(Hash, HBR, SlotC),
    PreSlot = Slot * 3,
    PostSlot = ((SlotC - (Slot - 1)) * 3) - 6,
    <<_Pre0:PreSlot/binary,
        StartPos:24/integer, EndPos:24/integer,
        _Post0:PostSlot/binary, SlotsBin/binary>> = GCSBin,
    SlotLength = EndPos - StartPos - 20,
    <<_Pre1:StartPos/binary,
        H0:32/integer, H1:32/integer, H2:32/integer, H3:32/integer,
        TopHash:32/integer, SlotBin:SlotLength/binary,
        _Post1/binary>> = SlotsBin,
    SlotCheckFun = 
        fun() ->
            check_slot(SlotHash, SlotBin, 0, FPR, 1 bsl FPR, TopHash, CrcCheck)
        end,
    check_hashes(T,
                    GCSBin, CrcCheck, SlotC, HBR, FPR,
                    check_helper(FH, {H0, H1, H2, H3}),
                    [SlotCheckFun|SlotChecks]).

-spec hash(binary(), hash_algo()) -> pos_integer().
hash(BinKey, fnva) ->
    fnv32a(BinKey);
hash(BinKey, md5) ->
    <<H:32/integer, _R/binary>> = crypto:hash(md5, BinKey),
    H.

-spec fnv32a(BinKey :: binary()) -> non_neg_integer().
fnv32a(BinKey) ->
    fnv32a(BinKey, ?FNV32_INIT).
         
fnv32a(<<H:8, T/bytes>>, State) ->
    Hash = ((State bxor H) * ?FNV32_PRIME) band ?FNV32_MASK,
    fnv32a(T, Hash);   
fnv32a(<<>>, State) ->
    State.
    


-spec bitshift(non_neg_integer(), non_neg_integer()) -> non_neg_integer().
bitshift(0, BitSize) ->
    BitSize;
bitshift(Length, BitSize) ->
    bitshift(Length bsr 1, BitSize + 1).

-spec create_hashlist(list(term()),
                        pos_integer(),
                        {hash_algo(), pos_integer()},
                        array:array()) -> array:array().
create_hashlist([], _, _, AccArray) ->
    AccArray;
create_hashlist([HeadKey|Rest], SlotCount, {HA, HashBitsRequired}, AccArray) ->
    {Slot, Hash, FH} = get_keyslot(HeadKey, HashBitsRequired, SlotCount, HA),
    {HelperInts, HashList} = array:get(Slot, AccArray),
    HelperInts0 = update_helper(FH, HelperInts),
    create_hashlist(Rest,
                    SlotCount,
                    {HA, HashBitsRequired},
                    array:set(Slot, {HelperInts0, [Hash|HashList]}, AccArray)).


-spec get_keyslot(term(), pos_integer(), pos_integer(), hash_algo())
                -> {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
get_keyslot({HA, Hash}, HashBitsRequired, SlotCount, HA) ->
    get_hashslot(Hash, HashBitsRequired, SlotCount);
get_keyslot(Key, HashBitsRequired, SlotCount, HA) ->
    get_hashslot(hash(Key, HA), HashBitsRequired, SlotCount).

-spec get_hashslot(non_neg_integer(), pos_integer(), pos_integer())
                -> {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
get_hashslot(Hash, HashBitsRequired, SlotCount) ->
    DownScale = ?HELPER_BITS + ?HASH_BITLIMIT - HashBitsRequired,
    SlimHash = Hash bsr DownScale,
    {SlimHash rem SlotCount, SlimHash div SlotCount, Hash band ?HELPER_BAND}.


check_helper(FH, {Helper0, Helper1, Helper2, Helper3}) ->
    {Slot, HelperInt} = get_helper(FH),
    case Slot of
        0 ->
            (Helper0 band HelperInt) == HelperInt;
        1 ->
            (Helper1 band HelperInt) == HelperInt;
        2 ->
            (Helper2 band HelperInt) == HelperInt;
        3 ->
            (Helper3 band HelperInt) == HelperInt
    end.


get_helper(FH) ->
    {FH rem 4, 1 bsl ((FH div 4) band 31)}.

update_helper(FH, {Helper0, Helper1, Helper2, Helper3}) ->
    {Slot, HelperInt} = get_helper(FH),
    case Slot of
        0 ->
            {Helper0 bor HelperInt, Helper1, Helper2, Helper3};
        1 ->
            {Helper0, Helper1 bor HelperInt, Helper2, Helper3};
        2 ->
            {Helper0, Helper1, Helper2 bor HelperInt, Helper3};
        3 ->
            {Helper0, Helper1, Helper2, Helper3 bor HelperInt}
    end.

-spec serialise_bloom(array:array(), {hash_algo(), pos_integer()}, pos_integer())
                                                                    -> gcs().
serialise_bloom(HashLists, {HA, HashBitsRequired}, FPRFactor) ->
    SlotCount = array:size(HashLists),
    serialise_bloom(HashLists, SlotCount, {HA, HashBitsRequired}, FPRFactor,
                    0, <<>>, <<>>, 0).

-spec serialise_bloom(array:array(),
                        pos_integer(), 
                        {hash_algo(), pos_integer()},
                        pos_integer(),
                        non_neg_integer(),
                        binary(), binary(),
                        non_neg_integer()) -> gcs().
serialise_bloom(_, SlotCount, HashInfo, FPRFactor, SlotCount,
                                                BloomBin, PosBin, PosAcc) ->
    finalise_bloom(BloomBin, SlotCount, HashInfo, FPRFactor,
                    <<PosBin/binary, PosAcc:24/integer>>);
serialise_bloom(HashListArray, SlotCount, HashInfo, FPRFactor, Counter,
                                                BloomBin, PosBin, PosAcc) ->
    {HelperInts, HashList} = array:get(Counter, HashListArray),
    SingleBloomBin =
        serialise_singlebloom(lists:usort(HashList),
                                HelperInts,
                                <<>>,
                                0,
                                1 bsl FPRFactor,
                                FPRFactor),
    serialise_bloom(HashListArray,
                    SlotCount, HashInfo, FPRFactor, Counter + 1,
                    <<BloomBin/binary, SingleBloomBin/binary>>,
                    <<PosBin/binary, PosAcc:24/integer>>,
                    PosAcc + byte_size(SingleBloomBin)).


-spec serialise_singlebloom(list(non_neg_integer()),
                                {non_neg_integer(),
                                    non_neg_integer(),
                                    non_neg_integer(),
                                    non_neg_integer()},
                                binary(),
                                non_neg_integer(),
                                pos_integer(), pos_integer()) -> binary().
serialise_singlebloom([], {HI0, HI1, HI2, HI3}, BloomBin, TopHash,
                                                        _Divisor, _Factor) ->
    BitSize = bit_size(BloomBin),
    TailBitsToAdd = 
        case BitSize rem 8 of
            0 ->
                0;
            N ->
                8 - N
        end,
    <<HI0:32/integer, HI1:32/integer, HI2:32/integer, HI3:32/integer,
        TopHash:32/integer, BloomBin/bitstring, 0:TailBitsToAdd/integer>>;
serialise_singlebloom([Hash|Rest],
                        HelperInts, BloomBin, TopHash, Divisor, Factor) ->
    HashGap = Hash - TopHash,
    Exp = buildexponent(HashGap div Divisor),
    Rem = HashGap rem Divisor,
    serialise_singlebloom(Rest,
                            HelperInts,
                            <<BloomBin/bitstring,
                                Exp/bitstring,
                                Rem:Factor/integer>>,
                            Hash,
                            Divisor,
                            Factor).

-spec finalise_bloom(binary(), pos_integer(), {hash_algo(), pos_integer()},
                                        pos_integer(), binary()) -> gcs().
finalise_bloom(BloomBin, SlotCount, {HA, HashBitsRequired}, FPRFactor, PosBin) ->
    HBR = HashBitsRequired - 16, % Always should be > 16, and < 32
    HAFlag = case HA of fnva -> 1; md5 -> 2 end,
    <<?CURRENT_VERSION:4/integer, HAFlag:4/integer,
        HBR:4/integer, FPRFactor:4/integer, (SlotCount - 1):8/integer,
        PosBin/binary, BloomBin/binary>>.


buildexponent(Exponent) ->
    buildexponent(Exponent, <<0:1>>).

buildexponent(0, OutputBits) ->
    OutputBits;
buildexponent(Exponent, OutputBits) ->
    buildexponent(Exponent - 1, <<1:1, OutputBits/bitstring>>).

findexponent(BitStr) ->
    findexponent(BitStr, 0).

findexponent(<<1:1/integer, T/bitstring>>, Acc) ->
    findexponent(T, Acc + 1);
findexponent(<<0:1/integer, T/bitstring>>, Acc) ->
    {ok, Acc, T};
findexponent(<<>>, _) ->
    error.

findremainder(BitStr, Factor) ->
    case BitStr of
        <<Remainder:Factor/integer, BitStrTail/bitstring>> ->
            {ok, Remainder, BitStrTail};
        _ ->
            error
    end.

-spec check_slot(non_neg_integer(), binary(), non_neg_integer(),
                    pos_integer(), pos_integer(), non_neg_integer(),
                    boolean()) -> boolean().
check_slot(TopHash, _SlotBin, 0, _Factor, _Divisor, TopHash, _Crc) ->
    %% The top hash in the slot is the has we're looking for, so this is
    %% immediately a match
    true;
check_slot(_SlotHash, _RemBin, TopHash, _Factor, _Divisor, TopHash, _Crc) ->
    %% The accumulator has reached to TopHash but we've not yet matched on the
    %% hash - so this must not be present
    false;
check_slot(SlotHash, _RemBin, SlotHash, _Factor, _Divisor, _TopHash, _Crc) ->
    %% The hash is equal to the accumulator - there is a match
    true;
check_slot(SlotHash, _RemBin, HashAcc, _Factor, _Divisor, _TopHash, false)
                                                        when HashAcc > SlotHash ->
    %% The accumulator has passed the hash, and we don't want to do a pseudo
    %% CRC check (i.e. roll through to the TopHash to confirm no corruption,
    %% and so if no CRC check we can sya no match 
    false;
check_slot(SlotHash, RemBin, HashAcc, Factor, Divisor, TopHash, Crc) ->
    %% Roll the hash accumulator forward to check for a new match - any error
    %% and we act as if it is present (as false positives are expected but
    %% false negatives may not be
    case findexponent(RemBin) of
        {ok, Exponent, RemBin1} ->
            case findremainder(RemBin1, Factor) of
                {ok, Remainder, RemBin2} ->
                    NextHash = HashAcc + Divisor * Exponent + Remainder,
                    check_slot(SlotHash, RemBin2, NextHash,
                                Factor, Divisor, TopHash, Crc);
                error ->
                    true
            end;
        error ->
            true
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-define(ZLIB_TEST_OPTS, [{compressed, 1}]).

pack(Term, lz4) ->
    {ok, B} = lz4:pack(erlang:term_to_binary(Term)),
    B;
pack(Term, none) ->
    erlang:term_to_binary(Term);
pack(Term, native) ->
    erlang:term_to_binary(Term, ?ZLIB_TEST_OPTS).

unpack(Binary, lz4) ->
    {ok, EBT} = lz4:unpack(Binary),
    erlang:binary_to_term(EBT);
unpack(Binary, _NativeOrNone) ->
    erlang:binary_to_term(Binary).

gen_keys(Seed, KeyCount, in_order) ->
    lists:map(fun(I) ->
                    list_to_binary(
                        lists:flatten(Seed ++ io_lib:format("~8..0B", [I])))
                end,
                lists:seq(1, KeyCount));
gen_keys(_Seed, KeyCount, uuid) ->
    lists:map(fun(_I) -> iolist_to_binary(leveled_util:generate_uuid()) end,
                lists:seq(1, KeyCount)).

size_test_() ->
    {timeout, 600, fun size_block_tester/0}.

function_test_() ->
    {timeout, 300, fun func_block_tester/0}.

bitsperkey_test_() ->
    {timeout, 300, fun bitrate_block_tester/0}.

multi_block_tester(KeyCount, FPR, TestCount, KeyType, HashAlgo, PackType) ->
    BlockCount = 24,
    TestLoops = 20,
    io:format(user,
                "~nTesting KeyCount ~w TargetBR ~w TestKeys ~w BlockCount ~w"
                    ++ " TestLoops ~w KeyType ~w HashAlgo ~w PackType ~w~n",
                [KeyCount, FPR, TestCount, BlockCount, TestLoops,
                    KeyType, HashAlgo, PackType]),
    Totals = 
        lists:foldl(fun(_I,  {AG0, AG1, AG2, AG3, AGCAND, AGCOR, AGS}) ->
                        {G0, G1, G2, G3, GCAND, GCOR, GS} =
                            block_tester(KeyCount, FPR, TestCount, BlockCount,
                                            KeyType, HashAlgo, PackType),
                        {AG0 + G0, AG1 + G1, AG2 + G2, AG3 + G3,
                            AGCAND + GCAND, AGCOR + GCOR, AGS + GS}
                    end,
                    {0, 0, 0, 0, 0, 0, 0},
                    lists:seq(1, TestLoops)),
    {TG0, TG1, TG2, TG3, TGCAND, TGCOR, TGS} = Totals,
    io:format(user,
                "GCS block times: build ~s open ~s check_and ~s check_or ~s~n"
                 ++
                "GCS ratios: fpr_and ~s fpr_or ~s size ~s bits pk~n",
                [fround(TG0/TestLoops),
                        fround(TG1/TestLoops),
                        fround(TG2/TestLoops),
                        fround(TG3/TestLoops),
                    fround(100 * TGCAND /
                            (TestLoops * BlockCount * TestCount)),
                    fround(100 * TGCOR /
                            (TestLoops * BlockCount * TestCount)),
                    fround(8 * TGS/(TestLoops * BlockCount * KeyCount))]).

fround(F) ->
    lists:flatten(io_lib:format("~.3f",[F])).

size_block_tester() ->
    KeyType = uuid,
    HashAlgo = fnva,
    PackType = lz4,
    BitsPK = 11,
    TestCount = 3,
    multi_block_tester(100, BitsPK, TestCount, KeyType, HashAlgo, PackType),
    multi_block_tester(200, BitsPK, TestCount, KeyType, HashAlgo, PackType),
    multi_block_tester(300, BitsPK, TestCount, KeyType, HashAlgo, PackType),
    multi_block_tester(500, BitsPK, TestCount, KeyType, HashAlgo, PackType),
    multi_block_tester(1000, BitsPK, TestCount, KeyType, HashAlgo, PackType),
    multi_block_tester(2000, BitsPK, TestCount, KeyType, HashAlgo, PackType),
    multi_block_tester(3000, BitsPK, TestCount, KeyType, HashAlgo, PackType),
    multi_block_tester(5000, BitsPK, TestCount, KeyType, HashAlgo, PackType),
    multi_block_tester(8000, BitsPK, TestCount, KeyType, HashAlgo, PackType).

bitrate_block_tester() ->
    KeyType = uuid,
    HashAlgo = fnva,
    PackType = lz4,
    multi_block_tester(6000, 9, 1, KeyType, HashAlgo, PackType),
    multi_block_tester(6000, 11, 1, KeyType, HashAlgo, PackType),
    multi_block_tester(6000, 14, 1, KeyType, HashAlgo, PackType).

func_block_tester() ->
    KeyCount = 7000,
    BitsPerKey = 13,
    TestCount = 1,
    multi_block_tester(KeyCount, BitsPerKey, TestCount, uuid, fnva, lz4),
    multi_block_tester(KeyCount, BitsPerKey, TestCount, uuid, md5, lz4),
    multi_block_tester(KeyCount, BitsPerKey, TestCount, in_order, fnva, lz4),
    multi_block_tester(KeyCount, BitsPerKey, TestCount, uuid, fnva, native),
    multi_block_tester(KeyCount, BitsPerKey, TestCount, uuid, fnva, none).



block_tester(KeyCount, FPR, TestKeys, BlockCount,
                    KeyType, HashAlgo, PackType) ->
    TestList = gen_keys("testm", TestKeys, KeyType),
    ListsOfKeys =
        lists:map(fun(I) ->
                        gen_keys("test" ++ io_lib:format("~4..0B", [I]),
                                    KeyCount,
                                    KeyType)
                    end,
                    lists:seq(1, BlockCount)),

    % Check we always find all the keys in the list
    PositiveTestList = lists:last(ListsOfKeys),
    GCSPosTest = create_gcs_metavalue(PositiveTestList, FPR, HashAlgo),
    lists:foreach(fun(PosK) ->
                        ?assertMatch(true, check_key(PosK, GCSPosTest, false))
                    end,
                    PositiveTestList),
    
    {TS0, GCSZ} =
        timer:tc(fun pack/2,
                    [lists:map(fun(KL) ->
                                    create_gcs_metavalue(KL, FPR, HashAlgo)
                                end,
                                ListsOfKeys),
                        PackType]),
    {TS1, GCS} = timer:tc(fun unpack/2, [GCSZ, PackType]),
    CheckGCSFunOr =
        fun(GBin) ->
            fun(K, Acc) ->
                % Check each key, looking for any to match
                case Acc of
                    true ->
                        true;
                    false ->
                        check_key(K, GBin, false)
                end
            end
        end,

    HashList = lists:map(fun(K) -> hash(K, HashAlgo) end, TestList),
    {TS2, HitListAND} =
        timer:tc(lists,
                    map,
                    [fun(G) ->
                            check_all_hashes(HashAlgo, HashList, G, false)
                        end,
                        GCS]),
    {TS3, HitListOR} =
        timer:tc(lists,
                    map,
                    [fun(G) ->
                            lists:foldl(CheckGCSFunOr(G), false, TestList)
                        end,
                        GCS]),
    
    FalseHitsAND = length(lists:filter(fun(X) -> X end, HitListAND)),
    FalseHitsOR = length(lists:filter(fun(X) -> X end, HitListOR)),

    {TS0, TS1, TS2, TS3, FalseHitsAND, FalseHitsOR, byte_size(GCSZ)}.
  


-endif.