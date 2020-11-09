%% -------------------------------------------------------------------
%%
%% riak_kv_mapreduce: convenience functions for defining common map/reduce phases
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Convenience functions for defining index pre-reduce phases.
-module(riak_kv_index_prereduce).

-export([extract_integer/1,
         extract_binary/1,
         extract_split/1,
         extract_regex/1,
         extract_binaryop/1,
         extract_mask/1,
         extract_hamming/1,
         extract_hash/1,
         extract_encoded/1,
         extract_buckets/1,
         extract_coalesce/1,
         apply_range/1,
         apply_regex/1,
         apply_mask/1,
         apply_remotebloom/1,
         log_identity/1]).

%% Helper functions for index manipulation
-export([hamming/2,
            simhash/1]).

-type keep() :: all|this.
-type attribute_name() :: atom().
-type attribute_output() :: binary()|integer()|tuple().
-type binary_op() :: binary().

%% @doc
%% Extract an integer from a binary term:
%% InputTerm - the name of the attribute from which to eprform the extract
%% OutputTerm - the name of the attribute for the extracted output
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% output attribute/value the only element in the IndexData after extract
%% PreBytes - the number of bytes in the term for the start of the integer
%% IntSize - the length of the integer in bits.
%% If the InputTerm is not present, or does not pattern match to find an
%% integer then the result will be filtered out
-spec extract_integer({attribute_name(), attribute_name(),
                                            keep(),
                                            non_neg_integer(), pos_integer()}) ->
                                                riak_kv_pipe_index:prereduce_fun().
extract_integer({InputTerm, OutputTerm, Keep,
                                    PreBytes, IntSize}) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case lists:keyfind(InputTerm, 1, KeyTermList) of
                {InputTerm,
                    <<_P:PreBytes/binary, I:IntSize/integer, _T/binary>>} ->
                    KeyTermList0 =
                        reduce_keepfun(Keep, {OutputTerm, I}, KeyTermList),
                    {{Bucket, Key}, KeyTermList0};
                _Other ->
                    none
            end;
        (_Other) ->
            none
    end.


%% @doc
%% As with extract_integer/1 except the Size is expressed in
%% bytes, and the atom 'all' can be used for size to extract a binary of
%% open-ended size
-spec extract_binary({attribute_name(),
                            attribute_name(),
                            keep(),
                            non_neg_integer(),
                            pos_integer()|all}) ->
                        riak_kv_pipe_index:prereduce_fun().
extract_binary({InputTerm, OutputTerm, Keep,
                                    PreBytes, BinSize}) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case {lists:keyfind(InputTerm, 1, KeyTermList), BinSize} of
                {{InputTerm,
                        <<_P:PreBytes/binary, B/binary>>}, all} ->
                    KeyTermList0 =
                        reduce_keepfun(Keep, {OutputTerm, B}, KeyTermList),
                    {{Bucket, Key}, KeyTermList0};
                {{InputTerm,
                        <<_P:PreBytes/binary, B:BinSize/binary, _T/binary>>},
                        BinSize} ->
                    KeyTermList0 =
                        reduce_keepfun(Keep, {OutputTerm, B}, KeyTermList),
                    {{Bucket, Key}, KeyTermList0};
                _Other ->
                    none
            end;
        (_Other) ->
            none
    end.


-spec extract_split({attribute_name(),
                            list(attribute_name()),
                            keep(),
                            binary()}) ->
                        riak_kv_pipe_index:prereduce_fun().
extract_split({InputTerm, OutputTerms, Keep, Delim}) ->
    EscapeChrs = [<<"[">>, <<"]">>, <<"(">>, <<")">>, <<"{">>, <<"}">>,
                    <<"*">>, <<"+">>, <<"?">>, <<"|">>,
                    <<"^">>, <<"$">>,
                    <<".">>, <<"\\">>],
    D =
        case lists:member(Delim, EscapeChrs) of
            true -> "\\" ++ binary_to_list(Delim);
            false -> binary_to_list(Delim)
        end,
    FoldFun = fun(OT, Acc) -> Acc ++ "(?<" ++ atom_to_list(OT) ++ ">[^" ++ D ++ "]*)" ++ D end,
    Re0 = lists:foldl(FoldFun, "", OutputTerms),
    Re = lists:sublist(Re0, length(Re0) - length(D)),
    extract_regex({InputTerm, OutputTerms, Keep, Re}).
    

%% @doc
%% Extract a list of attribute/value pairs via regular expression, using the
%% capture feature in regular expressions:
%% InputTerm - the attribute to which the regular expression should be
%% applied
%% OutputTerms - a list of attribute names which must match up with names of
%% capturing groups within the regular expression
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% output attributes/values the only elements in the IndexData after extract
%% Regex - a regular expression (string)
-spec extract_regex({attribute_name(),
                            list(attribute_name()),
                            keep(),
                            string()}) ->
                        riak_kv_pipe_index:prereduce_fun().
extract_regex({InputTerm, OutputTerms, Keep, Regex}) ->
    {ok, CompiledRe} = re:compile(Regex),
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case lists:keyfind(InputTerm, 1, KeyTermList) of
                {InputTerm, TermBin} when is_binary(TermBin) ->
                    Opts = [{capture, OutputTerms, binary}],
                    case re:run(TermBin, CompiledRe, Opts) of
                        {match, MatchList} ->
                            Output = lists:zip(OutputTerms, MatchList),
                            KeyTermList0 =
                                reduce_keepfun(Keep, Output, KeyTermList),
                            {{Bucket, Key}, KeyTermList0};
                        _ ->
                            none
                    end;
                _Other ->
                    none
            end;
        (_Other) ->
            none
    end.


%% @doc
%% Apply a mask to a bitmap to make sure only bits in the bitmap aligning
%% with a bit in the mask retain their value, all other bits are zeroed. 
%% InputTerm - the name of the attribute from which to perform the extract
%% OutputTerm - the name of the attribute for the extracted output
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% output attribute/value the only element in the IndexData after extract
%% Mask - mask expressed as an integer
-spec extract_mask({attribute_name(),
                            attribute_name(),
                            keep(),
                            non_neg_integer()}) ->
                        riak_kv_pipe_index:prereduce_fun().
extract_mask({InputTerm, OutputTerm, Keep, Mask}) ->
    extract_binaryop({{InputTerm, Mask}, OutputTerm, Keep, <<"band">>}).


-spec extract_binaryop({{attribute_name(), attribute_name()|integer()},
                            attribute_name(),
                            keep(),
                            binary_op()}) ->
                        riak_kv_pipe_index:prereduce_fun().
extract_binaryop({{PrimaryInput, SecondaryInput}, OutputTerm, Keep, Op}) ->
    BinaryOp = get_binary_fun(Op),
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            SecondaryInt =
                case is_integer(SecondaryInput) of
                    true ->
                        SecondaryInput;
                    false ->
                        case lists:keyfind(SecondaryInput, 1, KeyTermList) of
                            {SecondaryInput, SI} when is_integer(SI) ->
                                SI;
                            _ ->
                                none
                        end
                end,
            PrimaryInt =
                case lists:keyfind(PrimaryInput, 1, KeyTermList) of
                    {PrimaryInput, PI} when is_integer(PI) ->
                        PI;
                    _ ->
                        none
                end,
            case {SecondaryInt, PrimaryInt} of
                {A, B} when is_integer(A), is_integer(B) ->
                    Output = BinaryOp(A, B),
                    KeyTermList0 =
                        reduce_keepfun(Keep, {OutputTerm, Output}, KeyTermList),
                    {{Bucket, Key}, KeyTermList0};
                _ ->
                    none
            end;
        (_Other) ->
            none
    end.


-spec get_binary_fun(binary_op()) -> fun((integer(), integer()) -> integer()).
get_binary_fun(<<"mult">>) ->
    fun(A, B) -> A * B end;
get_binary_fun(<<"plus">>) ->
    fun(A, B) -> A + B end;
get_binary_fun(<<"minus">>) ->
    fun(A, B) -> A - B end;
get_binary_fun(<<"div">>) ->
    fun(A, B) -> A div B end;
get_binary_fun(<<"rem">>) ->
    fun(A, B) -> A rem B end;
get_binary_fun(<<"band">>) ->
    fun(A, B) -> A band B end;
get_binary_fun(<<"bor">>) ->
    fun(A, B) -> A bor B end;
get_binary_fun(<<"bxor">>) ->
    fun(A, B) -> A bxor B end;
get_binary_fun(<<"bsl">>) ->
    fun(A, B) -> A bsl B end;
get_binary_fun(<<"bsr">>) ->
    fun(A, B) -> A bsr B end.


%% @doc
%% Where an attribute value is a simlarity hash, calculate and extract a
%% hamming distance between that similarity hash and one passed in for
%% comparison:
%% InputTerm - the attribute name whose value is to be tested
%% OutputTerm - the name of the attribute for the extracted hamming distance
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% calculated hamming distance the only output
%% Comparator - binary sim hash for comparison
-spec extract_hamming({attribute_name(), attribute_name(),
                                            keep(),
                                            binary()})
                                        -> riak_kv_pipe_index:prereduce_fun().
extract_hamming({InputTerm, OutputTerm, Keep, Comparator}) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            BitSize = bit_size(Comparator),
            H = 
                case lists:keyfind(InputTerm, 1, KeyTermList) of
                    {InputTerm, Int} when is_integer(Int) ->
                        hamming(<<Int:BitSize/integer>>, Comparator);
                    {InputTerm, B} when
                                    is_binary(B), bit_size(B) == BitSize ->
                        hamming(B, Comparator);
                    _Other ->
                        none
                end,
            case H of
                none ->
                    none;
                H ->
                    KeyTermList0 =
                        reduce_keepfun(Keep, {OutputTerm, H}, KeyTermList),
                    {{Bucket, Key}, KeyTermList0}
            end;
        (_Other) ->
            none
    end.


%% @doc
%% Calculate the hash of the Key, and extract it to a projected attribute:
%% InputTerm - the attribute value to hash, use key to hash the key
%% OutputTerm - the name of the attribute for the extracted hash
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% calculated hamming distance the only output
%% Algorithm - supports md5 or fnva
-spec extract_hash({attribute_name()|key,
                            attribute_name(),
                            keep(),
                            riak_kv_hints:hash_algo()}) ->
                        riak_kv_pipe_index:prereduce_fun().
extract_hash({key, OutputTerm, Keep, Algo}) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            H = riak_kv_hints:hash(Key, Algo),
            KeyTermList0 =
                reduce_keepfun(Keep, {OutputTerm, H}, KeyTermList),
            {{Bucket, Key}, KeyTermList0};
        (_Other) ->
            none
    end;
extract_hash({InputTerm, OutputTerm, Keep, Algo}) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case lists:keyfind(InputTerm, 1, KeyTermList) of
                {InputTerm, Bin} when is_binary(Bin) ->
                    H = riak_kv_hints:hash(Bin, Algo),
                    KeyTermList0 =
                        reduce_keepfun(Keep, {OutputTerm, H}, KeyTermList),
                    {{Bucket, Key}, KeyTermList0};
                _ ->
                    none
            end;
        (_Other) ->
            none
    end.
                    

%% @doc
%% Take a base 64 encoded binary term that has been extracted, and decode to
%% a binary.  Non-ascii binary indexes will otherwise fail when objects are
%% added and read via the HTTP API - so encoding may be necessary.
%% InputTerm - the attribute value to decode
%% OutputTerm - the name of the attribute for the decoded binary
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% decoded term the only attribute carried forward
-spec extract_encoded({attribute_name(),
                            attribute_name(),
                            keep()}) ->
                        riak_kv_pipe_index:prereduce_fun().
extract_encoded({InputTerm, OutputTerm, Keep}) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case lists:keyfind(InputTerm, 1, KeyTermList) of
                {InputTerm, EncodedBin} when is_binary(EncodedBin) ->
                    Bin = base64:decode(EncodedBin),
                    KeyTermList0 =
                        reduce_keepfun(Keep, {OutputTerm, Bin}, KeyTermList),
                    {{Bucket, Key}, KeyTermList0};
                _ ->
                    none
            end;
        (_Other) ->
            none
    end.


%% @doc
%% Break the value of attributes into ranges of values.  The input attribute is
%% compared to each comparator in the bucket list - the bucket list being a
%% a list of tuples containing a comparator and an output.  The matching output
%% will be the value of the OutputTerm attribute name.
%% InputTerm - the attribute value to split into buckets
%% OutputTerm - the name of the attribute for the output
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% decoded term the only attribute carried forward
%% BucketList - a list of tuples of {Comparator, Output} where the Output will
%% be the value of the output attribute for results in the bucket where the
%% Comparator is the maximum value.
%% MaxOutput - the output value to use when the input attribute value exceeds
%% all the values in the list
-spec extract_buckets({attribute_name(),
                            attribute_name(),
                            keep(),
                            list({binary()|integer(), binary()}),
                            binary()}) ->
                        riak_kv_pipe_index:prereduce_fun().
extract_buckets({InputTerm, OutputTerm, Keep, BucketList, MaxOutput}) ->
    BL = lists:reverse(lists:ukeysort(1, BucketList)),
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case lists:keyfind(InputTerm, 1, KeyTermList) of
                {InputTerm, InputValue} ->
                    SplitFun = fun({C, _O}) -> InputValue =< C end,
                    Bin =
                        case lists:splitwith(SplitFun, BL) of
                            {[], _NotBelow} ->
                                MaxOutput;
                            {Below, _NotBelow} ->
                                element(2, lists:last(Below))
                        end,
                    KeyTermList0 =
                        reduce_keepfun(Keep, {OutputTerm, Bin}, KeyTermList),
                    {{Bucket, Key}, KeyTermList0};
                _ -> 
                    none
            end;
        (_Other) ->
            none
    end.


%% @doc
%% Take a list of input attributes, and produce an output that joins each
%% input value together, seperated by a provided delimiter
%% InputTerms - a list of attribute values to be coalesced, where the attribute
%% values are required to be binaries (if a delimiter is to be used)
%% OutputTerm - the name of the attribute for the output
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% decoded term the only attribute carried forward
%% Delimiter - a binary used to seperate each input in the output, or the
%% keyword tuple will return the attribute values as a tuple, with undefined
%% in place of any missing values
-spec extract_coalesce({list(attribute_name()),
                            attribute_name(),
                            keep(),
                            binary()|tuple}) ->
                        riak_kv_pipe_index:prereduce_fun().
extract_coalesce({InputTerms, OutputTerm, Keep, Delimiter})
        when is_binary(Delimiter), length(InputTerms) > 0 ->
    DelimSize = byte_size(Delimiter),
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            FoldFun =
                fun(InputTerm, Acc) ->
                    case lists:keyfind(InputTerm, 1, KeyTermList) of
                        {InputTerm, InputBin} when is_binary(InputBin) ->
                            <<Acc/binary, Delimiter/binary, InputBin/binary>>;
                        _ ->
                            <<Acc/binary, Delimiter/binary>>
                    end
                end,
            <<Delimiter:DelimSize/binary, OutBin/binary>> =
                lists:foldl(FoldFun, <<>>, InputTerms),
            KeyTermList0 =
                reduce_keepfun(Keep, {OutputTerm, OutBin}, KeyTermList),
            {{Bucket, Key}, KeyTermList0};
        (_Other) ->
            none
    end;
extract_coalesce({InputTerms, OutputTerm, Keep, tuple})
                    when length(InputTerms) > 0 ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            FoldFun =
                fun(InputTerm, Acc) ->
                    case lists:keyfind(InputTerm, 1, KeyTermList) of
                        {InputTerm, InputVal} ->
                            [InputVal|Acc];
                        _ ->
                            [undefined|Acc]
                    end
                end,
            OutT = list_to_tuple(lists:foldr(FoldFun, [], InputTerms)),
            KeyTermList0 =
                reduce_keepfun(Keep, {OutputTerm, OutT}, KeyTermList),
            {{Bucket, Key}, KeyTermList0};
        (_Other) ->
            none
    end.


%% @doc
%% Filter results based on whether the value for a given attribute is in a
%% range
%% InputTerm - the attribute name whose value is to be tested
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% tested attribute/value the only element in the IndexData after extract
%% LowRange - inclusive, using erlang comparator
%% HighRange - inclusive, using erlang comparator
-spec apply_range({attribute_name(),
                        keep(), 
                        term(),
                        term()}) ->
                    riak_kv_pipe_index:prereduce_fun().
apply_range({InputTerm, Keep, LowRange, HighRange}) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case lists:keyfind(InputTerm, 1, KeyTermList) of
                {InputTerm, ToTest} when ToTest >=LowRange, ToTest =< HighRange ->
                    reduce_keepfun(Keep,
                                    {Bucket, Key},
                                    {InputTerm, ToTest},
                                    KeyTermList);
                _ ->
                    none
            end;
        (_Other) ->
            none
    end.
            

%% @doc
%% Filter an attribute with a binary value by ensuring a match against a
%% compiled regular expression
%% InputTerm - the attribute name whose value is to be tested
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% tested attribute/value the only element in the IndexData after extract
%% Regex - a regular expression (string)
-spec apply_regex({attribute_name(),
                        keep(),
                        string()}) ->
                    riak_kv_pipe_index:prereduce_fun().
apply_regex({InputTerm, Keep, Regex}) ->
    {ok, CompiledRe} = re:compile(Regex),
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case lists:keyfind(InputTerm, 1, KeyTermList) of
                {InputTerm, ToTest} ->
                    case re:run(ToTest, CompiledRe) of
                        {match, _} ->
                            reduce_keepfun(Keep,
                                            {Bucket, Key},
                                            {InputTerm, ToTest},
                                            KeyTermList);
                        _ ->
                            none
                    end;
                false ->
                    none
            end;
        (_Other) ->
            none
    end.


%% @doc
%% Filter an attribute with a bitmap (integer) value by confirming that all
%% bits in the passed mask
%% InputTerm - the attribute name whose value is to be tested
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% tested attribute/value the only element in the IndexData after extract
%% Mask - bitmap maks as an integer
-spec apply_mask({attribute_name(), keep(), non_neg_integer()}) ->
                                riak_kv_pipe_index:prereduce_fun().
apply_mask({InputTerm, Keep, Mask}) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case lists:keyfind(InputTerm, 1, KeyTermList) of
                {InputTerm, ToTest} when is_integer(ToTest) ->
                    case ToTest band Mask of
                        Mask ->
                            reduce_keepfun(Keep,
                                            {Bucket, Key},
                                            {InputTerm, ToTest},
                                            KeyTermList);
                        _ ->
                            none
                    end;
                false ->
                    none
            end;
        (_Other) ->
            none
    end.


%% @doc
%% Filter an attribute by checking if it exists in a passed in bloom filter
%% InputTerm - the attribute name whose binary value is to be checked - use
%% the atom key if the key is to be checked
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% tested attribute/value the only element in the IndexData after extract.  If
%% key is the tested attribute all IndexKeyData will be dropped if Keep is 
%% `this`
%% {Module, Bloom} - the module for the bloom code, which must have a check_key
%% function, and a bloom (produced by that module) for checking.
-spec apply_remotebloom({attribute_name(),
                        keep(),
                        {module(), any()}}) ->
                    riak_kv_pipe_index:prereduce_fun().
apply_remotebloom({key, Keep, {BloomMod, Bloom}}) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case BloomMod:check_key(Key, Bloom) of
                true ->
                    case Keep of
                        all ->
                            {{Bucket, Key}, KeyTermList};
                        this ->
                            {Bucket, Key}
                    end;
                false ->
                    none
            end;
        ({Bucket, Key}) when is_binary(Key) ->
            case BloomMod:check_key(Key, Bloom) of
                true ->
                    {Bucket, Key};
                false ->
                    none
            end;
        
        (_Other) ->
            none
    end;
apply_remotebloom({InputTerm, Keep, {BloomMod, Bloom}}) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case lists:keyfind(InputTerm, 1, KeyTermList) of
                {InputTerm, ToTest} when is_binary(ToTest) ->
                    case BloomMod:check_key(ToTest, Bloom) of
                        true ->
                            reduce_keepfun(Keep,
                                            {Bucket, Key},
                                            {InputTerm, ToTest},
                                            KeyTermList);
                        false ->
                            none
                    end;
                _ ->
                    none
            end;
        (_Other) ->
            none
    end.


-spec log_identity(atom()) -> riak_kv_pipe_index:prereduce_fun().
log_identity(Term) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case Term of
                key ->
                    lager:info("Key=~w passed through fun", [Key]);
                Term ->
                    case lists:keyfind(Term, 1, KeyTermList) of
                        {Term, Value} ->
                            lager:info("Key=~w passed with term=~w ~w",
                                        [Key, Term, Value]);
                        _ ->
                            lager:info("Key=~w missing term=~w",
                                        [Key, Term])
                    end
            end,
            {{Bucket, Key}, KeyTermList};
        (_Other) ->
            none
    end.


%% @doc
%% Handle keep in extract prereduce functions 
-spec reduce_keepfun(keep(),
                    {attribute_name(), attribute_output()}|
                        list({attribute_name(), attribute_output()}),
                    list({attribute_name(), attribute_output()})) ->
                        list({attribute_name(), attribute_output()}).
reduce_keepfun(all, ExtractOutput, KeyTermList) when is_list(ExtractOutput) ->
    lists:ukeysort(1, KeyTermList ++ ExtractOutput);
reduce_keepfun(all, ExtractOutput, KeyTermList) ->
    lists:ukeysort(1, [ExtractOutput|KeyTermList]);
reduce_keepfun(this, ExtractOutput, _KeyTermList) when is_list(ExtractOutput) ->
    ExtractOutput;
reduce_keepfun(this, ExtractOutput, _KeyTermList) ->
    [ExtractOutput].

%% @doc
%% Handle keep in apply prereduce functions
-spec reduce_keepfun(keep(),
                            {riak_object:bucket(), riak_object:key()},
                            {attribute_name(), binary()|integer()},
                            list({attribute_name(), attribute_output()})) ->
                        riak_kv_pipe_index:index_keydata().
reduce_keepfun(all, {Bucket, Key}, _Input, KeyTermList) ->
    {{Bucket, Key}, KeyTermList};
reduce_keepfun(this, {Bucket, Key}, Input, _KeyTermList) ->
    {{Bucket, Key}, [Input]}.



%%%============================================================================
%%% Similarity hashing
%%%
%%% https://github.com/ferd/simhash
%%%
%%%============================================================================

-spec hamming(binary(), binary()) -> non_neg_integer().
hamming(X, Y) ->
    hamming(X, Y, bit_size(X) - 1, 0).

hamming(_, _, -1, Sum) ->
    Sum;
hamming(X, Y, Pos, Sum) ->
    case {X, Y} of
        {<<_:Pos, Bit:1, _/bitstring>>, <<_:Pos, Bit:1, _/bitstring>>} ->
            hamming(X, Y, Pos - 1, Sum);
        _ ->
            hamming(X, Y, Pos - 1, Sum + 1)
    end.

-spec simhash(binary()) -> binary().
simhash(Bin = <<_/binary>>) ->
    hashed_shingles(Bin, 2).

%% Takes a given set of features and hashes them according
%% to the algorithm used when compiling the module.
simhash_features(Features) ->
    Hashes =
        [{W, erlang:md5(Feature)} || {W, Feature} <- Features],
    to_sim(reduce(Hashes, 127)).

%% Returns a set of shingles, hashed according to the algorithm
%% used when compiling the module.
hashed_shingles(Bin, Size) ->
    simhash_features(shingles(Bin, Size)).

%% The vector returned from reduce/2 is taken and flattened
%% by its content -- values greater or equal to 0 end up being 1,
%% and those smaller than 0 end up being 0.
to_sim(HashL) ->
    << <<(case Val >= 0 of
              true -> 1;
              false -> 0
          end):1>> || Val <- HashL >>.

%% Takes individually hashed shingles and flattens them
%% as the numeric simhash.
%% Each N bit hash is treated as an N-vector, which is
%% added bit-per-bit over an empty N-vector. The resulting
%% N-vector can be used to create the sim hash.
reduce(_, -1) -> [];
reduce(L, Size) -> [add(L, Size, 0) | reduce(L, Size-1)].

%% we add it left-to-right through shingles,
%% rather than shingle-by-shingle first.
add([], _, Acc) -> Acc;
add([{W, Bin}|T], Pos, Acc) ->
    <<_:Pos, Bit:1, _Rest/bitstring>> = Bin,
    add(T, Pos,
        case Bit of
            1 -> Acc + W;
            0 -> Acc - W
        end).

%% shingles are built using a sliding window of ?SIZE bytes,
%% moving 1 byte at a time over the data. It might be interesting
%% to move to a bit size instead.
shingles(Bin, Size) ->
    build(shingles(Bin, Size, (byte_size(Bin) - 1) - Size, [])).

shingles(Bin, Size, Pos, Acc) when Pos > 0 ->
    <<_:Pos/binary, X:Size/binary, _/binary>> = Bin,
    shingles(Bin, Size, Pos - 1, [X|Acc]);
shingles(_, _, _, Acc) ->
    Acc.

build(Pieces) ->
    build(lists:sort(Pieces), []).

build([], Acc) ->
    Acc;
build([H|T], [{N, H}|Acc]) ->
    build(T, [{N + 1, H}|Acc]);
build([H|T], Acc) ->
    build(T, [{1, H}|Acc]).

%%%============================================================================
%%% Test
%%%============================================================================
