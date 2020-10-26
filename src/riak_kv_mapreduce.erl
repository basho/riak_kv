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

%% @doc Convenience functions for defining common map/reduce phases.
-module(riak_kv_mapreduce).

%% phase spec producers
-export([map_identity/1,
         map_object_value/1,
         map_object_value_list/1]).
-export([reduce_identity/1,
         reduce_set_union/1,
         reduce_sort/1,
         reduce_string_to_integer/1,
         reduce_sum/1,
         reduce_plist_sum/1,
         reduce_count_inputs/1]).

%% phase function definitions
-export([map_identity/3,
         map_object_value/3,
         map_object_value_list/3]).

-export([reduce_identity/2,
         reduce_set_union/2,
         reduce_sort/2,
         reduce_string_to_integer/2,
         reduce_sum/2,
         reduce_plist_sum/2,
         reduce_count_inputs/2]).

%% Index manipulation definitions
-export([reduce_index_extractinteger/2,
         reduce_index_extractbinary/2,
         reduce_index_extractregex/2,
         reduce_index_extractmask/2,
         reduce_index_extracthamming/2,
         reduce_index_extracthash/2,
         reduce_index_extractencoded/2,
         reduce_index_extractbuckets/2,
         reduce_index_applyrange/2,
         reduce_index_applyregex/2,
         reduce_index_applymask/2,
         reduce_index_applybloom/2,
         reduce_index_identity/2,
         reduce_index_sort/2,
         reduce_index_max/2,
         reduce_index_min/2,
         reduce_index_union/2,
         reduce_index_countby/2]).

-export([prereduce_index_extractinteger_fun/1,
         prereduce_index_extractbinary_fun/1,
         prereduce_index_extractregex_fun/1,
         prereduce_index_extractmask_fun/1,
         prereduce_index_extracthamming_fun/1,
         prereduce_index_extracthash_fun/1,
         prereduce_index_extractencoded_fun/1,
         prereduce_index_extractbuckets_fun/1,
         prereduce_index_applyrange_fun/1,
         prereduce_index_applyregex_fun/1,
         prereduce_index_applymask_fun/1,
         prereduce_index_applybloom_fun/1,
         prereduce_index_logidentity_fun/1]).

%% Helper functions for index manipulation
-export([hamming/2,
            simhash/1]).

-type keep() :: all|this.
-type attribute_name() :: atom().

%-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%-endif.

%%
%% Map Phases
%%

%% @spec map_identity(boolean()) -> map_phase_spec()
%% @doc Produces a spec for a map phase that simply returns
%%      each object it's handed.  That is:
%%      riak_kv_mrc_pipe:mapred(BucketKeys, [map_identity(true)]).
%%      Would return all of the objects named by BucketKeys.
map_identity(Acc) ->
    {map, {modfun, riak_kv_mapreduce, map_identity}, none, Acc}.

%% @spec map_identity(riak_object:riak_object(), term(), term()) ->
%%                   [riak_object:riak_object()]
%% @doc map phase function for map_identity/1
map_identity(RiakObject, _, _) -> [RiakObject].

%% @spec map_object_value(boolean()) -> map_phase_spec()
%% @doc Produces a spec for a map phase that simply returns
%%      the values of the objects from the input to the phase.
%%      That is:
%%      riak_kv_mrc_pipe:mapred(BucketKeys, [map_object_value(true)]).
%%      Would return a list that contains the value of each
%%      object named by BucketKeys.
map_object_value(Acc) ->
    {map, {modfun, riak_kv_mapreduce, map_object_value}, none, Acc}.

%% @spec map_object_value(riak_object:riak_object(), term(), term()) -> [term()]
%% @doc map phase function for map_object_value/1
%%      If the RiakObject is the tuple {error, notfound}, the
%%      behavior of this function is defined by the Action argument.
%%      Values for Action are:
%%        `<<"filter_notfound">>' : produce no output (literally [])
%%        `<<"include_notfound">>' : produce the not-found as the result
%%                                   (literally [{error, notfound}])
%%        `<<"include_keydata">>' : produce the keydata as the result
%%                                  (literally [KD])
%%        `{struct,[{<<"sub">>,term()}]}' : produce term() as the result
%%                                          (literally term())
%%      The last form has a strange stucture, in order to allow
%%      its specification over the HTTP interface
%%      (as JSON like ..."arg":{"sub":1234}...).
map_object_value({error, notfound}=NF, KD, Action) ->
    notfound_map_action(NF, KD, Action);
map_object_value(RiakObject, _, _) ->
    [riak_object:get_value(RiakObject)].

%% @spec map_object_value_list(boolean) -> map_phase_spec()
%% @doc Produces a spec for a map phase that returns the values of
%%      the objects from the input to the phase.  The difference
%%      between this phase and that of map_object_value/1 is that
%%      this phase assumes that the value of the riak object is
%%      a list.  Thus, if the input objects to this phase have values
%%      of [a,b], [c,d], and [e,f], the output of this phase is
%%      [a,b,c,d,e,f].
map_object_value_list(Acc) ->
    {map, {modfun, riak_kv_mapreduce, map_object_value_list}, none, Acc}.

%% @spec map_object_value_list(riak_object:riak_object(), term(), term()) ->
%%                            [term()]
%% @doc map phase function for map_object_value_list/1
%%      See map_object_value/3 for a description of the behavior of
%%      this function with the RiakObject is {error, notfound}.
map_object_value_list({error, notfound}=NF, KD, Action) ->
    notfound_map_action(NF, KD, Action);
map_object_value_list(RiakObject, _, _) ->
    riak_object:get_value(RiakObject).

%% implementation of the notfound behavior for
%% map_object_value and map_object_value_list
notfound_map_action(_NF, _KD, <<"filter_notfound">>)    -> [];
notfound_map_action(NF, _KD, <<"include_notfound">>)    -> [NF];
notfound_map_action(_NF, KD, <<"include_keydata">>)     -> [KD]; 
notfound_map_action(_NF, _KD, {struct,[{<<"sub">>,V}]}) -> V.

%%
%% Reduce Phases
%%

%% @spec reduce_identity(boolean()) -> reduce_phase_spec()
%% @doc Produces a spec for a reduce phase that simply returns
%%      back [Bucket, Key] for each BKey it's handed.  
reduce_identity(Acc) ->
    {reduce, {modfun, riak_kv_mapreduce, reduce_identity}, none, Acc}.

%% @spec reduce_identity([term()], term()) -> [term()]
%% @doc map phase function for reduce_identity/1
reduce_identity(List, _) -> 
    F = fun({{Bucket, Key}, _}, Acc) ->
                %% Handle BKeys with extra data.
                [[Bucket, Key]|Acc];
           ({Bucket, Key}, Acc) ->
                %% Handle BKeys.
                [[Bucket, Key]|Acc];
           ([Bucket, Key], Acc) ->
                %% Handle re-reduces.
                [[Bucket, Key]|Acc];
           ([Bucket, Key, KeyData], Acc) ->
                [[Bucket, Key, KeyData]|Acc];
           (Other, _Acc) ->
                %% Fail loudly on anything unexpected.
                lager:error("Unhandled entry: ~p", [Other]),
                throw({unhandled_entry, Other})
        end,
    lists:foldl(F, [], List).


-spec reduce_index_identity(list(riak_kv_pipe_index:index_keydata()|
                                    list(riak_kv_pipe_index:index_keydata())),
                                list()|none|undefined|attribute_name()) ->
                                    list(riak_kv_pipe_index:index_keydata()).
reduce_index_identity(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_identity(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_identity(List, Args) ->
    lists:foldl(reduce_index_identity_fun(Args), [], List).

reduce_index_identity_fun(Null) when Null == undefined; Null == none ->
    fun({{Bucket, Key}, undefined}, Acc) ->
            [{Bucket, Key}|Acc];
        ({{Bucket, Key}, KeyData}, Acc) when is_list(KeyData) ->
            [{{Bucket, Key}, KeyData}|Acc];
        (PrevAcc, Acc) when is_list(PrevAcc) ->
            Acc ++ PrevAcc;
        ({Bucket, Key}, Acc) ->
            [{Bucket, Key}|Acc];
        (Other, Acc) ->
            lager:warning("Unhandled entry: ~p", [Other]),
            Acc
    end;
reduce_index_identity_fun(Term) ->
    fun({{Bucket, Key}, IndexData}, Acc) when is_list(IndexData) ->
            case lists:keymember(Term, 1, IndexData) of
                true ->
                    [{{Bucket, Key}, IndexData}|Acc];
                false ->
                    Acc
            end;
        (PrevAcc, Acc) when is_list(PrevAcc) ->
            Acc ++ PrevAcc;
        (_Other, Acc) ->
            Acc
    end.


-spec reduce_index_sort(list(riak_kv_pipe_index:index_keydata()|
                                    list(riak_kv_pipe_index:index_keydata())),
                                list()|attribute_name()) ->
                                    list(riak_kv_pipe_index:index_keydata()).
reduce_index_sort(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_sort(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_sort(List, key) ->
    lists:sort(lists:foldl(reduce_index_identity_fun(undefined), [], List));
reduce_index_sort(List, Term) ->
    SortFun = 
        fun({{_B0, K0}, TL0}, {{_B1, K1}, TL1}) ->
            case {lists:keyfind(Term, 1, TL0), lists:keyfind(Term, 1, TL1)} of
                {{Term, T0}, {Term, T1}} when T0 < T1 ->
                    true;
                {{Term, T0}, {Term, T0}} when K0 < K1 ->
                    true;
                {false, {Term, _T1}} ->
                    true;
                _ ->
                    false
            end
        end,
    lists:sort(SortFun,
                lists:foldl(reduce_index_identity_fun(Term), [], List)).


-spec reduce_index_max(list(riak_kv_pipe_index:index_keydata()|
                                    list(riak_kv_pipe_index:index_keydata())),
                                list()|{attribute_name(), pos_integer()}) ->
                                    list(riak_kv_pipe_index:index_keydata()).
reduce_index_max(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_max(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_max(List, {Term, MaxCount}) ->
    L = lists:reverse(reduce_index_sort(List, Term)),
    L1 = 
        case length(L) > MaxCount of
            true ->
                element(1, lists:split(MaxCount, L));
            false ->
                L
        end,
    lists:reverse(L1).

-spec reduce_index_min(list(riak_kv_pipe_index:index_keydata()|
                                    list(riak_kv_pipe_index:index_keydata())),
                                list()|{attribute_name(), pos_integer()}) ->
                                    list(riak_kv_pipe_index:index_keydata()).
reduce_index_min(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_min(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_min(List, {Term, MinCount}) ->
    L = reduce_index_sort(List, Term),
    L1 = 
        case length(L) > MinCount of
            true ->
                element(1, lists:split(MinCount, L));
            false ->
                L
        end,
    L1.



-spec prereduce_index_logidentity_fun(atom()) -> riak_kv_pipe_index:prereduce_fun().
prereduce_index_logidentity_fun(Term) ->
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


reduce_filterfun(FilterFun) ->
    %% The filter will be re-applied if an element is passed through twice
    fun(PrevAcc, Acc) when is_list(PrevAcc) ->
            Acc ++ PrevAcc;
        (IndexKeyData, Acc) ->
            case FilterFun(IndexKeyData) of
                none ->
                    Acc;
                IndexKeyData0 ->
                    [IndexKeyData0|Acc]
            end
    end.

reduce_extractfun(ExtractFun, OutputTerms) when is_list(OutputTerms) ->
    %% If the output terms are present - assume extract fun has been run, and
    %% this is re-reduce.  Don't try re-apply as InputTerm may no longer be
    %% present 
    fun(PrevAcc, Acc) when is_list(PrevAcc) ->
            Acc ++ PrevAcc;
        ({{Bucket, Key}, KeyTermList}, Acc) when is_list(KeyTermList) ->
            IsOutputFun = fun({T, _R}) -> lists:member(T, OutputTerms) end,
            ExpectedOutputs = length(OutputTerms),
            case length(lists:filter(IsOutputFun, KeyTermList)) of
                ExpectedOutputs ->
                    % Extraction performed
                    [{{Bucket, Key}, KeyTermList}|Acc];
                _ ->
                    case ExtractFun({{Bucket, Key}, KeyTermList}) of
                        none ->
                            Acc;
                        IndexKeyData0 ->
                            [IndexKeyData0|Acc]
                    end
            end;
        (_Other, Acc) ->
            Acc
    end;
reduce_extractfun(ExtractFun, OutputTerm) when is_atom(OutputTerm) ->
    reduce_extractfun(ExtractFun, [OutputTerm]).

%% @doc
%% Handle keep in extract prereduce functions 
-spec reduce_keepfun(keep(),
                    {attribute_name(), binary()|integer()}|
                        list({attribute_name(), binary()|integer()}),
                    list({attribute_name(), binary()|integer()})) ->
                        list({attribute_name(), binary()|integer()}).
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
                            list({attribute_name(), binary()|integer()})) ->
                        riak_kv_pipe_index:index_keydata().
reduce_keepfun(all, {Bucket, Key}, _Input, KeyTermList) ->
    {{Bucket, Key}, KeyTermList};
reduce_keepfun(this, {Bucket, Key}, Input, _KeyTermList) ->
    {{Bucket, Key}, [Input]}.

-spec reduce_index_extractinteger(list(riak_kv_pipe_index:index_keydata()|
                                        list(riak_kv_pipe_index:index_keydata())),
                                    list()|
                                        {attribute_name(), attribute_name(),
                                            keep(),
                                            non_neg_integer(), pos_integer()})
                                    -> list(riak_kv_pipe_index:index_keydata()).
reduce_index_extractinteger(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_extractinteger(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_extractinteger(List, Args) ->
    ExtractFun = prereduce_index_extractinteger_fun(Args),
    lists:foldl(reduce_extractfun(ExtractFun, element(2, Args)), [], List).


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
-spec prereduce_index_extractinteger_fun({attribute_name(), attribute_name(),
                                            keep(),
                                            non_neg_integer(), pos_integer()}) ->
                                                riak_kv_pipe_index:prereduce_fun().
prereduce_index_extractinteger_fun({InputTerm, OutputTerm, Keep,
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


-spec reduce_index_extractbinary(list(riak_kv_pipe_index:index_keydata()|
                                        list(riak_kv_pipe_index:index_keydata())),
                                    list()|
                                        {atom(), atom(), keep(), 
                                            non_neg_integer(),
                                            pos_integer()|all}) ->
                                            list(riak_kv_pipe_index:index_keydata()).
reduce_index_extractbinary(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_extractbinary(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_extractbinary(List, Args) ->
    ExtractFun = prereduce_index_extractbinary_fun(Args),
    lists:foldl(reduce_extractfun(ExtractFun, element(2, Args)), [], List).


%% @doc
%% As with prereduce_index_extractinteger_fun/4 except the Size is expressed in
%% bytes, and the atom 'all' can be used for size to extract a binary of
%% open-ended size
-spec prereduce_index_extractbinary_fun({attribute_name(), attribute_name(), keep(),
                                            non_neg_integer(), pos_integer()|all}) ->
                                                riak_kv_pipe_index:prereduce_fun().
prereduce_index_extractbinary_fun({InputTerm, OutputTerm, Keep,
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

-spec reduce_index_extractregex(list(riak_kv_pipe_index:index_keydata()|
                                        list(riak_kv_pipe_index:index_keydata())),
                                    list()|
                                        {attribute_name(),
                                            list(attribute_name()), keep(), string()}) ->
                                        list(riak_kv_pipe_index:index_keydata()).
reduce_index_extractregex(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_extractregex(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_extractregex(List, Args) ->
    ExtractFun = prereduce_index_extractregex_fun(Args),
    lists:foldl(reduce_extractfun(ExtractFun, element(2, Args)), [], List).

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
-spec prereduce_index_extractregex_fun({attribute_name(),
                                        list(attribute_name()),
                                        keep(),
                                        string()}) ->
                                            riak_kv_pipe_index:prereduce_fun().
prereduce_index_extractregex_fun({InputTerm, OutputTerms, Keep, Regex}) ->
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


-spec reduce_index_extractmask(list(riak_kv_pipe_index:index_keydata()|
                                        list(riak_kv_pipe_index:index_keydata())),
                                    list()|
                                        {attribute_name(),
                                            attribute_name(),
                                            keep(),
                                            non_neg_integer()}) ->
                                            list(riak_kv_pipe_index:index_keydata()).
reduce_index_extractmask(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_extractmask(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_extractmask(List, Args) ->
    ExtractFun = prereduce_index_extractmask_fun(Args),
    lists:foldl(reduce_extractfun(ExtractFun, element(2, Args)), [], List).

%% @doc
%% Apply a mask to a bitmap to make sure only bits in the bitmap aligning
%% with a bit in the mask retain their value, all other bits are zeroed. 
%% InputTerm - the name of the attribute from which to perform the extract
%% OutputTerm - the name of the attribute for the extracted output
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% output attribute/value the only element in the IndexData after extract
%% Mask - mask expressed as an integer
-spec prereduce_index_extractmask_fun({attribute_name(), attribute_name(),
                                        keep(),
                                        non_neg_integer()}) ->
                                        riak_kv_pipe_index:prereduce_fun().
prereduce_index_extractmask_fun({InputTerm, OutputTerm, Keep, Mask}) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            case lists:keyfind(InputTerm, 1, KeyTermList) of
                {InputTerm, Int} when is_integer(Int) ->
                    Output = {OutputTerm, Int band Mask},
                    KeyTermList0 =
                        case Keep of
                            all ->
                                lists:ukeysort(1, [Output|KeyTermList]);
                            this ->
                                [Output]
                        end,
                    {{Bucket, Key}, KeyTermList0};
                _Other ->
                    none
            end;
        (_Other) ->
            none
    end.


-spec reduce_index_extracthamming(riak_kv_pipe_index:index_keydata()|
                                    list(riak_kv_pipe_index:index_keydata()),
                                    list()|
                                        {attribute_name(), attribute_name(),
                                            keep(),
                                            binary()}) ->
                                        list(riak_kv_pipe_index:index_keydata()).
reduce_index_extracthamming(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_extracthamming(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_extracthamming(List, Args) ->
    ExtractFun = prereduce_index_extracthamming_fun(Args),
    lists:foldl(reduce_extractfun(ExtractFun, element(2, Args)), [], List).

%% @doc
%% Where an attribute value is a simlarity hash, calculate and extract a
%% hamming distance between that similarity hash and one passed in for
%% comparison:
%% InputTerm - the attribute name whose value is to be tested
%% OutputTerm - the name of the attribute for the extracted hamming distance
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% calculated hamming distance the only output
%% Comparator - binary sim hash for comparison
-spec prereduce_index_extracthamming_fun({attribute_name(), attribute_name(),
                                            keep(),
                                            binary()})
                                        -> riak_kv_pipe_index:prereduce_fun().
prereduce_index_extracthamming_fun({InputTerm, OutputTerm, Keep, Comparator}) ->
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

-spec reduce_index_extracthash(riak_kv_pipe_index:index_keydata()|
                                    list(riak_kv_pipe_index:index_keydata()),
                                    list()|
                                        {attribute_name()|key,
                                            attribute_name(),
                                            keep(),
                                            riak_kv_hints:hash_algo()}) ->
                                        list(riak_kv_pipe_index:index_keydata()).
reduce_index_extracthash(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_extracthash(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_extracthash(List, Args) ->
    ExtractFun = prereduce_index_extracthash_fun(Args),
    lists:foldl(reduce_extractfun(ExtractFun, element(2, Args)), [], List).

%% @doc
%% Calculate the hash of the Key, and extract it to a projected attribute:
%% InputTerm - the attribute value to hash, use key to hash the key
%% OutputTerm - the name of the attribute for the extracted hash
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% calculated hamming distance the only output
%% Algorithm - supports md5 or fnva
-spec prereduce_index_extracthash_fun({attribute_name()|key,
                                            attribute_name(),
                                            keep(),
                                            riak_kv_hints:hash_algo()}) ->
                                        riak_kv_pipe_index:prereduce_fun().
prereduce_index_extracthash_fun({key, OutputTerm, Keep, Algo}) ->
    fun({{Bucket, Key}, KeyTermList}) when is_list(KeyTermList) ->
            H = riak_kv_hints:hash(Key, Algo),
            KeyTermList0 =
                reduce_keepfun(Keep, {OutputTerm, H}, KeyTermList),
            {{Bucket, Key}, KeyTermList0};
        (_Other) ->
            none
    end;
prereduce_index_extracthash_fun({InputTerm, OutputTerm, Keep, Algo}) ->
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
                    

-spec reduce_index_extractencoded(list(riak_kv_pipe_index:index_keydata()|
                                        list(riak_kv_pipe_index:index_keydata())),
                                    list()|
                                        {attribute_name(), attribute_name(), keep()}) ->
                                            list(riak_kv_pipe_index:index_keydata()).
reduce_index_extractencoded(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_extractencoded(List, 
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_extractencoded(List, Args) ->
    ExtractFun = prereduce_index_extractencoded_fun(Args),
    lists:foldl(reduce_extractfun(ExtractFun, element(2, Args)), [], List).

%% @doc
%% Take a base 64 encoded binary term that has been extracted, and decode to
%% a binary.  Non-ascii binary indexes will otherwise fail when objects are
%% added and read via the HTTP API - so encoding may be necessary.
%% InputTerm - the attribute value to decode
%% OutputTerm - the name of the attribute for the decoded binary
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% decoded term the only attribute carried forward
-spec prereduce_index_extractencoded_fun({attribute_name(),
                                                attribute_name(),
                                                keep()}) ->
                                            riak_kv_pipe_index:prereduce_fun().
prereduce_index_extractencoded_fun({InputTerm, OutputTerm, Keep}) ->
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


-spec reduce_index_extractbuckets(list(riak_kv_pipe_index:index_keydata()|
                                    list(riak_kv_pipe_index:index_keydata())),
                                    list()|
                                        {attribute_name(), 
                                            attribute_name(),
                                            keep(),
                                            list({binary()|integer(), binary()}),
                                            binary()}) ->
                                list(riak_kv_pipe_index:index_keydata()).
reduce_index_extractbuckets(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_extractbuckets(List, 
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_extractbuckets(List, Args) ->
    ExtractFun = prereduce_index_extractbuckets_fun(Args),
    lists:foldl(reduce_extractfun(ExtractFun, element(2, Args)), [], List).

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
-spec prereduce_index_extractbuckets_fun({attribute_name(),
                                            attribute_name(),
                                            keep(),
                                            list({binary()|integer(), binary()}),
                                            binary()}) ->
                                            riak_kv_pipe_index:prereduce_fun().
prereduce_index_extractbuckets_fun({InputTerm, OutputTerm, Keep, BucketList, MaxOutput}) ->
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

-spec reduce_index_extractcoalesce(list(riak_kv_pipe_index:index_keydata()|
                                    list(riak_kv_pipe_index:index_keydata())),
                                    list()|
                                        {list(attribute_name()), 
                                            attribute_name(),
                                            keep(),
                                            binary()}) ->
                                list(riak_kv_pipe_index:index_keydata()).
reduce_index_extractcoalesce(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_extractcoalesce(List, 
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_extractcoalesce(List, Args) ->
    ExtractFun = prereduce_index_extractcoalesce_fun(Args),
    lists:foldl(reduce_extractfun(ExtractFun, element(2, Args)), [], List).

%% @doc
%% Take a list of input attributes, and produce an output that joins each
%% input value together, seperated by a provided delimiter
%% InputTerms - a list of attribute values to be coalesced, where the attribute
%% values are required to be binaries
%% OutputTerm - the name of the attribute for the output
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% decoded term the only attribute carried forward
%% Delimiter - a binary used to seperate each input in the output
-spec prereduce_index_extractcoalesce_fun({list(attribute_name()),
                                            attribute_name(),
                                            keep(),
                                            binary()}) ->
                                            riak_kv_pipe_index:prereduce_fun().
prereduce_index_extractcoalesce_fun({InputTerms, OutputTerm, Keep, Delimiter})
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
    end.


-spec reduce_index_applyrange(list(riak_kv_pipe_index:index_keydata()|
                                list(riak_kv_pipe_index:index_keydata())),
                            list()|
                                {attribute_name(), keep(), term(), term()}) ->
                                    list(riak_kv_pipe_index:index_keydata()).
reduce_index_applyrange(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_applyrange(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_applyrange(List, Args) ->
    FilterFun = prereduce_index_applyrange_fun(Args),
    lists:foldl(reduce_filterfun(FilterFun), [], List).

%% @doc
%% Filter results based on whether the value for a given attribute is in a
%% range
%% InputTerm - the attribute name whose value is to be tested
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% tested attribute/value the only element in the IndexData after extract
%% LowRange - inclusive, using erlang comparator
%% HighRange - inclusive, using erlang comparator
-spec prereduce_index_applyrange_fun({attribute_name(), keep(),
                                    term(), term()}) ->
                                        riak_kv_pipe_index:prereduce_fun().
prereduce_index_applyrange_fun({InputTerm, Keep, LowRange, HighRange}) ->
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
            
-spec reduce_index_applyregex(list(riak_kv_pipe_index:index_keydata()|
                                list(riak_kv_pipe_index:index_keydata())),
                            list()|{attribute_name(), keep(), string()}) ->
                                    list(riak_kv_pipe_index:index_keydata()).
reduce_index_applyregex(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_applyregex(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_applyregex(List, Args) ->
    FilterFun = prereduce_index_applyregex_fun(Args),
    lists:foldl(reduce_filterfun(FilterFun), [], List).

%% @doc
%% Filter an attribute with a binary value by ensuring a match against a
%% compiled regular expression
%% InputTerm - the attribute name whose value is to be tested
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% tested attribute/value the only element in the IndexData after extract
%% Regex - a regular expression (string)
-spec prereduce_index_applyregex_fun({attribute_name(), keep(), string()}) ->
                                riak_kv_pipe_index:prereduce_fun().
prereduce_index_applyregex_fun({InputTerm, Keep, Regex}) ->
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

-spec reduce_index_applymask(list(riak_kv_pipe_index:index_keydata()|
                                list(riak_kv_pipe_index:index_keydata())),
                            list()|{attribute_name(), keep(), non_neg_integer()}) ->
                                    list(riak_kv_pipe_index:index_keydata()).
reduce_index_applymask(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_applymask(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_applymask(List, Args) ->
    FilterFun = prereduce_index_applymask_fun(Args),
    lists:foldl(reduce_filterfun(FilterFun), [], List).

%% @doc
%% Filter an attribute with a bitmap (integer) value by confirming that all
%% bits in the passed mask
%% InputTerm - the attribute name whose value is to be tested
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% tested attribute/value the only element in the IndexData after extract
%% Mask - bitmap maks as an integer
-spec prereduce_index_applymask_fun({attribute_name(), keep(), non_neg_integer()}) ->
                                riak_kv_pipe_index:prereduce_fun().
prereduce_index_applymask_fun({InputTerm, Keep, Mask}) ->
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

-spec reduce_index_applybloom(list(riak_kv_pipe_index:index_keydata()|
                                list(riak_kv_pipe_index:index_keydata())),
                            list()|{attribute_name()|key,
                                    keep(),
                                    {module(), any()}}) ->
                                list(riak_kv_pipe_index:index_keydata()).
reduce_index_applybloom(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_applybloom(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_applybloom(List, Args) ->
    FilterFun = prereduce_index_applybloom_fun(Args),
    lists:foldl(reduce_filterfun(FilterFun), [], List).

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
-spec prereduce_index_applybloom_fun({attribute_name(),
                                        keep(),
                                        {module(), any()}}) ->
                                riak_kv_pipe_index:prereduce_fun().
prereduce_index_applybloom_fun({key, Keep, {BloomMod, Bloom}}) ->
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
prereduce_index_applybloom_fun({InputTerm, Keep, {BloomMod, Bloom}}) ->
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


-spec reduce_index_union(list(any()), list()|{attribute_name(), binary|integer})
                            -> list(any()).
reduce_index_union(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_union(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_union(List, {InputTerm, InputType}) ->
    FoldFun = 
        fun({{_Bucket, _Key}, KeyTermList}, Acc) when is_list(KeyTermList) ->
                case lists:keyfind(InputTerm, 1, KeyTermList) of
                    {InputTerm, ToAdd} when
                            InputType == binary, is_binary(ToAdd); 
                            InputType == integer, is_integer(ToAdd) ->
                        lists:usort([ToAdd|Acc]);
                    _ ->
                        Acc
                end;
            (PrevAcc, Acc) when is_list(PrevAcc) ->
                lists:usort(Acc ++ PrevAcc);
            (PrevAcc, Acc) when
                            InputType == binary, is_binary(PrevAcc); 
                            InputType == integer, is_integer(PrevAcc) ->
                lists:usort([PrevAcc|Acc])
            end,
    lists:foldl(FoldFun, [], List).


-spec reduce_index_countby(list(riak_kv_pipe_index:index_keydata()|
                                {binary()|integer(), non_neg_integer()}),
                                list()|{attribute_name(), binary|integer}) ->
                                    list({binary()|integer(), non_neg_integer()}).
reduce_index_countby(List, ArgPropList) when is_list(ArgPropList) ->
    reduce_index_countby(List,
        element(2, lists:keyfind(args, 1, ArgPropList)));
reduce_index_countby(List, {InputTerm, InputType}) ->
    FoldFun = 
        fun({{_Bucket, _Key}, KeyTermList}, Acc) when is_list(KeyTermList) ->
                case lists:keyfind(InputTerm, 1, KeyTermList) of
                    {InputTerm, Term}  when
                            InputType == binary, is_binary(Term); 
                            InputType == integer, is_integer(Term) ->
                        case lists:keyfind(Term, 1, Acc) of
                            {Term, CurrCount}->
                                lists:ukeysort(1, [{Term, CurrCount + 1}|Acc]);
                            false ->
                                lists:ukeysort(1, [{Term, 1}|Acc])
                        end;
                    _ ->
                        Acc
                end;
            (PrevAcc, Acc) when is_list(PrevAcc), is_list(Acc) ->
                F = fun({Term, C0}, A) ->
                        case lists:keyfind(Term, 1, A) of
                            {Term, CurrCount} ->
                                lists:ukeysort(1, [{Term, CurrCount + C0}|A]);
                            false ->
                                lists:ukeysort(1, [{Term, C0}|A])
                        end
                    end,
                lists:foldl(F, Acc, PrevAcc);
            ({Term, C0}, Acc) when
                    InputType == binary, is_binary(Term), is_integer(C0); 
                    InputType == integer, is_integer(Term), is_integer(C0) ->
                case lists:keyfind(Term, 1, Acc) of
                    {Term, CurrCount} ->
                        lists:ukeysort(1, [{Term, CurrCount + C0}|Acc]);
                    false ->
                        lists:ukeysort(1, [{Term, C0}|Acc])
                end
        end,
    lists:foldl(FoldFun, [], List).



%% @spec reduce_set_union(boolean()) -> reduce_phase_spec()
%% @doc Produces a spec for a reduce phase that produces the
%%      union-set of its input.  That is, given an input of:
%%         [a,a,a,b,c,b]
%%      this phase will output
%%         [a,b,c]
reduce_set_union(Acc) ->
    {reduce, {modfun, riak_kv_mapreduce, reduce_set_union}, none, Acc}.

%% @spec reduce_set_union([term()], term()) -> [term()]
%% @doc reduce phase function for reduce_set_union/1
reduce_set_union(List, _) ->
    sets:to_list(sets:from_list(List)).

%% @spec reduce_sum(boolean()) -> reduce_phase_spec()
%% @doc Produces a spec for a reduce phase that produces the
%%      sum of its inputs.  That is, given an input of:
%%         [1,2,3]
%%      this phase will output
%%         [6]
reduce_sum(Acc) ->
    {reduce, {modfun, riak_kv_mapreduce, reduce_sum}, none, Acc}.

%% @spec reduce_sum([number()], term()) -> [number()]
%% @doc reduce phase function for reduce_sum/1
reduce_sum(List, _) ->
    [lists:foldl(fun erlang:'+'/2, 0, not_found_filter(List))].

%% @spec reduce_plist_sum(boolean()) -> reduce_phase_spec()
%% @doc Produces a spec for a reduce phase that expects a proplist or
%%      a list of proplists.  where all values are numbers, and
%%      produces a proplist where all values are the sums of the
%%      values of each property from input proplists.
reduce_plist_sum(Acc) ->
    {reduce, {modfun, riak_kv_mapreduce, reduce_plist_sum}, none, Acc}.

%% @spec reduce_plist_sum([{term(),number()}|[{term(),number()}]], term())
%%       -> [{term(), number()}]
%% @doc reduce phase function for reduce_plist_sum/1
reduce_plist_sum([], _) -> [];
reduce_plist_sum(PList, _) ->
    dict:to_list(
      lists:foldl(
        fun({K,V},Dict) ->
                dict:update(K, fun(DV) -> V+DV end, V, Dict)
        end,
        dict:new(),
        if is_tuple(hd(PList)) -> PList;
           true -> lists:flatten(PList)
        end)).

%% @spec reduce_sort(boolean()) -> reduce_phase_spec()
%% @doc Produces a spec for a reduce phase that sorts its
%%      inputs in ascending order using lists:sort/1.
reduce_sort(Acc) ->
    {reduce, {modfun, riak_kv_mapreduce, reduce_sort}, none, Acc}.

%% @spec reduce_sort([term()], term()) -> [term()]
%% @doc reduce phase function for reduce_sort/1
reduce_sort(List, _) ->
    lists:sort(List).


%% @spec reduce_count_inputs(boolean()) -> reduce_phase_spec()
%% @doc Produces a spec for a reduce phase that counts its
%%      inputs.  Inputs to this phase must not be integers, or
%%      they will confuse the counting.  The output of this
%%      phase is a list of one integer.
%%
%%      The original purpose of this function was to count
%%      the results of a key-listing.  For example:
%%```
%%      [KeyCount] = riak_kv_mrc_pipe:mapred(<<"my_bucket">>,
%%                      [riak_kv_mapreduce:reduce_count_inputs(true)]).
%%'''
%%      KeyCount will contain the number of keys found in "my_bucket".
reduce_count_inputs(Acc) ->
    {reduce, {modfun, riak_kv_mapreduce, reduce_count_inputs}, none, Acc}.

%% @spec reduce_count_inputs([term()|integer()], term()) -> [integer()]
%% @doc reduce phase function for reduce_count_inputs/1
reduce_count_inputs(Results, _) ->
    [ lists:foldl(fun input_counter_fold/2, 0, Results) ].

%% @spec input_counter_fold(term()|integer(), integer()) -> integer()
input_counter_fold(PrevCount, Acc) when is_integer(PrevCount) ->
    PrevCount+Acc;
input_counter_fold(_, Acc) ->
    1+Acc.


%% @spec reduce_string_to_integer(boolean()) -> reduce_phase_spec()
%% @doc Produces a spec for a reduce phase that converts
%%      its inputs to integers. Inputs can be either Erlang
%%      strings or binaries.
reduce_string_to_integer(Acc) ->
    {reduce, {modfun, riak_kv_mapreduce, reduce_string_to_integer}, none, Acc}.

%% @spec reduce_string_to_integer([number()], term()) -> [number()]
%% @doc reduce phase function for reduce_sort/1
reduce_string_to_integer(List, _) ->
    [value_to_integer(I) || I <- not_found_filter(List)].

value_to_integer(V) when is_list(V) ->
    list_to_integer(V);
value_to_integer(V) when is_binary(V) ->
    value_to_integer(binary_to_list(V));
value_to_integer(V) when is_integer(V) ->
    V.

%% Helper functions
not_found_filter(Values) ->
    [Value || Value <- Values,
              is_datum(Value)].
is_datum({not_found, _}) ->
    false;
is_datum({not_found, _, _}) ->
    false;
is_datum(_) ->
    true.

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


%% unit tests %%
map_identity_test() ->
    O1 = riak_object:new(<<"a">>, <<"1">>, "value1"),
    [O1] = map_identity(O1, test, test).

map_object_value_test() ->
    O1 = riak_object:new(<<"a">>, <<"1">>, "value1"),
    O2 = riak_object:new(<<"a">>, <<"1">>, ["value1"]),
    ["value1"] = map_object_value(O1, test, test),
    ["value1"] = map_object_value_list(O2, test, test),
    [] = map_object_value({error, notfound}, test, <<"filter_notfound">>),
    [{error,notfound}] = map_object_value({error, notfound}, test, <<"include_notfound">>).

reduce_set_union_test() ->
    [bar,baz,foo] = lists:sort(reduce_set_union([foo,foo,bar,baz], test)).

reduce_sum_test() ->
    [10] = reduce_sum([1,2,3,4], test).

reduce_plist_sum_test() ->
    PLs = [[{a, 1}], [{a, 2}],
           [{b, 1}], [{b, 4}]],
    [{a,3},{b,5}] = reduce_plist_sum(PLs, test),
    [{a,3},{b,5}] = reduce_plist_sum(lists:flatten(PLs), test),
    [] = reduce_plist_sum([], test).

map_spec_form_test_() ->
    lists:append(
      [ [?_assertMatch({map, {modfun, riak_kv_mapreduce, F}, _, true},
                       riak_kv_mapreduce:F(true)),
         ?_assertMatch({map, {modfun, riak_kv_mapreduce, F}, _, false},
                       riak_kv_mapreduce:F(false))]
        || F <- [map_identity, map_object_value, map_object_value_list] ]).

reduce_spec_form_test_() ->
    lists:append(
      [ [?_assertMatch({reduce, {modfun, riak_kv_mapreduce, F}, _, true},
                       riak_kv_mapreduce:F(true)),
         ?_assertMatch({reduce, {modfun, riak_kv_mapreduce, F}, _, false},
                       riak_kv_mapreduce:F(false))]
        || F <- [reduce_set_union, reduce_sum, reduce_plist_sum] ]).

reduce_sort_test() ->
    [a,b,c] = reduce_sort([b,a,c], none),
    [1,2,3,4,5] = reduce_sort([4,2,1,3,5], none),
    ["a", "b", "c"] = reduce_sort(["c", "b", "a"], none),
    [<<"a">>, <<"is">>, <<"test">>, <<"this">>] = reduce_sort([<<"this">>, <<"is">>, <<"a">>, <<"test">>], none).

reduce_string_to_integer_test() ->
    [1,2,3] = reduce_string_to_integer(["1", "2", "3"], none),
    [1,2,3] = reduce_string_to_integer([<<"1">>, <<"2">>, <<"3">>], none),
    [1,2,3,4,5] = reduce_string_to_integer(["1", <<"2">>, <<"3">>, "4", "5"], none),
    [1,2,3,4,5] = reduce_string_to_integer(["1", <<"2">>, <<"3">>, 4, 5], none).

reduce_count_inputs_test() ->
    ?assertEqual([1], reduce_count_inputs([{"b1","k1"}], none)),
    ?assertEqual([2], reduce_count_inputs([{"b1","k1"},{"b2","k2"}],
                                          none)),
    ?assertEqual([9], reduce_count_inputs(
                        [{"b1","k1"},{"b2","k2"},{"b3","k3"}]
                        ++ reduce_count_inputs([{"b4","k4"},{"b5","k5"}],
                                               none)
                        ++ reduce_count_inputs(
                             [{"b4","k4"},{"b5","k5"},
                              {"b5","k5"},{"b5","k5"}],
                             none),
                        none)).

reduce_index_identity_test() ->
    A = {{<<"B1">>, <<"K1">>}, [{term, <<"KD1">>}]},
    B = {{<<"B2">>, <<"K2">>}, [{term, <<"KD2">>}]},
    C = {{<<"B3">>, <<"K3">>}, [{term, <<"KD3">>}, {extract, 4}]},
    D = {{<<"B4">>, <<"K4">>}, undefined},
    E = {<<"B5">>, <<"K5">>},
    F = {{<<"B6">>, <<"K6">>}, undefined},
    G = {{<<"B7">>, <<"K7">>}, undefined},
    H = {{<<"B8">>, <<"K8">>}, undefined},
    R0 = commassidem_check(fun reduce_index_identity/2, undefined, A, B, C, D),
    R1 = commassidem_check(fun reduce_index_identity/2, undefined, A, B, C, E),
    R2 = commassidem_check(fun reduce_index_identity/2, undefined, D, F, G, H),
    D0 = element(1, D),
    ?assertMatch([A, B, C, D0], R0),
    ?assertMatch([A, B, C, E], R1),
    ?assertMatch([{<<"B4">>, <<"K4">>},
                    {<<"B6">>, <<"K6">>},
                    {<<"B7">>, <<"K7">>},
                    {<<"B8">>, <<"K8">>}], R2).

reduce_index_extractinteger_test() ->
    A = {{<<"B1">>, <<"K1">>}, [{term, <<0:8/integer, 1:32/integer, 0:8/integer>>}]},
    B = {{<<"B2">>, <<"K2">>}, [{term, <<0:8/integer, 2:32/integer>>}, {extract, 26}]},
    C = {{<<"B3">>, <<"K3">>}, [{extract, 99}, {term, <<0:8/integer, 3:32/integer, 0:16/integer>>}]},
    D = {{<<"B4">>, <<"K4">>}, undefined},
    E = {{<<"EB5">>, <<"EK5">>}, <<0:4/integer>>},
    R0 = commassidem_check(fun reduce_index_extractinteger/2, {term, extint, all, 1, 32}, A, B, C, D),
    R1 = commassidem_check(fun reduce_index_extractinteger/2, {term, extint, this, 1, 32}, A, B, C, E),
    ExpR0 = 
        [{{<<"B1">>, <<"K1">>}, [{extint, 1}, {term, <<0:8/integer, 1:32/integer, 0:8/integer>>}]},
        {{<<"B2">>, <<"K2">>}, [{extint, 2}, {extract, 26}, {term, <<0:8/integer, 2:32/integer>>}]},
        {{<<"B3">>, <<"K3">>}, [{extint, 3}, {extract, 99}, {term, <<0:8/integer, 3:32/integer, 0:16/integer>>}]}
        ],
    ExpR1 =
        [{{<<"B1">>, <<"K1">>}, [{extint, 1}]},
        {{<<"B2">>, <<"K2">>}, [{extint, 2}]},
        {{<<"B3">>, <<"K3">>}, [{extint, 3}]}
        ],
    ?assertMatch(ExpR0, R0),
    ?assertMatch(ExpR1, R1).

reduce_index_extractbinary_test() ->
    [Bin1, Bin2, Bin3, Bin4] = [<<"Bin1">>, <<"Bin2">>, <<"Bin3">>, <<"BigBin4">>],
    A = {{<<"B1">>, <<"K1">>}, [{term, <<0:8/integer, Bin1/binary, 0:8/integer>>}]},
    B = {{<<"B2">>, <<"K2">>}, [{term, <<0:8/integer, Bin2/binary>>}, {extract, 26}]},
    C = {{<<"B3">>, <<"K3">>}, [{extract, 99}, {term, <<0:8/integer, Bin3/binary, 0:16/integer>>}]},
    D = {{<<"B4">>, <<"K4">>}, undefined},
    E = {{<<"EB5">>, <<"EK5">>}, <<0:4/integer>>},
    R0 = commassidem_check(fun reduce_index_extractbinary/2, {term, extbin, all, 1, 4}, A, B, C, D),
    R1 = commassidem_check(fun reduce_index_extractbinary/2, {term, extbin, this, 1, 4}, A, B, C, E),
    ExpR0 = 
        [{{<<"B1">>, <<"K1">>}, [{extbin, Bin1}, {term, <<0:8/integer, Bin1/binary, 0:8/integer>>}]},
        {{<<"B2">>, <<"K2">>}, [{extbin, Bin2}, {extract, 26}, {term, <<0:8/integer, Bin2/binary>>}]},
        {{<<"B3">>, <<"K3">>}, [{extbin, Bin3}, {extract, 99}, {term, <<0:8/integer, Bin3/binary, 0:16/integer>>}]}
        ],
    ExpR1 =
        [{{<<"B1">>, <<"K1">>}, [{extbin, Bin1}]},
        {{<<"B2">>, <<"K2">>}, [{extbin, Bin2}]},
        {{<<"B3">>, <<"K3">>}, [{extbin, Bin3}]}
        ],
    ?assertMatch(ExpR0, R0),
    ?assertMatch(ExpR1, R1),

    A1 = {{<<"B1">>, <<"K1">>}, [{term, <<0:8/integer, Bin1/binary>>}]},
    B1 = {{<<"B2">>, <<"K2">>}, [{term, <<0:8/integer, Bin2/binary>>}]},
    C1 = {{<<"B3">>, <<"K3">>}, [{term, <<0:8/integer, Bin3/binary>>}]},
    D1 = {{<<"B4">>, <<"K4">>}, [{term, <<0:8/integer, Bin4/binary>>}]},
    ExpR2 = ExpR1 ++ [{{<<"B4">>, <<"K4">>}, [{extbin, Bin4}]}],
    R2 = commassidem_check(fun reduce_index_extractbinary/2, {term, extbin, this, 1, all}, A1, B1, C1, D1),
    ?assertMatch(ExpR2, R2).


reduce_index_extractregex_test() ->
    RE = "[0-9]+\\|(?<surname>[^|]*)\\|(?<preferred>[^|]*)$",
    A = {{<<"B1">>, <<"K1">>}, [{term, <<"19421209|Bremner|Billy">>}]},
    B = {{<<"B2">>, <<"K2">>}, [{extract, 26}, {term, <<"19311227|Charles|John">>}]},
    C = {{<<"B3">>, <<"K3">>}, [{extract, 99}, {term, <<"19431029|Hunter">>}]},
    D = {{<<"B4">>, <<"K4">>}, undefined},
    R0 = commassidem_check(fun reduce_index_extractregex/2,
                            {term,
                                [surname, preferred],
                                this,
                                RE},
                            A, B, C, D),
    ExpR0 =
        [{{<<"B1">>, <<"K1">>}, [{surname, <<"Bremner">>}, {preferred, <<"Billy">>}]},
        {{<<"B2">>, <<"K2">>}, [{surname, <<"Charles">>}, {preferred, <<"John">>}]}],
    ?assertMatch(ExpR0, R0).

reduce_index_byrange_test() ->
    A = {{<<"B1">>, <<"K1">>}, [{extint, 1}]},
    B = {{<<"B2">>, <<"K2">>}, [{extint, 2}]},
    C = {{<<"B3">>, <<"K3">>}, [{extint, 3}]},
    D = {{<<"B4">>, <<"K4">>}, [{extint, 4}]},
    E = {{<<"E5">>}},
    F = {{<<"F6">>, <<"F7">>}, undefined},
    R0 = commassidem_check(fun reduce_index_applyrange/2, {extint, all, 2, 3}, A, B, C, D),
    commassidem_check(fun reduce_index_applyrange/2, {extint, this, 2, 3}, A, B, C, E),
    commassidem_check(fun reduce_index_applyrange/2, {extint, all, 2, 3}, A, B, C, F),
    ?assertMatch([B, C], R0).

reduce_index_regex_test() ->
    R = ".*99.*",
    A = {{<<"B1">>, <<"K1">>}, [{term, <<"v99a">>}]},
    B = {{<<"B2">>, <<"K2">>}, [{term, <<"v99b">>}]},
    C = {{<<"B3">>, <<"K3">>}, [{term, <<"v98a">>}]},
    D = {{<<"B4">>, <<"K4">>}, [{term, <<"v99d">>}]},
    E = {{<<"E5">>}},
    F = {{<<"F6">>, <<"F7">>}, undefined},
    R0 = commassidem_check(fun reduce_index_applyregex/2, {term, this, R}, A, B, C, D),
    commassidem_check(fun reduce_index_applyregex/2, {term, this, R}, A, B, C, E),
    commassidem_check(fun reduce_index_applyregex/2, {term, all, R}, A, B, C, F),
    ?assertMatch([A, B, D], R0).

reduce_index_max_test() ->
    A = {{<<"B1">>, <<"K1">>}, [{int, 5}]},
    B = {{<<"B2">>, <<"K2">>}, [{int, 7}, {term, <<"v7">>}]},
    C = {{<<"B3">>, <<"K3">>}, [{int, 8}]},
    D = {{<<"B4">>, <<"K4">>}, [{term, 9}]},
    R0 = commassidem_check(fun reduce_index_max/2, {int, 1}, A, B, C, D),
    R1 = commassidem_check(fun reduce_index_max/2, {int, 2}, A, B, C, D),
    ?assertMatch([C], R0),
    ?assertMatch([B, C], R1).

reduce_index_min_test() ->
    A = {{<<"B1">>, <<"K1">>}, [{int, 5}]},
    B = {{<<"B2">>, <<"K2">>}, [{int, 7}, {term, <<"v7">>}]},
    C = {{<<"B3">>, <<"K3">>}, [{int, 8}]},
    D = {{<<"B4">>, <<"K4">>}, [{term, 9}]},
    E = {{<<"B3">>, <<"K5">>}, [{int, 8}]},
    R0 = commassidem_check(fun reduce_index_min/2, {int, 1}, A, B, C, D),
    R1 = commassidem_check(fun reduce_index_min/2, {int, 2}, A, B, C, D),
    R2 = commassidem_check(fun reduce_index_min/2, {int, 3}, A, B, C, E),
    R3 = commassidem_check(fun reduce_index_min/2, {int, 3}, A, B, E, C),
    ?assertMatch([A], R0),
    ?assertMatch([A, B], R1),
    ?assertMatch([A, B, C], R2),
    ?assertMatch([A, B, C], R3).

reduce_index_hamming_test() ->
    reduce_index_hamming_tester(fun simhash/1),
    reduce_index_hamming_tester(fun(X) -> <<I:128/integer>> = simhash(X), I end).

reduce_index_hamming_tester(HashFun) ->
    A = {{<<"B1">>, <<"K1">>}, [{sim, HashFun(<<"pablo is king">>)}]},
    B = {{<<"B2">>, <<"K2">>}, [{sim, HashFun(<<"marching on together">>)}]},
    C = {{<<"B3">>, <<"K3">>}, [{sim, HashFun(<<"ups and downs">>)}]},
    D = {{<<"B4">>, <<"K4">>}, [{sim, HashFun(<<"oliver's army">>)}]},
    R = commassidem_check(fun reduce_index_extracthamming/2,
                            {sim, hamming, this, simhash(<<"pabs is king">>)},
                            A, B, C, D),
    R0 = lists:keysort(2, lists:map(fun({BK, [{hamming, H}]}) -> {BK, H} end, R)),
    [{BK1, _H1}|_T] = R0,
    ?assertMatch({<<"B1">>, <<"K1">>}, BK1).

reduce_index_decode_test() ->
    HashFun = fun(Bin) -> base64:encode(simhash(Bin)) end,
    A = {{<<"B1">>, <<"K1">>}, [{sim, HashFun(<<"pablo is king">>)}]},
    B = {{<<"B2">>, <<"K2">>}, [{sim, HashFun(<<"marching on together">>)}]},
    C = {{<<"B3">>, <<"K3">>}, [{sim, HashFun(<<"ups and downs">>)}]},
    D = {{<<"B4">>, <<"K4">>}, [{sim, HashFun(<<"oliver's army">>)}]},
    R = commassidem_check(fun reduce_index_extractencoded/2,
                            {sim, sim_decoded, this},
                            A, B, C, D),
    R0 = reduce_index_extracthamming(R, {sim_decoded, hamming, this, simhash(<<"pabs is king">>)}),
    R1 = lists:keysort(2, lists:map(fun({BK, [{hamming, H}]}) -> {BK, H} end, R0)),
    [{BK1, _H1}|_T] = R1,
    ?assertMatch({<<"B1">>, <<"K1">>}, BK1).

reduce_index_union_test() ->
    A = {{<<"B1">>, <<"K1">>}, [{int, 5}]},
    B = {{<<"B2">>, <<"K2">>}, [{int, 5}, {term, <<"v7">>}]},
    C = {{<<"B3">>, <<"K3">>}, [{int, 8}]},
    D = {{<<"B4">>, <<"K4">>}, [{term, 9}]},
    R0 = commassidem_check(fun reduce_index_union/2, {int, integer}, A, B, C, D),
    R1 = commassidem_check(fun reduce_index_union/2, {term, binary}, A, B, C, D),
    ?assertMatch([5, 8], R0),
    ?assertMatch([<<"v7">>], R1).

reduce_index_countby_test() ->
    A = {{<<"B1">>, <<"K1">>}, [{int, 5}, {term, <<"v8">>}]},
    B = {{<<"B2">>, <<"K2">>}, [{int, 5}, {term, <<"v7">>}]},
    C = {{<<"B3">>, <<"K3">>}, [{int, 8}, {term, <<"v7">>}]},
    D = {{<<"B4">>, <<"K4">>}, [{term, 9}]},
    R0 = commassidem_check(fun reduce_index_countby/2, {int, integer}, A, B, C, D),
    R1 = commassidem_check(fun reduce_index_countby/2, {term, binary}, A, B, C, D),
    ?assertMatch([{5, 2}, {8, 1}], R0),
    ?assertMatch([{<<"v7">>, 2}, {<<"v8">>, 1}], R1).

reduce_index_extractmask_test() ->
    A = {{<<"B1">>, <<"K1">>}, [{int, 1}, {term, <<"v8">>}]},
    B = {{<<"B2">>, <<"K2">>}, [{int, 9}, {term, <<"v7">>}]},
    C = {{<<"B3">>, <<"K3">>}, [{int, 2}, {term, <<"v7">>}]},
    D = {{<<"B4">>, <<"K4">>}, [{term, 9}]},
    R0 = commassidem_check(fun reduce_index_extractmask/2, {int, mask, this, 3}, A, B, C, D),
    ?assertMatch([{{<<"B1">>, <<"K1">>}, [{mask, 1}]},
                    {{<<"B2">>, <<"K2">>}, [{mask, 1}]},
                    {{<<"B3">>, <<"K3">>}, [{mask, 2}]}], R0).

reduce_index_applymask_test() ->
    A = {{<<"B1">>, <<"K1">>}, [{int, 1}, {term, <<"v8">>}]},
    B = {{<<"B2">>, <<"K2">>}, [{int, 9}, {term, <<"v7">>}]},
    C = {{<<"B3">>, <<"K3">>}, [{int, 2}, {term, <<"v7">>}]},
    D = {{<<"B4">>, <<"K4">>}, [{term, 9}]},
    R0 = commassidem_check(fun reduce_index_applymask/2, {int, this, 1}, A, B, C, D),
    ?assertMatch([{{<<"B1">>, <<"K1">>}, [{int, 1}]},
                    {{<<"B2">>, <<"K2">>}, [{int, 9}]}], R0).

reduce_index_sort_test() ->
    A = {{<<"B1">>, <<"K1">>}, [{int, 1}, {term, <<"v8">>}]},
    B = {{<<"B1">>, <<"K2">>}, [{int, 9}, {term, <<"v7">>}]},
    C = {{<<"B1">>, <<"K3">>}, [{int, 2}, {term, <<"v7">>}]},
    D = {{<<"B1">>, <<"K4">>}, [{int, 0}, {term, <<"v9">>}]},
    R0 = commassidem_check(fun reduce_index_sort/2, key, A, B, C, D, false),
    R1 = commassidem_check(fun reduce_index_sort/2, term, A, B, C, D, false),
    R1A = commassidem_check(fun reduce_index_sort/2, term, A, C, B, D, false),
    R2 = commassidem_check(fun reduce_index_sort/2, int, A, B, C, D, false),
    ?assertMatch([A, B, C, D], R0),
    ?assertMatch([B, C, A, D], R1),
    ?assertMatch([B, C, A, D], R1A),
    ?assertMatch([D, A, C, B], R2).

reduce_index_extracthash_test() ->
    Hashes0 =
        lists:sort(
            lists:map(fun(K) -> riak_kv_hints:hash(K, fnva) end,
                    [<<"K1">>, <<"K2">>, <<"K3">>, <<"K4">>])),
    Hashes1 =
        lists:sort(
            lists:map(fun(K) -> riak_kv_hints:hash(K, fnva) end,
                    [<<"v8">>, <<"v7">>])),
    A = {{<<"B1">>, <<"K1">>}, [{int, 1}, {term, <<"v8">>}]},
    B = {{<<"B1">>, <<"K2">>}, [{int, 9}, {term, <<"v7">>}]},
    C = {{<<"B1">>, <<"K3">>}, [{int, 2}, {term, <<"v7">>}]},
    D = {{<<"B1">>, <<"K4">>}, [{term, 9}]},
    R0A = commassidem_check(fun reduce_index_extracthash/2, {key, hash, this, fnva}, A, B, C, D),
    R0B = reduce_index_union(R0A, {hash, integer}),
    ?assertMatch(Hashes0, lists:sort(R0B)),
    R1A = commassidem_check(fun reduce_index_extracthash/2, {term, hash, this, fnva}, A, B, C, D),
    R1B = reduce_index_union(R1A, {hash, integer}),
    ?assertMatch(Hashes1, lists:sort(R1B)).

reduce_index_apply_bloom_test() ->
    Keys0 = [<<"K1">>, <<"K2">>, <<"K3">>, <<"K4">>,
                <<"K5">>, <<"K6">>, <<"K7">>, <<"K8">>,
                <<"K9">>, <<"K10">>, <<"K11">>, <<"K12">>],
    Keys1 = [<<"K1">>, <<"K4">>],
    Bloom0 = riak_kv_hints:create_gcs_metavalue(Keys0, 10, fnva),
    Bloom1 = riak_kv_hints:create_gcs_metavalue(Keys1, 12, md5),
    A = {{<<"B1">>, <<"K1">>}, [{int, 1}, {term, <<"v8">>}]},
    B = {{<<"B1">>, <<"K2">>}, [{int, 9}, {term, <<"v7">>}]},
    C = {{<<"B1">>, <<"K3">>}, [{int, 2}, {term, <<"v7">>}]},
    D = {{<<"B1">>, <<"K4">>}, [{term, 9}]},
    R0A = commassidem_check(fun reduce_index_applybloom/2, {key, this, {riak_kv_hints, Bloom0}}, A, B, C, D),
    ?assertMatch([{<<"B1">>, <<"K1">>}, 
                    {<<"B1">>, <<"K2">>},
                    {<<"B1">>, <<"K3">>},
                    {<<"B1">>, <<"K4">>}], R0A),
    R1A = commassidem_check(fun reduce_index_applybloom/2, {key, all, {riak_kv_hints, Bloom1}}, A, B, C, D),
    ?assertMatch([A, D], lists:sort(R1A)),
    Vals0 = [<<"v7">>, <<"v77">>, <<"v777">>],
    BloomV0 = riak_kv_hints:create_gcs_metavalue(Vals0, 12, md5),
    RV0 = commassidem_check(fun reduce_index_applybloom/2, {term, this, {riak_kv_hints, BloomV0}}, A, B, C, D),
    ?assertMatch([{{<<"B1">>, <<"K2">>}, [{term, <<"v7">>}]},
                        {{<<"B1">>, <<"K3">>}, [{term, <<"v7">>}]}],
                    lists:sort(RV0)).


reduce_index_extractbuckets_test() ->
    A = {{<<"B1">>, <<"K1">>}, [{int, 1}, {term, <<"v8">>}]},
    B = {{<<"B1">>, <<"K2">>}, [{int, 9}, {term, <<"v7">>}]},
    C = {{<<"B1">>, <<"K3">>}, [{int, 2}, {term, <<"v7">>}]},
    D = {{<<"B1">>, <<"K4">>}, [{int, 5}, {term, <<"v7">>}]},
    E = {{<<"B1">>, <<"K5">>}, [{int, 5}, {term, <<"v6">>}]},
    R0A = commassidem_check(fun reduce_index_extractbuckets/2,
                                {term, version, this,
                                    [{<<"v7">>, <<"fix">>},
                                        {<<"v6">>, <<"pre_fix">>}], 
                                <<"post_fix">>},
                                A, B, C, D),
    ?assertMatch([{{<<"B1">>, <<"K1">>}, [{version, <<"post_fix">>}]},
                    {{<<"B1">>, <<"K2">>}, [{version, <<"fix">>}]},
                    {{<<"B1">>, <<"K3">>}, [{version, <<"fix">>}]},
                    {{<<"B1">>, <<"K4">>}, [{version, <<"fix">>}]}],
                    R0A),
    R0B = commassidem_check(fun reduce_index_extractbuckets/2,
                                {term, version, this,
                                    [{<<"v7">>, <<"fix">>},
                                        {<<"v6">>, <<"pre_fix">>}], 
                                <<"post_fix">>},
                                A, B, C, E),
    ?assertMatch([{{<<"B1">>, <<"K1">>}, [{version, <<"post_fix">>}]},
                    {{<<"B1">>, <<"K2">>}, [{version, <<"fix">>}]},
                    {{<<"B1">>, <<"K3">>}, [{version, <<"fix">>}]},
                    {{<<"B1">>, <<"K5">>}, [{version, <<"pre_fix">>}]}],
                    R0B),
    R1A = commassidem_check(fun reduce_index_extractbuckets/2,
                                {int, size, all,
                                    [{3, <<"small">>},
                                        {5, <<"meh">>}], 
                                <<"big">>},
                                A, B, C, D),
    ?assertMatch([{{<<"B1">>, <<"K1">>},
                        [{int, 1}, {size, <<"small">>}, {term, <<"v8">>}]},
                    {{<<"B1">>, <<"K2">>},
                        [{int, 9}, {size, <<"big">>}, {term, <<"v7">>}]},
                    {{<<"B1">>, <<"K3">>}, 
                        [{int, 2}, {size, <<"small">>}, {term, <<"v7">>}]},
                    {{<<"B1">>, <<"K4">>},
                        [{int, 5}, {size, <<"meh">>}, {term, <<"v7">>}]}],
                    R1A).

reduce_index_extractcoalesce_test() ->
    A = {{<<"B1">>, <<"K1">>}, [{size, <<"small">>}, {int, 1}, {version, <<"v8">>}]},
    B = {{<<"B1">>, <<"K2">>}, [{size, <<"big">>}, {int, 1}, {version, <<"v7">>}]},
    C = {{<<"B1">>, <<"K3">>}, [{size, <<"small">>}, {int, 1}, {version, <<"v7">>}]},
    D = {{<<"B1">>, <<"K4">>}, [{size, <<"meh">>}, {int, 1}, {version, <<"v7">>}]},
    E = {{<<"B1">>, <<"K5">>}, [{int, 1}, {version, <<"v7">>}]},
    F = {{<<"B1">>, <<"K6">>}, [{size, <<"meh">>}, {int, 1}]},
    R0A = commassidem_check(fun reduce_index_extractcoalesce/2,
                            {[version, size], vsize, this, <<"|">>},
                            A, B, C, D),
    ?assertMatch([{{<<"B1">>, <<"K1">>}, [{vsize, <<"v8|small">>}]},
                    {{<<"B1">>, <<"K2">>}, [{vsize, <<"v7|big">>}]},
                    {{<<"B1">>, <<"K3">>}, [{vsize, <<"v7|small">>}]},
                    {{<<"B1">>, <<"K4">>}, [{vsize, <<"v7|meh">>}]}],
                    R0A),
    R0B = commassidem_check(fun reduce_index_extractcoalesce/2,
                            {[version, size], vsize, this, <<"">>},
                            A, B, C, D),
    ?assertMatch([{{<<"B1">>, <<"K1">>}, [{vsize, <<"v8small">>}]},
                    {{<<"B1">>, <<"K2">>}, [{vsize, <<"v7big">>}]},
                    {{<<"B1">>, <<"K3">>}, [{vsize, <<"v7small">>}]},
                    {{<<"B1">>, <<"K4">>}, [{vsize, <<"v7meh">>}]}],
                    R0B),
    R1A = commassidem_check(fun reduce_index_extractcoalesce/2,
                            {[version, size], vsize, this, <<"|">>},
                            A, B, E, F),
    ?assertMatch([{{<<"B1">>, <<"K1">>}, [{vsize, <<"v8|small">>}]},
                    {{<<"B1">>, <<"K2">>}, [{vsize, <<"v7|big">>}]},
                    {{<<"B1">>, <<"K5">>}, [{vsize, <<"v7|">>}]},
                    {{<<"B1">>, <<"K6">>}, [{vsize, <<"|meh">>}]}],
                    R1A).
    

commassidem_check(F, Args, A, B, C, D) ->
    commassidem_check(F, Args, A, B, C, D, true).

commassidem_check(F, Args, A, B, C, D, Sort) ->
    % Is the reduce function commutative, associative and idempotent
    ID1 = F([A, B, C, D], Args),
    ID2 = F([A, D] ++ F([C, B], Args), Args),
    ID3 = F([F([A], Args), F([B], Args), F([C], Args), F([D], Args)], Args),
    io:format("ID1 ~w ID2 ~w ID3 ~w~n", [ID1, ID2, ID3]),
    R = lists:sort(ID1),
    ?assertMatch(R, lists:sort(ID2)),
    ?assertMatch(R, lists:sort(ID3)),
    ID4 = F([A, B, C, D], [{reduce_phase_batch_size, 50}, {args, Args}]),
    ?assertMatch(ID1, ID4),
    case Sort of
        true -> R;
        false -> ID1
    end.