%% -------------------------------------------------------------------
%%
%% riak_kv_mapreduce: convenience functions for defining common map/reduce phases
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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
         map_object_value_list/1,
         map_delete/1,
         map_datasize/1,
         map_large_objects/1,
         map_id/1,
         map_key/1,
         map_siblings/1
         ]).
-export([reduce_identity/1,
         reduce_set_union/1,
         reduce_sort/1,
         reduce_string_to_integer/1,
         reduce_sum/1,
         reduce_plist_sum/1,
         reduce_count_inputs/1,
         reduce_limit/1,
         reduce_delete/1
         ]).

%% phase function definitions
-export([map_identity/3,
         map_object_value/3,
         map_object_value_list/3,
         map_delete/3,
         map_datasize/3,
         map_large_objects/3,
         map_id/3,
         map_key/3,
         map_siblings/3
         ]).
-export([reduce_identity/2,
         reduce_set_union/2,
         reduce_sort/2,
         reduce_string_to_integer/2,
         reduce_sum/2,
         reduce_plist_sum/2,
         reduce_count_inputs/2,
         reduce_limit/2,
         reduce_delete/2
         ]).

-define(DEFAULT_REDUCE_LIMIT_SIZE, 100).
-define(DEFAULT_LARGE_OBJECT_LIMIT, 1048576).

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
    case riakc_obj:value_count(RiakObject) of
        1 ->
            riak_object:get_value(RiakObject);
        _ ->
            [[<<"siblings">>, riak_object:bucket(RiakObject), riak_object:key(RiakObject)]]
    end.

%% implementation of the notfound behavior for
%% map_object_value and map_object_value_list
notfound_map_action(_NF, _KD, <<"filter_notfound">>)    -> [];
notfound_map_action(NF, _KD, <<"include_notfound">>)    -> [NF];
notfound_map_action(_NF, KD, <<"include_keydata">>)     -> [KD]; 
notfound_map_action(_NF, _KD, {struct,[{<<"sub">>,V}]}) -> V;
notfound_map_action(_NF, _KD, _) -> [].

%% @spec map_delete(boolean()) -> map_phase_spec()
%% @doc Produces a spec for a map phase that deletes
%%      each object it's handed.
map_delete(Acc) ->
    {map, {modfun, riak_kv_mapreduce, map_delete}, none, Acc}.

%% @spec map_delete(riak_object:riak_object(), term(), term()) ->
%%                   [] | [integer()]
%% @doc map phase function for map_delete/1
map_delete({error, notfound}, _, _) ->
    [];
map_delete(RiakObject, _, _) ->
    case is_deleted(RiakObject) of
        true ->
            [];
        false ->
            {ok, C} = riak:local_client(),
            Bucket = riak_object:bucket(RiakObject),
            Key = riak_object:key(RiakObject),
            C:delete(Bucket, Key),
            [1]  
    end.

%% @spec map_datasize(boolean()) -> map_phase_spec()
%% @doc Produces a spec for a map phase that returns the total
%%      size in bytes of all siblings of the object it's handed.
map_datasize(Acc) ->
    {map, {modfun, riak_kv_mapreduce, map_datasize}, none, Acc}.

%% @spec map_datasize(riak_object:riak_object(), term(), term()) ->
%%                   [integer()]
%% @doc map phase function for map_datasize/1
map_datasize({error, notfound}, _, _) ->
    [];
map_datasize(RiakObject, _, _) ->
    [lists:foldl(fun({MD,Val}, A) ->
                     case dict:is_key(<<"X-Riak-Deleted">>, MD) of
                         true ->
                             A;
                         false ->
                             (byte_size(Val) + A)
                     end
                 end, 0, riak_object:get_contents(RiakObject))].




%% @spec map_large_objects(boolean()) -> map_phase_spec()
%% @doc Produces a spec for a map phase that returns id and size
%%      in bytes of all object exceeding a specific size.
%%      If siblings exist, the combined size of these is used.
map_large_objects(Acc) ->
    {map, {modfun, riak_kv_mapreduce, map_large_objects}, none, Acc}.

%% @spec map_large_objects(riak_object:riak_object(), term(), term()) ->
%%                   [[Bucket :: binary(), Key :: binary(), integer()]] | []
%% @doc map phase function for map_datasize/1
map_large_objects({error, notfound}, _, _) ->
    [];
map_large_objects(RiakObject, KeyData, Arg) when is_binary(Arg) ->
    try 
        map_large_objects(RiakObject, KeyData, list_to_integer(binary_to_list(Arg)))
    catch
        _:_ ->
            try
                {struct, Props} = mochijson2:decode(Arg),
                map_large_objects(RiakObject, KeyData, Props)
            catch
                _:_  ->
                    []
            end
    end;
map_large_objects(RiakObject, KeyData, Props) when is_list(Props) ->
    case proplists:get_value(<<"limit">>, Props, ?DEFAULT_LARGE_OBJECT_LIMIT) of
        Limit when is_integer(Limit) ->
            map_large_objects(RiakObject, KeyData, Limit);
        _ ->
            map_large_objects(RiakObject, KeyData, ?DEFAULT_LARGE_OBJECT_LIMIT)      
    end;
map_large_objects(RiakObject, _, Limit) when is_integer(Limit) ->
    Size = lists:foldl(fun({MD,Val}, A) ->
                     case dict:is_key(<<"X-Riak-Deleted">>, MD) of
                         true ->
                             A;
                         false ->
                             (byte_size(Val) + A)
                     end
                 end, 0, riak_object:get_contents(RiakObject)),
    case Size > Limit of
        true ->
            [[riak_object:bucket(RiakObject), riak_object:key(RiakObject), Size]];
        false ->
            []
    end.

%% @spec map_id(boolean()) -> map_phase_spec()
%% @doc Produces a spec for a map phase that returns
%%      the ID of each object it's handed.
map_id(Acc) ->
    {map, {modfun, riak_kv_mapreduce, map_id}, none, Acc}.

%% @spec map_id(riak_object:riak_object(), term(), term()) ->
%%                   [[Bucket :: binary(), Key :: binary()]]
%% @doc map phase function returning bucket name and key in a readable format
map_id({error, notfound}, _, _) ->
    [];
map_id(RiakObject, _, _) ->
    case is_deleted(RiakObject) of
        true ->
            [];
        false ->
            [[riak_object:bucket(RiakObject), riak_object:key(RiakObject)]]
    end.

%% @spec map_key(boolean()) -> map_phase_spec()
%% @doc Produces a spec for a map phase that returns
%%      the key of each object it's handed.
map_key(Acc) ->
    {map, {modfun, riak_kv_mapreduce, map_key}, none, Acc}.

%% @spec map_key(riak_object:riak_object(), term(), term()) ->
%%                   [Key :: binary()]
%% @doc map phase function returning object key in a readable format
map_key({error, notfound}, _, _) ->
    [];
map_key(RiakObject, _, _) ->
    case is_deleted(RiakObject) of
        true ->
            [];
        false ->
            [riak_object:key(RiakObject)]    
    end. 

%% @spec map_siblings(boolean()) -> map_phase_spec()
%% @doc Produces a spec for a map phase that returns
%%      the ID of each object it's handed.
map_siblings(Acc) ->
    {map, {modfun, riak_kv_mapreduce, map_siblings}, none, Acc}.

%% @spec map_siblings(riak_object:riak_object(), term(), term()) ->
%%                   [[Bucket :: binary(), Key :: binary()]]
%% @doc map phase function returning bucket name and key in a readable format
%%      for any record passed in that contains siblings
map_siblings({error, notfound}, _, _) ->
    [];
map_siblings(RiakObject, _, _) ->
    case is_deleted(RiakObject) of
        true ->
            [];
        false ->
            case riak_object:value_count(RiakObject) of
                1 ->
                    [];
                _ ->
                    [[riak_object:bucket(RiakObject), riak_object:key(RiakObject)]]
            end
    end.

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
                %% Handle BKeys with a extra data.
                [[Bucket, Key]|Acc];
           ({Bucket, Key}, Acc) ->
                %% Handle BKeys.
                [[Bucket, Key]|Acc];
           ([Bucket, Key], Acc) ->
                %% Handle re-reduces.
                [[Bucket, Key]|Acc];
           (Other, _Acc) ->
                %% Fail loudly on anything unexpected.
                lager:error("Unhandled entry: ~p", [Other]),
                throw({unhandled_entry, Other})
        end,
    lists:foldl(F, [], List).

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

%% @spec reduce_limit(boolean()) -> reduce_phase_spec()
%% @doc Produces a spec for a reduce phase that limits the size
%%      of the list to a configurable number
reduce_limit(Acc) ->
    {reduce, {modfun, riak_kv_mapreduce, reduce_limit}, none, Acc}.

%% @spec reduce_limit([term()], term()) -> [term()]
%% @doc reduce phase function for reduce_limit/1
reduce_limit(List, Arg) when is_binary(Arg) ->
    try 
        reduce_limit(List, list_to_integer(binary_to_list(Arg)))
    catch
        _:_ ->
            try
                {struct, Props} = mochijson2:decode(Arg),
                case proplists:get_value(<<"limit">>, Props, ?DEFAULT_REDUCE_LIMIT_SIZE) of
                    Limit when is_integer(Limit) ->
                        reduce_limit(List, Limit);
                    Limit when is_binary(Limit) ->
                        reduce_limit(List, list_to_integer(binary_to_list(Limit)))
                end
            catch
                _:_  ->
                    []
            end
    end;
reduce_limit(List, {struct, Props}) ->
    case proplists:get_value(<<"limit">>, Props, ?DEFAULT_REDUCE_LIMIT_SIZE) of
        Limit when is_integer(Limit) ->
            reduce_limit(List, Limit);
        Limit when is_binary(Limit) ->
            reduce_limit(List, list_to_integer(binary_to_list(Limit)))
    end;
reduce_limit(List, Limit) when is_integer(Limit) andalso (length(List) >= Limit) ->
    {L , _} = lists:split(Limit, List),
    L;
reduce_limit(List, Limit) when is_integer(Limit) ->
    List;
reduce_limit(List, _) ->
    reduce_limit(List, ?DEFAULT_REDUCE_LIMIT_SIZE).

%% @spec reduce_delete(boolean()) -> reduce_phase_spec()
%% @doc Produces a spec for a reduce phase that deletes
%%      any [Bucket, Key] it's handed and returns a count of deleted records.  
reduce_delete(Acc) ->
    {reduce, {modfun, riak_kv_mapreduce, reduce_delete}, none, Acc}.

%% @spec reduce_delete([term()], term()) -> [integer()]
%% @doc map phase function for reduce_delete/1
reduce_delete(List, _) -> 
    F = fun(Count, {C, Acc}) when is_integer(Count) ->
                {C, (Acc+Count)};
           ({error, notfound}, {C, Acc}) ->
                {C, Acc};
           ({{Bucket, Key}, _}, {C, Acc}) ->
                C:delete(Bucket, Key),
                {C, (Acc+1)};
           ({Bucket, Key}, {C, Acc}) ->
                C:delete(Bucket, Key),
                {C, (Acc+1)};
            ([Bucket, Key], {C, Acc}) ->
                C:delete(Bucket, Key),
                {C, (Acc+1)};
           (Other, _Acc) ->
                %% Fail loudly on anything unexpected.
                lager:error("Unhandled entry: ~p", [Other]),
                throw({unhandled_entry, Other})
        end,
    {ok, C} = riak:local_client(),
    {C, N} = lists:foldl(F, {C,0}, List),
    [N].

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

%% Helper function
is_deleted([]) ->
    true;
is_deleted([Dict | D]) ->
    case dict:is_key(<<"X-Riak-Deleted">>, Dict) of
        false ->
            false;
        true ->
            is_deleted(D)
    end;
is_deleted(RiakObject) ->
    is_deleted(riak_object:get_metadatas(RiakObject)).

%%
%% EUNIT tests...
%%

-ifdef (TEST).

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

map_datasize_test() ->
    MD1 = dict:from_list([{<<"X-Riak-Deleted">>,<<"">>}]),
    MD2 = dict:new(),
    O1 = riak_object:new(<<"test">>, <<"a">>, <<"value">>),
    O2 = riak_object:set_contents(O1, [{MD1,<<"value">>}]),
    O3 = riak_object:set_contents(O1, [{MD2,<<"value">>},{MD2,<<"value">>}]),
    O4 = riak_object:set_contents(O1, [{MD1,<<"value">>},{MD2, <<"value">>}]),
    ?assertEqual([5], map_datasize(O1, none, none)),
    ?assertEqual([0], map_datasize(O2, none, none)),
    ?assertEqual([10], map_datasize(O3, none, none)),
    ?assertEqual([5], map_datasize(O4, none, none)).

map_large_objects_test() ->
    MD1 = dict:from_list([{<<"X-Riak-Deleted">>,<<"">>}]),
    MD2 = dict:new(),
    O1 = riak_object:new(<<"b">>, <<"k">>, <<"0123456789">>),
    O2 = riak_object:set_contents(O1, [{MD1,<<"0123456789">>}]),
    O3 = riak_object:set_contents(O1, [{MD2,<<"0123456789">>},{MD2,<<"0123456789">>}]),
    O4 = riak_object:set_contents(O1, [{MD1,<<"0123456789">>},{MD2, <<"0123456789">>}]),
    ?assertEqual([[<<"b">>,<<"k">>,10]], map_large_objects(O1, none, [{<<"limit">>, 7}])),
    ?assertEqual([], map_large_objects(O2, none, <<"7">>)),
    ?assertEqual([[<<"b">>,<<"k">>,20]], map_large_objects(O3, none, <<"7">>)),
    ?assertEqual([[<<"b">>,<<"k">>,10]], map_large_objects(O4, none, <<"{\"limit\":7}">>)).

map_id_test() ->
    O1 = riak_object:new(<<"a">>, <<"1">>, "value1"),
    ?assertEqual([[<<"a">>,<<"1">>]], map_id(O1, test, test)).

map_key_test() ->
    O1 = riak_object:new(<<"a">>, <<"1">>, "value1"),
    ?assertEqual([<<"1">>], map_key(O1, test, test)).

map_siblings_test() ->
    MD = dict:new(),
    O1 = riak_object:new(<<"b">>, <<"k">>, <<"value">>),
    O2 = riak_object:set_contents(O1, [{MD,<<"value">>},{MD,<<"value">>}]),
    ?assertEqual([], map_siblings(O1, test, test)),
    ?assertEqual([[<<"b">>,<<"k">>]], map_siblings(O2, test, test)).

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

reduce_limit_test() ->
    [1,2,3] = reduce_limit([1,2,3], none),
    [1,2] = reduce_limit([1,2,3,4,5], 2),
    [1,2] = reduce_limit([1,2,3,4], <<"2">>),
    [1,2] = reduce_limit([1,2,3,4], <<"{\"limit\":2}">>),
    ?DEFAULT_REDUCE_LIMIT_SIZE = length(reduce_limit(lists:seq(1,200), none)).

-endif.
