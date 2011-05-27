%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
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

%% Riak KV MapReduce <-> Riak Pipe Compatibility
-module(riak_kv_mrc_pipe).

-export([
         mapred/2,
         mapred_stream/1,
         mapred_plan/1,
         mapred_plan/2
        ]).
%% NOTE: Example functions are used by EUnit tests
-export([example/0, example_bucket/0, example_reduce/0,
         example_setup/0, example_setup/1]).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("riak_pipe/include/riak_pipe_log.hrl").

%% ignoring ResultTransformer option
%% TODO: Timeout
%% TODO: Streaming output
mapred(Inputs, Query) ->
    {{ok, Head, Sink}, NumKeeps} = mapred_stream(Query),
    send_inputs(Head, Inputs),
    collect_outputs(Sink, NumKeeps).

mapred_stream(Query0) ->
    Query = keep_ify_query(Query0),
    NumKeeps = count_keeps_in_query(Query),
    {riak_pipe:exec(mr2pipe_phases(Query), []), NumKeeps}.

mapred_plan(Query) ->
    mr2pipe_phases(Query).

mapred_plan(BucketOrList, Query) ->
    BKeys = if is_list(BucketOrList) ->
                    BucketOrList;
               is_binary(BucketOrList) ->
                    {ok, C} = riak:local_client(),
                    {ok, Keys} = C:list_keys(BucketOrList),
                    [{BucketOrList, Key} || Key <- Keys]
            end,
    [{bkeys, BKeys}|mapred_plan(Query)].

mr2pipe_phases([]) ->
    [#fitting_spec{name=empty_pass,
                   module=riak_pipe_w_pass,
                   chashfun=follow}];
mr2pipe_phases(Query) ->
    Now = now(),
    QueryType = list_to_tuple([Type || {Type, _, _, _} <- Query]),
    Numbered = lists:zip(Query, lists:seq(0, length(Query)-1)),
    Fittings0 = lists:flatten([mr2pipe_phase(P,I,Now,QueryType) ||
                                  {P,I} <- Numbered]),
    Fs = fix_final_fitting(Fittings0),
    case lists:last(Query) of
        {_, _, _, false} ->
            %% The default action is to send results down to the next
            %% fitting in the pipe.  However, the last MapReduce query
            %% doesn't want those results.  So, add a "black hole"
            %% fitting that will stop all work items from getting to
            %% the sink and thus polluting our expected results.
            Fs ++ [#fitting_spec{name=black_hole,
                                 module=riak_pipe_w_pass,
                                 arg=black_hole,
                                 chashfun=follow}];
        _ ->
            Fs
    end.

mr2pipe_phase({map, FunSpec, Arg, Keep}, I, _ConstHashCookie, _QueryType) ->
    map2pipe(FunSpec, Arg, Keep, I);
mr2pipe_phase({reduce, FunSpec, Arg, Keep}, I, ConstHashCookie, QueryType) ->
    reduce2pipe(FunSpec, Arg, Keep, I, ConstHashCookie, QueryType);
mr2pipe_phase({link, Bucket, Tag, Keep}, I, _ConstHashCookie, _QueryType) ->
    link2pipe(Bucket, Tag, Keep, I).

map2pipe(FunSpec, Arg, Keep, I) ->
    [#fitting_spec{name={kvget_map,I},
                   module=riak_kv_pipe_get,
                   %% TODO: perform bucket prop 'chash_keyfun' lookup at
                   %% spec-translation time, not at each input send
                   chashfun=fun riak_core_util:chash_key/1,
                   nval=fun bkey_nval/1},
     #fitting_spec{name={xform_map,I},
                   module=riak_pipe_w_xform,
                   arg=map_xform_compat(FunSpec, Arg),
                   chashfun=follow}
     |[#fitting_spec{name=I,
                     module=riak_pipe_w_tee,
                     arg=sink,
                     chashfun=follow}
       ||Keep]].

reduce2pipe(FunSpec, Arg, Keep, I, ConstHashCookie, QueryType) ->
    PreviousIsReduceP = I > 0 andalso element(I, QueryType) == reduce,
    ConstantFun = fun(_) -> chash:key_of(ConstHashCookie) end,
    [#fitting_spec{name={reduce,I},
                   module=riak_kv_w_mapred,
                   arg=reduce_compat(FunSpec, Arg, PreviousIsReduceP),
                   chashfun=ConstantFun}
     |[#fitting_spec{name=I,
                     module=riak_pipe_w_tee,
                     arg=sink,
                     chashfun=follow}
       ||Keep]].

link2pipe(Bucket, Tag, Keep, I) ->
    throw({todo, get, Bucket, get_link, make_map, Tag, map2pipe, Keep, I}).

fix_final_fitting(Fittings) ->
    case lists:reverse(Fittings) of
        [#fitting_spec{module=riak_pipe_w_tee,
                       name=Int},
         #fitting_spec{}=RealFinal|Rest]
          when is_integer(Int) ->
            %% chop off tee so we don't get double answers
            lists:reverse([RealFinal#fitting_spec{name=Int}|Rest]);
        [#fitting_spec{name={_Type,Int}}=Final|Rest]
          when is_integer(Int) ->
            %% fix final name so outputs look like old API
            lists:reverse([Final#fitting_spec{name=Int}|Rest])
    end.

%% TODO: JavaScript, strfun, KV-stored funs
map_xform_compat({modfun, Module, Function}, Arg) ->
    map_xform_compat({qfun, erlang:make_fun(Module, Function, 3)}, Arg);
map_xform_compat({qfun, Fun}, Arg) ->
    fun(Input, Partition, FittingDetails) ->
            ?T(FittingDetails, [map], {mapping, Input}),
            %%TODO: keydata
            Results = Fun(Input, undefined, Arg),
            ?T(FittingDetails, [map], {produced, Results}),
            [ riak_pipe_vnode_worker:send_output(R, Partition, FittingDetails)
              || R <- Results ],
            ok
    end.

reduce_compat({modfun, Module, Function}, Arg, PreviousIsReduceP) ->
    reduce_compat({qfun, erlang:make_fun(Module, Function, 2)}, Arg,
                  PreviousIsReduceP);
reduce_compat({qfun, Fun}, Arg, PreviousIsReduceP) ->
    fun(_Key, Inputs0, _Partition, _FittingDetails) ->
            %% Concatenate reduce output lists, if previous stage was reduce
            Inputs = if PreviousIsReduceP ->
                             lists:append(Inputs0);
                        true ->
                             Inputs0
                     end,
            ?T(_FittingDetails, [reduce], {reducing, length(Inputs)}),
            Output = Fun(Inputs, Arg),
            ?T(_FittingDetails, [reduce], {reduced, length(Output)}),
            {ok, Output}
    end.

%% chashfun for 0 or 1 reducer per node.
%% reduce_local_chashfun(_) ->
%%     {ok, Ring} = riak_core_ring_manager:get_my_ring(),
%%     riak_pipe_vnode:hash_for_partition(
%%       hd(riak_core_ring:my_indices(Ring))).

bkey_nval({Bucket, _Key}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    {n_val, NVal} = lists:keyfind(n_val, 1, BucketProps),
    NVal.

keep_ify_query([]) ->
    [];
keep_ify_query(Query) ->
    case lists:all(fun({_, _, _, false}) -> true;
                      (_)                -> false
                   end, Query) of
        true ->
            {AllBut, [{A, B, C, _}]} = lists:split(length(Query) - 1, Query),
            AllBut ++ [{A, B, C, true}];
        false ->
            Query
    end.

count_keeps_in_query(Query) ->
    lists:foldl(fun({_, _, _, true}, Acc) -> Acc + 1;
                   (_, Acc)                 -> Acc
                end, 0, Query).

%% TODO: dynamic inputs, filters
send_inputs(Fitting, BucketKeyList) when is_list(BucketKeyList) ->
    [riak_pipe_vnode:queue_work(Fitting, BKey)
     || BKey <- BucketKeyList],
    riak_pipe_fitting:eoi(Fitting),
    ok;
send_inputs(Fitting, Bucket) when is_binary(Bucket) ->
    %% TODO: riak_kv_listkeys_pipe
    {ok, C} = riak:local_client(),
    {ok, ReqId} = C:stream_list_keys(Bucket),
    send_key_list(Fitting, Bucket, ReqId).

send_key_list(Fitting, Bucket, ReqId) ->
    receive
        {ReqId, {keys, Keys}} ->
            [riak_pipe_vnode:queue_work(Fitting, {Bucket, Key})
             || Key <- Keys],
            send_key_list(Fitting, Bucket, ReqId);
        {ReqId, done} ->
            riak_pipe_fitting:eoi(Fitting),
            ok
    end.

collect_outputs(Sink, NumKeeps) ->
    {Result, Outputs, []} = riak_pipe:collect_results(Sink),
    %%TODO: Outputs needs post-processing?
    case Result of
        eoi ->
            %% normal result
            {ok, group_outputs(Outputs, NumKeeps)};
        Other ->
            {error, {Other, Outputs}}
    end.

group_outputs(Outputs, NumKeeps) ->
    Merged = lists:foldl(fun({I,O}, Acc) when is_list(O) ->
                                 dict:append_list(I, O, Acc);
                            ({I,O}, Acc) ->
                                 dict:append(I, O, Acc)
                         end,
                         dict:new(),
                         Outputs),
    if NumKeeps < 2 ->                          % 0 or 1
            [{_, O}] = dict:to_list(Merged),
            O;
       true ->
            [ O || {_, O} <- lists:keysort(1, dict:to_list(Merged)) ]
    end.

%%%

example() ->
    mapred([{<<"foo">>, <<"bar">>}],
           [{map, {modfun, riak_kv_mapreduce, map_object_value},
             none, true}]).

example_bucket() ->
    mapred(<<"foo">>,
           [{map, {modfun, riak_kv_mapreduce, map_object_value},
             none, true}]).

example_reduce() ->
    mapred(<<"foonum">>,
           [{map, {modfun, riak_kv_mapreduce, map_object_value},
             none, true},
            {reduce, {qfun, fun(Inputs, _) -> [lists:sum(Inputs)] end},
             none, true}]).

example_setup() ->
    example_setup(5).

example_setup(Num) when Num > 0 ->
    {ok, C} = riak:local_client(),
    C:put(riak_object:new(<<"foo">>, <<"bar">>, <<"what did you expect?">>)),
    [C:put(riak_object:new(<<"foo">>,
                           list_to_binary("bar"++integer_to_list(X)),
                           list_to_binary("bar val "++integer_to_list(X))))
     || X <- lists:seq(1, Num)],
    [C:put(riak_object:new(<<"foonum">>,
                           list_to_binary("bar"++integer_to_list(X)),
                           X)) ||
        X <- lists:seq(1, Num)],
    ok.
