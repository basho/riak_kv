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
         mapred_stream/1
        ]).
-export([example/0, example_bucket/0, example_reduce/0,
         example_setup/0, example_setup/1]).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("riak_pipe/include/riak_pipe_log.hrl").

%% ignoring ResultTransformer option
%% TODO: Timeout
%% TODO: Streaming output
mapred(Inputs, Query) ->
    {ok, Head, Sink} = mapred_stream(Query),
    send_inputs(Head, Inputs),
    collect_outputs(Sink).

mapred_stream(Query) ->
    riak_pipe:exec(mr2pipe_phases(Query), []).

mr2pipe_phases(Query) ->
    Numbered = lists:zip(Query, lists:seq(0, length(Query)-1)),
    Fittings = lists:flatten([mr2pipe_phase(P,I) || {P,I} <- Numbered]),
    fix_final_fitting(Fittings).

mr2pipe_phase({map, FunSpec, Arg, Keep}, I) ->
    map2pipe(FunSpec, Arg, Keep, I);
mr2pipe_phase({reduce, FunSpec, Arg, Keep}, I) ->
    reduce2pipe(FunSpec, Arg, Keep, I);
mr2pipe_phase({link, Bucket, Tag, Keep}, I) ->
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

reduce2pipe(FunSpec, Arg, Keep, I) ->
    [#fitting_spec{name={xform_reduce,I},
                   module=riak_pipe_w_xform,
                   arg=fun reduce_add_key_compat/3,
                   chashfun=follow},
     #fitting_spec{name={reduce,I},
                   module=riak_pipe_w_reduce,
                   arg=reduce_compat(FunSpec, Arg),
                   chashfun=fun reduce_local_chashfun/1},
     #fitting_spec{name={xform_reduce,I},
                   module=riak_pipe_w_xform,
                   arg=fun reduce_remove_key_compat/3,
                   chashfun=follow}
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

reduce_add_key_compat(Input, Partition, FittingDetails) ->
    %% reduce fitting expects {Key, Input} form,
    %% but the key is bogus for our reduce
    riak_pipe_vnode_worker:send_output({1, Input}, Partition, FittingDetails).

reduce_remove_key_compat({_Key, Input}, Partition, FittingDetails) ->
    %% reduce fitting expects {Key, Input} form,
    %% but the key is bogus for our reduce
    riak_pipe_vnode_worker:send_output(Input, Partition, FittingDetails).

reduce_compat({modfun, Module, Function}, Arg) ->
    reduce_compat({qfun, erlang:make_fun(Module, Function, 2)}, Arg);
reduce_compat({qfun, Fun}, Arg) ->
    fun(_Key, Inputs, _Partition, _FittingDetails) ->
            ?T(_FittingDetails, [reduce], {reducing, length(Inputs)}),
            Output = Fun(Inputs, Arg),
            ?T(_FittingDetails, [reduce], {reduced, length(Output)}),
            {ok, Output}
    end.

reduce_local_chashfun(_) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_pipe_vnode:hash_for_partition(
      hd(riak_core_ring:my_indices(Ring))).

bkey_nval({Bucket, _Key}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    {n_val, NVal} = lists:keyfind(n_val, 1, BucketProps),
    NVal.

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

collect_outputs(Sink) ->
    {Result, Outputs, []} = riak_pipe:collect_results(Sink),
    %%TODO: Outputs needs post-processing?
    case Result of
        eoi ->
            %% normal result
            {ok, group_outputs(Outputs)};
        Other ->
            {error, {Other, Outputs}}
    end.

group_outputs(Outputs) ->
    Merged = lists:foldl(fun({I,O}, Acc) when is_list(O) ->
                                 dict:append_list(I, O, Acc);
                            ({I,O}, Acc) ->
                                 dict:append(I, O, Acc)
                         end,
                         dict:new(),
                         Outputs),
    [ O || {_, O} <- lists:keysort(1, dict:to_list(Merged)) ].

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
    [C:put(riak_object:new(<<"foo">>,
                           list_to_binary("bar"++integer_to_list(X)),
                           list_to_binary("bar val "++integer_to_list(X))))
     || X <- lists:seq(1, Num)],
    [C:put(riak_object:new(<<"foonum">>,
                           list_to_binary("bar"++integer_to_list(X)),
                           X)) ||
        X <- lists:seq(1, Num)],
    ok.
