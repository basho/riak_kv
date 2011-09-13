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

%% @doc Riak KV MapReduce / Riak Pipe Compatibility
%%
%% == About using `{modfun, Mod, Fun, Arg}' generator to a MapReduce job
%%
%% An uncommonly-used option for Riak KV MapReduce is the option to
%% use a fourth method of specifying inputs to the beginning of the
%% MapReduce workflow.  All four methods are:
%%
%% <ol>
%% <li> Specify a bucket name (to emit all bucket/key pairs for that
%%  bucket) </li>
%% <li> Specify a bucket name and keyfilter spec, `{Bucket, KeyFilter}' </li>
%% <li> Specify an explicit list of bucket/key pairs </li>
%% <li> Specify `{modfun, Mod, Fun, Arg}' to generate the raw input data
%% for the rest of the workflow </li>
%% </ol>
%%
%% For the fourth method, "raw input data" means that the output of the
%% function will be used as-is by the next item MapReduce workflow.
%% If that next item is a map phase, then that item's input is
%% expected to be a bucket/key pair.  If the next item is a reduce
%% phase, then the input can be an arbitrary term.
%%
%% The type specification for a `{modfun, Mod, Fun, Arg}' generator
%% function is:
%% ```
%% -spec generator_func(Pipe::riak_pipe:pipe(), Arg::term(), Timeout::integer() | 'infinity').
%% '''
%%
%% This generator function is responsible for using
%% `riak_pipe:queue_work()' to send any data to the pipe, and it is
%% responsible for calling `riak_pipe:eoi()' to signal the end of
%% input.
%%
%% == About reduce phase compatibility ==
%%
%% An Erlang reduce phase is defined by the tuple:
%% `{reduce, Fun::function(2), Arg::term(), Keep::boolean()}'.
%%
%% <ul>
%% <li> `Fun' takes the form of `Fun(InputList, Arg)' where `Arg' is
%% the argument specified in the definition 4-tuple above.
%% NOTE: Unlike a fold function (e.g., `lists:foldl/3'), the `Arg' argument
%%       is constant for each iteration of the reduce function. </li>
%% <li> The `Arg' may be any term, as the caller sees fit.  However, if
%%      the caller wishes to have more control over the reduce phase,
%%      then `Arg' must be a property list.  The control knobs that may
%%      be specified are:
%%      <ul>
%%      <li> `reduce_phase_only_1' will buffer all inputs to the reduce
%%           phase fitting and only call the reduce function once.
%%           NOTE: Use with caution to avoid excessive memory use. </li>
%%      <li> `{reduce_phase_batch_size, Max::integer()}' will buffer all
%%           inputs to the reduce phase fitting and call the reduce function
%%           after `Max' items have been buffered. </li>
%%      </ul>
%% If neither `reduce_phase_only_1' nor `{reduce_phase_batch_size, Max}'
%% are present, then the batching size will default to the value of the
%% application environment variable
%% `mapred_reduce_phase_batch_size' in the `riak_kv' application.
%%
%% NOTE: This mixing of user argument data and MapReduce implementation
%%       metadata is suboptimal, but to do separate the two types of
%%       data would require a change that is incompatible with the current
%%       Erlang MapReduce input specification, e.g., a 5-tuple such as
%%       `{reduce, Fun, Arg, Keep, MetaData}' or else a custom wrapper
%%       around the 3rd arg,
%%       e.g. `{reduce, Fun, {magic_tag, Arg, Metadata}, Keep}'.
%% </li>
%% <li> If `Keep' is `true', then the output of this phase will be returned
%%      to the caller (i.e. the output will be "kept"). </li>
%% </ul>

-module(riak_kv_mrc_pipe).

%% TODO: Stolen from old-style MapReduce interface, but is 60s a good idea?
-define(DEFAULT_TIMEOUT, 60000).

-export([
         mapred/2,
         mapred/3,
         mapred_stream/1,
         send_inputs/2,
         send_inputs/3,
         send_inputs_async/2,
         send_inputs_async/3,
         collect_outputs/2,
         collect_outputs/3,
         group_outputs/2,
         mapred_plan/1,
         mapred_plan/2,
         compile_string/1
        ]).
%% NOTE: Example functions are used by EUnit tests
-export([example/0, example_bucket/0, example_reduce/0,
         example_setup/0, example_setup/1]).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("riak_pipe/include/riak_pipe_log.hrl").

%% ignoring ResultTransformer option
%% TODO: Streaming output

mapred(Inputs, Query) ->
    mapred(Inputs, Query, ?DEFAULT_TIMEOUT).

mapred(Inputs, Query, Timeout) ->
    {{ok, Pipe}, NumKeeps} = mapred_stream(Query),
    case send_inputs(Pipe, Inputs, Timeout) of
        ok ->
            collect_outputs(Pipe, NumKeeps, Timeout);
        Error ->
            riak_pipe:eoi(Pipe),
            {error, Error, collect_outputs(Pipe, NumKeeps, Timeout)}
    end.

mapred_stream(Query) ->
    NumKeeps = count_keeps_in_query(Query),
    {riak_pipe:exec(mr2pipe_phases(Query), [{log, sink},{trace,[error]}]),
     NumKeeps}.

%% The plan functions are useful for seeing equivalent (we hope) pipeline.

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
    QueryT = list_to_tuple(Query),
    Numbered = lists:zip(Query, lists:seq(0, length(Query)-1)),
    Fittings0 = lists:flatten([mr2pipe_phase(P,I,Now,QueryT) ||
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

mr2pipe_phase({map,FunSpec,Arg,Keep}, I, _ConstHashCookie, QueryT) ->
    map2pipe(FunSpec, Arg, Keep, I, QueryT);
mr2pipe_phase({reduce,FunSpec,Arg,Keep}, I, ConstHashCookie, _QueryT) ->
    reduce2pipe(FunSpec, Arg, Keep, I, ConstHashCookie);
mr2pipe_phase({link,Bucket,Tag,Keep}, I, _ConstHashCookie, QueryT)->
    link2pipe(Bucket, Tag, Keep, I, QueryT).

%% Prereduce logic: add pre_reduce fittings to the pipe line if
%% the current item is a map (if you're calling this func, yes it is)
%% and if the next item in the query is a reduce and if the map's arg
%% or system config wants us to use prereduce.
%% Remember: `I` starts counting at 0, but the element BIF starts at 1,
%% so the element of the next item is I+2.

map2pipe(FunSpec, Arg, Keep, I, QueryT) ->
    PrereduceP = I+2 =< size(QueryT) andalso
        query_type(I+2, QueryT) == reduce andalso
        want_prereduce_p(I+1, QueryT),
    [#fitting_spec{name={kvget_map,I},
                   module=riak_kv_pipe_get,
                   chashfun=fun bkey_chash/1,
                   nval=fun bkey_nval/1},
     #fitting_spec{name={xform_map,I},
                   module=riak_kv_mrc_map,
                   arg={FunSpec, Arg},
                   chashfun=follow}]
     ++
     [#fitting_spec{name=I,
                    module=riak_pipe_w_tee,
                    arg=sink,
                    chashfun=follow} || Keep]
     ++
     if PrereduceP ->
             {reduce, R_FunSpec, _R_Arg, _Keep} = element(I+2, QueryT),
             [#fitting_spec{name={prereduce,I},
                            module=riak_kv_w_reduce,
                            arg={rct,
                                 riak_kv_w_reduce:reduce_compat(R_FunSpec),
                                 Arg},
                            chashfun=follow}];
        true ->
             []
     end.              

want_prereduce_p(Idx, QueryT) ->
    {map, _FuncSpec, Arg, _Keep} = element(Idx, QueryT),
    Props = case Arg of
                L when is_list(L) -> L;         % May or may not be a proplist
                _                 -> []
            end,
    AppDefault = app_helper:get_env(riak_kv, mapred_always_prereduce, false),
    proplists:get_value(do_prereduce, Props, AppDefault).

query_type(Idx, QueryT) ->
    element(1, element(Idx, QueryT)).

query_arg(Idx, QueryT) ->
    element(3, element(Idx, QueryT)).

reduce2pipe(FunSpec, Arg, Keep, I, ConstHashCookie) ->
    Hash = chash:key_of(ConstHashCookie),
    ConstantFun = fun(_) -> Hash end,
    [#fitting_spec{name={reduce,I},
                   module=riak_kv_w_reduce,
                   arg={rct,
                        riak_kv_w_reduce:reduce_compat(FunSpec),
                        Arg},
                   chashfun=ConstantFun}
     |[#fitting_spec{name=I,
                     module=riak_pipe_w_tee,
                     arg=sink,
                     chashfun=follow}
       ||Keep]].

link2pipe(Bucket, Tag, Keep, I, QueryT) ->
    Arg = query_arg(I+1, QueryT),
    [#fitting_spec{name={kvget_map,I},
                   module=riak_kv_pipe_get,
                   chashfun=fun bkey_chash/1,
                   nval=fun bkey_nval/1},
     #fitting_spec{name={xform_map,I},
                   module=riak_pipe_w_xform,
                   arg=link_xform_compat(Bucket, Tag, Arg),
                   chashfun=follow}|
     [#fitting_spec{name=I,
                    module=riak_pipe_w_tee,
                    arg=sink,
                    chashfun=follow} || Keep]].

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

link_xform_compat(Bucket, Tag, _Arg) ->
    fun({ok, Input, _Keydata}, Partition, FittingDetails) ->
            ?T(FittingDetails, [map], {mapping, Input}),
            LinkFun = bucket_linkfun(Bucket),
            Results = LinkFun(Input, none, {Bucket, Tag}),
            ?T(FittingDetails, [map], {produced, Results}),
            [ riak_pipe_vnode_worker:send_output(R, Partition,
                                                 FittingDetails)
              || R <- Results ],
            ok;
       ({{error,_},_,_}, _Partition, _FittingDetails) ->
            ok
    end.

bkey_chash(Input) ->
    riak_core_util:chash_key(riak_kv_pipe_get:bkey(Input)).

bkey_nval(Input) ->
    {Bucket,_} = riak_kv_pipe_get:bkey(Input),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    {n_val, NVal} = lists:keyfind(n_val, 1, BucketProps),
    NVal.

bucket_linkfun(Bucket) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    {_, {modfun, Module, Function}} = lists:keyfind(linkfun, 1, BucketProps),
    erlang:make_fun(Module, Function, 3).

count_keeps_in_query(Query) ->
    lists:foldl(fun({_, _, _, true}, Acc) -> Acc + 1;
                   (_, Acc)                 -> Acc
                end, 0, Query).

%% TODO: dynamic inputs, filters
send_inputs_async(Pipe, Arg) ->
    send_inputs_async(Pipe, Arg, ?DEFAULT_TIMEOUT).

send_inputs_async(Pipe, Arg, Timeout) ->
    spawn_monitor(
      fun() ->
              %% tear this process down if the pipeline goes away;
              %% also automatically tears down the pipeline if feeding
              %% it inputs fails (which is what the users of this
              %% function, riak_kv_pb_socket and riak_kv_wm_mapred, want)
              erlang:link(Pipe#pipe.builder),
              case send_inputs(Pipe, Arg, Timeout) of
                  ok ->
                      %% monitoring process sees a 'normal' exit
                      %% (and linked builder is left alone)
                      ok;
                  Error ->
                      %% monitoring process sees an 'error' exit
                      %% (and linked builder dies)
                      exit(Error)
              end
      end).

send_inputs(Pipe, Arg) ->
    send_inputs(Pipe, Arg, ?DEFAULT_TIMEOUT).

send_inputs(Pipe, BucketKeyList, _Timeout) when is_list(BucketKeyList) ->
    [riak_pipe:queue_work(Pipe, BKey)
     || BKey <- BucketKeyList],
    riak_pipe:eoi(Pipe),
    ok;
send_inputs(Pipe, Bucket, Timeout) when is_binary(Bucket) ->
    riak_kv_pipe_listkeys:queue_existing_pipe(Pipe, Bucket, Timeout);
send_inputs(Pipe, {Bucket, FilterExprs}, Timeout) ->
    case riak_kv_mapred_filters:build_filter(FilterExprs) of
        {ok, Filters} ->
            riak_kv_pipe_listkeys:queue_existing_pipe(
              Pipe, {Bucket, Filters}, Timeout);
        Error ->
            Error
    end;
send_inputs(Pipe, {index, Bucket, Index, Key}, Timeout) ->
    Query = {eq, Index, Key},
    NewInput = {modfun, riak_index, mapred_index, [Bucket, Query]},
    send_inputs(Pipe, NewInput, Timeout);
send_inputs(Pipe, {index, Bucket, Index, StartKey, EndKey}, Timeout) ->
    Query = {range, Index, StartKey, EndKey},
    NewInput = {modfun, riak_index, mapred_index, [Bucket, Query]},
    send_inputs(Pipe, NewInput, Timeout);
send_inputs(Pipe, {search, Bucket, Query}, Timeout) ->
    NewInput = {modfun, riak_search, mapred_search, [Bucket, Query, []]},
    send_inputs(Pipe, NewInput, Timeout);
send_inputs(Pipe, {search, Bucket, Query, Filter}, Timeout) ->
    NewInput = {modfun, riak_search, mapred_search, [Bucket, Query, Filter]},
    send_inputs(Pipe, NewInput, Timeout);
send_inputs(Pipe, {modfun, Mod, Fun, Arg} = Modfun, Timeout) ->
    try Mod:Fun(Pipe, Arg, Timeout) of
        {ok, Bucket, ReqId} ->
            send_key_list(Pipe, Bucket, ReqId);
        Other ->
            Other
    catch
        X:Y ->
            {Modfun, X, Y, erlang:get_stacktrace()}
    end.

send_key_list(Pipe, Bucket, ReqId) ->
    receive
        {ReqId, {keys, Keys}} ->
            %% Get results from list keys operation.
            [riak_pipe:queue_work(Pipe, {Bucket, Key})
             || Key <- Keys],
            send_key_list(Pipe, Bucket, ReqId);

        {ReqId, {results, Results}} ->
            %% Get results from 2i operation. Handle both [Keys] and [{Key,
            %% Props}] formats. If props exists, use it as keydata.
            F = fun
                    ({Key, Props}) ->
                        riak_pipe:queue_work(Pipe, {{Bucket, Key}, Props});
                    (Key) ->
                        riak_pipe:queue_work(Pipe, {Bucket, Key})
                end,
            [F(X) || X <- Results],
            send_key_list(Pipe, Bucket, ReqId);

        {ReqId, done} ->
            %% Operation has finished.
            riak_pipe:eoi(Pipe),
            ok
    end.

collect_outputs(Pipe, NumKeeps) ->
    collect_outputs(Pipe, NumKeeps, ?DEFAULT_TIMEOUT).

collect_outputs(Pipe, NumKeeps, Timeout) ->
    {Result, Outputs, []} = riak_pipe:collect_results(Pipe, Timeout),
    %%TODO: Outputs needs post-processing?
    case Result of
        eoi ->
            %% normal result
            {ok, group_outputs(Outputs, NumKeeps)};
        Other ->
            {error, {Other, Outputs}}
    end.

group_outputs(Outputs, NumKeeps) ->
    Merged = lists:foldl(fun({I,O}, Acc) ->
                                 dict:append(I, O, Acc)
                         end,
                         dict:new(),
                         Outputs),
    if NumKeeps < 2 ->                          % 0 or 1
            case dict:to_list(Merged) of
                [{_, O}] ->
                    O;
                [] ->
                    %% Shouldn't ever happen unless an error happened elsewhere
                    []
            end;
       true ->
            [ O || {_, O} <- lists:keysort(1, dict:to_list(Merged)) ]
    end.

compile_string(Binary) when is_binary(Binary) ->
    compile_string(binary_to_list(Binary));
compile_string(String) when is_list(String) ->
    try
        {ok, Tokens, _} = erl_scan:string(String),
        {ok, [Form]} = erl_parse:parse_exprs(Tokens),
        {value, Value, _} = erl_eval:expr(Form, erl_eval:new_bindings()),
        {ok, Value}
    catch Type:Error ->
            {Type, Error}
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
