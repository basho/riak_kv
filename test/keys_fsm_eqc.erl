%% -------------------------------------------------------------------
%%
%% keys_fsm_eqc: Quickcheck testing for the key listing fsm.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

-module(keys_fsm_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_kv_vnode.hrl").

-compile(export_all).
-define(DEFAULT_BUCKET_PROPS,
        [{allow_mult, false},
         {chash_keyfun, {riak_core_util, chash_std_keyfun}},
         {r, quorum},
         {pr, 0}]).
-define(TEST_ITERATIONS, 100).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-record(state,{bucket,
               object_count}).

%%====================================================================
%% eunit test
%%====================================================================

eqc_test_() ->
    {spawn,
     [{setup,
       fun setup/0,
       fun cleanup/1,
       [%% Run the quickcheck tests
        {timeout, 60000, % timeout is in msec
         ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_basic_listkeys()))))}
       ]
      }
     ]
    }.

setup() ->
    %% Shut logging up - too noisy.
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "keys_fsm_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "keys_fsm_eqc.log"}),

    %% Start erlang node
    {ok, _} = net_kernel:start([testnode, shortnames]),
    do_dep_apps(start, dep_apps()),

    %% Create and store some objects
    {ok, Client} = riak:local_client(),
    [Client:put(riak_object:new(<<"bucket1">>, list_to_binary(integer_to_list(X)), <<"val">>)) || X <- lists:seq(1, 10)],
    [Client:put(riak_object:new(<<"bucket2">>, list_to_binary(integer_to_list(X)), <<"val">>)) || X <- lists:seq(1, 100)],
    [Client:put(riak_object:new(<<"bucket3">>, list_to_binary(integer_to_list(X)), <<"val">>)) || X <- lists:seq(1, 1000)],
    ok.

cleanup(_) ->
    %% fsm_eqc_util:cleanup_mock_servers(),
    %% ok.
    do_dep_apps(stop, lists:reverse(dep_apps())),
    catch exit(whereis(riak_kv_vnode_master), kill), %% Leaks occasionally
    catch exit(whereis(riak_sysmon_filter), kill), %% Leaks occasionally
    net_kernel:stop(),
    %% Reset the riak_core vnode_modules
    application:set_env(riak_core, vnode_modules, []),
    ok.

%% Call unused callback functions to clear them in the coverage
%% checker so the real code stands out.
coverage_test() ->
    riak_kv_test_util:call_unused_fsm_funs(riak_kv_keys_fsm).

%% ====================================================================
%% eqc property
%% ====================================================================

prop_basic_listkeys() ->
    ?FORALL(Cmds, commands(?MODULE),
        ?TRAPEXIT(
           begin
               {_History, _State, Result} = run_commands(?MODULE, Cmds),
               aggregate(command_names(Cmds), Result == ok)
           end
          )).

%% ====================================================================
%% eqc_statem callbacks
%% ====================================================================

initial_state() ->
    #state{}.

command(_State) ->
    {call, ?MODULE, start_link, [g_reqid(), g_input(), g_timeout(), g_client_type()]}.

next_state(State, _Result, {call,_,_,_}) -> State.

precondition(_,_) ->
    true.

postcondition(_State, {call, _, start_link, [_, Input, _, _]}, Result) ->
    case Input of
        <<"bucket1">> ->
            ok == ?assertEqual(10, length(Result));
        <<"bucket2">> ->
            ok == ?assertEqual(100, length(Result));
        <<"bucket3">> ->
            ok == ?assertEqual(1000, length(Result))
    end;
postcondition(_,_,_) -> true.

%%====================================================================
%% Wrappers
%%====================================================================

start_link(ReqId, Input, Timeout, ClientType) ->
    Sink = spawn(?MODULE, data_sink, [ReqId, [], false]),
    From = {raw, ReqId, Sink},
    {ok, _FsmPid} = riak_kv_keys_fsm:start_link(From, Input, Timeout, ClientType),
    wait_for_replies(Sink, ReqId).

%%====================================================================
%% Generators
%%====================================================================

g_input() ->
    elements([<<"bucket1">>, <<"bucket2">>, <<"bucket3">>]).

g_reqid() ->
    ?LET(X, noshrink(largeint()), abs(X)).

g_timeout() ->
    choose(1000, 60000).

g_client_type() ->
    %% TODO: Incorporate mapred type
    plain.

%%====================================================================
%% Helpers
%%====================================================================

prepare() ->
    application:load(sasl),
    error_logger:delete_report_handler(sasl_report_tty_h),
    error_logger:delete_report_handler(error_logger_tty_h),

    {ok, _} = net_kernel:start([testnode, longnames]),
    do_dep_apps(start, dep_apps()),
    ok.

test() ->
    test(100).

test(N) ->
    quickcheck(numtests(N, prop_basic_listkeys())).

check() ->
    check(prop_basic_listkeys(), current_counterexample()).

dep_apps() ->
    SetupFun =
        fun(start) ->
            %% Set some missing env vars that are normally
            %% part of release packaging.
            application:set_env(riak_core, ring_creation_size, 64),
            application:set_env(riak_kv, storage_backend, riak_kv_ets_backend),
            %% Create a fresh ring for the test
            Ring = riak_core_ring:fresh(),
            riak_core_ring_manager:set_ring_global(Ring),

            %% Start riak_kv
            timer:sleep(500);
           (stop) ->
            ok
        end,
    XX = fun(_) -> error_logger:info_msg("Registered: ~w\n", [lists:sort(registered())]) end,
    [sasl, crypto, riak_sysmon, webmachine, XX, riak_core, XX, luke, erlang_js,
     mochiweb, os_mon, SetupFun, riak_kv].

do_dep_apps(StartStop, Apps) ->
    lists:map(fun(A) when is_atom(A) -> application:StartStop(A);
                 (F)                 -> F(StartStop)
              end, Apps).

data_sink(ReqId, KeyList, Done) ->
    receive
        {ReqId, {keys, Keys}} ->
            data_sink(ReqId, KeyList++Keys, false);
        {ReqId, done} ->
            data_sink(ReqId, KeyList, true);
        {ReqId, _Error} ->
            data_sink(ReqId, [], true);
        {keys, From, ReqId} ->
            From ! {ok, ReqId, KeyList};
        {'done?', From, ReqId} ->
            From ! {ok, ReqId, Done},
            data_sink(ReqId, KeyList, Done);
        Other ->
            ?debugFmt("Unexpected msg: ~p~n", [Other]),
            data_sink(ReqId, KeyList, Done)
    end.

wait_for_replies(Sink, ReqId) ->
    S = self(),
    Sink ! {'done?', S, ReqId},
    receive
        {ok, ReqId, true} ->
            Sink ! {keys, S, ReqId},
            receive
                {ok, ReqId, Keys} ->
                    Keys;
                {ok, ORef, _} ->
                    ?debugFmt("Received keys for older run: ~p~n", [ORef])
            end;
        {ok, ReqId, false} ->
            timer:sleep(100),
            wait_for_replies(Sink, ReqId);
        {ok, ORef, _} ->
            ?debugFmt("Received keys for older run: ~p~n", [ORef])
    end.

-endif. % EQC
