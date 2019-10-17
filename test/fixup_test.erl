%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(fixup_test).

-include_lib("eunit/include/eunit.hrl").
-include("riak_repl.hrl").

-define(REPL_HOOK, [?REPL_HOOK12, ?REPL_HOOK_BNW]).

fixup_test_() ->
    {foreach,
        fun() ->
                application:load(riak_core),
                application:set_env(riak_core, bucket_fixups, [{riak_repl,
                            riak_repl}]),
                application:set_env(riak_repl, rtenabled, true),
                riak_core_bucket:append_bucket_defaults([{postcommit, []}]),
                RingEvtPid = maybe_start_link(riak_core_ring_events:start_link()),
                RingMgrPid = maybe_start_link(riak_core_ring_manager:start_link(test)),
                {RingEvtPid, RingMgrPid}

        end,
        fun({RingEvtPid, RingMgrPid}) ->
                stop_pid(RingMgrPid),
                stop_pid(RingEvtPid),
                application:unset_env(riak_core, bucket_fixups),
                application:unset_env(riak_core, default_bucket_props),
                application:unset_env(riak_repl, rtenabled)
        end,
        [
            fun simple/0,
            fun preexisting_repl_hook/0,
            fun other_postcommit_hook/0,
            fun blank_bucket/0,
            fun inherit_from_default_bucket/0
        ]
    }.

maybe_start_link({ok, Pid}) -> 
    Pid;
maybe_start_link({error, {already_started, _}}) ->
    undefined.

stop_pid(undefined) ->
    ok;
stop_pid(Pid) ->
    unlink(Pid),
    exit(Pid, kill).

simple() ->
    Props = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([],
        proplists:get_value(postcommit, Props)),
    riak_core_bucket:set_bucket("testbucket", [{repl, true}]),
    Props2 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual(?REPL_HOOK,
        proplists:get_value(postcommit, Props2)),
    riak_core_bucket:set_bucket("testbucket", [{repl, false}]),
    Props3 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([],
        proplists:get_value(postcommit, Props3)),
    ok.

preexisting_repl_hook() ->
    riak_core_bucket:set_bucket("testbucket", [{postcommit,
                ?REPL_HOOK}]),
    Props = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([],
        proplists:get_value(postcommit, Props)),
    riak_core_bucket:set_bucket("testbucket", [{repl, true}]),
    Props2 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual(?REPL_HOOK,
        proplists:get_value(postcommit, Props2)),
    ok.

other_postcommit_hook() ->
    riak_core_bucket:set_bucket("testbucket", [{postcommit,
                [my_postcommit_def()]}]),
    Props = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([my_postcommit_def()],
        proplists:get_value(postcommit, Props)),
    riak_core_bucket:set_bucket("testbucket", [{repl, true}]),
    Props2 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([my_postcommit_def() | ?REPL_HOOK],
        proplists:get_value(postcommit, Props2)),
    riak_core_bucket:set_bucket("testbucket", [{repl, false}]),
    Props3= riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([my_postcommit_def()],
        proplists:get_value(postcommit, Props3)),
    ok.

blank_bucket() ->
    application:set_env(riak_core, default_bucket_props, []),
    Props = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual(undefined,
        proplists:get_value(postcommit, Props)),
    riak_core_bucket:set_bucket("testbucket", [{repl, true}]),
    Props2 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual(?REPL_HOOK,
        proplists:get_value(postcommit, Props2)),
    riak_core_bucket:set_bucket("testbucket", [{repl, false}]),
    Props3 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual([],
        proplists:get_value(postcommit, Props3)),
    ok.

inherit_from_default_bucket() ->
    riak_core_bucket:set_bucket("testbucket", [{foo, bar}]),
    Props = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual(undefined,
        proplists:get_value(repl, Props)),
    riak_kv_hooks:create_table(),
    riak_repl:install_hook(),
    Props2 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual(true,
        proplists:get_value(repl, Props2)),
    ?assertEqual(?REPL_HOOK,
        proplists:get_value(postcommit, Props2)),
    riak_core_bucket:set_bucket("testbucket", [{repl, false}]),
    Props3 = riak_core_bucket:get_bucket("testbucket"),
    ?assertEqual(false,
        proplists:get_value(repl, Props3)),
    ?assertEqual([],
        proplists:get_value(postcommit, Props3)),
    Props4 = riak_core_bucket:get_bucket("noncustombucket"),
    ?assertEqual(true,
        proplists:get_value(repl, Props4)),
    ?assertEqual(?REPL_HOOK,
        proplists:get_value(postcommit, Props4)),
    ok.

my_postcommit_def() ->
    {struct, [{<<"mod">>,atom_to_binary(?MODULE, latin1)},
              {<<"fun">>,<<"postcommit">>}]}.
