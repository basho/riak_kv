%% Riak EnterpriseDS
%% Copyright 2007-2009 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl).
-author('Andy Gross <andy@basho.com>').
-include("riak_repl.hrl").
-export([start/0, stop/0]).
-export([install_hook/0, uninstall_hook/0]).
-export([fixup/2]).
-export([conditional_hook/3]).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

start() ->
    riak_core_util:start_app_deps(riak_repl),
    application:start(riak_repl).

%% @spec stop() -> ok
stop() -> 
    application:stop(riak_repl).

install_hook() ->
    riak_kv_hooks:add_conditional_postcommit({?MODULE, conditional_hook}),
    riak_core_bucket:append_bucket_defaults([{repl, true}]),
    ok.

uninstall_hook() ->
    riak_kv_hooks:del_conditional_postcommit({?MODULE, conditional_hook}),
    %% Cannot remove bucket defaults, best we can do is disable
    riak_core_bucket:append_bucket_defaults([{repl, false}]),
    ok.

conditional_hook(_BucketType, _Bucket, BucketProps) ->
    RTEnabled = app_helper:get_env(riak_repl, rtenabled, false),
    BucketEnabled = not lists:member({repl, false}, BucketProps),
    case RTEnabled and BucketEnabled of
        true ->
            riak_repl_util:get_hooks_for_modes();
        false ->
            false
    end.

fixup(_Bucket, BucketProps) ->
    CleanPostcommit = strip_postcommit(BucketProps),
    RTEnabled = app_helper:get_env(riak_repl, rtenabled, false),
    case proplists:get_value(repl, BucketProps) of
        Val when (Val==true orelse Val==realtime orelse Val==both),
                 RTEnabled == true  ->
            lager:debug("Hooks for repl modes = ~p", [riak_repl_util:get_hooks_for_modes()]),
            UpdPostcommit = CleanPostcommit ++ riak_repl_util:get_hooks_for_modes(),

            {ok, lists:keystore(postcommit, 1, BucketProps, 
                    {postcommit, UpdPostcommit})};
        _ ->
            %% Update the bucket properties
            UpdBucketProps = lists:keystore(postcommit, 1, BucketProps, 
                {postcommit, CleanPostcommit}),
            {ok, UpdBucketProps}
    end.

%% Get the postcommit hook from the bucket and strip any
%% existing repl hooks.
strip_postcommit(BucketProps) ->
    %% Get the current postcommit hook
    case proplists:get_value(postcommit, BucketProps, []) of
        X when is_list(X) ->
            CurrentPostcommit=X;
        {struct, _}=X ->
            CurrentPostcommit=[X]
    end,
    %% Add repl hook - make sure there are not duplicate entries
    AllHooks = [Hook || {_Mode, Hook} <- ?REPL_MODES],
    lists:filter(fun(H) -> not lists:member(H, AllHooks) end, CurrentPostcommit).


-ifdef(TEST).

-define(MY_HOOK1, {struct,
                    [{<<"mod">>, <<"mymod1">>},
                     {<<"fun">>, <<"myhook1">>}]}).
-define(MY_HOOK2, {struct,
                    [{<<"mod">>, <<"mymod2">>},
                     {<<"fun">>, <<"myhook2">>}]}).
strip_postcommit_test_() ->
    [?_assertEqual(
        [], % remember, returns the stipped postcommit from bprops
        lists:sort(strip_postcommit([{blah, blah}, {postcommit, []}]))),
     ?_assertEqual(
        [?MY_HOOK1, ?MY_HOOK2],
        lists:sort(strip_postcommit([{postcommit, [?REPL_HOOK_BNW,
                                                   ?MY_HOOK1,
                                                   ?REPL_HOOK_BNW,
                                                   ?MY_HOOK2,
                                                   ?REPL_HOOK12]},
                                     {blah, blah}])))].



-endif.

