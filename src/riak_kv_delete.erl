%% -------------------------------------------------------------------
%%
%% riak_delete: two-step object deletion
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

%% @doc two-step object deletion

-module(riak_kv_delete).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include("riak_kv_wm_raw.hrl").

-export([start_link/6, start_link/7, start_link/8, delete/8]).

start_link(ReqId, Bucket, Key, Options, Timeout, Client) ->
    {ok, proc_lib:spawn_link(?MODULE, delete, [ReqId, Bucket, Key,
                                               Options, Timeout, Client, undefined,
                                               undefined])}.

start_link(ReqId, Bucket, Key, Options, Timeout, Client, ClientId) ->
    {ok, proc_lib:spawn_link(?MODULE, delete, [ReqId, Bucket, Key,
                                               Options, Timeout, Client, ClientId,
                                               undefined])}.

start_link(ReqId, Bucket, Key, Options, Timeout, Client, ClientId, VClock) ->
    {ok, proc_lib:spawn_link(?MODULE, delete, [ReqId, Bucket, Key,
                                               Options, Timeout, Client, ClientId,
                                               VClock])}.

%% @spec delete(ReqId :: binary(), riak_object:bucket(), riak_object:key(),
%%             RW :: integer(), TimeoutMillisecs :: integer(), Client :: pid())
%%           -> term()
%% @doc Delete the object at Bucket/Key.  Direct return value is uninteresting,
%%      see riak_client:delete/3 for expected gen_server replies to Client.
delete(ReqId,Bucket,Key,Options,Timeout,Client,ClientId,undefined) ->
    case get_r_options(Bucket, Options) of
        {error, Reason} ->
            Client ! {ReqId, {error, Reason}};
        {R, PR} ->
            RealStartTime = riak_core_util:moment(),
            {ok, C} = riak:local_client(),
            case C:get(Bucket,Key,[{r,R},{pr,PR},{timeout,Timeout}]) of
                {ok, OrigObj} ->
                    RemainingTime = Timeout - (riak_core_util:moment() - RealStartTime),
                    delete(ReqId,Bucket,Key,Options,RemainingTime,Client,ClientId,riak_object:vclock(OrigObj));
                {error, notfound} ->
                    Client ! {ReqId, {error, notfound}};
                X ->
                    Client ! {ReqId, X}
            end
    end;
delete(ReqId,Bucket,Key,Options,Timeout,Client,ClientId,VClock) ->
    case get_w_options(Bucket, Options) of
        {error, Reason} ->
            Client ! {ReqId, {error, Reason}};
        {W, PW, DW} ->
            Obj0 = riak_object:new(Bucket, Key, <<>>, dict:store(?MD_DELETED,
                                                                 "true", dict:new())),
            Tombstone = riak_object:set_vclock(Obj0, VClock),
            {ok,C} = riak:local_client(ClientId),
            Reply = C:put(Tombstone, [{w,W},{pw,PW},{dw, DW},{timeout,Timeout}]),
            Client ! {ReqId, Reply},
            case Reply of
                ok ->
                    {ok, C2} = riak:local_client(),
                    AsyncTimeout = 60*1000,     % Avoid client-specified value
                    C2:get(Bucket, Key, all, AsyncTimeout);
                _ -> nop
            end
    end.

get_r_options(Bucket, Options) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    N = proplists:get_value(n_val,BucketProps),
    %% specifying R/W AND RW together doesn't make sense, so check if R or W
    %is defined first. If not, use RW or default.
    R = case proplists:is_defined(r, Options) orelse proplists:is_defined(w, Options) of
        true ->
            HasRW = false,
            R0 = proplists:get_value(r, Options, default),
            R1 = riak_kv_util:expand_rw_value(r, R0, BucketProps, N),
            R1;
        false ->
            HasRW = true,
            RW0 = proplists:get_value(rw, Options, default),
            RW = riak_kv_util:expand_rw_value(rw, RW0, BucketProps, N),
            RW
    end,
    %% check for errors
    case {R, HasRW} of
        {error, false} ->
            {error, {r_val_violation, proplists:get_value(r, Options)}};
        {error, true} ->
            {error, {rw_val_violation, proplists:get_value(rw, Options)}};
        _ ->
            %% ok, the R/W or the RW values were OK, get PR/PW values
            PR0 = proplists:get_value(pr, Options, default),
            case riak_kv_util:expand_rw_value(pr, PR0, BucketProps, N) of
                error ->
                    {error, {pr_val_violation, PR0}};
                PR ->
                    {R, PR}
           end
    end.

get_w_options(Bucket, Options) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    N = proplists:get_value(n_val,BucketProps),
    %% specifying R/W AND RW together doesn't make sense, so check if R or W
    %is defined first. If not, use RW or default.
    W = case proplists:is_defined(w, Options) of
        true ->
            HasRW = false,
            W0 = proplists:get_value(w, Options, default),
            W1 = riak_kv_util:expand_rw_value(w, W0, BucketProps, N),
            W1;
        false ->
            HasRW = true,
            RW0 = proplists:get_value(rw, Options, default),
            RW = riak_kv_util:expand_rw_value(rw, RW0, BucketProps, N),
            RW
    end,
    %% check for errors
    case {W, HasRW} of
        {error, false} ->
            {error, {w_val_violation, proplists:get_value(w, Options)}};
        {error, true} ->
            {error, {rw_val_violation, proplists:get_value(rw, Options)}};
        _ ->
            PW0 = proplists:get_value(pw, Options, default),
            case riak_kv_util:expand_rw_value(pw, PW0, BucketProps, N) of
                error ->
                    {error, {pw_val_violation, PW0}};
                PW ->
                    DW0 = proplists:get_value(dw, Options, default),
                    case riak_kv_util:expand_rw_value(dw, DW0, BucketProps, N) of
                        error ->
                            {error, {dw_val_violation, DW0}};
                        DW ->
                            {W, PW, DW}
                    end
            end
    end.











%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

delete_test_() ->
    cleanup(ignored_arg),
    %% Execute the test cases
    { foreach, 
      fun setup/0,
      fun cleanup/1,
      [
          fun invalid_r_delete/0,
          fun invalid_rw_delete/0,
          fun invalid_w_delete/0,
          fun invalid_pr_delete/0,
          fun invalid_pw_delete/0
      ]
  }.

invalid_rw_delete() ->
    RW = <<"abc">>,
    %% Start the gen_fsm process
    RequestId = erlang:phash2(erlang:now()),
    Bucket = <<"testbucket">>,
    Key = <<"testkey">>,
    Timeout = 60000,
    riak_kv_delete_sup:start_delete(node(), [RequestId, Bucket, Key, [{rw,RW}], Timeout, self()]),
    %% Wait for error response
    receive
        {_RequestId, Result} ->
            ?assertEqual({error, {rw_val_violation, <<"abc">>}}, Result)
    after
        5000 ->
            ?assert(false)
    end.

invalid_r_delete() ->
    R = <<"abc">>,
    %% Start the gen_fsm process
    RequestId = erlang:phash2(erlang:now()),
    Bucket = <<"testbucket">>,
    Key = <<"testkey">>,
    Timeout = 60000,
    riak_kv_delete_sup:start_delete(node(), [RequestId, Bucket, Key, [{r,R}], Timeout, self()]),
    %% Wait for error response
    receive
        {_RequestId, Result} ->
            ?assertEqual({error, {r_val_violation, <<"abc">>}}, Result)
    after
        5000 ->
            ?assert(false)
    end.

invalid_w_delete() ->
    W = <<"abc">>,
    %% Start the gen_fsm process
    RequestId = erlang:phash2(erlang:now()),
    Bucket = <<"testbucket">>,
    Key = <<"testkey">>,
    Timeout = 60000,
    riak_kv_delete_sup:start_delete(node(), [RequestId, Bucket, Key, [{w,W}],
            Timeout, self(), undefined, vclock:fresh()]),
    %% Wait for error response
    receive
        {_RequestId, Result} ->
            ?assertEqual({error, {w_val_violation, <<"abc">>}}, Result)
    after
        5000 ->
            ?assert(false)
    end.

invalid_pr_delete() ->
    PR = <<"abc">>,
    %% Start the gen_fsm process
    RequestId = erlang:phash2(erlang:now()),
    Bucket = <<"testbucket">>,
    Key = <<"testkey">>,
    Timeout = 60000,
    riak_kv_delete_sup:start_delete(node(), [RequestId, Bucket, Key, [{pr,PR}], Timeout, self()]),
    %% Wait for error response
    receive
        {_RequestId, Result} ->
            ?assertEqual({error, {pr_val_violation, <<"abc">>}}, Result)
    after
        5000 ->
            ?assert(false)
    end.

invalid_pw_delete() ->
    PW = <<"abc">>,
    %% Start the gen_fsm process
    RequestId = erlang:phash2(erlang:now()),
    Bucket = <<"testbucket">>,
    Key = <<"testkey">>,
    Timeout = 60000,
    riak_kv_delete_sup:start_delete(node(), [RequestId, Bucket, Key,
            [{pw,PW}], Timeout, self(), undefined, vclock:fresh()]),
    %% Wait for error response
    receive
        {_RequestId, Result} ->
            ?assertEqual({error, {pw_val_violation, <<"abc">>}}, Result)
    after
        5000 ->
            ?assert(false)
    end.

setup() ->
    %% Shut logging up - too noisy.
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "riak_kv_delete_test_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "riak_kv_delete_test.log"}),
    %% Start erlang node
    TestNode = list_to_atom("testnode" ++ integer_to_list(element(3, now())) ++
                                integer_to_list(element(2, now()))),
    case net_kernel:start([TestNode, shortnames]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end,
    do_dep_apps(start, dep_apps()),
    application:set_env(riak_core, default_bucket_props, [{r, quorum},
            {w, quorum}, {pr, 0}, {pw, 0}, {rw, quorum}, {n_val, 3}]),
    %% There's some weird interaction with the quickcheck tests in put_fsm_eqc
    %% that somehow makes the riak_kv_delete sup not be running if those tests
    %% run before these. I'm sick of trying to figure out what is not being
    %% cleaned up right, thus the following workaround.
    case whereis(riak_kv_delete_sup) of
        undefined ->
            {ok, _} = riak_kv_delete_sup:start_link();
        _ ->
            ok
    end,
    riak_kv_get_fsm_sup:start_link(),
    timer:sleep(500).

cleanup(_Pid) ->
    do_dep_apps(stop, lists:reverse(dep_apps())),
    catch exit(whereis(riak_kv_vnode_master), kill), %% Leaks occasionally
    catch exit(whereis(riak_sysmon_filter), kill), %% Leaks occasionally
    catch unlink(whereis(riak_kv_get_fsm_sup)),
    catch unlink(whereis(riak_kv_delete_sup)),
    catch exit(whereis(riak_kv_get_fsm_sup), kill), %% Leaks occasionally
    catch exit(whereis(riak_kv_delete_sup), kill), %% Leaks occasionally
    net_kernel:stop(),
    %% Reset the riak_core vnode_modules
    application:unset_env(riak_core, default_bucket_props),
    application:unset_env(sasl, sasl_error_logger),
    error_logger:tty(true),
    application:set_env(riak_core, vnode_modules, []).

dep_apps() ->
    SetupFun =
        fun(start) ->
            %% Set some missing env vars that are normally 
            %% part of release packaging.
            application:set_env(riak_core, ring_creation_size, 64),
            application:set_env(riak_kv, storage_backend, riak_kv_memory_backend),
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
     inets, mochiweb, os_mon, SetupFun, riak_kv].

do_dep_apps(StartStop, Apps) ->
    lists:map(fun(A) when is_atom(A) -> application:StartStop(A);
                 (F)                 -> F(StartStop)
              end, Apps).

-endif.
