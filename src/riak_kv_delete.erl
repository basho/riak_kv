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

%-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%-endif.

-export([start_link/6, start_link/7, start_link/8, delete/8]).

start_link(ReqId, Bucket, Key, RW, Timeout, Client) ->
    {ok, proc_lib:spawn_link(?MODULE, delete, [ReqId, Bucket, Key,
                                               RW, Timeout, Client, undefined,
                                               undefined])}.

start_link(ReqId, Bucket, Key, RW, Timeout, Client, ClientId) ->
    {ok, proc_lib:spawn_link(?MODULE, delete, [ReqId, Bucket, Key,
                                               RW, Timeout, Client, ClientId,
                                               undefined])}.

start_link(ReqId, Bucket, Key, RW, Timeout, Client, ClientId, VClock) ->
    {ok, proc_lib:spawn_link(?MODULE, delete, [ReqId, Bucket, Key,
                                               RW, Timeout, Client, ClientId,
                                               VClock])}.

%% @spec delete(ReqId :: binary(), riak_object:bucket(), riak_object:key(),
%%             RW :: integer(), TimeoutMillisecs :: integer(), Client :: pid())
%%           -> term()
%% @doc Delete the object at Bucket/Key.  Direct return value is uninteresting,
%%      see riak_client:delete/3 for expected gen_server replies to Client.
delete(ReqId,Bucket,Key,RW0,Timeout,Client,ClientId,undefined) ->
    case get_rw_val(Bucket, RW0) of
        error ->
            Client ! {ReqId, {error, {rw_val_violation, RW0}}};
        RW ->
            RealStartTime = riak_core_util:moment(),
            {ok, C} = riak:local_client(),
            case C:get(Bucket,Key,RW,Timeout) of
                {ok, OrigObj} ->
                    RemainingTime = Timeout - (riak_core_util:moment() - RealStartTime),
                    delete(ReqId,Bucket,Key,RW,RemainingTime,Client,ClientId,riak_object:vclock(OrigObj));
                {error, notfound} ->
                    Client ! {ReqId, {error, notfound}};
                X ->
                    Client ! {ReqId, X}
            end
    end;
delete(ReqId,Bucket,Key,RW0,Timeout,Client,ClientId,VClock) ->
    case get_rw_val(Bucket, RW0) of
        error ->
            Client ! {ReqId, {error, {rw_val_violation, RW0}}};
        RW ->

            Obj0 = riak_object:new(Bucket, Key, <<>>, dict:store(<<"X-Riak-Deleted">>,
                                                                 "true", dict:new())),
            Tombstone = riak_object:set_vclock(Obj0, VClock),
            {ok,C} = riak:local_client(ClientId),
            Reply = C:put(Tombstone, RW, RW, Timeout),
            Client ! {ReqId, Reply},
            case Reply of
                ok ->
                    {ok, C2} = riak:local_client(),
                    C2:get(Bucket, Key, all, Timeout);
                _ -> nop
            end
    end.

get_rw_val(Bucket, RW0) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    N = proplists:get_value(n_val,BucketProps),
    riak_kv_util:expand_rw_value(rw, RW0, BucketProps, N).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

delete_test_() ->
    %% Execute the test cases
    {spawn, [
    { foreach, 
      fun setup/0,
      fun cleanup/1,
      [
       fun invalid_rw_delete/0
      ]
    }]}.

invalid_rw_delete() ->
    RW = <<"abc">>,
    %% Start the gen_fsm process
    RequestId = erlang:phash2(erlang:now()),
    Bucket = <<"testbucket">>,
    Key = <<"testkey">>,
    Timeout = 60000,
    riak_kv_delete_sup:start_delete(node(), [RequestId, Bucket, Key, RW, Timeout, self()]),
    %% Wait for error response
    receive
        {_RequestId, Result} ->
            ?assertEqual({error, {rw_val_violation, <<"abc">>}}, Result)
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
    {ok, _} = net_kernel:start([testnode, shortnames]),
    cleanup(unused_arg),
    do_dep_apps(start, dep_apps()),
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
    timer:sleep(500).

cleanup(_Pid) ->
    do_dep_apps(stop, lists:reverse(dep_apps())),
    catch exit(whereis(riak_kv_vnode_master), kill), %% Leaks occasionally
    catch exit(whereis(riak_sysmon_filter), kill), %% Leaks occasionally
    net_kernel:stop(),
    %% Reset the riak_core vnode_modules
    application:set_env(riak_core, vnode_modules, []).

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

-endif.
