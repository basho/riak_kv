%% -------------------------------------------------------------------
%%
%% riak_delete: two-step object deletion
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

%% @doc two-step object deletion

-module(riak_kv_delete).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include("riak_kv_wm_raw.hrl").

-export([start_link/6, start_link/7, start_link/8, delete/8]).

-include("riak_kv_dtrace.hrl").

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
    riak_core_dtrace:put_tag(io_lib:format("~p,~p", [Bucket, Key])),
    ?DTRACE(?C_DELETE_INIT1, [0], []),
    case get_r_options(Bucket, Options) of
        {error, Reason} ->
            ?DTRACE(?C_DELETE_INIT1, [-1], []),
            Client ! {ReqId, {error, Reason}};
        {R, PR, PassThruOpts} ->
            RealStartTime = riak_core_util:moment(),
            {ok, C} = riak:local_client(),
            case C:get(Bucket,Key,[{r,R},{pr,PR},{timeout,Timeout}]++PassThruOpts) of
                {ok, OrigObj} ->
                    RemainingTime = Timeout - (riak_core_util:moment() - RealStartTime),
                    delete(ReqId,Bucket,Key,Options,RemainingTime,Client,ClientId,riak_object:vclock(OrigObj));
                {error, notfound} ->
                    ?DTRACE(?C_DELETE_INIT1, [-2], []),
                    Client ! {ReqId, {error, notfound}};
                X ->
                    ?DTRACE(?C_DELETE_INIT1, [-3], []),
                    Client ! {ReqId, X}
            end
    end;
delete(ReqId,Bucket,Key,Options,Timeout,Client,ClientId,VClock) ->
    riak_core_dtrace:put_tag(io_lib:format("~p,~p", [Bucket, Key])),
    ?DTRACE(?C_DELETE_INIT2, [0], []),
    case get_w_options(Bucket, Options) of
        {error, Reason} ->
            ?DTRACE(?C_DELETE_INIT2, [-1], []),
            Client ! {ReqId, {error, Reason}};
        {W, PW, DW, PassThruOptions} ->
            Obj0 = riak_object:new(Bucket, Key, <<>>, dict:store(?MD_DELETED,
                                                                 "true", dict:new())),
            Tombstone = riak_object:set_vclock(Obj0, VClock),
            {ok,C} = riak:local_client(ClientId),
            Reply = C:put(Tombstone, [{w,W},{pw,PW},{dw, DW},{timeout,Timeout}]++PassThruOptions),
            Client ! {ReqId, Reply},
            HasCustomN_val = proplists:get_value(n_val, Options) /= undefined,
            case Reply of
                ok when HasCustomN_val == false ->
                    ?DTRACE(?C_DELETE_INIT2, [1], [<<"reap">>]),
                    {ok, C2} = riak:local_client(),
                    AsyncTimeout = 60*1000,     % Avoid client-specified value
                    Res = C2:get(Bucket, Key, all, AsyncTimeout),
                    ?DTRACE(?C_DELETE_REAPER_GET_DONE, [1], [<<"reap">>]),
                    Res;
                _ ->
                    ?DTRACE(?C_DELETE_INIT2, [2], [<<"nop">>]),
                    nop
            end
    end.

get_r_options(Bucket, Options) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    N = case proplists:get_value(n_val, Options) of
            undefined ->
                proplists:get_value(n_val, BucketProps);
            N_val when is_integer(N_val), N_val > 0 ->
                %% TODO: No sanity check of this value vs. "real" n_val.
                N_val
        end,
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
                    {R, PR, extract_passthru_options(Options)}
           end
    end.

get_w_options(Bucket, Options) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    N = case proplists:get_value(n_val, Options) of
            undefined ->
                proplists:get_value(n_val, BucketProps);
            N_val when is_integer(N_val), N_val > 0 ->
                N_val
        end,
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
                            {W, PW, DW, extract_passthru_options(Options)}
                    end
            end
    end.

%% NOTE: The types are defined so that this isn't really proplist for
%%       these values but a 2-tuple list, namely, 'sloppy_quorum'
%%       cannot appear as a proplist-style property but must always
%%       appear as {sloppy_quorum, boolean()}.

extract_passthru_options(Options) ->
    [Opt || {K, _} = Opt <- Options,
            K == sloppy_quorum orelse K == n_val].

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

delete_test_() ->
    %% Execute the test cases
    {foreach,
      setup(),
      cleanup(),
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

configure(load) ->
    application:set_env(riak_core, default_bucket_props,
                        [{r, quorum}, {w, quorum}, {pr, 0}, {pw, 0},
                         {rw, quorum}, {n_val, 3}]),
    application:set_env(riak_kv, storage_backend, riak_kv_memory_backend);
configure(_) -> ok.

setup() ->
    riak_kv_test_util:common_setup(?MODULE, fun configure/1).

cleanup() ->
    riak_kv_test_util:common_cleanup(?MODULE, fun configure/1).


-endif.
