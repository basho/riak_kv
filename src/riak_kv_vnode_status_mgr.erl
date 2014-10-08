%% -------------------------------------------------------------------
%%
%% riak_kv_vnode_status_mgr: Manages persistence of vnode status data
%% like vnodeid, vnode op counter etc
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_vnode_status_mgr).

-behaviour(gen_server).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.


%% API
-export([start_link/2, get_vnodeid_and_counter/2, lease_counter/3, clear_vnodeid/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
          %% vnode status directory
          status_file :: undefined | file:filename(),
          %% vnode index
          index :: undefined | non_neg_integer(),
          %% The vnode pid this mgr belongs to
          vnode_pid :: undefined | pid()
         }).

-type status() :: [proplists:property()].

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(VnodePid, Index) ->
    gen_server:start_link(?MODULE, [VnodePid, Index], []).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
get_vnodeid_and_counter(Pid, CounterThreshold) ->
    gen_server:call(Pid, {vnodeid, CounterThreshold}, infinity).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
lease_counter(Pid, Lease, LeaseSize) when is_integer(LeaseSize),
                                          LeaseSize > 0 ->
    %% How long to time out on this, default 5 seconds ok?
    gen_server:cast(Pid, {lease, Lease, LeaseSize}).

clear_vnodeid(Pid) ->
    gen_server:call(Pid, clear).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([VnodePid, Index]) ->
    StatusFilename = vnode_status_filename(Index),
    {ok, #state{status_file=StatusFilename, index=Index, vnode_pid=VnodePid}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({vnodeid, CounterThreshold}, _From, State) ->
    #state{status_file=File} = State,
    {ok, Status} = read_vnode_status(File),
    {VnodeId, Counter, Status2} = case {proplists:get_value(vnodeid, Status, undefined),
                                        proplists:get_value(counter, Status, undefined)} of
                                      V when element(1, V) == undefined;
                                             element(2, V) == undefined ->
                                          %% If the counter is missing we need a new
                                          %% vnodeid as we have no idea what the count
                                          %% for this vnodeid is. Should only be
                                          %% possible in the case of an upgrade or bad
                                          %% file on disk.
                                          {VId, Status1} = assign_vnodeid(erlang:now(),
                                                                              %%Using now as we want
                                                                              %% different values per vnode id on a node
                                                                              riak_core_nodeid:get(),
                                                                              Status),
                                          {VId, 0, Status1};
                                      {VId, Cntr} ->
                                          {VId, Cntr, Status}
                                  end,
    %% Note: this is subtle change to this
    %% function, now it will _always_ trigger a
    %% store of the new status, since the lease
    %% will always be moving upwards. A vnode that
    %% starts, and crashes, and starts, and
    %% crashes over and over will burn through a
    %% lot of counter.
    {LeaseTo, Status3} = get_counter_lease(Status2, Counter, CounterThreshold),
    ok = write_vnode_status(Status3, File),
    Res = {ok, {VnodeId, Counter, LeaseTo}},
    {reply, Res, State};
handle_call(clear, _From, State) ->
    #state{status_file=File} = State,
    {ok, Status} = read_vnode_status(File),
    Status2 = proplists:delete(counter, proplists:delete(vnodeid, Status)),
    ok = write_vnode_status(Status2, File),
    {reply, {ok, cleared}, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%%--------------------------------------------------------------------
handle_cast({lease, Lease, LeaseSize}, State) ->
    #state{status_file=File, vnode_pid=Pid} = State,
    Status = read_vnode_status(File),
    {LeaseTo, UpdStatus} = get_counter_lease(Status, Lease, LeaseSize),
    ok = write_vnode_status(UpdStatus, File),
    Pid ! {counter_lease, {ok, LeaseTo}},
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private monotonically advance the counter lease
-spec lease_counter(status(), non_neg_integer(), non_neg_integer()) ->
                           {non_neg_integer(), status()}.
get_counter_lease(Status, Lease, LeaseSize) when is_integer(LeaseSize),
                                                 LeaseSize > 0  ->
    CurrentLease = proplists:get_value(counter, Status, Lease),
    LeaseTo = max(Lease, CurrentLease) + LeaseSize,
    {LeaseTo, [{counter, LeaseTo} | proplists:delete(counter, Status)]}.

vnode_status_filename(Index) ->
    P_DataDir = app_helper:get_env(riak_core, platform_data_dir),
    VnodeStatusDir = app_helper:get_env(riak_kv, vnode_status,
                                        filename:join(P_DataDir, "kv_vnode")),
    Filename = filename:join(VnodeStatusDir, integer_to_list(Index)),
    ok = filelib:ensure_dir(Filename),
    Filename.

%% Assign a unique vnodeid, making sure the timestamp is unique by incrementing
%% into the future if necessary.
-spec assign_vnodeid(erlang:timestamp(), binary(), proplists:proplist()) ->
                            {binary(), [proplist:property()]}.
assign_vnodeid(Now, NodeId, Status) ->
    {_Mega, Sec, Micro} = Now,
    NowEpoch = 1000000*Sec + Micro,
    LastVnodeEpoch = proplists:get_value(last_epoch, Status, 0),
    VnodeEpoch = erlang:max(NowEpoch, LastVnodeEpoch+1),
    VnodeId = <<NodeId/binary, VnodeEpoch:32/integer>>,
    UpdStatus = [{vnodeid, VnodeId}, {last_epoch, VnodeEpoch} |
                 proplists:delete(vnodeid,
                                  proplists:delete(last_epoch, Status))],
    {VnodeId, UpdStatus}.

read_vnode_status(File) ->
    case file:consult(File) of
        {ok, [Status]} when is_list(Status) ->
            {ok, proplists:delete(version, Status)};
        {error, enoent} ->
            %% doesn't exist? same as empty list
            {ok, []};
        Er ->
            Er
    end.

-define(VNODE_STATUS_VERSION, 2).

write_vnode_status(Status, File) ->
    VersionedStatus = [{version, ?VNODE_STATUS_VERSION} | proplists:delete(version, Status)],
    riak_core_util:replace_file(File, io_lib:format("~p.", [VersionedStatus])).

-ifdef(TEST).

-ifdef(EQC).
%% prop_monotonic_counter() ->
%%     ok.

-endif.

%% Check assigning a vnodeid twice in the same second
assign_vnodeid_restart_same_ts_test() ->
    Now1 = {1314,224520,343446}, %% TS=(224520 * 100000) + 343446
    Now2 = {1314,224520,343446}, %% as unsigned net-order int <<70,116,143,150>>
    NodeId = <<1, 2, 3, 4>>,
    {Vid1, Status1} = assign_vnodeid(Now1, NodeId, []),
    ?assertEqual(<<1, 2, 3, 4, 70, 116, 143, 150>>, Vid1),
    %% Simulate clear
    Status2 = proplists:delete(vnodeid, Status1),
    %% Reassign
    {Vid2, _Status3} = assign_vnodeid(Now2, NodeId, Status2),
    ?assertEqual(<<1, 2, 3, 4, 70, 116, 143, 151>>, Vid2).

%% Check assigning a vnodeid with a later date, but less than 11.57
%% days later!
assign_vnodeid_restart_later_ts_test() ->
    Now1 = {1000,224520,343446}, %% <<70,116,143,150>>
    Now2 = {1000,224520,343546}, %% <<70,116,143,250>>
    NodeId = <<1, 2, 3, 4>>,
    {Vid1, Status1} = assign_vnodeid(Now1, NodeId, []),
    ?assertEqual(<<1, 2, 3, 4, 70,116,143,150>>, Vid1),
    %% Simulate clear
    Status2 = proplists:delete(vnodeid, Status1),
    %% Reassign
    {Vid2, _Status3} = assign_vnodeid(Now2, NodeId, Status2),
    ?assertEqual(<<1, 2, 3, 4, 70,116,143,250>>, Vid2).

%% Check assigning a vnodeid with a earlier date - just in case of clock skew
assign_vnodeid_restart_earlier_ts_test() ->
    Now1 = {1000,224520,343546}, %% <<70,116,143,150>>
    Now2 = {1000,224520,343446}, %% <<70,116,143,250>>
    NodeId = <<1, 2, 3, 4>>,
    {Vid1, Status1} = assign_vnodeid(Now1, NodeId, []),
    ?assertEqual(<<1, 2, 3, 4, 70,116,143,250>>, Vid1),
    %% Simulate clear
    Status2 = proplists:delete(vnodeid, Status1),
    %% Reassign
    %% Should be greater than last offered - which is the 2mil timestamp
    {Vid2, _Status3} = assign_vnodeid(Now2, NodeId, Status2),
    ?assertEqual(<<1, 2, 3, 4, 70,116,143,251>>, Vid2).

%% Test
vnode_status_test_() ->
    {setup,
     fun() ->
             filelib:ensure_dir("kv_vnode_status_test/.test"),
             ?cmd("chmod u+rwx kv_vnode_status_test"),
             ?cmd("rm -rf kv_vnode_status_test"),
             application:set_env(riak_kv, vnode_status, "kv_vnode_status_test"),
             ok
     end,
     fun(_) ->
             application:unset_env(riak_kv, vnode_status),
             ?cmd("chmod u+rwx kv_vnode_status_test"),
             ?cmd("rm -rf kv_vnode_status_test"),
             ok
     end,
     [?_test(begin % initial create failure
                 ?cmd("rm -rf kv_vnode_status_test || true"),
                 ?cmd("mkdir kv_vnode_status_test"),
                 ?cmd("chmod -w kv_vnode_status_test"),
                 Index = 0,
                 File = vnode_status_filename(Index),
                 ?assertEqual({error, eacces}, write_vnode_status([], File))
             end),
      ?_test(begin % create successfully
                 ?cmd("chmod +w kv_vnode_status_test"),
                 Index = 0,
                 File = vnode_status_filename(Index),
                 ?assertEqual(ok, write_vnode_status([created], File))
             end),
      ?_test(begin % update successfully
                 Index = 0,
                 File = vnode_status_filename(Index),
                 {ok, [created]} = read_vnode_status(File),
                 ?assertEqual(ok, write_vnode_status([updated], File))
             end),
      ?_test(begin % update failure
                 ?cmd("chmod 000 kv_vnode_status_test/0"),
                 ?cmd("chmod 500 kv_vnode_status_test"),
                 Index = 0,
                 File = vnode_status_filename(Index),
                 ?assertEqual({error, eacces},  read_vnode_status(File))
             end)

     ]}.
-endif.
