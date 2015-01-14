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
-export([start_link/2, get_vnodeid_and_counter/2, lease_counter/2, clear_vnodeid/1, status/1, stop/1]).

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

-type status() :: orddict:orddict().
-type init_args() :: {pid(), non_neg_integer()}.
-type blocking_req() :: clear | {vnodeid, LeaseSize :: non_neg_integer()}.

%% only 32 bits per counter, when you hit that, get a new vnode id
-define(MAX_CNTR, 4294967295).
-define(VNODE_STATUS_VERSION, 2).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(pid(), non_neg_integer()) -> {ok, pid()} | {error, term()}.
start_link(VnodePid, Index) ->
    gen_server:start_link(?MODULE, {VnodePid, Index}, []).

%%--------------------------------------------------------------------
%% @doc You can't ever have a `LeaseSize' greater than the maximum 32
%% bit unsigned integer, since that would involve breaking the
%% invariant of an vnodeid+cntr pair being used more than once to
%% start a key epoch, the counter is encoded in a 32 bit binary, and
%% going over 4billion+etc would wrap around and re-use integers.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_vnodeid_and_counter(pid(), non_neg_integer()) ->
                                     {ok, {VnodeId :: binary(),
                                           Counter :: non_neg_integer(),
                                           LeaseSize :: non_neg_integer()}}.
get_vnodeid_and_counter(Pid, LeaseSize) when is_integer(LeaseSize),
                                             LeaseSize > 0 ->
    %% @TODO decide on a sane timeout for getting a vnodeid/cntr pair
    %% onto disk.
    gen_server:call(Pid, {vnodeid, LeaseSize}).

%%--------------------------------------------------------------------
%% @doc Asynchronously lease increments for a counter. `Pid' is the
%% server pid, and `LeaseSize' is the number of increments to lease. A
%% `LeaseSize' of 10,000 means that a vnode can handle 10,000 new key
%% epochs before asking for a new counter. The trade-off here is
%% between the frequency of flushing and the speed with which a
%% frequently crashing vnode burns through the 32bit integer space,
%% thus requiring a new vnodeid. The calling vnode should handle the
%% response message of `{counter_lease, {From :: pid(), VnodeId ::
%% binary(), NewLease :: non_neg_integer()}}'
%%
%% @end
%%--------------------------------------------------------------------
-spec lease_counter(pid(), non_neg_integer()) -> ok.
lease_counter(Pid, LeaseSize) when is_integer(LeaseSize),
                                   LeaseSize > 0  ->
    gen_server:cast(Pid, {lease, LeaseSize}).

%%--------------------------------------------------------------------
%% @doc Blocking call to remove the vnode id and counter and from
%% disk. Used when a vnode has finished and will not act again.
%%
%% @end
%%--------------------------------------------------------------------
-spec clear_vnodeid(pid()) -> {ok, cleared}.
clear_vnodeid(Pid) ->
    gen_server:call(Pid, clear).

status(Pid) ->
    gen_server:call(Pid, status).

stop(Pid) ->
    gen_server:call(Pid, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Initializes the server, Init Args must be `[VnodePid
%% :: pid(), Index :: non_neg_integer()]' where the first element is
%% the pid of the vnode this manager works for, and the second is the
%% vnode's index/partition number (used for locating the status file.)
%%
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: init_args()) -> {ok, #state{}}.
init({VnodePid, Index}) ->
    StatusFilename = vnode_status_filename(Index),
    {ok, #state{status_file=StatusFilename, index=Index, vnode_pid=VnodePid}}.

%%--------------------------------------------------------------------
%% @private handle calls
%%--------------------------------------------------------------------
-spec handle_call(blocking_req(), {pid(), term()}, #state{}) ->
                         {reply, {ok, {VnodeId :: binary(),
                                       Counter :: non_neg_integer(),
                                       LeaseTo :: non_neg_integer()}},
                          #state{}}.
handle_call({vnodeid, LeaseSize}, _From, State) ->
    #state{status_file=File} = State,
    {ok, Status} = read_vnode_status(File),
    %% Note: this is subtle change to this function, now it will
    %% _always_ trigger a store of the new status, since the lease
    %% will always be moving upwards. A vnode that starts, and
    %% crashes, and starts, and crashes over and over will burn
    %% through a lot of counter (or vnode ids (if the leases are very
    %% large.))
    {Counter, LeaseTo, VnodeId, Status2} = get_counter_lease(LeaseSize, Status),
    ok = write_vnode_status(Status2, File),
    Res = {ok, {VnodeId, Counter, LeaseTo}},
    {reply, Res, State};
handle_call(clear, _From, State) ->
    #state{status_file=File} = State,
    {ok, Status} = read_vnode_status(File),
    Status2 = orddict:erase(counter, orddict:erase(vnodeid, Status)),
    ok = write_vnode_status(Status2, File),
    {reply, {ok, cleared}, State};
handle_call(status, _From, State) ->
    #state{status_file=File} = State,
    {ok, Status} = read_vnode_status(File),
    {reply, {ok, Status}, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%%--------------------------------------------------------------------
%% @private
%%--------------------------------------------------------------------

-spec handle_cast({lease, non_neg_integer()}, #state{}) ->
                         {noreply, #state{}}.
handle_cast({lease, LeaseSize}, State) ->
    #state{status_file=File, vnode_pid=Pid} = State,
    {ok, Status} = read_vnode_status(File),
    {_Counter, LeaseTo, VnodeId, UpdStatus} = get_counter_lease(LeaseSize, Status),
    ok = write_vnode_status(UpdStatus, File),
    Pid ! {counter_lease, {self(), VnodeId, LeaseTo}},
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

%% @private monotonically advance the counter lease. Guarded at
%% interface to server.
-spec get_counter_lease(non_neg_integer(), status()) ->
                           {non_neg_integer(), non_neg_integer(), binary(), status()}.
get_counter_lease(LeaseSize0, Status) ->
    PrevLease = get_status_item(counter, Status, undefined),
    VnodeId0 = get_status_item(vnodeid, Status, undefined),
    Version = get_status_item(version, Status, 1),

    LeaseSize = min(LeaseSize0, ?MAX_CNTR),

    case {Version, PrevLease, VnodeId0} of
        {_, _, undefined} ->
            new_id_and_counter(Status, LeaseSize);
        {1, undefined, ID} ->
            %% Upgrade, no counter existed, don't force a new vnodeid
            %% Is there still some edge here, with UP->DOWN->UP grade?
            %% We think not. Downgrade would keep the same vnode file,
            %% and the pre-epochal vnodeid would be used. Upgrade
            %% again picks up the same counter. Or, if the file is
            %% re-written while downgraded, it can only be for a new
            %% ID, so still safe.
            {0, LeaseSize, ID, orddict:store(counter, LeaseSize, Status)};
        {?VNODE_STATUS_VERSION, undefined, _ID} ->
            %% Lost counter? Wha? New ID
            new_id_and_counter(Status, LeaseSize);
        {?VNODE_STATUS_VERSION, Leased, _ID} when Leased + LeaseSize > ?MAX_CNTR ->
            new_id_and_counter(Status, LeaseSize);
        {?VNODE_STATUS_VERSION, Leased, ID} ->
            NewLease = Leased + LeaseSize,
            {PrevLease, NewLease, ID, orddict:store(counter, NewLease, Status)}
    end.

%% @private generate a new ID and assign a new counter, and lease up
%% to `LeaseSize'.
-spec new_id_and_counter(status(), non_neg_integer()) ->
                                {non_neg_integer(), non_neg_integer(), binary(), status()}.
new_id_and_counter(Status, LeaseSize) ->
    {VnodeId, Status2} = assign_vnodeid(erlang:now(),
                                        riak_core_nodeid:get(),
                                        Status),
    {0, LeaseSize, VnodeId, orddict:store(counter, LeaseSize, Status2)}.


%% @private Provide a `proplists:get_value/3' like function for status
%% orddict.
-spec get_status_item(term(), status(), term()) -> term().
get_status_item(Item, Status, Default) ->
    case orddict:find(Item, Status) of
        {ok, Val} ->
            Val;
        error ->
            Default
    end.

%% @private generate a file name for the vnode status, and ensure the
%% path to exists.
-spec vnode_status_filename(non_neg_integer()) -> file:filename().
vnode_status_filename(Index) ->
    P_DataDir = app_helper:get_env(riak_core, platform_data_dir),
    VnodeStatusDir = app_helper:get_env(riak_kv, vnode_status,
                                        filename:join(P_DataDir, "kv_vnode")),
    Filename = filename:join(VnodeStatusDir, integer_to_list(Index)),
    ok = filelib:ensure_dir(Filename),
    Filename.

%% @private Assign a unique vnodeid, making sure the timestamp is
%% unique by incrementing into the future if necessary.
-spec assign_vnodeid(erlang:timestamp(), binary(), status()) ->
                            {binary(), status()}.
assign_vnodeid(Now, NodeId, Status) ->
    {_Mega, Sec, Micro} = Now,
    NowEpoch = 1000000*Sec + Micro,
    LastVnodeEpoch = get_status_item(last_epoch, Status, 0),
    VnodeEpoch = erlang:max(NowEpoch, LastVnodeEpoch+1),
    VnodeId = <<NodeId/binary, VnodeEpoch:32/integer>>,
    UpdStatus = orddict:store(vnodeid, VnodeId,
                              orddict:store(last_epoch, VnodeEpoch, Status)),
    {VnodeId, UpdStatus}.

%% @private read the vnode status from `File'. Returns `{ok,
%% status()}' or `{error, Reason}'. If the file does not exist, and
%% empty status is returned.
-spec read_vnode_status(file:filename()) -> {ok, status()} |
                                       {error, term()}.
read_vnode_status(File) ->
    case file:consult(File) of
        {ok, [Status]} when is_list(Status) ->
            {ok, orddict:from_list(Status)};
        {error, enoent} ->
            %% doesn't exist? same as empty list
            {ok, orddict:new()};
        Er ->
            Er
    end.

-ifdef(TEST).
%% @private don't make testers suffer through the fsync time
-spec write_vnode_status(status(), file:filename()) -> ok.
write_vnode_status(Status, File) ->
    VersionedStatus = orddict:store(version, ?VNODE_STATUS_VERSION, Status),
    ok = file:write_file(File, io_lib:format("~p.", [orddict:to_list(VersionedStatus)])).
-else.
%% @private write the vnode status. This is why the file is guarded by
%% the process. This file should have no concurrent access, and MUST
%% not be written at any other place/time in the system.
-spec write_vnode_status(status(), file:filename()) -> ok.
write_vnode_status(Status, File) ->
    VersionedStatus = orddict:store(version, ?VNODE_STATUS_VERSION, Status),
    ok = riak_core_util:replace_file(File, io_lib:format("~p.", [orddict:to_list(VersionedStatus)])).
-endif.

-ifdef(TEST).

%% Check assigning a vnodeid twice in the same second
assign_vnodeid_restart_same_ts_test() ->
    Now1 = {1314,224520,343446}, %% TS=(224520 * 100000) + 343446
    Now2 = {1314,224520,343446}, %% as unsigned net-order int <<70,116,143,150>>
    NodeId = <<1, 2, 3, 4>>,
    {Vid1, Status1} = assign_vnodeid(Now1, NodeId, []),
    ?assertEqual(<<1, 2, 3, 4, 70, 116, 143, 150>>, Vid1),
    %% Simulate clear
    Status2 = orddict:erase(vnodeid, Status1),
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
    Status2 = orddict:erase(vnodeid, Status1),
    %% Reassign
    {Vid2, _Status3} = assign_vnodeid(Now2, NodeId, Status2),
    ?assertEqual(<<1, 2, 3, 4, 70,116,143,250>>, Vid2).

%% Check assigning a vnodeid with a earlier date - just in case of clock skew
assign_vnodeid_restart_earlier_ts_test() ->
    Now1 = {1000,224520,343546}, %% <<70,116,143,250>>
    Now2 = {1000,224520,343446}, %% <<70,116,143,150>>
    NodeId = <<1, 2, 3, 4>>,
    {Vid1, Status1} = assign_vnodeid(Now1, NodeId, []),
    ?assertEqual(<<1, 2, 3, 4, 70,116,143,250>>, Vid1),
    %% Simulate clear
    Status2 = orddict:erase(vnodeid, Status1),
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
                 try
                     write_vnode_status(orddict:new(), File)
                 catch _Err:{badmatch, Reason} ->
                         ?assertEqual({error, eacces}, Reason)
                 end
             end),
      ?_test(begin % create successfully
                 ?cmd("chmod +w kv_vnode_status_test"),
                 Index = 0,
                 File = vnode_status_filename(Index),
                 ?assertEqual(ok, write_vnode_status([{created, true}], File))
             end),
      ?_test(begin % update successfully
                 Index = 0,
                 File = vnode_status_filename(Index),
                 {ok, [{created, true}, {version, 2}]} = read_vnode_status(File),
                 ?assertEqual(ok, write_vnode_status([{updated, true}], File))
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
