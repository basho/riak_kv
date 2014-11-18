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
-export([start_link/2, get_vnodeid_and_counter/2, lease_counter/2, clear_vnodeid/1, status/1]).

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

-type status() :: [proplists:property()] | [].
-type init_args() :: [init_arg()].
-type init_arg() :: pid() | non_neg_integer().
-type blocking_req() :: clear | {vnodeid, LeaseSize :: non_neg_integer()}.

%% only 32 bits per counter, when you hit that, get a new vnode id
-define(MAX_CNTR, 4294967295).

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
    gen_server:start_link(?MODULE, [VnodePid, Index], []).

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
                                          LeaseSize > 0,
                                          LeaseSize =< ?MAX_CNTR ->
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
                                          LeaseSize > 0,
                                          LeaseSize =< ?MAX_CNTR ->
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
init([VnodePid, Index]) ->
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
    Status2 = proplists:delete(counter, proplists:delete(vnodeid, Status)),
    ok = write_vnode_status(Status2, File),
    {reply, {ok, cleared}, State};
handle_call(status, _From, State) ->
    #state{status_file=File} = State,
    {ok, Status} = read_vnode_status(File),
    {reply, {ok, Status}, State}.

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
get_counter_lease(LeaseSize, Status) ->
    PrevLease = proplists:get_value(counter, Status, ?MAX_CNTR),
    VnodeId0 = proplists:get_value(vnodeid, Status, undefined),
    if
        (VnodeId0 == undefined) or (PrevLease == ?MAX_CNTR) ->
            {VnodeId, Status2} = assign_vnodeid(erlang:now(),
                                                riak_core_nodeid:get(),
                                                Status),
            {0, LeaseSize, VnodeId, replace(counter, LeaseSize, Status2)};
        true ->
            %% The lease on disk exists, and is less than the max.
            %% @TODO (rdb) fudge factor? Should it be at least N greater than max?
            %% otherwise you might flush, only to hand out a lease of 1!
            %%
            %% Make a lease that is greater than that on disk, but not
            %%  larger than the 32 bit int limit.  This may be a lease
            %%  smaller than request. Say LeaseSize is 4 billion, and
            %%  this is the second lease, you're only getting a lease
            %%  of 500million-ish. I thought this made more sense than
            %%  giving the full size lease by generating a new
            %%  vnodeid, since a new vnodeid means more actors for all
            %%  keys touched by the vnode. A very badly configured
            %%  vnode (trigger lease at 1% usage, lease size of
            %%  4billion) will casue many vnode ids to be generated if
            %%  we don't use this "best lease" scheme.
            LeaseTo = min(PrevLease + LeaseSize, ?MAX_CNTR),
            {PrevLease, LeaseTo, VnodeId0, replace(counter, LeaseTo, Status)}
    end.

%% @private replace tuple with `Key' with in proplist `Status' with a
%% tuple of `{Key, Value}'.
-spec replace(term(), term(), status()) -> status().
replace(Key, Value, Status) ->
    [{Key, Value} | proplists:delete(Key, Status)].

%% @private generate a file name for the vnode status, and ensure the
%% path to exixts.
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
    LastVnodeEpoch = proplists:get_value(last_epoch, Status, 0),
    VnodeEpoch = erlang:max(NowEpoch, LastVnodeEpoch+1),
    VnodeId = <<NodeId/binary, VnodeEpoch:32/integer>>,
    UpdStatus = replace(vnodeid, VnodeId, replace(last_epoch, VnodeEpoch, Status)),
    {VnodeId, UpdStatus}.

%% @private read the vnode status from `File'. Returns `{ok,
%% status()}' or `{error, Reason}'. If the file does not exist, and
%% empty status is returned.
-spec read_vnode_status(file:filename()) -> {ok, status()} |
                                       {error, term()}.
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
%% @private write the vnode status. This is why the file is guarded by
%% the process. This file should have no concurrent access, and MUST
%% not be written at any other place/time in the system.
-spec write_vnode_status(status(), file:filename()) -> ok.
write_vnode_status(Status, File) ->
    VersionedStatus = replace(version, ?VNODE_STATUS_VERSION, Status),
    riak_core_util:replace_file(File, io_lib:format("~p.", [VersionedStatus])).

-ifdef(TEST).

-ifdef(EQC).
%% prop_monotonic_counter() ->
%%     ok.

-endif.


replace_test() ->
    Status = [],
    ?assertEqual([{rdb, yay}], replace(rdb, yay, Status)).

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
