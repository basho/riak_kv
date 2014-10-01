
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
-include_lib("eunit/include/eunit.hrl").
-endif.


%% API
-export([start_link/1, get_vnodeid_and_counter/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
          %% vnode status directory
          status_file :: file:filename(),
          %% vnode index
          index :: non_neg_integer()
         }).

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
start_link(Index) ->
    gen_server:start_link(?MODULE, [Index], []).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
get_vnodeid_and_counter(Pid, CounterThreshold) ->
    gen_server:call(Pid, {vnodeid, CounterThreshold}, infinity).

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
init([Index]) ->
    StatusFilename = vnode_status_filename(Index),
    {ok, #state{status_file=StatusFilename, index=Index}}.

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
    #state{index=Index} = State,
    F = fun(Status) ->
                case {proplists:get_value(vnodeid, Status, undefined),
                      proplists:get_value(counter, Status, undefined)} of
                    V when element(1, V) == undefined;
                           element(2, V) == undefined  ->
                        %% If the counter is missing we need a new
                        %% vnodeid as we have no idea what the count
                        %% for this vnodeid is. Should only be
                        %% possible in the case of an upgrade or bad
                        %% file on disk.
                        Counter = 0,
                        {VnodeId, Status2} = assign_vnodeid(erlang:now(), %%Using now as we want
                                                            %% different values per vnode id on a node
                                                            riak_core_nodeid:get(),
                                                            Status),
                        {LeaseTo, Status3} = get_counter_lease(Counter, CounterThreshold, Status2),
                        {{VnodeId, Counter, LeaseTo}, Status3};
                    {VnodeId, Counter} ->
                        %% Note: this is subtle change to this
                        %% function, now it will _always_ trigger a
                        %% store of the new status, since the lease
                        %% will always be moving upwards. A vnode that
                        %% starts, and crashes, and starts, and
                        %% crashes over and over will burn through a
                        %% lot of counter.
                        {LeaseTo, Status2} = get_counter_lease(Counter, CounterThreshold, Status),
                        {{VnodeId, Counter, LeaseTo}, Status2}
                end
        end,
    Res = update_vnode_status(F, Index),
    {reply, Res, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
vnode_status_filename(Index) ->
    P_DataDir = app_helper:get_env(riak_core, platform_data_dir),
    VnodeStatusDir = app_helper:get_env(riak_kv, vnode_status,
                                        filename:join(P_DataDir, "kv_vnode")),
    filename:join(VnodeStatusDir, integer_to_list(Index)).


%% @private get the current counter lease, and update the status
%% proplist with same for persisting
-spec get_counter_lease(non_neg_integer(), non_neg_integer(), [proplists:property()]) ->
                         {non_neg_integer(), non_neg_integer(), [proplists:property()]}.
get_counter_lease(Counter, CounterThreshold, Status) ->
    Leased = Counter + CounterThreshold,
    {Leased, [{counter, Leased} | proplists:delete(counter, Status)]}.

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

update_vnode_status(F, Index) ->
    VnodeFile = vnode_status_filename(Index),
    ok = filelib:ensure_dir(VnodeFile),
    case read_vnode_status(VnodeFile) of
        {ok, Status} ->
            update_vnode_status2(F, Status, VnodeFile);
        {error, enoent} ->
            update_vnode_status2(F, [], VnodeFile);
        ER ->
            ER
    end.

update_vnode_status2(F, Status, VnodeFile) ->
    case F(Status) of
        {Ret, Status} -> % No change
            {ok, Ret};
        {Ret, UpdStatus} ->
            case write_vnode_status(UpdStatus, VnodeFile) of
                ok ->
                    {ok, Ret};
                ER ->
                    ER
            end
    end.

read_vnode_status(File) ->
    case file:consult(File) of
        {ok, [Status]} when is_list(Status) ->
            {ok, proplists:delete(version, Status)};
        ER ->
            ER
    end.

-define(VNODE_STATUS_VERSION, 2).

write_vnode_status(Status, File) ->
    VersionedStatus = [{version, ?VNODE_STATUS_VERSION} | proplists:delete(version, Status)],
    TmpFile = File ++ "~",
    case file:write_file(TmpFile, io_lib:format("~p.", [VersionedStatus])) of
        ok ->
            file:rename(TmpFile, File);
        ER ->
            ER
    end.

-ifdef(TEST).

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
                 F = fun([]) ->
                             {shouldfail, [badperm]}
                     end,
                 Index = 0,
                 ?assertEqual({error, eacces},  update_vnode_status(F, Index))
             end),
      ?_test(begin % create successfully
                 ?cmd("chmod +w kv_vnode_status_test"),

                 F = fun([]) ->
                             {created, [created]}
                     end,
                 Index = 0,
                 ?assertEqual({ok, created}, update_vnode_status(F, Index))
             end),
      ?_test(begin % update successfully
                 F = fun([created]) ->
                             {updated, [updated]}
                     end,
                 Index = 0,
                 ?assertEqual({ok, updated}, update_vnode_status(F, Index))
             end),
      ?_test(begin % update failure
                 ?cmd("chmod 000 kv_vnode_status_test/0"),
                 ?cmd("chmod 500 kv_vnode_status_test"),
                 F = fun([updated]) ->
                             {shouldfail, [updatedagain]}
                     end,
                 Index = 0,
                 ?assertEqual({error, eacces},  update_vnode_status(F, Index))
             end)

     ]}.
-endif.
