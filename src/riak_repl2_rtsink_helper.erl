%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.

%% @doc Realtime replication sink module
%%
%% High level responsibility...
%%  consider moving out socket responsibilities to another process
%%  to keep this one responsive (but it would pretty much just do status)
%%
-module(riak_repl2_rtsink_helper).

%% API
-export([start_link/1,
         stop/1,
         write_objects/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-behavior(gen_server).

-record(state, {parent           %% Parent process
               }).

start_link(Parent) ->
    gen_server:start_link(?MODULE, [Parent], []).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

write_objects(Pid, BinObjs, DoneFun, Ver) ->
    gen_server:cast(Pid, {write_objects, BinObjs, DoneFun, Ver}).

%% Callbacks
init([Parent]) ->
    {ok, #state{parent = Parent}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({write_objects, BinObjs, DoneFun, Ver}, State) ->
    do_write_objects(BinObjs, DoneFun, Ver),
    {noreply, State};
handle_cast({unmonitor, Ref}, State) ->
    demonitor(Ref),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, _Pid, Reason}, State)
  when Reason == normal; Reason == shutdown ->
    {noreply, State};
handle_info({'DOWN', _MRef, process, Pid, Reason}, State) ->
    %% TODO: Log worker failure
    %% TODO: Needs graceful way to let rtsink know so it can die
    {stop, {worker_died, {Pid, Reason}}, State}.

terminate(_Reason, _State) ->
    %% TODO: Consider trying to do something graceful with poolboy?
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Receive TCP data - decode framing and dispatch
do_write_objects(BinObjs, DoneFun, Ver) ->
    Worker = poolboy:checkout(riak_repl2_rtsink_pool, true, infinity),
    MRef = monitor(process, Worker),
    Me = self(),
    WrapperFun = fun() -> DoneFun(), gen_server:cast(Me, {unmonitor, MRef}) end,
    ok = riak_repl_fullsync_worker:do_binputs(Worker, BinObjs, WrapperFun,
                                              riak_repl2_rtsink_pool, Ver).
