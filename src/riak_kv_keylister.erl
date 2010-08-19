-module(riak_kv_keylister).

-behaviour(gen_fsm).

%% API
-export([start_link/3,
         list_keys/2]).

%% States
-export([waiting/2]).

%% gen_fsm callbacks
-export([init/1, state_name/2, state_name/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {reqid,
                caller,
                bucket,
                bloom}).

list_keys(ListerPid, VNode) ->
    gen_fsm:send_event(ListerPid, {lk, VNode}).

start_link(ReqId, Caller, Bucket) ->
    gen_fsm:start_link(?MODULE, [ReqId, Caller, Bucket], []).

init([ReqId, Caller, Bucket]) ->
    process_flag(trap_exit, true),
    {ok, Bloom} = ebloom:new(10000000, 0.0001, crypto:rand_uniform(1, 5000)),
    {ok, waiting, #state{reqid=ReqId, caller=Caller, bloom=Bloom, bucket=Bucket}}.

waiting({lk, VNode}, #state{reqid=ReqId, bucket=Bucket}=State) ->
    riak_kv_vnode:list_keys2(VNode, ReqId, self(), Bucket),
    {next_state, waiting, State}.

state_name(_Event, State) ->
    {next_state, state_name, State}.

state_name(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, state_name, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ignored, StateName, State}.

handle_info({ReqId, {kl, Idx, Keys0}}, waiting, #state{reqid=ReqId, bloom=Bloom,
                                                  caller=Caller}=State) ->
    F = fun(Key, Acc) ->
                case ebloom:contains(Bloom, Key) of
                    true ->
                        Acc;
                    false ->
                        ebloom:insert(Bloom, Key),
                        [Key|Acc]
                end end,
    Keys = lists:foldl(F, [], Keys0),
    gen_fsm:send_event(Caller, {ReqId, {kl, Idx, Keys}}),
    {next_state, waiting, State};
handle_info({ReqId, Idx, done}, waiting, #state{reqid=ReqId, caller=Caller}=State) ->
    gen_fsm:send_event(Caller, {ReqId, Idx, done}),
    {next_state, waiting, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% Internal functions
