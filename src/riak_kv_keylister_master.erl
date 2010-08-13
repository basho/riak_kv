-module(riak_kv_keylister_master).

-behaviour(gen_server).

%% API
-export([start_link/0,
         start_keylist/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

start_keylist(Node, ReqId, Bucket) ->
    case gen_server:call({?SERVER, Node}, {start_kl, ReqId, self(), Bucket}) of
        {ok, Pid} ->
            %% Link processes so the keylister doesn't run forever
            erlang:link(Pid),
            {ok, Pid};
        Error ->
            Error
    end.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #state{}}.

handle_call({start_kl, ReqId, Caller, Bucket}, _From, State) ->
    Reply = riak_kv_keylister_sup:new_lister(ReqId, Caller, Bucket),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
