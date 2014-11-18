-module(riak_kv_http_cache).

-export([start_link/0,
	 get_stats/0]).

-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-define(SERVER, ?MODULE).

-record(st, {ts, stats = []}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_stats() ->
    gen_server:call(?MODULE, get_stats).

init(_) ->
    {ok, #st{}}.

handle_call(get_stats, _From, #st{} = S) ->
    #st{stats = Stats} = S1 = check_cache(S),
    {reply, Stats, S1}.

handle_cast(_, S) ->
    {noreply, S}.

handle_info(_, S) ->
    {noreply, S}.

terminate(_, _) ->
    ok.

code_change(_, S, _) ->
    {ok, S}.

check_cache(#st{ts = undefined} = S) ->
    S#st{ts = os:timestamp(), stats = do_get_stats()};
check_cache(#st{ts = Then} = S) ->
    Now = os:timestamp(),
    case timer:now_diff(Now, Then) < 1000000 of
	true ->
	    S;
	false ->
	    S#st{ts = Now, stats = do_get_stats()}
    end.

do_get_stats() ->
    riak_kv_wm_stats:get_stats().
