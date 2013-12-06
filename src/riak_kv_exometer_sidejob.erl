-module(riak_kv_exometer_sidejob).

-behaviour(exometer_entry).

%% API
-export([new_entry/2]).

%% Callback API
-export([new/3, delete/3, get_value/4, update/4, reset/3, sample/3,
	 get_datapoints/3, setopts/4]).

new_entry(Name, SjName) ->
    exometer:new(Name, sidejob, [{sj_name, SjName}]).

-define(UNSUP, {error, unsupported}).

new(_Name, _Type, Options) ->
    {_, SjName} = lists:keyfind(sj_name, 1, Options),
    {ok, SjName}.

delete(_Name, _Type, _Ref) ->
    ok.

get_value(_Name, _Type, SjName, DPs) ->
    try filter_datapoints(sidejob_resource_stats:stats(SjName), DPs)
    catch
	error:_ ->
	    unavailable
    end.

get_datapoints(_, _, _) ->
    [usage,rejected, in_rate, out_rate, usage_60s, rejected_60s,
     avg_in_rate_60s, max_in_rate_60s, avg_out_rate_60s,
     max_out_rate_60s, usage_total, rejected_total,
     avg_in_rate_total, max_in_rate_total, avg_out_rate_total].

update(_Name, _Type, _Ref, _Value) -> ?UNSUP.
reset(_Name, _Type, _Ref)  -> ?UNSUP.
sample(_Name, _Type, _Ref) -> ?UNSUP.
setopts(_Name, _Options, _Type, _Ref) -> ?UNSUP.

filter_datapoints(Stats, default) ->
    Stats;
filter_datapoints(Stats, DPs) ->
    [S || {K, _} = S <- Stats,
	  lists:member(K, DPs)].

