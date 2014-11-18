%% -------------------------------------------------------------------
%%
%% riak_kv_exometer_sidejob: Access sidejob stats via the Exometer API
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

%% @doc riak_kv_exometer_sidejob is a wrapper module making sidejob
%% stats available via the Exometer API. It adds no overhead to the
%% stats update, nor interfere with the 'old' way of reading the
%% sidejob stats; it is purely complementary.
%% @end

-module(riak_kv_exometer_sidejob).

-behaviour(exometer_entry).

%% API
-export([new_entry/3]).

%% Callback API
-export([behaviour/0,
	 new/3, delete/3, get_value/4, update/4, reset/3, sample/3,
         get_datapoints/3, setopts/3]).

behaviour() ->
    entry.

new_entry(Name, SjName, Opts) ->
    exometer:new(Name, ad_hoc, [{module, ?MODULE},
				{type, sidejob},
				{sj_name, SjName}|Opts]).

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
setopts(_Entry, _Options, _NewStatus) -> ok.

filter_datapoints(Stats, default) ->
    Stats;
filter_datapoints(Stats, DPs) ->
    [S || {K, _} = S <- Stats,
          lists:member(K, DPs)].
