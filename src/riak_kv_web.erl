%% -------------------------------------------------------------------
%%
%% riak_kv_web: setup Riak's KV HTTP interface
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Convenience functions for setting up the HTTP interface
%%      of Riak.  This module loads parameters from the application
%%      environment:
%%
%%<dl><dt> raw_name
%%</dt><dd>   the base path under which the riak_kv_wm_raw
%%            should be exposed; defaulted to "raw"
%%</dd></dl>
-module(riak_kv_web).

-export([dispatch_table/0]).
-include("riak_kv_wm_raw.hrl").

dispatch_table() ->
    MapredProps = mapred_props(),
    StatsProps = stats_props(),

    lists:append(
      raw_dispatch(),
      [{[proplists:get_value(prefix, MapredProps)],
        riak_kv_wm_mapred, MapredProps},
       {[proplists:get_value(prefix, StatsProps)],
        riak_kv_wm_stats, StatsProps},
       {["ping"], riak_kv_wm_ping, []}]).

raw_dispatch() ->
    case app_helper:get_env(riak_kv, raw_name) of
        undefined -> raw_dispatch("riak");
        Name -> lists:append(raw_dispatch(Name), raw_dispatch("riak"))
    end.

raw_dispatch(Name) ->
    Props = raw_props(Name),
    [
     %% OLD API
     {[Name],
      riak_kv_wm_buckets, Props},

     {[Name, bucket], fun is_post/1,
      riak_kv_wm_object, Props},

     {[Name, bucket], fun is_props/1,
      riak_kv_wm_props, Props},

     {[Name, bucket], fun is_keylist/1,
      riak_kv_wm_keylist, [{allow_props_param, true}|Props]},

     {[Name, bucket, key],
      riak_kv_wm_object, Props},

     {[Name, bucket, key, '*'],
      riak_kv_wm_link_walker, Props},

     %% NEW API
     {["buckets"],
      riak_kv_wm_buckets, Props},

     {["buckets", bucket, "props"],
      riak_kv_wm_props, Props},

     {["buckets", bucket, "keys"], fun is_post/1,
      riak_kv_wm_object, Props},

     {["buckets", bucket, "keys"],
      riak_kv_wm_keylist, Props},

     {["buckets", bucket, "keys", key],
      riak_kv_wm_object, Props},

     {["buckets", bucket, "keys", key, '*'],
      riak_kv_wm_link_walker, Props}
    ].

is_post(Req) ->
    wrq:method(Req) == 'POST'.

is_props(Req) ->
    (wrq:get_qs_value(?Q_PROPS, Req) /= ?Q_FALSE) andalso (not is_keylist(Req)).

is_keylist(Req) ->
    X = wrq:get_qs_value(?Q_KEYS, Req),
    (X == ?Q_STREAM) orelse (X == ?Q_TRUE).

raw_props(Prefix) ->
    [{prefix, Prefix}, {riak, local}].

mapred_props() ->
    [{prefix, app_helper:get_env(riak_kv, mapred_name, "mapred")}].

stats_props() ->
    [{prefix, app_helper:get_env(riak_kv, stats_urlpath, "stats")}].
