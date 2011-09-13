%% -------------------------------------------------------------------
%%
%% riak_kv_wm_ping: simple Webmachine resource for availability test
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

%% @doc simple Webmachine resource for availability test

-module(riak_kv_wm_ping).
-author('Dave Smith <dizzyd@dizzyd.com>').

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         to_html/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

init([]) ->
    {ok, undefined}.

%% @spec service_available(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine the status of the local node. Unless the node is in the 
%%      'valid' state this resource will return 501
service_available(ReqData, Ctx) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case riak_core_ring:member_status(Ring, node()) of
        valid ->
            {true, ReqData, Ctx};
        Status ->
            {false,
             wrq:set_resp_body(
                io_lib:format("~w~n", [Status]),
                wrq:set_resp_header(?HEAD_CTYPE, "text/plain", ReqData)),
             Ctx}
    end.

to_html(ReqData, Ctx) ->
    {"OK", ReqData, Ctx}.
