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

%% webmachine resource exports
-export([
         init/1,
         is_authorized/2,
         to_html/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").

init([]) ->
    {ok, undefined}.

is_authorized(ReqData, Ctx) ->
    Res = case app_helper:get_env(riak_core, security, false) of
        true ->
            lager:info("Auth header ~p",
                       [wrq:get_req_header("Authorization", ReqData)]),
            case wrq:get_req_header("Authorization", ReqData) of
                "Basic " ++ Base64 ->
                    UserPass = base64:decode_to_string(Base64),
                    [User, Pass] = string:tokens(UserPass, ":"),
                    {ok, Peer} = inet_parse:address(wrq:peer(ReqData)),
                    lager:info("credentials ~p ~p from ~p", [User, Pass,
                                                             Peer]),
                    case riak_core_security:authenticate(User, Pass,
                                                         Peer)
                    of
                        {ok, _} ->
                            true;
                        {error, _} ->
                            false
                    end;
                _ ->
                    false
            end;
        false ->
            true
    end,
    case Res of
        false ->
            {"Basic realm=\"Riak\"", ReqData, Ctx};
        true ->
            {true, ReqData, Ctx}
    end.

to_html(ReqData, Ctx) ->
    {"OK", ReqData, Ctx}.
