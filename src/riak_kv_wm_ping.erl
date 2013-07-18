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
            Scheme = wrq:scheme(ReqData),
            case Scheme == https of
                true ->
                    case wrq:get_req_header("Authorization", ReqData) of
                        "Basic " ++ Base64 ->
                            UserPass = base64:decode_to_string(Base64),
                            [User, Pass] = string:tokens(UserPass, ":"),
                            {ok, Peer} = inet_parse:address(wrq:peer(ReqData)),
                            case riak_core_security:authenticate(User, Pass,
                                                                 [{ip, Peer}])
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
                    %% security is enabled, but they're connecting over HTTP.
                    %% which means if they authed, the credentials would be in
                    %% plaintext
                    insecure
            end;
        false ->
            true
    end,
    case Res of
        false ->
            {"Basic realm=\"Riak\"", ReqData, Ctx};
        true ->
            {true, ReqData, Ctx};
        insecure ->
            %% XXX 301 may be more appropriate here, but since the http and
            %% https port are different and configurable, it is hard to figure
            %% out the redirect URL to serve.
            {{halt, 426}, wrq:append_to_resp_body(<<"Security is enabled and "
                    "Riak does not accept credentials over HTTP. Try HTTPS "
                    "instead.">>, ReqData), Ctx}
    end.

to_html(ReqData, Ctx) ->
    {"OK", ReqData, Ctx}.
