%% -------------------------------------------------------------------
%%
%% riak_kv_wm_apiep: Webmachine resource providing a `location service'
%%                   to external clients for optimal access to hosts
%%                   with partitions containing known buckets/key.
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Webmachine resource providing a `location service' to external
%%      clients for optimal access to hosts with partitions containing
%%      known buckets/key ranges.
%%
%%  ```
%%  GET /ring/coverage/bucket/BUCKET/key/KEY?proto=PROTO'''
%%
%%   For known bucket and key, return host:port of http and pb API
%%   entry points at riak nodes containing the data, as JSON
%%   object like this:
%%
%%    `[{"IP": [Port]}]'
%%
%%   By default, returned value is cached until the next multiple of
%%   15 seconds (configurable via ring_coverage_cache_expiry_time init parameter).

-module(riak_kv_wm_apiep).

%% webmachine resource exports
-export([
         init/1,
         is_authorized/2,
         allowed_methods/2,
         content_types_provided/2,
         malformed_request/2,
         last_modified/2,
         generate_etag/2,
         make_response/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").

-type wm_reqdata() :: #wm_reqdata{}.
-type unixtime() :: non_neg_integer().
-type proto() :: pbc|http.
-define(DEFAULT_CACHE_EXPIRY_TIME, 15).

-record(ctx, {bucket :: string(),
              key :: string(),
              proto = pbc :: proto(),
              expiry_time = ?DEFAULT_CACHE_EXPIRY_TIME :: unixtime()
             }).
-type ctx() :: #ctx{}.


%% ===================================================================
%% Webmachine API
%% ===================================================================

-spec init(list()) -> {ok, ctx()}.
init(Props) ->
    {ok, #ctx{expiry_time =
                  proplists:get_value(
                    ring_coverage_cache_expiry_time, Props,
                    ?DEFAULT_CACHE_EXPIRY_TIME)}}.


-spec is_authorized(wm_reqdata(), ctx()) ->
    {term(), wm_reqdata(), ctx()}.
is_authorized(RD, Ctx) ->
    case riak_api_web_security:is_authorized(RD) of
        false ->
            {"Basic realm=\"Riak\"", RD, Ctx};
        {true, _SecContext} ->
            {true, RD, Ctx};
        insecure ->
            {{halt, 426}, wrq:append_to_response_body(
                            "Security is enabled and Riak only accepts HTTPS connections",
                            RD), Ctx}
    end.


-spec malformed_request(wm_reqdata(), ctx()) ->
    {boolean(), wm_reqdata(), ctx()}.
malformed_request(RD, Ctx) ->
    %% 1. extract bucket and key from path
    [Bucket, Key] =
        [list_to_binary(
           riak_kv_wm_utils:maybe_decode_uri(
             RD, case wrq:path_info(PathElement, RD) of
                     undefined -> <<>>;
                     X -> X
                 end))
         || PathElement <- [bucket, key]],
    %% 2. extract optional fields from parameters
    CheckOptionalParm =
        fun(_, undefined, _) ->
                true;
           (P, V, VL) ->
                lists:member(V, VL) orelse
                    begin
                        lager:log(warning, self(), "parameter ~s must be one of ~9999p", [P, VL]),
                        false
                    end
        end,
    case lists:foldl(
           fun({P, VL, F}, {AllValid, ValueAcc}) ->
                   V = wrq:get_qs_value(P, hd(VL), RD),
                   {F(P, V, VL) andalso AllValid, [{P, V} | ValueAcc]}
           end,
           {true, []},
           [%% placeholder for eventual new parameters
            {"proto",  ["pbc", "http"], CheckOptionalParm}]) of
        {true, AssignedList} ->
            F = fun(P) -> proplists:get_value(P, AssignedList) end,
            {false, RD, Ctx#ctx{bucket = Bucket,
                                key    = Key,
                                proto  = list_to_atom(F("proto"))}};
        _ ->
            {true,
             error_response("invalid/insufficient parameters", RD),
             Ctx}
    end.


-spec allowed_methods(wm_reqdata(), ctx()) ->
    {[atom()], wm_reqdata(), ctx()}.
allowed_methods(RD, Ctx) ->
    {['GET'], RD, Ctx}.


-spec content_types_provided(wm_reqdata(), ctx()) ->
    {[{string(), atom()}], wm_reqdata(), ctx()}.
content_types_provided(RD, Ctx) ->
    {[{"application/json", make_response}], RD, Ctx}.


-spec make_response(wm_reqdata(), ctx()) ->
    {iolist(), wm_reqdata(), ctx()}.
make_response(RD, Ctx = #ctx{proto = Proto, bucket = <<>>, key = <<>>}) ->
    {riak_kv_apiep:get_entrypoints_json(Proto), RD, Ctx};
make_response(RD, Ctx = #ctx{proto = Proto, bucket = Bucket, key = Key}) ->
    {riak_kv_apiep:get_entrypoints_json(Proto, {Bucket, Key}), RD, Ctx}.

-spec last_modified(wm_reqdata(), ctx()) ->
    {string(), wm_reqdata(), ctx()}.
last_modified(RD, Ctx = #ctx{expiry_time = Exp}) ->
    {calendar:gregorian_seconds_to_datetime(
       719528 * 86400  %% seconds in 1970 years since 0 AD (man calendar)
       + cached_timebin(Exp)), RD, Ctx}.

-spec generate_etag(wm_reqdata(), ctx()) ->
    {string(), wm_reqdata(), ctx()}.
generate_etag(RD, Ctx = #ctx{expiry_time = Exp}) ->
    {riak_core_util:integer_to_list(cached_timebin(Exp), 62), RD, Ctx}.


%% ===================================================================
%% Local functions
%% ===================================================================

-spec error_response(string(), wm_reqdata()) ->
    wm_reqdata().
%% @doc Wrap wrq:set_resp_header() and log a warning as a side-effect
error_response(Msg, RD) ->
    lager:log(warning, self(), "bad request ~p from ~s: ~s",
              [RD#wm_reqdata.path, RD#wm_reqdata.peer, Msg]),
    wrq:set_resp_header("Content-Type", "text/plain",
                        wrq:append_to_response_body(Msg, RD)).


-spec unixtime() -> unixtime().
%% @doc Return current unixtime.
unixtime() ->
    {NowMega, NowSec, _} = os:timestamp(),
    NowMega * 1000 * 1000 + NowSec.

-spec cached_timebin(non_neg_integer()) -> unixtime().
%% #doc Get the timestamp of an epoch for which the response
%%      will be deemed valid.  It is simply a multiple of
%%      Binsize seconds.  It is used as a suitable ETag mark.
cached_timebin(Binsize) ->
    round(unixtime() / Binsize) * Binsize.
