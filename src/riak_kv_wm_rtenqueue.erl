%% --------------------------------------------------------------------------
%%
%% riak_kv_wm_rtenqueue - Webmachine resource for posting a bucket/key to the
%%                       realtime repl queue.
%%
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
%% --------------------------------------------------------------------------

%% @doc Resource for putting Riak bucket/key onto the realtime repl queue, over HTTP.
%%
%% Available operations:
%%
%% POST /rtq/Type/Bucket/Key
%%  POST /rtq/Bucket/Key
%%   POST a particular bucket/key to the realtime repl queue

-module(riak_kv_wm_rtenqueue).

%% webmachine resource exports
-export([
         init/1,
         is_authorized/2,
         forbidden/2,
         allowed_methods/2,
         resource_exists/2,
         malformed_request/2,
         service_available/2,
         process_post/2
        ]).

-record(ctx, {
              client,     %% :: riak_client()             %% the store client
              riak,       %% local | {node(), atom()}     %% params for riak client
              bucket_type :: binary(),                    %% bucket type (from uri)
              bucket      :: binary(),                    %% Bucket name (from uri)
              key         :: binary(),                    %% Key (from uri)
              r,            %% integer() - r-value for reads
              pr,           %% integer() - number of primary nodes required in preflist on read
              basic_quorum, %% boolean() - whether to use basic_quorum
              notfound_ok,  %% boolean() - whether to treat notfounds as successes
              prefix,       %% string() - prefix for resource uris
              doc,          %% {ok, riak_object()}|{error, term()} - the object found
              timeout,      %% integer() - passed-in timeout value in ms
              security    :: riak_core_security:context() %% security context
             }).

-type context() :: #ctx{}.

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

-spec init(proplists:proplist()) -> {ok, context()}.
%% @doc Initialize this resource.  This function extracts the
%%      'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{
            riak=proplists:get_value(riak, Props),
            bucket_type=proplists:get_value(bucket_type, Props)}}.

is_authorized(ReqData, Ctx) ->
    case riak_api_web_security:is_authorized(ReqData) of
        false ->
            {"Basic realm=\"Riak\"", ReqData, Ctx};
        {true, SecContext} ->
            {true, ReqData, Ctx#ctx{security=SecContext}};
        insecure ->
            %% XXX 301 may be more appropriate here, but since the http and
            %% https port are different and configurable, it is hard to figure
            %% out the redirect URL to serve.
            {{halt, 426}, wrq:append_to_resp_body(<<"Security is enabled and "
                    "Riak does not accept credentials over HTTP. Try HTTPS "
                    "instead.">>, ReqData), Ctx}
    end.

-spec service_available(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether or not a connection to Riak
%%      can be established. Also, extract query params.
service_available(RD, Ctx0=#ctx{riak=RiakProps}) ->
    Ctx = riak_kv_wm_utils:ensure_bucket_type(RD, Ctx0, #ctx.bucket_type),
    case riak_kv_wm_utils:get_riak_client(RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            {true,
             RD,
             Ctx#ctx{
               client=C,
               bucket=case wrq:path_info(bucket, RD) of
                         undefined -> undefined;
                         B -> list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, B))
                      end,
               key=case wrq:path_info(key, RD) of
                       undefined -> undefined;
                       K -> list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, K))
                   end
              }};
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

forbidden(RD, Ctx = #ctx{security=undefined}) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx};
forbidden(RD, Ctx) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
        false ->
            Res = riak_core_security:check_permission({"riak_kv.rtenqueue",
                                                       {Ctx#ctx.bucket_type, Ctx#ctx.bucket}},
                                                      Ctx#ctx.security),
            case Res of
                {false, Error, _} ->
                    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
                    {true, wrq:append_to_resp_body(
                             unicode:characters_to_binary(
                               Error, utf8, utf8), RD1), Ctx};
                {true, _} ->
                    {false, RD, Ctx}
            end
    end.

-spec allowed_methods(#wm_reqdata{}, context()) ->
    {[atom()], #wm_reqdata{}, context()}.
%% @doc Get the list of methods this resource supports.
%%      Properties allows, POST.
allowed_methods(RD, Ctx) ->
    {['POST'], RD, Ctx}.

resource_exists(RD, #ctx{bucket_type=BType}=Ctx) ->
    {riak_kv_wm_utils:bucket_type_exists(BType), RD, Ctx}.

-spec malformed_request(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
malformed_request(RD, Ctx) ->
    case malformed_timeout_param(RD, Ctx) of
        {false, RD2, Ctx2} ->
            malformed_rw_params(RD2, Ctx2);
        Mal ->
            Mal
    end.

-spec process_post(#wm_reqdata{}, context()) ->
    {true, #wm_reqdata{}, context()}.
%% @doc do the deed
process_post(RD, Ctx) ->
    #ctx{client=C, bucket_type=T, bucket=B, key=K,
        basic_quorum=Quorum, notfound_ok=NotFoundOK, r=R, pr=PR} = Ctx,
            Options = [
                        {basic_quorum, Quorum},
                        {notfound_ok, NotFoundOK},
                        {r, R},
                        {pr, PR}
                       ],
    Res = C:rt_enqueue(riak_kv_wm_utils:maybe_bucket_type(T,B), K, Options),
    case Res of
        ok ->
            {true, RD, Ctx};
        {error, Reason} ->
            handle_common_error(Reason, RD, Ctx)
    end.

-spec malformed_timeout_param(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check that the timeout parameter is are a
%%      string-encoded integer.  Store the integer value
%%      in context() if so.
malformed_timeout_param(RD, Ctx) ->
    case wrq:get_qs_value("timeout", none, RD) of
        none ->
            {false, RD, Ctx};
        TimeoutStr ->
            try
                Timeout = list_to_integer(TimeoutStr),
                {false, RD, Ctx#ctx{timeout=Timeout}}
            catch
                _:_ ->
                    {true,
                     wrq:append_to_resp_body(io_lib:format("Bad timeout "
                                                           "value ~p~n",
                                                           [TimeoutStr]),
                                             wrq:set_resp_header(?HEAD_CTYPE,
                                                                 "text/plain", RD)),
                     Ctx}
            end
    end.

-spec malformed_rw_params(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check that r, w, dw, and rw query parameters are
%%      string-encoded integers.  Store the integer values
%%      in context() if so.
malformed_rw_params(RD, Ctx) ->
    Res =
        lists:foldl(fun malformed_rw_param/2,
                    {false, RD, Ctx},
                    [{#ctx.r, "r", "default"},
                     {#ctx.pr, "pr", "default"}]),
    lists:foldl(fun malformed_boolean_param/2,
                Res,
                [{#ctx.basic_quorum, "basic_quorum", "default"},
                 {#ctx.notfound_ok, "notfound_ok", "default"}]).

-spec malformed_rw_param({Idx::integer(), Name::string(), Default::string()},
                         {boolean(), #wm_reqdata{}, context()}) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check that a specific r, or pr query param is a string-encoded
%% integer.  Store its result in context() if it is, or print an error
%% message in #wm_reqdata{} if it is not.
malformed_rw_param({Idx, Name, Default}, {Result, RD, Ctx}) ->
    case catch normalize_rw_param(wrq:get_qs_value(Name, Default, RD)) of
        P when (is_atom(P) orelse is_integer(P)) ->
            {Result, RD, setelement(Idx, Ctx, P)};
        _ ->
            {true,
             wrq:append_to_resp_body(
               io_lib:format("~s query parameter must be an integer or "
                   "one of the following words: 'one', 'quorum' or 'all'~n",
                             [Name]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

-spec malformed_boolean_param({Idx::integer(), Name::string(), Default::string()},
                              {boolean(), #wm_reqdata{}, context()}) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check that a specific query param is a
%%      string-encoded boolean.  Store its result in context() if it
%%      is, or print an error message in #wm_reqdata{} if it is not.
malformed_boolean_param({Idx, Name, Default}, {Result, RD, Ctx}) ->
    case string:to_lower(wrq:get_qs_value(Name, Default, RD)) of
        "true" ->
            {Result, RD, setelement(Idx, Ctx, true)};
        "false" ->
            {Result, RD, setelement(Idx, Ctx, false)};
        "default" ->
            {Result, RD, setelement(Idx, Ctx, default)};
        _ ->
            {true,
            wrq:append_to_resp_body(
              io_lib:format("~s query parameter must be true or false~n",
                            [Name]),
              wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

normalize_rw_param("default") -> default;
normalize_rw_param("one") -> one;
normalize_rw_param("quorum") -> quorum;
normalize_rw_param("all") -> all;
normalize_rw_param(V) -> list_to_integer(V).

handle_common_error(Reason, RD, Ctx) ->
    case {error, Reason} of
        {error, timeout} ->
            {{halt, 503},
                wrq:set_resp_header("Content-Type", "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("request timed out~n",[]),
                        RD)),
                Ctx};
        {error, notfound} ->
            {{halt, 404},
                wrq:set_resp_header("Content-Type", "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("not found~n",[]),
                        RD)),
                Ctx};
        {error, bucket_type_unknown} ->
            {{halt, 404},
             wrq:set_resp_body(
               io_lib:format("Unknown bucket type: ~s", [Ctx#ctx.bucket_type]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx};
        {error, {r_val_unsatisfied, Requested, Returned}} ->
            {{halt, 503},
                wrq:set_resp_header("Content-Type", "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("R-value unsatisfied: ~p/~p~n",
                            [Returned, Requested]),
                        RD)),
                Ctx};
        {error, {pr_val_unsatisfied, Requested, Returned}} ->
            {{halt, 503},
                wrq:set_resp_header("Content-Type", "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("PR-value unsatisfied: ~p/~p~n",
                            [Returned, Requested]),
                        RD)),
                Ctx};
        {error, failed} ->
            {{halt, 412}, RD, Ctx};
        {error, Err} ->
            {{halt, 500},
                wrq:set_resp_header("Content-Type", "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("Error:~n~p~n",[Err]),
                        RD)),
                Ctx}
    end.
