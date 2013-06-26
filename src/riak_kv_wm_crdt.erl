%% -------------------------------------------------------------------
%%
%% riak_kv_wm_counter: Webmachine resource for counters
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

%% @doc Resource for serving Counters over HTTP.
%%
%% Available operations:
%%
%% POST /buckets/Bucket/counters/Key
%%   Increment the counter at `Bucket', `Key' by
%%   the integer amount of the request body. If the request body cannot be
%%   parsed (by erlang `list_to_integer/1') then a `400 bad request'
%%   is the result.
%%   The following query params are accepted (@see `riak_kv_wm_object' docs, too):
%%
%%   <dl>
%%     <dt>w</dt><dd>The write quorum. See below for defaults and values.</dd>
%%     <dt>pw</dt><dd>The primary write quorum. See below for defaults and values.</dd>
%%     <dt>dw</dt><dd>The durable write quorum. See below for default and values.</dd>
%%     <dt>returnvalue</dt><dd>Boolean. Default is `false' if not provided. When `true'
%%                             the response body will be the value of the counter.</dd>
%%   </dl>
%%
%%  GET /buckets/Bucket/counters/Key
%%    Get the current value of the counter at `Bucket', `Key'. Result is a text/plain
%%    body with an integer value, or `not_found' if no counter exists at that resource location.
%%    The following query params are accepted:
%%
%%    <dl>
%%      <dt>r</dt><dd>Read quorum. See below for defaults and values.</dd>
%%      <dt>pr</dt><dd>Primary read quorum. See below for defaults and values.</dd>
%%      <dt>basic_quorum</dt><dd>Boolean. Return as soon as a quorum of responses are received
%%                               if true. Default is the bucket default, if absent.</dd>
%%      <dt>notfound_ok</dt><dd>Boolean. A `not_found` response from a vnode counts toward
%%                              `r' quorum if true. Default is the bucket default, if absent.</dd>
%%    </dl>
%%
%%   Quorum values (r/pr/w/pw/dw):
%%     <dl>
%%       <dt>default</dt<dd>Whatever the bucket default is. This is the value used
%%                          for any absent value.</dd>
%%      <dt>quorum</dt><dd>(Bucket N val / 2) + 1</dd>
%%      <dt>all</dt><dd>All replicas must respond</dd>
%%      <dt>one</dt><dd>Any one response is enough</dd>
%%      <dt>Integer</dt><dd>That specific number of vnodes must respond. Must be =< N</dd>
%%    </dl>
%% Please see http://docs.basho.com for details of all the quorum values and there effect.



-module(riak_kv_wm_crdt).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         forbidden/2,
         allowed_methods/2,
         malformed_request/2,
         resource_exists/2,
         content_types_provided/2,
         post_is_create/2,
         process_post/2,
         accept_doc_body/2,
         to_text/2,
         known_type/1
        ]).

%% @type context() = term()
-record(ctx, {api_version,  %% integer() - Determine which version of the API to use.
              bucket,       %% binary() - Bucket name (from uri)
              key,          %% binary() - Key (from uri)
              client,       %% riak_client() - the store client
              r,            %% integer() - r-value for reads
              w,            %% integer() - w-value for writes
              dw,           %% integer() - dw-value for writes
              rw,           %% integer() - rw-value for deletes
              pr,           %% integer() - number of primary nodes required in preflist on read
              pw,           %% integer() - number of primary nodes required in preflist on write
              basic_quorum, %% boolean() - whether to use basic_quorum
              notfound_ok,  %% boolean() - whether to treat notfounds as successes
              prefix,       %% string() - prefix for resource uris
              riak,         %% local | {node(), atom()} - params for riak client
              doc,          %% {ok, riak_object()}|{error, term()} - the object found
              bucketprops,  %% proplist() - properties of the bucket
              method,       %% atom() - HTTP method for the request
              crdt_op    :: term() | undefined %% A parsed CRDT Op
             }).
%% @type link() = {{Bucket::binary(), Key::binary()}, Tag::binary()}
%% @type index_field() = {Key::string(), Value::string()}

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").
-include_lib("riak_kv_types.hrl").

%% @spec init(proplist()) -> {ok, context()}
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{api_version=proplists:get_value(api_version, Props),
              prefix=proplists:get_value(prefix, Props),
              riak=proplists:get_value(riak, Props)}}.

service_available(RD, Ctx=#ctx{riak=RiakProps}) ->
    Type = riak_kv_crdt:to_type(wrq:path_info(crdt, RD)),
    case riak_kv_crdt:supported(Type) of
        true ->
            case riak_kv_wm_utils:get_riak_client(RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
                {ok, C} ->
                    {true,
                     RD,
                     Ctx#ctx{
                       method=wrq:method(RD),
                       client=C,
                       bucket=case wrq:path_info(bucket, RD) of
                                  undefined -> undefined;
                                  B -> list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, B))
                              end,
                       key=case wrq:path_info(key, RD) of
                               undefined -> undefined;
                               K -> list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, K))
                           end,
                       crdt_op=?CRDT_OP{mod=Type}
                      }};
                Error ->
                    {false,
                     wrq:set_resp_body(
                       io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
                       wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
                     Ctx}
            end;
        false  ->
            {false,
             wrq:set_resp_body(io_lib:format("~p are not supported.~n", [Type]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

forbidden(RD, Ctx) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx}.

allowed_methods(RD, Ctx) ->
    {['GET', 'POST'], RD, Ctx}.

malformed_request(RD, Ctx0) when Ctx0#ctx.method =:= 'POST' ->
    case riak_kv_crdt:parse_operation(Ctx0#ctx.crdt_op, wrq:req_body(RD)) of
        {error, Reason} ->
            {true,
             wrq:set_resp_body(
               io_lib:format("Invalid operation: ~p~n", [Reason]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx0};
        {ok, Op} ->
            Ctx = Ctx0#ctx{crdt_op = Op},
            case malformed_rw_params(RD, Ctx) of
                Result={true, _, _} ->
                    Result;
                {false, RWRD, RWCtx} ->
                    {false, RWRD, RWCtx}
            end
    end;
malformed_request(RD, Ctx) ->
    case malformed_rw_params(RD, Ctx) of
        Result = {true, _, _} ->
            Result;
        {false, ResRD, ResCtx} ->
            DocCtx = ensure_doc(ResCtx),
            case DocCtx#ctx.doc of
                {error, Reason} ->
                    handle_common_error(Reason, ResRD, DocCtx);
                _ ->
                    {false, ResRD, DocCtx}
            end
    end.

malformed_rw_params(RD, Ctx) ->
    Res =
    lists:foldl(fun malformed_rw_param/2,
                {false, RD, Ctx},
                [{#ctx.r, "r", "default"},
                 {#ctx.w, "w", "default"},
                 {#ctx.dw, "dw", "default"},
                 {#ctx.pw, "pw", "default"},
                 {#ctx.pr, "pr", "default"}]),
    lists:foldl(fun malformed_boolean_param/2,
                Res,
                [{#ctx.basic_quorum, "basic_quorum", "default"},
                 {#ctx.notfound_ok, "notfound_ok", "default"}]).

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

content_types_provided(RD, Ctx) ->
    {[{"text/plain", to_text}], RD, Ctx}.

resource_exists(RD, Ctx0) when Ctx0#ctx.method =:= 'GET' ->
    DocCtx = ensure_doc(Ctx0),
    case DocCtx#ctx.doc of
        {ok, _Doc} ->
            {true, RD, DocCtx};
        {error, _} ->
            %% This should never actually be reached because all the error
            %% conditions from ensure_doc are handled up in malformed_request.
            {false, RD, DocCtx}
    end;
resource_exists(RD, Ctx) ->
    {true, RD, Ctx}.

post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

process_post(RD, Ctx) -> accept_doc_body(RD, Ctx).

accept_doc_body(RD, Ctx=#ctx{bucket=B, key=K, client=C,
                            crdt_op=Op}) ->
    case allow_mult(B) of
        true ->
            Doc = riak_kv_crdt:new(B, K, Op?CRDT_OP.mod),
            Options = [{crdt_op, Op}] ++ return_value(RD),
            case C:put(Doc, [{w, Ctx#ctx.w}, {dw, Ctx#ctx.dw}, {pw, Ctx#ctx.pw}, {timeout, 60000} |
                                   Options]) of
                {error, Reason} ->
                    handle_common_error(Reason, RD, Ctx);
                ok ->
                    {true, RD, Ctx#ctx{doc={ok, Doc}}};
                {ok, RObj} ->
                    ?CRDT_OP{mod=Type} = Ctx#ctx.crdt_op,
                    Body = produce_doc_body(RObj, Type),
                    {true, wrq:append_to_resp_body(Body, RD), Ctx#ctx{doc={ok, RObj}}}
            end;
        false ->
            handle_common_error(allow_mult_false, RD, Ctx)
    end.

return_value(RD) ->
    case wrq:get_qs_value(?Q_RETURNVALUE, RD) of
        ?Q_TRUE ->
            [returnbody];
        _ ->
            []
end.

allow_mult(Bucket) ->
    proplists:get_value(allow_mult, riak_core_bucket:get_bucket(Bucket)).

to_text(RD, Ctx=#ctx{doc={ok, Doc}}) ->
    ?CRDT_OP{mod=Type} = Ctx#ctx.crdt_op,
    {produce_doc_body(Doc, Type), RD, Ctx}.

produce_doc_body(Doc, Type) ->
    riak_kv_crdt:value(Doc, Type).

ensure_doc(Ctx=#ctx{doc=undefined, key=undefined}) ->
    Ctx#ctx{doc={error, notfound}};
ensure_doc(Ctx=#ctx{doc=undefined, bucket=B, key=K, client=C, r=R,
        pr=PR, basic_quorum=Quorum, notfound_ok=NotFoundOK}) ->
    Ctx#ctx{doc=C:get(B, K, [{r, R}, {pr, PR},
                {basic_quorum, Quorum}, {notfound_ok, NotFoundOK}])};
ensure_doc(Ctx) -> Ctx.

handle_common_error(Reason, RD, Ctx) ->
    case {error, Reason} of
        {error, too_many_fails} ->
            {{halt, 503}, wrq:append_to_response_body("Too Many write failures"
                    " to satisfy W/DW\n", RD), Ctx};
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
        {error, {deleted, _VClock}} ->
            {{halt, 404},
                wrq:set_resp_header("Content-Type", "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("not found~n",[]),
                        RD)),
                Ctx};
        {error, {n_val_violation, N}} ->
            Msg = io_lib:format("Specified w/dw/pw values invalid for bucket"
                " n value of ~p~n", [N]),
            {{halt, 400}, wrq:append_to_response_body(Msg, RD), Ctx};
        {error, allow_mult_false} ->
            Msg = "Counters require bucket property 'allow_mult=true'",
            {{halt, 409}, wrq:append_to_response_body(Msg, RD), Ctx};
        {error, {r_val_unsatisfied, Requested, Returned}} ->
            {{halt, 503},
                wrq:set_resp_header("Content-Type", "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("R-value unsatisfied: ~p/~p~n",
                            [Returned, Requested]),
                        RD)),
                Ctx};
        {error, {w_val_unsatisfied, NumW, NumDW, W, DW}} ->
            {{halt, 503},
                wrq:set_resp_header("Content-Type", "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("W/DW-value unsatisfied: w=~p/~p dw=~p/~p~n",
                            [NumW, W, NumDW, DW]),
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
        {error, {pw_val_unsatisfied, Requested, Returned}} ->
            Msg = io_lib:format("PW-value unsatisfied: ~p/~p~n", [Returned,
                    Requested]),
            {{halt, 503}, wrq:append_to_response_body(Msg, RD), Ctx};
        {error, Err} ->
            {{halt, 500},
                wrq:set_resp_header("Content-Type", "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("Error:~n~p~n",[Err]),
                        RD)),
                Ctx}
    end.

%% @dc Webmachine dispatch guard for crdt resource
known_type(ReqData) ->
    undefined /= riak_kv_crdt:to_type(wrq:path_info(crdt, ReqData)).
