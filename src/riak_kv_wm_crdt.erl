%% -------------------------------------------------------------------
%%
%% riak_kv_wm_crdt: Webmachine resource for convergent data types
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
%% @doc Resource for serving data-types over HTTP.
%%
%% Available operations:
%%
%% GET /types/BucketType/buckets/Bucket/datatypes/Key
%%   Get the current value of the data-type at `BucketType', `Bucket', `Key'.
%%   Result is a JSON body with a structured value, or `404 Not Found' if no
%%   datatype exists at that resource location.
%%
%%   The format of the JSON response will be roughly
%%   <code>{"type":..., "value":..., "context":...}</code>, where the
%%   `type' is a string designating which data-type is presented, the
%%   `value' is a representation of the data-type's value (see below),
%%   and the `context' is the opaque context, if needed or requested.
%%
%%   The type and structure of the `value' field in the response
%%   depends on the `type' field.
%%   <dl>
%%     <dt>counter</dt><dd>an integer</dd>
%%     <dt>set</dt><dd>an array of strings</dd>
%%     <dt>map</dt><dd>an object where the fields are as described below.</dd>
%%   </dl>
%%
%%   The format of a field name in the map value determines both the
%%   name of the entry and the type, joined with an underscore. For
%%   example, a `register' with name `firstname' would be
%%   `"firstname_register"'. Valid types embeddable in a map are
%%   `counter', `flag', `register', `set', and `map'.
%%
%%   The following query params are accepted:
%%
%%   <dl>
%%     <dt>r</dt><dd>Read quorum. See below for defaults and values.</dd>
%%     <dt>pr</dt><dd>Primary read quorum. See below for defaults and values.</dd>
%%     <dt>basic_quorum</dt><dd>Boolean. Return as soon as a quorum of responses are received
%%                              if true. Default is the bucket default, if absent.</dd>
%%     <dt>notfound_ok</dt><dd>Boolean. A `not_found` response from a vnode counts toward
%%                             `r' quorum if true. Default is the bucket default, if absent.</dd>
%%     <dt>include_context</dt><dd>Boolean. If the datatype requires the opaque "context" for
%%                                 safe removal, include it in the response. Defaults to `true'.</dd>
%%   </dl>
%%
%% POST /types/BucketType/buckets/Bucket/datatypes
%% POST /types/BucketType/buckets/Bucket/datatypes/Key
%%   Mutate the data-type at `BucketType', `Bucket', `Key' by applying
%%   the submitted operation contained in a JSON payload. If `Key' is
%%   not specified, one will be generated for the client and included
%%   in the returned `Location' header.
%%
%%   The format of the operation payload depends on the data-type.
%%   <dl>
%%     <dt>counter</dt><dd>An integer, or an object containing a single field, either
%%                         `"increment"' or `"decrement"', and an associated integer value.</dd>
%%     <dt>set</dt><dd>An object containing any combination of `"add"', `"add_all"',
%%                     `"remove"', `"remove_all"' fields. `"add"' and `"remove"' should refer to
%%                     single string values, while `"add_all"' and `"remove_all"' should be arrays
%%                     of strings. The `"context"' field may be included.</dd>
%%     <dt>map</dt><dd>An object containing any of the fields `"add"', `"remove"', or `"update"'.
%%                     `"add"' and `"remove"' should be lists of field names as described above.
%%                     `"update"` should be an object containing fields and the operation to apply
%%                     to the type associated with the field.</dd>
%%     <dt>register (embedded in map only)</dt><dd>`{"assign":Value}' where `Value' is the new string
%%                                                 value of the register.</dd>
%%     <dt>flag (embedded in map only)</dt><dd>The string "enable" or "disable".</dd>
%%   </dl>
%%
%%   The following query params are accepted (@see `riak_kv_wm_object' docs, too):
%%
%%   <dl>
%%     <dt>w</dt><dd>The write quorum. See below for defaults and values.</dd>
%%     <dt>pw</dt><dd>The primary write quorum. See below for defaults and values.</dd>
%%     <dt>dw</dt><dd>The durable write quorum. See below for default and values.</dd>
%%     <dt>returnbody</dt><dd>Boolean. Default is `false' if not provided. When `true'
%%                             the response body will be the value of the datatype.</dd>
%%     <dt>include_context</dt><dd>Boolean. Default is `true' if not provided. When `true'
%%                             and `returnbody' is `true', the opaque context will be included.</dd>
%%   </dl>
%%
%%   Quorum values (r/pr/w/pw/dw):
%%     <dl>
%%      <dt>default</dt<dd>Whatever the bucket default is. This is the value used
%%                          for any absent value.</dd>
%%      <dt>quorum</dt><dd>(Bucket N val / 2) + 1</dd>
%%      <dt>all</dt><dd>All replicas must respond</dd>
%%      <dt>one</dt><dd>Any one response is enough</dd>
%%      <dt>Integer</dt><dd>That specific number of vnodes must respond. Must be =< N</dd>
%%    </dl>
%%
%% Please see http://docs.basho.com for details of all the quorum values and their effects.

-module(riak_kv_wm_crdt).
-record(ctx, {
          api_version,
          client, %% riak:local_client()
          bucket_type,
          bucket,
          key,
          crdt_type,
          data,
          module,
          r,
          w,
          dw,
          rw,
          pr,
          pw,
          basic_quorum,
          notfound_ok,
          include_context,
          returnbody,
          method,
          timeout,
          security
         }).
-include("riak_kv_wm_raw.hrl").
-include("riak_kv_types.hrl").

-export([
         init/1,
         service_available/2,
         malformed_request/2,
         is_authorized/2,
         forbidden/2,
         allowed_methods/2,
         content_types_provided/2,
         encodings_provided/2,
         resource_exists/2,
         process_post/2,           %% POST handler
         produce_json/2           %% GET/HEAD handler
        ]).

-include_lib("webmachine/include/webmachine.hrl").

init(Props) ->
    {ok, #ctx{api_version=proplists:get_value(api_version, Props)}}.

service_available(RD, Ctx0) ->
    Ctx = riak_kv_wm_utils:ensure_bucket_type(RD, Ctx0, #ctx.bucket_type),
    {ok, Client} = riak_kv_wm_utils:get_riak_client(
                     local, riak_kv_wm_utils:get_client_id(RD)),
    {true, RD,
     Ctx#ctx{client=Client,
             bucket=path_segment_to_bin(bucket, RD),
             key=path_segment_to_bin(key, RD),
             method=wrq:method(RD)}}.

allowed_methods(RD, Ctx) ->
    {['GET', 'HEAD', 'POST'], RD, Ctx}.

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
            halt_with_message(426,
                              <<"Security is enabled and Riak does not accept "
                                "credentials over HTTP. Try HTTPS instead.">>,
                              ReqData, Ctx)
    end.

malformed_request(RD, Ctx=#ctx{method='POST'}) ->
    malformed_check_post_ctype(RD, Ctx);
malformed_request(RD, Ctx) ->
    malformed_rw_params(RD, Ctx).

malformed_check_post_ctype(RD, Ctx) ->
    CType = wrq:get_req_header(?HEAD_CTYPE, RD),
    case mochiweb_util:parse_header(CType) of
        {"application/json",_} ->
            malformed_rw_params(RD, Ctx);
        _Other ->
            {{halt, 406}, RD, Ctx}
    end.

malformed_rw_params(RD, Ctx) ->
    Res = lists:foldl(fun malformed_rw_param/2,
                      {false, RD, Ctx},
                      [{#ctx.r,  "r",  "default"},
                       {#ctx.w,  "w",  "default"},
                       {#ctx.dw, "dw", "default"},
                       {#ctx.pw, "pw", "default"},
                       {#ctx.pr, "pr", "default"}]),
    Res1 = lists:foldl(fun malformed_boolean_param/2,
                       Res,
                       [{#ctx.basic_quorum,    "basic_quorum",    "default"},
                        {#ctx.notfound_ok,     "notfound_ok",     "default"},
                        {#ctx.include_context, "include_context", "true"},
                        {#ctx.returnbody,      "returnbody",      "false"}]),
    malformed_timeout_param(Res1).

malformed_rw_param({Idx, Name, Default}, {Result, RD, Ctx}) ->
    case catch normalize_rw_param(wrq:get_qs_value(Name, Default, RD)) of
        P when (is_atom(P) orelse is_integer(P)) ->
            {Result, RD, setelement(Idx, Ctx, P)};
        _ ->
            {true,
             error_response("~s query parameter must be an integer or "
                            "one of the following words: 'one', 'quorum' or 'all'~n",
                            [Name], RD),
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
             error_response("~s query parameter must be true or false~n",
                            [Name], RD),
             Ctx}
    end.

malformed_timeout_param({Result, RD, Ctx}) ->
    case wrq:get_qs_value("timeout", undefined, RD) of
        undefined ->
            {Result, RD, Ctx};
        TimeoutStr when is_list(TimeoutStr) ->
            try list_to_integer(TimeoutStr) of
                0 ->
                    {Result, RD, Ctx#ctx{timeout=infinity}};
                Timeout when is_integer(Timeout), Timeout > 0 ->
                    {Result, RD, Ctx#ctx{timeout=Timeout}};
                _Other ->
                    {true,
                     error_response("timeout query parameter must be an "
                                    "integer greater than 0 (or 0 for disabled), "
                                    "~s is invalid~n",
                                    [TimeoutStr], RD),
                     Ctx}
            catch
                error:badarg ->
                    {true,
                     error_response("timeout query parameter must be an "
                                    "integer greater than 0, ~s is invalid~n",
                                    [TimeoutStr], RD),
                     Ctx}
            end
    end.

forbidden(RD, Ctx) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
        false ->
            forbidden_check_security(RD, Ctx)
    end.

forbidden_check_security(RD, Ctx=#ctx{security=undefined}) ->
    forbidden_check_bucket_type(RD, Ctx);
forbidden_check_security(RD, Ctx=#ctx{bucket_type=BType, bucket=Bucket,
                                      security=SecContext, method=Method}) ->
    Perm = permission(Method),
    case riak_core_security:check_permission({Perm, {BType, Bucket}},
                                             SecContext) of
        {false, Error, _} ->
            {true, error_response(Error, RD), Ctx};
        {true, _} ->
            forbidden_check_bucket_type(RD, Ctx)
    end.

%% @doc Detects whether the requested object's bucket-type exists.
forbidden_check_bucket_type(RD, Ctx) ->
    case riak_kv_wm_utils:bucket_type_exists(Ctx#ctx.bucket_type) of
        true ->
            forbidden_check_crdt_type(RD, Ctx);
        false ->
            handle_common_error(bucket_type_unknown, RD, Ctx)
    end.

forbidden_check_crdt_type(RD, Ctx=#ctx{bucket_type = <<"default">>,
                                       bucket=B0,
                                       key=K0}) ->
    %% Only legacy/1.4 counters are supported in the default/undefined
    %% bucket type. Since we don't want to confuse semantics of the
    %% new types or duplicate code, we redirect to the old resource
    %% instead.
    B = mochiweb_util:quote_plus(B0),
    K = mochiweb_util:quote_plus(K0),
    CountersUrl = lists:flatten(
                    io_lib:format("/buckets/~s/counters/~s",[B, K])),
    halt_with_message(301,
                      "Counters in the default bucket-type should use the "
                      "legacy URL\n",
                      wrq:set_resp_header("Location", CountersUrl, RD),
                      Ctx);
forbidden_check_crdt_type(RD, Ctx=#ctx{bucket_type=T, bucket=B}) ->
    case riak_core_bucket:get_bucket({T, B}) of
        BProps when is_list(BProps) ->
            DataType = proplists:get_value(datatype, BProps),
            AllowMult = proplists:get_value(allow_mult, BProps),
            Mod = riak_kv_crdt:to_mod(DataType),
            case {AllowMult, riak_kv_crdt:supported(Mod)} of
                {false, _} ->
                    {true, error_response("Bucket must be allow_mult=true~n",
                                          [], RD), Ctx};
                {_, false} ->
                    {true, error_response("Bucket datatype '~s' is not a "
                                          "supported type.~n", [DataType], RD), Ctx};
                _ ->
                    {false, RD, Ctx#ctx{module=Mod, crdt_type=DataType}}
            end;
        {error, no_type} ->
            %% This should be handled by forbidden_check_bucket_type/2
            handle_common_error(bucket_type_unknown, RD, Ctx)
    end.

content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_json}], RD, Ctx}.

encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.

resource_exists(RD, Ctx=#ctx{method='POST'}) ->
    %% When submitting an operation, the resource always exists, even
    %% if key is unspecified.
    {true, RD, Ctx};
resource_exists(RD, Ctx=#ctx{key=undefined}) ->
    %% When fetching, if the key does not exist, we should give a not
    %% found.
    handle_common_error(notfound, RD, Ctx);
resource_exists(RD, Ctx=#ctx{client=C, bucket_type=T, bucket=B, key=K, module=Mod}) ->
    Options = make_options(Ctx),
    case C:get({T,B}, K, [{crdt_op, Mod}|Options]) of
        {ok, O} ->
            {true, RD, Ctx#ctx{data=O}};
        {error, Reason} ->
            handle_common_error(Reason, RD, Ctx)
    end.

process_post(RD0, Ctx0=#ctx{client=C, bucket_type=T, bucket=B, module=Mod}) ->
    case check_post_body(RD0, Ctx0) of
        {error, RD} ->
            {{halt, 400}, RD, Ctx0};
        {ok, {_Type, Op, OpCtx}} ->
            {RD, Ctx} = maybe_generate_key(RD0, Ctx0),
            O = riak_kv_crdt:new({T, B}, Ctx#ctx.key, Mod),
            Options0 = make_options(Ctx),
            CrdtOp = make_operation(Mod, Op, OpCtx),
            Options = [{crdt_op, CrdtOp},
                       {retry_put_coordinator_failure,false}|Options0],
            case C:put(O, Options) of
                ok ->
                    {true, RD, Ctx};
                {ok, RObj} ->
                    {Body, RD1, Ctx1} = produce_json(RD, Ctx#ctx{data=RObj}),
                    {true,
                     wrq:set_resp_body(Body, wrq:set_resp_header(
                                               ?HEAD_CTYPE, "application/json",
                                               RD1)),
                     Ctx1};
                {error, Reason} ->
                    handle_common_error(Reason, RD, Ctx)
            end
    end.

produce_json(RD, Ctx=#ctx{module=Mod, data=RObj, include_context=I}) ->
    Type = riak_kv_crdt:from_mod(Mod),
    {{RespCtx, Value}, Stats} = riak_kv_crdt:value(RObj, Mod),
    [ riak_kv_stat:update(S) || S <- Stats ],
    Body = riak_kv_crdt_json:fetch_response_to_json(
                     Type, Value, get_context(RespCtx,I), ?MOD_MAP),
    {mochijson2:encode(Body), RD, Ctx}.

%% Internal functions

check_post_body(RD, #ctx{crdt_type=CRDTType}) ->
    try
        JSON = mochijson2:decode(wrq:req_body(RD)),
        Data = {CRDTType, _Op, _Context} =
            riak_kv_crdt_json:update_request_from_json(CRDTType, JSON,
                                                       ?MOD_MAP),
        {ok, Data}
    catch
        throw:{invalid_operation, {BadType, BadOp}} ->
            {error,
             error_response("Invalid operation on datatype '~s': ~s~n",
                            [BadType, mochijson2:encode(BadOp)], RD)};
        throw:{invalid_field_name, Field} ->
            {error,
             error_response("Invalid map field name '~s'~n", [Field], RD)};
        throw:invalid_utf8 ->
            {error,
             error_response("Malformed JSON submitted, invalid UTF-8", RD)};
        _Other:Reason ->
            {error,
             error_response("Couldn't decode JSON: ~p~n", [Reason], RD)}
    end.


%% @doc Converts a query string value into a quorum value.
normalize_rw_param("default") -> default;
normalize_rw_param("one")     -> one;
normalize_rw_param("quorum")  -> quorum;
normalize_rw_param("all")     -> all;
normalize_rw_param(V)         -> list_to_integer(V).

%% @doc Returns the appropriate permission for a given request method.
permission('POST') -> "riak_kv.put";
permission('GET')  -> "riak_kv.get";
permission('HEAD') -> "riak_kv.get".

%% @doc Halts the resource with the given formatted response message.
halt_with_message(Status, Format, Args, RD, Ctx) ->
    halt_with_message(Status, io_lib:format(Format, Args), RD, Ctx).

%% @doc Halts the resource with the given response message.
halt_with_message(Status, Message, RD, Ctx) ->
    {{halt, Status}, error_response(Message,RD), Ctx}.

%% @doc Outputs a formatted error response with the text/plain content type.
error_response(Fmt, Args, RD) ->
    error_response(io_lib:format(Fmt, Args), RD).

%% @doc Outputs an error response with the text/plain content type.
error_response(Msg, RD) ->
    wrq:set_resp_header(?HEAD_CTYPE, "text/plain",
                        wrq:append_to_response_body(Msg, RD)).

%% @doc Converts an error into the appropriate resource halt and message.
handle_common_error(Reason, RD, Ctx) ->
    case Reason of
        too_many_fails ->
            halt_with_message(503, "Too many write failures to satisfy W/DW\n",
                              RD, Ctx);
        timeout ->
            halt_with_message(503, "request timed out\n", RD, Ctx);
        notfound ->
            {{halt, 404}, notfound_body(RD, Ctx), Ctx};
        bucket_type_unknown ->
            halt_with_message(404, "Unknown bucket type: ~s~n",
                              [Ctx#ctx.bucket_type], RD, Ctx);
        {deleted, _VClock} ->
            {{halt,404},
             wrq:set_resp_header(?HEAD_DELETED, "true", notfound_body(RD, Ctx)),
                              Ctx};
        {n_val_violation, N} ->
            halt_with_message(400,
                              "Specified w/dw/pw values invalid for bucket n "
                              "value of ~p~n",[N], RD, Ctx);
        {r_val_unsatisfied, Requested, Returned} ->
            halt_with_message(503, "R-value unsatisfied: ~p/~p~n",
                              [Returned, Requested], RD, Ctx);
        {dw_val_unsatisfied, DW, NumDW} ->
            halt_with_message(503, "DW-value unsatisfied: ~p/~p~n", [NumDW, DW],
                              RD, Ctx);
        {pr_val_unsatisfied, Requested, Returned} ->
            halt_with_message(503, "PR-value unsatisfied: ~p/~p~n",
                              [Returned, Requested], RD, Ctx);
        {pw_val_unsatisfied, Requested, Returned} ->
            halt_with_message(503, "PW-value unsatisfied: ~p/~p~n",
                              [Returned, Requested], RD, Ctx);
        failed ->
            halt_with_message(412, "", RD, Ctx);
        Err ->
            halt_with_message(500, "Error:~n~p~n", [Err], RD, Ctx)
    end.

%% @doc Converts a path segment into a binary by key.
path_segment_to_bin(Key, RD) ->
    Segment = proplists:get_value(Key, wrq:path_info(RD)),
    case Segment of
        undefined -> undefined;
        _ ->
            list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, Segment))
    end.

%% @doc If the key is not submitted on POST, generate a key and set
%% the appropriate redirect location.
maybe_generate_key(RD, Ctx=#ctx{api_version=V, bucket_type=T, bucket=B,
                                key=undefined}) ->
    K = riak_core_util:unique_id_62(),
    {wrq:set_resp_header("Location",
                         riak_kv_wm_utils:format_uri(T, B, K, undefined, V), RD),
     Ctx#ctx{key=list_to_binary(K)}};
maybe_generate_key(RD, Ctx) ->
    {RD, Ctx}.

make_operation(Mod, Op, Ctx) ->
    #crdt_op{mod=Mod, op=Op, ctx=Ctx}.

get_context(_Ctx, false) ->
    undefined;
get_context(Ctx, true) ->
    Ctx.

make_options(Ctx) ->
    OptList = [{r, Ctx#ctx.r},
               {w, Ctx#ctx.w},
               {dw, Ctx#ctx.dw},
               {rw, Ctx#ctx.rw},
               {pr, Ctx#ctx.pr},
               {pw, Ctx#ctx.pw},
               {basic_quorum, Ctx#ctx.basic_quorum},
               {notfound_ok, Ctx#ctx.notfound_ok},
               {timeout, Ctx#ctx.timeout},
               {returnbody, Ctx#ctx.returnbody}],
    [ {K,V} || {K,V} <- OptList, V /= default, V /= undefined ].

notfound_body(RD, #ctx{module=Mod}) ->
    JSON = {struct, [{<<"type">>, atom_to_binary(riak_kv_crdt:from_mod(Mod), utf8)},
                     {<<"error">>, <<"notfound">>}]},
    wrq:set_resp_header(?HEAD_CTYPE, "application/json",
                        wrq:set_resp_body(mochijson2:encode(JSON), RD)).
