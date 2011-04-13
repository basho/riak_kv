%% -------------------------------------------------------------------
%%
%% raw_http_resource: Webmachine resource for serving Riak data
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

%% @doc Resource for serving Riak objects over HTTP.
%%
%% Available operations:
%%
%% GET /Prefix
%%   Get information about available buckets.
%%   Include the query param "buckets=true" to get a list of buckets.
%%   The bucket list is excluded by default, because generating it is
%%   expensive.
%%
%% GET /Prefix/Bucket
%%   Get information about the named Bucket, in JSON form:
%%     {"props":{Prop1:Val1,Prop2:Val2,...},
%%      "keys":[Key1,Key2,...]}.
%%   Each bucket property will be included in the "props" object.
%%   "linkfun" and "chash_keyfun" properties will be encoded as
%%   JSON objects of the form:
%%     {"mod":ModuleName,
%%      "fun":FunctionName}
%%   Where ModuleName and FunctionName are each strings representing
%%   a module and function.
%%   Including the query param "props=false" will cause the "props"
%%   field to be omitted from the response.
%%   Including the query param "keys=false" will cause the "keys"
%%   field to be omitted from the response.
%%
%% PUT /Prefix/Bucket
%%   Modify bucket properties.
%%   Content-type must be application/json, and the body must have
%%   the form:
%%     {"props":{Prop:Val}}
%%   Where the "props" object takes the same form as returned from
%%   a GET of the same resource.
%%
%% POST /Prefix/Bucket
%%   Equivalent to "PUT /Prefix/Bucket/Key" where Key is chosen
%%   by the server.
%%
%% GET /Prefix/Bucket/Key
%%   Get the data stored in the named Bucket under the named Key.
%%   Content-type of the response will be whatever incoming
%%   Content-type was used in the request that stored the data.
%%   Additional headers will include:
%%     X-Riak-Vclock: The vclock of the object.
%%     Link: The links the object has
%%     Etag: The Riak "vtag" metadata of the object
%%     Last-Modified: The last-modified time of the object
%%     Encoding: The value of the incoming Encoding header from
%%       the request that stored the data.
%%     X-Riak-Meta-: Any headers prefixed by X-Riak-Meta- supplied
%%       on PUT are returned verbatim
%%   Specifying the query param "r=R", where R is an integer will
%%   cause Riak to use R as the r-value for the read request. A
%%   default r-value of 2 will be used if none is specified.
%%   If the object is found to have siblings (only possible if the
%%   bucket property "allow_mult" has been set to true), then
%%   Content-type will be text/plain; Link, Etag, and Last-Modified
%%   headers will be omitted; and the body of the response will
%%   be a list of the vtags of each sibling.  To request a specific
%%   sibling, include the query param "vtag=V", where V is the vtag
%%   of the sibling you want.
%%
%% PUT /Prefix/Bucket/Key
%%   Store new data in the named Bucket under the named Key.
%%   A Content-type header *must* be included in the request.  The
%%   value of this header will be used in the response to subsequent
%%   GET requests.
%%   The body of the request will be stored literally as the value
%%   of the riak_object, and will be served literally as the body of
%%   the response to subsequent GET requests.
%%   Include an X-Riak-Vclock header to modify data without creating
%%   siblings.
%%   Include a Link header to set the links of the object.
%%   Include an Encoding header if you would like an Encoding header
%%   to be included in the response to subsequent GET requests.
%%   Include custom metadata using headers prefixed with X-Riak-Meta-.
%%   They will be returned verbatim on subsequent GET requests.
%%   Specifying the query param "w=W", where W is an integer will
%%   cause Riak to use W as the w-value for the write request. A
%%   default w-value of 2 will be used if none is specified.
%%   Specifying the query param "dw=DW", where DW is an integer will
%%   cause Riak to use DW as the dw-value for the write request. A
%%   default dw-value of 0 will be used if none is specified.
%%   Specifying the query param "r=R", where R is an integer will
%%   cause Riak to use R as the r-value for the read request (used
%%   to determine whether or not the resource exists). A default
%%   r-value of 2 will be used if none is specified.
%%
%% POST /Prefix/Bucket/Key
%%   Equivalent to "PUT /Prefix/Bucket/Key" (useful for clients that
%%   do not support the PUT method).
%%
%% DELETE /Prefix/Bucket/Key
%%   Delete the data stored in the named Bucket under the named Key.
%%   Specifying the query param "rw=RW", where RW is an integer will
%%   cause Riak to use RW as the rw-value for the delete request. A
%%   default rw-value of 2 will be used if none is specified.
%%
%% Webmachine dispatch lines for this resource should look like:
%%
%%  {["riak"],
%%   riak_kv_wm_raw,
%%   [{prefix, "riak"},
%%    {riak, local} %% or {riak, {'riak@127.0.0.1', riak_cookie}}
%%   ]}.
%%  {["riak", bucket],
%%   riak_kv_wm_raw,
%%   [{prefix, "riak"},
%%    {riak, local} %% or {riak, {'riak@127.0.0.1', riak_cookie}}
%%   ]}.
%%  {["riak", bucket, key],
%%   riak_kv_wm_raw,
%%   [{prefix, "riak"},
%%    {riak, local} %% or {riak, {'riak@127.0.0.1', riak_cookie}}
%%   ]}.
%%
%% These example dispatch lines will expose this resource at /riak,
%% /riak/Bucket, and /riak/Bucket/Key.  The resource will attempt to
%% connect to Riak on the same Erlang node one which the resource
%% is executing.  Using the alternate {riak, {Node, Cookie}} form
%% will cause the resource to connect to riak on the specified
%% Node with the specified Cookie.
-module(riak_kv_wm_raw).
-author('Bryan Fink <bryan@basho.com>').

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         allowed_methods/2,
         allow_missing_post/2,
         malformed_request/2,
         resource_exists/2,
         last_modified/2,
         generate_etag/2,
         content_types_provided/2,
         charsets_provided/2,
         encodings_provided/2,
         content_types_accepted/2,
         produce_toplevel_body/2,
         produce_bucket_body/2,
         accept_bucket_body/2,
         post_is_create/2,
         create_path/2,
         process_post/2,
         produce_doc_body/2,
         accept_doc_body/2,
         produce_sibling_message_body/2,
         produce_multipart_body/2,
         multiple_choices/2,
         delete_resource/2
        ]).

%% utility exports (used in raw_http_link_walker_resource)
-export([
         vclock_header/1,
         format_link/2, format_link/4,
         multipart_encode_body/3
        ]).

%% @type context() = term()
-record(ctx, {bucket,       %% binary() - Bucket name (from uri)
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
              vtag,         %% string() - vtag the user asked for
              bucketprops,  %% proplist() - properties of the bucket
              links,        %% [link()] - links of the object
              method        %% atom() - HTTP method for the request
             }).
%% @type link() = {{Bucket::binary(), Key::binary()}, Tag::binary()}

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

%% @spec init(proplist()) -> {ok, context()}
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{prefix=proplists:get_value(prefix, Props),
              riak=proplists:get_value(riak, Props)}}.

%% @spec service_available(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether or not a connection to Riak
%%      can be established.  This function also takes this
%%      opportunity to extract the 'bucket' and 'key' path
%%      bindings from the dispatch, as well as any vtag
%%      query parameter.
service_available(RD, Ctx=#ctx{riak=RiakProps}) ->
    case get_riak_client(RiakProps, get_client_id(RD)) of
        {ok, C} ->
            {true,
             RD,
             Ctx#ctx{
               method=wrq:method(RD),
               client=C,
               bucket=case wrq:path_info(bucket, RD) of
                         undefined -> undefined;
                         B -> list_to_binary(B)
                      end,
               key=case wrq:path_info(key, RD) of
                       undefined -> undefined;
                       K -> list_to_binary(K)
                   end,
               vtag=wrq:get_qs_value(?Q_VTAG, RD)
              }};
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

%% @spec get_riak_client(local|{node(),Cookie::atom()}, term()) ->
%%          {ok, riak_client()} | error()
%% @doc Get a riak_client.
get_riak_client(local, ClientId) ->
    riak:local_client(ClientId);
get_riak_client({Node, Cookie}, ClientId) ->
    erlang:set_cookie(node(), Cookie),
    riak:client_connect(Node, ClientId).

%% @spec get_client_id(reqdata()) -> term()
%% @doc Extract the request's preferred client id from the
%%      X-Riak-ClientId header.  Return value will be:
%%        'undefined' if no header was found
%%        32-bit binary() if the header could be base64-decoded
%%           into a 32-bit binary
%%        string() if the header could not be base64-decoded
%%           into a 32-bit binary
get_client_id(RD) ->
    case wrq:get_req_header(?HEAD_CLIENT, RD) of
        undefined -> undefined;
        RawId ->
            case catch base64:decode(RawId) of
                ClientId= <<_:32>> -> ClientId;
                _ -> RawId
            end
    end.

%% @spec allowed_methods(reqdata(), context()) ->
%%          {[method()], reqdata(), context()}
%% @doc Get the list of methods this resource supports.
%%      HEAD, GET, POST, and PUT are supported at both
%%      the bucket and key levels.  DELETE is supported
%%      at the key level only.
allowed_methods(RD, Ctx=#ctx{bucket=undefined}) ->
    %% top-level: no modification allowed
    {['HEAD', 'GET'], RD, Ctx};
allowed_methods(RD, Ctx=#ctx{key=undefined}) ->
    %% bucket-level: no delete
    {['HEAD', 'GET', 'POST', 'PUT'], RD, Ctx};
allowed_methods(RD, Ctx) ->
    %% key-level: just about anything
    {['HEAD', 'GET', 'POST', 'PUT', 'DELETE'], RD, Ctx}.

%% @spec allow_missing_post(reqdata(), context()) ->
%%           {true, reqdata(), context()}
%% @doc Makes POST and PUT equivalent for creating new
%%      bucket entries.
allow_missing_post(RD, Ctx) ->
    {true, RD, Ctx}.

%% @spec is_bucket_put(reqdata(), context()) -> boolean()
%% @doc Determine whether this request is of the form
%%      PUT /Prefix/Bucket
%%      This method expects the 'key' path binding to have
%%      been set in the 'key' field of the context().
is_bucket_put(RD, Ctx) ->
    {undefined, 'PUT'} =:= {Ctx#ctx.key, wrq:method(RD)}.

%% @spec malformed_request(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether query parameters, request headers,
%%      and request body are badly-formed.
%%      Body format is checked to be valid JSON, including
%%      a "props" object for a bucket-PUT.  Body format
%%      is not tested for a key-level request (since the
%%      body may be any content the client desires).
%%      Query parameters r, w, dw, and rw are checked to
%%      be valid integers.  Their values are stored in
%%      the context() at this time.
%%      Link headers are checked for the form:
%%        &lt;/Prefix/Bucket/Key&gt;; riaktag="Tag",...
%%      The parsed links are stored in the context()
%%      at this time.
malformed_request(RD, Ctx) when Ctx#ctx.method =:= 'POST'
                                orelse Ctx#ctx.method =:= 'PUT' ->
    case is_bucket_put(RD, Ctx) of
        true ->
            malformed_bucket_put(RD, Ctx);
        false ->
            case wrq:get_req_header("Content-Type", RD) of
                undefined ->
                    {true, missing_content_type(RD), Ctx};
                _ ->
                    case malformed_rw_params(RD, Ctx) of
                        Result={true, _, _} -> Result;
                        {false, RWRD, RWCtx} ->
                            malformed_link_headers(RWRD, RWCtx)
                    end
            end
    end;
malformed_request(RD, Ctx) ->
    {ResBool, ResRD, ResCtx} = case malformed_rw_params(RD, Ctx) of
        Result={true, _, _} -> Result;
        {false, RWRD, RWCtx} ->
            malformed_link_headers(RWRD, RWCtx)
    end,
    case ((ResBool == true) orelse (ResCtx#ctx.key == undefined)) of
        true ->
            {ResBool, ResRD, ResCtx};
        false ->
            DocCtx = ensure_doc(ResCtx),
            case DocCtx#ctx.doc of
                {error, notfound} ->
                    {{halt, 404},
                     wrq:set_resp_header("Content-Type", "text/plain",
                                         wrq:append_to_response_body(
                                           io_lib:format("not found~n",[]),
                                           ResRD)),
                     DocCtx};
                {error, timeout} ->
                    {{halt, 503},
                     wrq:set_resp_header("Content-Type", "text/plain",
                                      wrq:append_to_response_body(
                                        io_lib:format("request timed out~n",[]),
                                        ResRD)),
                     DocCtx};
                {error, {n_val_violation, _}} ->
                    {{halt, 400},
                     wrq:set_resp_header("Content-Type", "text/plain",
                                  wrq:append_to_response_body(
                                    io_lib:format("R-value unsatisfiable~n",[]),
                                    ResRD)),
                     DocCtx};
                {error, {r_val_unsatisfied, Requested, Returned}} ->
                    {{halt, 503},
                     wrq:set_resp_header("Content-Type", "text/plain",
                                  wrq:append_to_response_body(
                                    io_lib:format("R-value unsatisfied: ~p/~p~n",
                                        [Returned, Requested]),
                                    ResRD)),
                    DocCtx};
                {error, {pr_val_unsatisfied, Requested, Returned}} ->
                    {{halt, 503},
                    wrq:set_resp_header("Content-Type", "text/plain",
                                 wrq:append_to_response_body(
                                   io_lib:format("PR-value unsatisfied: ~p/~p~n",
                                        [Returned, Requested]),
                                   ResRD)),
                    DocCtx};
                {error, Err} ->
                    {{halt, 500},
                     wrq:set_resp_header("Content-Type", "text/plain",
                                  wrq:append_to_response_body(
                                    io_lib:format("Error:~n~p~n",[Err]),
                                    ResRD)),
                     DocCtx};
                _ ->
                    {false, ResRD, DocCtx}
            end
    end.

%% @spec malformed_bucket_put(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Check the JSON format of a bucket-level PUT.
%%      Must be a valid JSON object, containing a "props" object.
malformed_bucket_put(RD, Ctx) ->
    case catch mochijson2:decode(wrq:req_body(RD)) of
        {struct, Fields} ->
            case proplists:get_value(?JSON_PROPS, Fields) of
                {struct, Props} ->
                    {false, RD, Ctx#ctx{bucketprops=Props}};
                _ ->
                    {true, bucket_format_message(RD), Ctx}
            end;
        _ ->
            {true, bucket_format_message(RD), Ctx}
    end.

%% @spec bucket_format_message(reqdata()) -> reqdata()
%% @doc Put an error about the format of the bucket-PUT body
%%      in the response body of the reqdata().
bucket_format_message(RD) ->
    wrq:append_to_resp_body(
      ["bucket PUT must be a JSON object of the form:\n",
       "{\"",?JSON_PROPS,"\":{...bucket properties...}}"],
      wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)).

%% @spec malformed_rw_params(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Check that r, w, dw, and rw query parameters are
%%      string-encoded integers.  Store the integer values
%%      in context() if so.
malformed_rw_params(RD, Ctx) ->
    Res =
    lists:foldl(fun malformed_rw_param/2,
                {false, RD, Ctx},
                [{#ctx.r, "r", "default"},
                 {#ctx.w, "w", "default"},
                 {#ctx.dw, "dw", "default"},
                 {#ctx.rw, "rw", "default"},
                 {#ctx.pw, "pw", "default"},
                 {#ctx.pr, "pr", "default"}]),
    lists:foldl(fun malformed_boolean_param/2,
                Res,
                [{#ctx.basic_quorum, "basic_quorum", "default"},
                 {#ctx.notfound_ok, "notfound_ok", "default"}]).

%% @spec malformed_rw_param({Idx::integer(), Name::string(), Default::string()},
%%                          {boolean(), reqdata(), context()}) ->
%%          {boolean(), reqdata(), context()}
%% @doc Check that a specific r, w, dw, or rw query param is a
%%      string-encoded integer.  Store its result in context() if it
%%      is, or print an error message in reqdata() if it is not.
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

%% @spec malformed_boolean_param({Idx::integer(), Name::string(), Default::string()},
%%                          {boolean(), reqdata(), context()}) ->
%%          {boolean(), reqdata(), context()}
%% @doc Check that a specific query param is a
%%      string-encoded boolean.  Store its result in context() if it
%%      is, or print an error message in reqdata() if it is not.
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

%% @spec malformed_link_headers(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Check that the Link header in the request() is valid.
%%      Store the parsed links in context() if the header is valid,
%%      or print an error in reqdata() if it is not.
%%      A link header should be of the form:
%%        &lt;/Prefix/Bucket/Key&gt;; riaktag="Tag",...
malformed_link_headers(RD, Ctx) ->
    case catch get_link_heads(RD, Ctx) of
        Links when is_list(Links) ->
            {false, RD, Ctx#ctx{links=Links}};
        _Error ->
            {true,
             wrq:append_to_resp_body(
               io_lib:format("Invalid Link header. Links must be of the form~n"
                             "</~s/BUCKET/KEY>; riaktag=\"TAG\"~n",
                             [Ctx#ctx.prefix]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

%% @spec content_types_provided(reqdata(), context()) ->
%%          {[{ContentType::string(), Producer::atom()}], reqdata(), context()}
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for bucket-level GET requests
%%      The content-type for a key-level request is the content-type that
%%      was used in the PUT request that stored the document in Riak.
content_types_provided(RD, Ctx=#ctx{bucket=undefined}) ->
    %% top-level: JSON description only
    {[{"application/json", produce_toplevel_body}], RD, Ctx};
content_types_provided(RD, Ctx=#ctx{key=undefined}) ->
    %% bucket-level: JSON description only
    {[{"application/json", produce_bucket_body}], RD, Ctx};
content_types_provided(RD, Ctx=#ctx{method=Method}=Ctx) when Method =:= 'PUT';
                                                             Method =:= 'POST' ->
    {ContentType, _} = extract_content_type(RD),
    {[{ContentType, produce_doc_body}], RD, Ctx};
content_types_provided(RD, Ctx0) ->
    DocCtx = ensure_doc(Ctx0),
    %% we can assume DocCtx#ctx.doc is {ok,Doc} because of malformed_request
    case select_doc(DocCtx) of
        {MD, V} ->
            {[{get_ctype(MD,V), produce_doc_body}], RD, DocCtx};
        multiple_choices ->
            {[{"text/plain", produce_sibling_message_body},
              {"multipart/mixed", produce_multipart_body}], RD, DocCtx}
    end.

%% @spec charsets_provided(reqdata(), context()) ->
%%          {no_charset|[{Charset::string(), Producer::function()}],
%%           reqdata(), context()}
%% @doc List the charsets available for representing this resource.
%%      No charset will be specified for a bucket-level request.
%%      The charset for a key-level request is the charset that was used
%%      in the PUT request that stored the document in Riak (none if
%%      no charset was specified at PUT-time).
charsets_provided(RD, Ctx=#ctx{key=undefined}) ->
    %% default charset for top-level and bucket-level requests
    {no_charset, RD, Ctx};
charsets_provided(RD, #ctx{method=Method}=Ctx) when Method =:= 'PUT';
                                                    Method =:= 'POST' ->
    case extract_content_type(RD) of
        {_, undefined} ->
            {no_charset, RD, Ctx};
        {_, Charset} ->
            {[{Charset, fun(X) -> X end}], RD, Ctx}
    end;

charsets_provided(RD, Ctx0) ->
    DocCtx = ensure_doc(Ctx0),
    case DocCtx#ctx.doc of
        {ok, _} ->
            case select_doc(DocCtx) of
                {MD, _} ->
                    case dict:find(?MD_CHARSET, MD) of
                        {ok, CS} ->
                            {[{CS, fun(X) -> X end}], RD, DocCtx};
                        error ->
                            {no_charset, RD, DocCtx}
                    end;
                multiple_choices ->
                    {no_charset, RD, DocCtx}
            end;
        {error, _} ->
            {no_charset, RD, DocCtx}
    end.

%% @spec encodings_provided(reqdata(), context()) ->
%%          {[{Encoding::string(), Producer::function()}], reqdata(), context()}
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available for bucket-level requests.
%%      The encoding for a key-level request is the encoding that was
%%      used in the PUT request that stored the document in Riak, or
%%      "identity" and "gzip" if no encoding was specified at PUT-time.
encodings_provided(RD, Ctx=#ctx{key=undefined}) ->
    %% identity and gzip for top-level and bucket-level requests
    {default_encodings(), RD, Ctx};
encodings_provided(RD, Ctx0) ->
    DocCtx = ensure_doc(Ctx0),
    case DocCtx#ctx.doc of
        {ok, _} ->
            case select_doc(DocCtx) of
                {MD, _} ->
                    case dict:find(?MD_ENCODING, MD) of
                        {ok, Enc} ->
                            {[{Enc, fun(X) -> X end}], RD, DocCtx};
                        error ->
                            {default_encodings(), RD, DocCtx}
                    end;
                multiple_choices ->
                    {default_encodings(), RD, DocCtx}
            end;
        {error, _} ->
            {default_encodings(), RD, DocCtx}
    end.

%% @spec default_encodings() -> [{Encoding::string(), Producer::function()}]
%% @doc The default encodings available: identity and gzip.
default_encodings() ->
    [{"identity", fun(X) -> X end},
     {"gzip", fun(X) -> zlib:gzip(X) end}].

%% @spec content_types_accepted(reqdata(), context()) ->
%%          {[{ContentType::string(), Acceptor::atom()}],
%%           reqdata(), context()}
%% @doc Get the list of content types this resource will accept.
%%      "application/json" is the only type accepted for bucket-PUT.
%%      Whatever content type is specified by the Content-Type header
%%      of a key-level PUT request will be accepted by this resource.
%%      (A key-level put *must* include a Content-Type header.)
content_types_accepted(RD, Ctx) ->
    case is_bucket_put(RD, Ctx) of
        true ->
            %% bucket-PUT: JSON only
            {[{"application/json", accept_bucket_body}], RD, Ctx};
        false ->
            case wrq:get_req_header(?HEAD_CTYPE, RD) of
                undefined ->
                    %% user must specify content type of the data
                    {[], RD, Ctx};
                CType ->
                    Media = hd(string:tokens(CType, ";")),
                    case string:tokens(Media, "/") of
                        [_Type, _Subtype] ->
                            %% accept whatever the user says
                            {[{Media, accept_doc_body}], RD, Ctx};
                        _ ->
                            {[],
                             wrq:set_resp_header(
                               ?HEAD_CTYPE,
                               "text/plain",
                               wrq:set_resp_body(
                                 ["\"", Media, "\""
                                  " is not a valid media type"
                                  " for the Content-type header.\n"],
                                 RD)),
                             Ctx}
                    end
            end
    end.

%% @spec resource_exists(reqdata(), context()) -> {boolean(), reqdata(), context()}
%% @doc Determine whether or not the requested item exists.
%%      All buckets exists, whether they have data in them or not.
%%      Documents exists if a read request to Riak returns {ok, riak_object()},
%%      and either no vtag query parameter was specified, or the value of the
%%      vtag param matches the vtag of some value of the Riak object.
resource_exists(RD, Ctx=#ctx{key=undefined}) ->
    %% top-level and all buckets exist
    {true, RD, Ctx};
resource_exists(RD, Ctx0) ->
    DocCtx = ensure_doc(Ctx0),
    case DocCtx#ctx.doc of
        {ok, Doc} ->
            case DocCtx#ctx.vtag of
                undefined ->
                    {true, RD, DocCtx};
                Vtag ->
                    MDs = riak_object:get_metadatas(Doc),
                    {lists:any(fun(M) ->
                                       dict:fetch(?MD_VTAG, M) =:= Vtag
                               end,
                               MDs),
                     RD, DocCtx#ctx{vtag=Vtag}}
            end;
        {error, _} ->
            %% This should never actually be reached because all the error
            %% conditions from ensure_doc are handled up in malformed_request.
            {false, RD, DocCtx}
    end.

%% @spec produce_toplevel_body(reqdata(), context()) -> {binary(), reqdata(), context()}
%% @doc Produce the JSON response to a bucket-level GET.
%%      Includes a list of known buckets if the "buckets=true" query
%%      param is specified.
produce_toplevel_body(RD, Ctx=#ctx{client=C}) ->
    {ListPart, LinkRD} =
        case wrq:get_qs_value(?Q_BUCKETS, RD) of
            ?Q_TRUE ->
                {ok, Buckets} = C:list_buckets(),
                {[{?JSON_BUCKETS, Buckets}],
                 lists:foldl(fun(B, Acc) -> add_bucket_link(B,Acc,Ctx) end,
                             RD, Buckets)};
            _ ->
                {[], RD}
        end,
    {mochijson2:encode({struct, ListPart}), LinkRD, Ctx}.

%% @spec produce_bucket_body(reqdata(), context()) -> {binary(), reqdata(), context()}
%% @doc Produce the JSON response to a bucket-level GET.
%%      Includes the bucket props unless the "props=false" query param
%%      is specified.
%%      Includes the keys of the documents in the bucket unless the
%%      "keys=false" query param is specified. If "keys=stream" query param
%%      is specified, keys will be streamed back to the client in JSON chunks
%%      like so: {"keys":[Key1, Key2,...]}.
%%      A Link header will also be added to the response by this function
%%      if the keys are included in the JSON object.  The Link header
%%      will include links to all keys in the bucket, with the property
%%      "rel=contained".
produce_bucket_body(RD, Ctx=#ctx{bucket=B, client=C}) ->
    SchemaPart =
        case wrq:get_qs_value(?Q_PROPS, RD) of
            ?Q_FALSE -> [];
            _ ->
                Props = C:get_bucket(B),
                JsonProps = lists:map(fun jsonify_bucket_prop/1, Props),
                [{?JSON_PROPS, {struct, JsonProps}}]
        end,
    {KeyPart, KeyRD} =
        case wrq:get_qs_value(?Q_KEYS, RD) of
            ?Q_STREAM -> {stream, RD};
            ?Q_TRUE ->
                {ok, KeyList} = C:list_keys(B),
                {[{?Q_KEYS, KeyList}],
                 lists:foldl(
                   fun(K, Acc) ->
                           add_link_head(B, K, "contained", Acc, Ctx)
                   end,
                   RD, KeyList)};
            _ -> {[], RD}
        end,
    case KeyPart of
        stream -> {{stream, {mochijson2:encode({struct, SchemaPart}),
                             fun() ->
                                     {ok, ReqId} = C:stream_list_keys(B),
                                     stream_keys(ReqId)
                             end}},
                   KeyRD,
                   Ctx};
       _ ->
            {mochijson2:encode({struct, SchemaPart++KeyPart}), KeyRD, Ctx}
    end.

stream_keys(ReqId) ->
    receive
        {ReqId, {keys, Keys}} ->
            {mochijson2:encode({struct, [{<<"keys">>, Keys}]}), fun() -> stream_keys(ReqId) end};
        {ReqId, done} -> {mochijson2:encode({struct, [{<<"keys">>, []}]}), done}
    end.

%% @spec accept_bucket_body(reqdata(), context()) -> {true, reqdata(), context()}
%% @doc Modify the bucket properties according to the body of the
%%      bucket-level PUT request.
accept_bucket_body(RD, Ctx=#ctx{bucket=B, client=C, bucketprops=Props}) ->
    ErlProps = lists:map(fun erlify_bucket_prop/1, Props),
    C:set_bucket(B, ErlProps),
    {true, RD, Ctx}.

%% @spec jsonify_bucket_prop({Property::atom(), erlpropvalue()}) ->
%%           {Property::binary(), jsonpropvalue()}
%% @type erlpropvalue() = integer()|string()|boolean()|
%%                        {modfun, atom(), atom()}|{atom(), atom()}
%% @type jsonpropvalue() = integer()|string()|boolean()|{struct,[jsonmodfun()]}
%% @type jsonmodfun() = {mod_binary(), binary()}|{fun_binary(), binary()}
%% @doc Convert erlang bucket properties to JSON bucket properties.
%%      Property names are converted from atoms to binaries.
%%      Integer, string, and boolean property values are left as integer,
%%      string, or boolean JSON values.
%%      {modfun, Module, Function} or {Module, Function} values of the
%%      linkfun and chash_keyfun properties are converted to JSON objects
%%      of the form:
%%        {"mod":ModuleNameAsString,
%%         "fun":FunctionNameAsString}
jsonify_bucket_prop({linkfun, {modfun, Module, Function}}) ->
    {?JSON_LINKFUN, {struct, [{?JSON_MOD,
                               list_to_binary(atom_to_list(Module))},
                              {?JSON_FUN,
                               list_to_binary(atom_to_list(Function))}]}};
jsonify_bucket_prop({linkfun, {qfun, _}}) ->
    {?JSON_LINKFUN, <<"qfun">>};
jsonify_bucket_prop({linkfun, {jsfun, Name}}) ->
    {?JSON_LINKFUN, {struct, [{?JSON_JSFUN, Name}]}};
jsonify_bucket_prop({linkfun, {jsanon, {Bucket, Key}}}) ->
    {?JSON_LINKFUN, {struct, [{?JSON_JSANON,
                               {struct, [{?JSON_JSBUCKET, Bucket},
                                         {?JSON_JSKEY, Key}]}}]}};
jsonify_bucket_prop({linkfun, {jsanon, Source}}) ->
    {?JSON_LINKFUN, {struct, [{?JSON_JSANON, Source}]}};
jsonify_bucket_prop({chash_keyfun, {Module, Function}}) ->
    {?JSON_CHASH, {struct, [{?JSON_MOD,
                             list_to_binary(atom_to_list(Module))},
                            {?JSON_FUN,
                             list_to_binary(atom_to_list(Function))}]}};
jsonify_bucket_prop({Prop, Value}) ->
    {list_to_binary(atom_to_list(Prop)), Value}.

%% @spec erlify_bucket_prop({Property::binary(), jsonpropvalue()}) ->
%%          {Property::atom(), erlpropvalue()}
%% @doc The reverse of jsonify_bucket_prop/1.  Converts JSON representation
%%      of bucket properties to their Erlang form.
erlify_bucket_prop({?JSON_LINKFUN, {struct, Props}}) ->
    case {proplists:get_value(?JSON_MOD, Props),
          proplists:get_value(?JSON_FUN, Props)} of
        {Mod, Fun} when is_binary(Mod), is_binary(Fun) ->
            {linkfun, {modfun,
                       list_to_existing_atom(binary_to_list(Mod)),
                       list_to_existing_atom(binary_to_list(Fun))}};
        {undefined, undefined} ->
            case proplists:get_value(?JSON_JSFUN, Props) of
                Name when is_binary(Name) ->
                    {linkfun, {jsfun, Name}};
                undefined ->
                    case proplists:get_value(?JSON_JSANON, Props) of
                        {struct, Bkey} ->
                            Bucket = proplists:get_value(?JSON_JSBUCKET, Bkey),
                            Key = proplists:get_value(?JSON_JSKEY, Bkey),
                            %% bomb if malformed
                            true = is_binary(Bucket) andalso is_binary(Key),
                            {linkfun, {jsanon, {Bucket, Key}}};
                        Source when is_binary(Source) ->
                            {linkfun, {jsanon, Source}}
                    end
            end
    end;
erlify_bucket_prop({?JSON_CHASH, {struct, Props}}) ->
    {chash_keyfun, {list_to_existing_atom(
                      binary_to_list(
                        proplists:get_value(?JSON_MOD, Props))),
                    list_to_existing_atom(
                      binary_to_list(
                        proplists:get_value(?JSON_FUN, Props)))}};
erlify_bucket_prop({?JSON_ALLOW_MULT, Value}) ->
    {allow_mult, any_to_bool(Value)};
erlify_bucket_prop({Prop, Value}) ->
    {list_to_existing_atom(binary_to_list(Prop)), Value}.

%% @spec post_is_create(reqdata(), context()) -> {boolean(), reqdata(), context()}
%% @doc POST is considered a document-creation operation for bucket-level
%%      requests (this makes webmachine call create_path/2, where the key
%%      for the created document will be chosen).
post_is_create(RD, Ctx=#ctx{key=undefined}) ->
    %% bucket-POST is create
    {true, RD, Ctx};
post_is_create(RD, Ctx) ->
    %% key-POST is not create
    {false, RD, Ctx}.

%% @spec create_path(reqdata(), context()) -> {string(), reqdata(), context()}
%% @doc Choose the Key for the document created during a bucket-level POST.
%%      This function also sets the Location header to generate a
%%      201 Created response.
create_path(RD, Ctx=#ctx{prefix=P, bucket=B}) ->
    K = riak_core_util:unique_id_62(),
    {K,
     wrq:set_resp_header("Location",
                         lists:append(["/",P,"/",binary_to_list(B),"/",K]),
                         RD),
     Ctx#ctx{key=list_to_binary(K)}}.

%% @spec process_post(reqdata(), context()) -> {true, reqdata(), context()}
%% @doc Pass-through for key-level requests to allow POST to function
%%      as PUT for clients that do not support PUT.
process_post(RD, Ctx) -> accept_doc_body(RD, Ctx).

%% @spec accept_doc_body(reqdata(), context()) -> {true, reqdat(), context()}
%% @doc Store the data the client is PUTing in the document.
%%      This function translates the headers and body of the HTTP request
%%      into their final riak_object() form, and executes the Riak put.
accept_doc_body(RD, Ctx=#ctx{bucket=B, key=K, client=C, links=L}) ->
    Doc0 = case Ctx#ctx.doc of
               {ok, D} -> D;
               _       -> riak_object:new(B, K, <<>>)
           end,
    VclockDoc = riak_object:set_vclock(Doc0, decode_vclock_header(RD)),
    {CType, Charset} = extract_content_type(RD),
    UserMeta = extract_user_meta(RD),
    CTypeMD = dict:store(?MD_CTYPE, CType, dict:new()),
    CharsetMD = if Charset /= undefined ->
                        dict:store(?MD_CHARSET, Charset, CTypeMD);
                   true -> CTypeMD
                end,
    EncMD = case wrq:get_req_header(?HEAD_ENCODING, RD) of
                undefined -> CharsetMD;
                E -> dict:store(?MD_ENCODING, E, CharsetMD)
            end,
    LinkMD = dict:store(?MD_LINKS, L, EncMD),
    UserMetaMD = dict:store(?MD_USERMETA, UserMeta, LinkMD),
    MDDoc = riak_object:update_metadata(VclockDoc, UserMetaMD),
    Doc = riak_object:update_value(MDDoc, accept_value(CType, wrq:req_body(RD))),
    Options = case wrq:get_qs_value(?Q_RETURNBODY, RD) of ?Q_TRUE -> [returnbody]; _ -> [] end,
    case C:put(Doc, [{w, Ctx#ctx.w}, {dw, Ctx#ctx.dw}, {pw, Ctx#ctx.pw}, {timeout, 60000} |
                Options]) of
        {error, precommit_fail} ->
            {{halt, 403}, send_precommit_error(RD, undefined), Ctx};
        {error, {precommit_fail, Reason}} ->
            {{halt, 403}, send_precommit_error(RD, Reason), Ctx};
        {error, {n_val_violation, N}} ->
            Msg = io_lib:format("Specified w/dw/pw values invalid for bucket"
                                " n value of ~p~n", [N]),
            {{halt, 400}, wrq:append_to_response_body(Msg, RD), Ctx};
        {error, {pw_val_unsatisfied, Requested, Returned}} ->
            Msg = io_lib:format("PW-value unsatisfied: ~p/~p~n", [Returned,
                    Requested]),
            {{halt, 503}, wrq:append_to_response_body(Msg, RD), Ctx};
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
        ok ->
            {true, RD, Ctx#ctx{doc={ok, Doc}}};
        {ok, RObj} ->
            DocCtx = Ctx#ctx{doc={ok, RObj}},
            HasSiblings = (select_doc(DocCtx) == multiple_choices),
            send_returnbody(RD, DocCtx, HasSiblings)
    end.

%% Handle the no-sibling case. Just send the object.
send_returnbody(RD, DocCtx, _HasSiblings = false) ->
    {Body, DocRD, DocCtx2} = produce_doc_body(RD, DocCtx),
    {true, wrq:append_to_response_body(Body, DocRD), DocCtx2};

%% Handle the sibling case. Send either the sibling message body, or a
%% multipart body, depending on what the client accepts.
send_returnbody(RD, DocCtx, _HasSiblings = true) ->
    AcceptHdr = wrq:get_req_header("Accept", RD),
    case webmachine_util:choose_media_type(["multipart/mixed", "text/plain"], AcceptHdr) of
        "multipart/mixed"  ->
            {Body, DocRD, DocCtx2} = produce_multipart_body(RD, DocCtx),
            {true, wrq:append_to_response_body(Body, DocRD), DocCtx2};
        _ ->
            {Body, DocRD, DocCtx2} = produce_sibling_message_body(RD, DocCtx),
            {true, wrq:append_to_response_body(Body, DocRD), DocCtx2}
    end.

%% @spec extract_content_type(reqdata()) ->
%%          {ContentType::string(), Charset::string()|undefined}
%% @doc Interpret the Content-Type header in the client's PUT request.
%%      This function extracts the content type and charset for use
%%      in subsequent GET requests.
extract_content_type(RD) ->
    case wrq:get_req_header(?HEAD_CTYPE, RD) of
        undefined ->
            undefined;
        RawCType ->
            [CType|RawParams] = string:tokens(RawCType, "; "),
            Params = [ list_to_tuple(string:tokens(P, "=")) || P <- RawParams],
            {CType, proplists:get_value("charset", Params)}
    end.

%% @spec extract_user_meta(reqdata()) -> proplist()
%% @doc Extract headers prefixed by X-Riak-Meta- in the client's PUT request
%%      to be returned by subsequent GET requests.
extract_user_meta(RD) ->
    lists:filter(fun({K,_V}) ->
                    lists:prefix(
                        ?HEAD_USERMETA_PREFIX,
                        string:to_lower(any_to_list(K)))
                end,
                mochiweb_headers:to_list(wrq:req_headers(RD))).

%% @spec multiple_choices(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether a document has siblings.  If the user has
%%      specified a specific vtag, the document is considered not to
%%      have sibling versions.  This is a safe assumption, because
%%      resource_exists will have filtered out requests earlier for
%%      vtags that are invalid for this version of the document.
multiple_choices(RD, Ctx=#ctx{key=undefined}) ->
    %% top-level and bucket operations never have multiple choices
    {false, RD, Ctx};
multiple_choices(RD, Ctx=#ctx{vtag=undefined, doc={ok, Doc}}) ->
    %% user didn't specify a vtag, so there better not be siblings
    case riak_object:get_update_value(Doc) of
        undefined ->
            case riak_object:value_count(Doc) of
                1 -> {false, RD, Ctx};
                _ -> {true, RD, Ctx}
            end;
        _ ->
            %% just updated can't have multiple
            {false, RD, Ctx}
    end;
multiple_choices(RD, Ctx) ->
    %% specific vtag was specified
    {false, RD, Ctx}.

%% @spec produce_doc_body(reqdata(), context()) -> {binary(), reqdata(), context()}
%% @doc Extract the value of the document, and place it in the response
%%      body of the request.  This function also adds the Link and X-Riak-Meta-
%%      headers to the response.  One link will point to the bucket, with the
%%      property "rel=container".  The rest of the links will be constructed
%%      from the links of the document.
produce_doc_body(RD, Ctx) ->
    case select_doc(Ctx) of
        {MD, Doc} ->
            Links = case dict:find(?MD_LINKS, MD) of
                        {ok, L} -> L;
                        error -> []
                    end,
            LinkRD = add_container_link(
                       lists:foldl(fun({{B,K},T},Acc) ->
                                           add_link_head(B,K,T,Acc,Ctx)
                                   end,
                                   RD, Links),
                       Ctx),
            UserMetaRD = case dict:find(?MD_USERMETA, MD) of
                        {ok, UserMeta} ->
                            lists:foldl(fun({K,V},Acc) ->
                                            wrq:merge_resp_headers([{K,V}],Acc)
                                        end,
                                        LinkRD, UserMeta);
                        error -> LinkRD
                    end,
            {encode_value(Doc), encode_vclock_header(UserMetaRD, Ctx), Ctx};
        multiple_choices ->
            throw({unexpected_code_path, ?MODULE, produce_doc_body, multiple_choices})
    end.

%% @spec produce_sibling_message_body(reqdata(), context()) ->
%%          {iolist(), reqdata(), context()}
%% @doc Produce the text message informing the user that there are multiple
%%      values for this document, and giving that user the vtags of those
%%      values so they can get to them with the vtag query param.
produce_sibling_message_body(RD, Ctx=#ctx{doc={ok, Doc}}) ->
    Vtags = [ dict:fetch(?MD_VTAG, M)
              || M <- riak_object:get_metadatas(Doc) ],
    {[<<"Siblings:\n">>, [ [V,<<"\n">>] || V <- Vtags]],
     wrq:set_resp_header(?HEAD_CTYPE, "text/plain",
                         encode_vclock_header(RD, Ctx)),
     Ctx}.

%% @spec produce_multipart_body(reqdata(), context()) ->
%%          {iolist(), reqdata(), context()}
%% @doc Produce a multipart body representation of an object with multiple
%%      values (siblings), each sibling being one part of the larger
%%      document.
produce_multipart_body(RD, Ctx=#ctx{doc={ok, Doc}, bucket=B, prefix=P}) ->
    Boundary = riak_core_util:unique_id_62(),
    {[[["\r\n--",Boundary,"\r\n",
        multipart_encode_body(P, B, Content)]
       || Content <- riak_object:get_contents(Doc)],
      "\r\n--",Boundary,"--\r\n"],
     wrq:set_resp_header(?HEAD_CTYPE,
                         "multipart/mixed; boundary="++Boundary,
                         encode_vclock_header(RD, Ctx)),
     Ctx}.

%% @spec multipart_encode_body(string(), binary(), {dict(), binary()}) -> iolist()
%% @doc Produce one part of a multipart body, representing one sibling
%%      of a multi-valued document.
multipart_encode_body(Prefix, Bucket, {MD, V}) ->
    [{LHead, Links}] =
        mochiweb_headers:to_list(
          mochiweb_headers:make(
            [{?HEAD_LINK, format_link(Prefix,Bucket)}|
             [{?HEAD_LINK, format_link(Prefix,B,K,T)}
              || {{B,K},T} <- case dict:find(?MD_LINKS, MD) of
                                  {ok, Ls} -> Ls;
                                  error -> []
                              end]])),
    [?HEAD_CTYPE, ": ",get_ctype(MD,V),
     case dict:find(?MD_CHARSET, MD) of
         {ok, CS} -> ["; charset=",CS];
         error -> []
     end,
     "\r\n",
     case dict:find(?MD_ENCODING, MD) of
         {ok, Enc} -> [?HEAD_ENCODING,": ",Enc,"\r\n"];
         error -> []
     end,
     LHead,": ",Links,"\r\n",
     "Etag: ",dict:fetch(?MD_VTAG, MD),"\r\n",
     "Last-Modified: ",
     case dict:fetch(?MD_LASTMOD, MD) of
         Now={_,_,_} ->
             httpd_util:rfc1123_date(
               calendar:now_to_local_time(Now));
         Rfc1123 when is_list(Rfc1123) ->
             Rfc1123
     end,
     "\r\n",
     case dict:find(?MD_USERMETA, MD) of
         {ok, M} ->
            lists:foldl(fun({Hdr,Val},Acc) ->
                            [Acc|[Hdr,": ",Val,"\r\n"]]
                        end,
                        [], M);
         error -> []
     end,
     "\r\n",encode_value(V)].


%% @spec select_doc(context()) -> {metadata(), value()}|multiple_choices
%% @doc Selects the "proper" document:
%%  - chooses update-value/metadata if update-value is set
%%  - chooses only val/md if only one exists
%%  - chooses val/md matching given Vtag if multiple contents exist
%%      (assumes a vtag has been specified)
select_doc(#ctx{doc={ok, Doc}, vtag=Vtag}) ->
    case riak_object:get_update_value(Doc) of
        undefined ->
            case riak_object:get_contents(Doc) of
                [Single] -> Single;
                Mult ->
                    case lists:dropwhile(
                           fun({M,_}) ->
                                   dict:fetch(?MD_VTAG, M) /= Vtag
                           end,
                           Mult) of
                        [Match|_] -> Match;
                        [] -> multiple_choices
                    end
            end;
        UpdateValue ->
            {riak_object:get_update_metadata(Doc), UpdateValue}
    end.

%% @spec encode_vclock_header(reqdata(), context()) -> reqdata()
%% @doc Add the X-Riak-Vclock header to the response.
encode_vclock_header(RD, #ctx{doc={ok, Doc}}) ->
    {Head, Val} = vclock_header(Doc),
    wrq:set_resp_header(Head, Val, RD).

%% @spec vclock_header(riak_object()) -> {Name::string(), Value::string()}
%% @doc Transform the Erlang representation of the document's vclock
%%      into something suitable for an HTTP header
vclock_header(Doc) ->
    {?HEAD_VCLOCK,
     binary_to_list(
       base64:encode(zlib:zip(term_to_binary(riak_object:vclock(Doc)))))}.

%% @spec decode_vclock_header(reqdata()) -> vclock()
%% @doc Translate the X-Riak-Vclock header value from the request into
%%      its Erlang representation.  If no vclock header exists, a fresh
%%      vclock is returned.
decode_vclock_header(RD) ->
    case wrq:get_req_header(?HEAD_VCLOCK, RD) of
        undefined -> vclock:fresh();
        Head      -> binary_to_term(zlib:unzip(base64:decode(Head)))
    end.

%% @spec ensure_doc(context()) -> context()
%% @doc Ensure that the 'doc' field of the context() has been filled
%%      with the result of a riak_client:get request.  This is a
%%      convenience for memoizing the result of a get so it can be
%%      used in multiple places in this resource, without having to
%%      worry about the order of executing of those places.
ensure_doc(Ctx=#ctx{doc=undefined, bucket=B, key=K, client=C, r=R,
        pr=PR, basic_quorum=Quorum, notfound_ok=NotFoundOK}) ->
    Ctx#ctx{doc=C:get(B, K, [{r, R}, {pr, PR}, {basic_quorum, Quorum},
                {notfound_ok, NotFoundOK}])};
ensure_doc(Ctx) -> Ctx.

%% @spec delete_resource(reqdata(), context()) -> {true, reqdata(), context()}
%% @doc Delete the document specified.
delete_resource(RD, Ctx=#ctx{bucket=B, key=K, client=C, rw=RW}) ->
    case C:delete(B, K, RW) of
        {error, notfound} ->
            {{halt, 404},
             wrq:set_resp_header("Content-Type", "text/plain",
                                 wrq:append_to_response_body(
                                   io_lib:format("not found~n",[]),
                                   RD)), Ctx};
        {error, precommit_fail} ->
            {{halt, 403}, send_precommit_error(RD, undefined), Ctx};
        {error, {precommit_fail, Reason}} ->
            {{halt, 403}, send_precommit_error(RD, Reason), Ctx};
        ok ->
            {true, RD, Ctx}
    end.

%% @spec generate_etag(reqdata(), context()) ->
%%          {undefined|string(), reqdata(), context()}
%% @doc Get the etag for this resource.
%%      Top-level and Bucket requests will have no etag.
%%      Documents will have an etag equal to their vtag.  No etag will be
%%      given for documents with siblings, if no sibling was chosen with the
%%      vtag query param.
generate_etag(RD, Ctx=#ctx{key=undefined}) ->
    {undefined, RD, Ctx};
generate_etag(RD, Ctx) ->
    case select_doc(Ctx) of
        {MD, _} ->
            {dict:fetch(?MD_VTAG, MD), RD, Ctx};
        multiple_choices ->
            {undefined, RD, Ctx}
    end.

%% @spec last_modified(reqdata(), context()) ->
%%          {undefined|datetime(), reqdata(), context()}
%% @doc Get the last-modified time for this resource.
%%      Top-level and Bucket requests will have no last-modified time.
%%      Documents will have the last-modified time specified by the riak_object.
%%      No last-modified time will be given for documents with siblings, if no
%%      sibling was chosen with the vtag query param.
last_modified(RD, Ctx=#ctx{key=undefined}) ->
    {undefined, RD, Ctx};
last_modified(RD, Ctx) ->
    case select_doc(Ctx) of
        {MD, _} ->
            {case dict:fetch(?MD_LASTMOD, MD) of
                 Now={_,_,_} ->
                     calendar:now_to_universal_time(Now);
                 Rfc1123 when is_list(Rfc1123) ->
                     httpd_util:convert_request_date(Rfc1123)
             end,
             RD, Ctx};
        multiple_choices ->
            {undefined, RD, Ctx}
    end.

%% @spec add_container_link(reqdata(), context()) -> reqdata()
%% @doc Add the Link header specifying the containing bucket of
%%      the document to the response.
add_container_link(RD, #ctx{prefix=Prefix, bucket=Bucket}) ->
    Val = format_link(Prefix, Bucket),
    wrq:merge_resp_headers([{?HEAD_LINK,Val}], RD).

%% @spec add_bucket_link(binary(), reqdata(), context()) -> reqdata()
%% @doc Add the Link header pointing to a bucket
add_bucket_link(Bucket, RD, #ctx{prefix=Prefix}) ->
    Val = format_link(Prefix, Bucket, "contained"),
    wrq:merge_resp_headers([{?HEAD_LINK,Val}], RD).

%% @spec add_link_head(binary(), binary(), binary(), reqdata(), context()) ->
%%          reqdata()
%% @doc Add a Link header specifying the given Bucket and Key
%%      with the given Tag to the response.
add_link_head(Bucket, Key, Tag, RD, #ctx{prefix=Prefix}) ->
    Val = format_link(Prefix, Bucket, Key, Tag),
    wrq:merge_resp_headers([{?HEAD_LINK,Val}], RD).

%% @spec format_link(string(), binary()) -> string()
%% @doc Produce the standard link header from an object up to its bucket.
format_link(Prefix, Bucket) ->
    format_link(Prefix, Bucket, "up").

%% @spec format_link(string(), binary(), string()) -> string()
%% @doc Format a Link header to a bucket.
format_link(Prefix, Bucket, Tag) ->
    io_lib:format("</~s/~s>; rel=\"~s\"",
                  [Prefix,
                   mochiweb_util:quote_plus(Bucket),
                   Tag]).

%% @spec format_link(string(), binary(), binary(), binary()) -> string()
%% @doc Format a Link header to another document.
format_link(Prefix, Bucket, Key, Tag) ->
    io_lib:format("</~s/~s/~s>; riaktag=\"~s\"",
                  [Prefix|
                   [mochiweb_util:quote_plus(E) ||
                       E <- [Bucket, Key, Tag] ]]).

%% @spec get_link_heads(reqdata(), context()) -> [link()]
%% @doc Extract the list of links from the Link request header.
%%      This function will die if an invalid link header format
%%      is found.
get_link_heads(RD, #ctx{prefix=Prefix, bucket=B}) ->
    case wrq:get_req_header(?HEAD_LINK, RD) of
        undefined -> [];
        Heads ->
            BucketLink = lists:flatten(format_link(Prefix, B)),
            {ok, Re} = re:compile("</([^/]+)/([^/]+)/([^/]+)>; ?riaktag=\"([^\"]+)\""),
            lists:map(
              fun(L) ->
                      {match,[InPrefix,Bucket,Key,Tag]} =
                          re:run(L, Re, [{capture,[1,2,3,4],binary}]),
                      Prefix = binary_to_list(InPrefix),
                      {{list_to_binary(mochiweb_util:unquote(Bucket)),
                        list_to_binary(mochiweb_util:unquote(Key))},
                       list_to_binary(mochiweb_util:unquote(Tag))}
              end,
              lists:delete(BucketLink, [string:strip(T) || T<- string:tokens(Heads, ",")]))
    end.

%% @spec get_ctype(dict(), term()) -> string()
%% @doc Work out the content type for this object - use the metadata if provided
get_ctype(MD,V) ->
    case dict:find(?MD_CTYPE, MD) of
        {ok, Ctype} ->
            Ctype;
        error when is_binary(V) ->
            "application/octet-stream";
        error ->
            "application/x-erlang-binary"
    end.

%% @spec encode_value(term()) -> binary()
%% @doc Encode the object value as a binary - content type can be used
%%      to decode
encode_value(V) when is_binary(V) ->
    V;
encode_value(V) ->
    term_to_binary(V).
    
%% @spec accept_value(string(), binary()) -> term()
%% @doc Accept the object value as a binary - content type can be used
%%      to decode
accept_value("application/x-erlang-binary",V) ->
    binary_to_term(V);
accept_value(_Ctype, V) ->
    V.

any_to_list(V) when is_list(V) ->
    V;
any_to_list(V) when is_atom(V) ->
    atom_to_list(V);
any_to_list(V) when is_binary(V) ->
    binary_to_list(V).

any_to_bool(V) when is_list(V) ->
    (V == "1") orelse (V == "true") orelse (V == "TRUE");
any_to_bool(V) when is_binary(V) ->
    any_to_bool(binary_to_list(V));
any_to_bool(V) when is_integer(V) ->
    V /= 0;
any_to_bool(V) when is_boolean(V) ->
    V.

missing_content_type(RD) ->
    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
    wrq:append_to_response_body(<<"Missing Content-Type request header">>, RD1).

send_precommit_error(RD, Reason) ->
    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
    Error = if
                Reason =:= undefined ->
                    list_to_binary([atom_to_binary(wrq:method(RD1), utf8),
                                    <<" aborted by pre-commit hook.">>]);
                true ->
                    Reason
            end,
    wrq:append_to_response_body(Error, RD1).
