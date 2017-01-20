%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
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

%% @doc Common functions used by riak_kv_wm_* modules.
-module(riak_kv_wm_utils).

%% webmachine resource exports
-export([
         maybe_decode_uri/2,
         get_riak_client/2,
         get_client_id/1,
         default_encodings/0,
         multipart_encode_body/4,
         format_links/3,
         format_uri/4,
         format_uri/5,
         encode_value/1,
         accept_value/3,
         any_to_list/1,
         any_to_bool/1,
         is_forbidden/1,
         is_forbidden/3,
         jsonify_bucket_prop/1,
         erlify_bucket_prop/1,
         ensure_bucket_type/3,
         bucket_type_exists/1,
         maybe_bucket_type/2,
         method_to_perm/1,
         register_allowable_origins/1
        ]).

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

-define(ALLOWABLE_ORIGINS_TABLE, 'cors_allowable_origins').

-ifdef(namespaced_types).
-type riak_kv_wm_utils_dict() :: dict:dict().
-else.
-type riak_kv_wm_utils_dict() :: dict().
-endif.

-type jsonpropvalue() :: integer()|string()|boolean()|{struct,[jsonmodfun()]}.
-type jsonmodfun() :: {ModBinary :: term(), binary()}|{FunBinary :: term(), binary()}.
-type erlpropvalue() :: integer()|string()|boolean().

maybe_decode_uri(RD, Val) ->
    case application:get_env(riak_kv, http_url_encoding) of
        {ok, on} ->
            mochiweb_util:unquote(Val);
        _ ->
            case wrq:get_req_header("X-Riak-URL-Encoding", RD) of
                "on" ->
                    mochiweb_util:unquote(Val);
                _ ->
                    Val
            end
    end.

-spec get_riak_client(local|{node(),Cookie::atom()}, term()) ->
    {ok, RiakClient :: term()} | {error, term()}.
%% @doc Get a riak_client.
get_riak_client(local, ClientId) ->
    riak:local_client(ClientId);
get_riak_client({Node, Cookie}, ClientId) ->
    erlang:set_cookie(node(), Cookie),
    riak:client_connect(Node, ClientId).

-spec get_client_id(#wm_reqdata{}) -> term().
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


-spec default_encodings() -> [{Encoding::string(), Producer::function()}].
%% @doc The default encodings available: identity and gzip.
default_encodings() ->
    [{"identity", fun(X) -> X end},
     {"gzip", fun(X) -> zlib:gzip(X) end}].

-spec multipart_encode_body(string(), binary(), {riak_kv_wm_utils_dict(), binary()}, term()) ->
    iolist().
%% @doc Produce one part of a multipart body, representing one sibling
%%      of a multi-valued document.
multipart_encode_body(Prefix, Bucket, {MD, V}, APIVersion) ->
    Links1 = case dict:find(?MD_LINKS, MD) of
                 {ok, Ls} -> Ls;
                 error -> []
             end,
    Links2 = format_links([{Bucket, "up"}|Links1], Prefix, APIVersion),
    Links3 = mochiweb_headers:make(Links2),
    [{?HEAD_LINK, Links4}] = mochiweb_headers:to_list(Links3),

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
     ?HEAD_LINK,": ",Links4,"\r\n",
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
     case dict:find(?MD_DELETED, MD) of
         {ok, "true"} ->
             [?HEAD_DELETED, ": true\r\n"];
         error ->
             []
     end,
     case dict:find(?MD_USERMETA, MD) of
         {ok, M} ->
            lists:foldl(fun({Hdr,Val},Acc) ->
                            [Acc|[Hdr,": ",Val,"\r\n"]]
                        end,
                        [], M);
         error -> []
     end,
     case dict:find(?MD_INDEX, MD) of
         {ok, IF} ->
             [[?HEAD_INDEX_PREFIX,Key,": ",any_to_list(Val),"\r\n"] || {Key,Val} <- IF];
         error -> []
     end,
     "\r\n",
     encode_value(V)].

format_links(Links, Prefix, APIVersion) ->
    format_links(Links, Prefix, APIVersion, []).
format_links([{{Bucket,Key}, Tag}|Rest], Prefix, APIVersion, Acc) ->
    format_links([{Bucket, Key, Tag}|Rest], Prefix, APIVersion, Acc);
format_links([{Bucket, Tag}|Rest], Prefix, APIVersion, Acc) ->
    Bucket1 = mochiweb_util:quote_plus(Bucket),
    Tag1 = mochiweb_util:quote_plus(Tag),
    Val =
        case APIVersion of
            1 ->
                io_lib:format("</~s/~s>; rel=\"~s\"",
                              [Prefix, Bucket1, Tag1]);
            Two when Two >= 2 ->
                io_lib:format("</buckets/~s>; rel=\"~s\"",
                              [Bucket1, Tag1])
        end,
    format_links(Rest, Prefix, APIVersion, [{?HEAD_LINK, Val}|Acc]);
format_links([{Bucket, Key, Tag}|Rest], Prefix, APIVersion, Acc) ->
    Bucket1 = mochiweb_util:quote_plus(Bucket),
    Key1 = mochiweb_util:quote_plus(Key),
    Tag1 = mochiweb_util:quote_plus(Tag),
    Val = io_lib:format("<~s>; riaktag=\"~s\"",
                        [format_uri(Bucket1, Key1, Prefix, APIVersion),
                         Tag1]),
    format_links(Rest, Prefix, APIVersion, [{?HEAD_LINK, Val}|Acc]);
format_links([], _Prefix, _APIVersion, Acc) ->
    Acc.

%% @doc Format the URI for a bucket/key correctly for the api version
%% used. (APIVersion is the final parameter.)
format_uri(Bucket, Key, Prefix, 1) ->
    io_lib:format("/~s/~s/~s", [Prefix, Bucket, Key]);
format_uri(Bucket, Key, _Prefix, 2) ->
    io_lib:format("/buckets/~s/keys/~s", [Bucket, Key]).


%% @doc Format the URI for a type/bucket/key correctly for the api version
%% used. (APIVersion is the final parameter.)
format_uri(_Type, Bucket, Key, Prefix, 1) ->
    format_uri(Bucket, Key, Prefix, 1);
format_uri(_Type, Bucket, Key, Prefix, 2) ->
    format_uri(Bucket, Key, Prefix, 2);
format_uri(Type, Bucket, Key, _Prefix, 3) ->
    io_lib:format("/types/~s/buckets/~s/keys/~s", [Type, Bucket, Key]).

-spec get_ctype(riak_kv_wm_utils_dict(), term()) -> string().
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

-spec encode_value(term()) -> binary().
%% @doc Encode the object value as a binary - content type can be used
%%      to decode
encode_value(V) when is_binary(V) ->
    V;
encode_value(V) ->
    term_to_binary(V).

-spec accept_value(string(), riak_kv_wm_utils_dict(), binary()) -> term().
%% @doc Accept the object value as a binary - content type can be used
%%      to decode
accept_value("application/x-erlang-binary",MD, V) ->
    case dict:find(?MD_ENCODING, MD) of
        {ok, "gzip"} ->
            binary_to_term(zlib:gunzip(V));
        _ ->
            binary_to_term(V)
    end;

accept_value(_Ctype, _MD, V) ->
    V.

any_to_list(V) when is_list(V) ->
    V;
any_to_list(V) when is_atom(V) ->
    atom_to_list(V);
any_to_list(V) when is_binary(V) ->
    binary_to_list(V);
any_to_list(V) when is_integer(V) ->
    integer_to_list(V).

any_to_bool(V) when is_list(V) ->
    (V == "1") orelse (V == "true") orelse (V == "TRUE");
any_to_bool(V) when is_binary(V) ->
    any_to_bool(binary_to_list(V));
any_to_bool(V) when is_integer(V) ->
    V /= 0;
any_to_bool(V) when is_boolean(V) ->
    V.

-spec is_forbidden(#wm_reqdata{}) -> boolean().
%% @doc Determine whether the request is forbidden.
is_forbidden(RD) ->
    % TODO: what log level(s) here?
    case is_null_origin(RD) of
        true ->
            _ = lager:info(
                    "Request from ~p denied due to null origin",
                    [{wrq:scheme(RD), wrq:peer(RD)}]),
            true;
        _ ->
            case app_helper:get_env(riak_kv, secure_referer_check, true)
                    andalso not is_valid_referer(RD) of
                true ->
                    _ = lager:info(
                            "Request from ~p denied due to invalid referrer",
                            [{wrq:scheme(RD), wrq:peer(RD)}]),
                    true;
                _ ->
                    false
            end
    end.

-spec is_forbidden(#wm_reqdata{}, term(), term())
        -> {boolean(), #wm_reqdata{}, term()}.
%% @doc Like is_forbidden/1, but also checks if the job class is enabled.
%% May modify RequestData, if we choose to include why it's forbidden.
%% ReqContext is passed through untouched, so that the result tuple can be
%% returned directly by WM forbidden callbacks.
is_forbidden(ReqData, JobClass, ReqContext) ->
    % Logging for non-job criteria is performed by is_forbidden/1.
    case is_forbidden(ReqData) of
        true ->
            {true, ReqData, ReqContext};
        _ ->
            Accept = riak_core_util:job_class_enabled(JobClass),
            _ = riak_core_util:report_job_request_disposition(
                    Accept, JobClass, ?MODULE, is_forbidden, ?LINE,
                    {wrq:scheme(ReqData), wrq:peer(ReqData)}),
            case Accept of
                true ->
                    {false, ReqData, ReqContext};
                _ ->
                    {true,
                        wrq:append_to_resp_body(
                            unicode:characters_to_binary(
                                riak_core_util:job_class_disabled_message(
                                    text, JobClass), utf8, utf8),
                            wrq:set_resp_header(
                                "Content-Type", "text/plain", ReqData)),
                        ReqContext}
            end
    end.

%% @doc Check if the Origin header is "null". This is useful to look for attempts
%%      at CSRF, but is not a complete answer to the problem.
is_null_origin(RD) ->
    case wrq:get_req_header("Origin", RD) of
        "null" ->
            true;
        _ ->
            false
    end.

%% @doc Validate that the Referer matches up with scheme, host and port of the
%%      machine that received the request.
is_valid_referer(RD) ->
    OriginTuple = {wrq:scheme(RD), string:join(wrq:host_tokens(RD), "."), wrq:port(RD)},
    case referer_tuple(RD) of
        undefined ->
            true;
        {invalid, Url} ->
            lager:debug("WM unparsable referer: ~s\n", [Url]),
            false;
        OriginTuple ->
            true;
        RefererTuple ->
            allowable_origin(OriginTuple, RefererTuple)
    end.

%% @doc Register allowable origins (for CORS), specified as a CSV of base urls,
%% i.e. "http://localhost". "*" is the special allow all case which is not
%% recommended, but is used in enough cases to provide support.
-spec register_allowable_origins(string()) -> ok.
register_allowable_origins(Origins) ->
    register_allowable_origins1(string:tokens(Origins, ",")).

register_allowable_origins1([]) -> ok;
register_allowable_origins1(Origins) ->
    maybe_create_allowable_origins_ets(),
    %% wrap the origin for full match on ets lookup
    Origins1 = [{normalize_referer(Origin)} || Origin <- Origins],
    ets:insert(?ALLOWABLE_ORIGINS_TABLE, Origins1).

allowable_origins_table_exists() ->
    undefined /= ets:info(?ALLOWABLE_ORIGINS_TABLE).

maybe_create_allowable_origins_ets() ->
    case allowable_origins_table_exists() of
        false ->
            ets:new(?ALLOWABLE_ORIGINS_TABLE,
                    [named_table, ordered_set]);
        _ ->
            ok
    end.

allowable_origin(OriginTuple, RefererTuple) ->
    RefererTuple1 = normalize_referer(RefererTuple),
    case allowable_origins_table_exists() andalso
        (ets:member(?ALLOWABLE_ORIGINS_TABLE, RefererTuple1) orelse
         ets:member(?ALLOWABLE_ORIGINS_TABLE, {"*"})) of
        false ->
            lager:debug("WM referrer not origin. Origin ~p != Referer ~p" ++
                        " and Referer not in allowable origins.\n",
                        [OriginTuple, RefererTuple]),
            false;
        _ -> true
    end.

normalize_referer({"*"}) -> {"*"};
normalize_referer("*") -> {"*"};
normalize_referer(BaseUrl) when is_list(BaseUrl) ->
    [Scheme, Host|_T] = string:tokens(BaseUrl, "://"),
    normalize_referer({list_to_atom(Scheme), Host});
normalize_referer({Scheme, Host, _Port}) ->
    normalize_referer({Scheme, Host});
normalize_referer({Scheme, "127.0.0.1"}) ->
    {Scheme, "localhost"};
normalize_referer(RefererTuple) ->
    RefererTuple.

referer_tuple(RD) ->
    case wrq:get_req_header("Referer", RD) of
        undefined ->
            undefined;
        Url ->
            case http_uri:parse(Url) of
                {ok, {Scheme, _, Host, Port, _, _}} -> %R15+
                    {Scheme, Host, Port};
                {error, _} ->
                    {invalid, Url}
            end
    end.

-spec jsonify_bucket_prop({Property::atom(), erlpropvalue()}) ->
    {Property::binary(), jsonpropvalue()}.
%%                        {modfun, atom(), atom()}|{atom(), atom()}
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
                               atom_to_binary(Module, utf8)},
                              {?JSON_FUN,
                               atom_to_binary(Function, utf8)}]}};
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
                             atom_to_binary(Module, utf8)},
                            {?JSON_FUN,
                             atom_to_binary(Function, utf8)}]}};
%% TODO Remove Legacy extractor prop in future version
jsonify_bucket_prop({rs_extractfun, {modfun, M, F}}) ->
    {?JSON_EXTRACT_LEGACY, {struct, [{?JSON_MOD,
                                      atom_to_binary(M, utf8)},
                                     {?JSON_FUN,
                                      atom_to_binary(F, utf8)}]}};
jsonify_bucket_prop({rs_extractfun, {{modfun, M, F}, Arg}}) ->
    {?JSON_EXTRACT_LEGACY, {struct, [{?JSON_MOD,
                                      atom_to_binary(M, utf8)},
                                     {?JSON_FUN,
                                      atom_to_binary(F, utf8)},
                                     {?JSON_ARG, Arg}]}};
jsonify_bucket_prop({search_extractor, {struct, _}=S}) ->
    {?JSON_EXTRACT, S};
jsonify_bucket_prop({search_extractor, {M, F}}) ->
    {?JSON_EXTRACT, {struct, [{?JSON_MOD,
                             atom_to_binary(M, utf8)},
                              {?JSON_FUN,
                               atom_to_binary(F, utf8)}]}};
jsonify_bucket_prop({search_extractor, {M, F, Arg}}) ->
    {?JSON_EXTRACT, {struct, [{?JSON_MOD,
                               atom_to_binary(M, utf8)},
                              {?JSON_FUN,
                               atom_to_binary(F, utf8)},
                              {?JSON_ARG, Arg}]}};
jsonify_bucket_prop({name, {_T, B}}) ->
    {<<"name">>, B};
jsonify_bucket_prop({ddl, _DDL}) ->
    {<<"ddl">>, <<"riak_ql_to_string:sql_to_string might be useful here.">>};
jsonify_bucket_prop({Prop, Value}) ->
    {atom_to_binary(Prop, utf8), Value}.

-spec erlify_bucket_prop({Property::binary(), jsonpropvalue()}) ->
          {Property::atom(), erlpropvalue()}.
%% @doc The reverse of jsonify_bucket_prop/1.  Converts JSON representation
%%      of bucket properties to their Erlang form.
erlify_bucket_prop({?JSON_DATATYPE, Type}) ->
    try
        {datatype, binary_to_existing_atom(Type, utf8)}
    catch
        error:badarg ->
            throw({bad_datatype, Type})
    end;
erlify_bucket_prop({?JSON_HLL_PRECISION, P}) when is_binary(P) ->
    {hll_precision, P};
erlify_bucket_prop({?JSON_LINKFUN, {struct, Props}}) ->
    case {proplists:get_value(?JSON_MOD, Props),
          proplists:get_value(?JSON_FUN, Props)} of
        {undefined, undefined} ->
            case proplists:get_value(?JSON_JSFUN, Props) of
                Name when is_binary(Name) ->
                    {linkfun, {jsfun, Name}};
                undefined ->
                    case proplists:get_value(?JSON_JSANON, Props) of
                        {struct, Bkey} ->
                            Bucket = proplists:get_value(?JSON_JSBUCKET, Bkey),
                            Key = proplists:get_value(?JSON_JSKEY, Bkey),
                            if is_binary(Bucket) andalso is_binary(Key) ->
                                    {linkfun, {jsanon, {Bucket, Key}}};
                               el/=se ->
                                    throw({bad_linkfun_bkey, {Bucket, Key}})
                            end;
                        Source when is_binary(Source) ->
                            {linkfun, {jsanon, Source}};
                        NotBinary ->
                            throw({bad_linkfun_modfun, {jsanon, NotBinary}})
                    end;
                NotBinary ->
                    throw({bad_linkfun_modfun, NotBinary})
            end;
        {Mod, Fun} ->
            try
                {linkfun, {modfun,
                           binary_to_existing_atom(Mod, utf8),
                           binary_to_existing_atom(Fun, utf8)}}
            catch
                error:badarg ->
                    throw({bad_linkfun_modfun, {Mod, Fun}})
            end
    end;
erlify_bucket_prop({?JSON_CHASH, {struct, Props}}) ->
    Mod = proplists:get_value(?JSON_MOD, Props),
    Fun = proplists:get_value(?JSON_FUN, Props),
    try
        {chash_keyfun, {binary_to_existing_atom(Mod, utf8),
                        binary_to_existing_atom(Fun, utf8)}}
    catch
        error:badarg ->
            throw({bad_chash_keyfun, {Mod, Fun}})
    end;
erlify_bucket_prop({<<"ddl">>, Value}) ->
    {ddl, Value};
erlify_bucket_prop({<<"ddl_compiler_version">>, Value}) ->
    {ddl_compiler_version, Value};
erlify_bucket_prop({Prop, Value}) ->
    {validate_bucket_property(binary_to_list(Prop)), Value}.

validate_bucket_property(P) ->
    %% there's no validation; free-form properties are all allowed
    try
        list_to_existing_atom(P)
    catch
        error:badarg ->
            lager:info("Setting new custom bucket type property '~s'", [P]),
            list_to_atom(P)
    end.

%% @doc Populates the resource's context/state with the bucket type
%% from the path info, if not already set.
ensure_bucket_type(RD, Ctx, FNum) ->
    case {element(FNum, Ctx), wrq:path_info(bucket_type, RD)} of
        {undefined, undefined} ->
            setelement(FNum, Ctx, <<"default">>);
        {_BType, undefined} ->
            Ctx;
        {_, B} ->
            BType = list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, B)),
            setelement(FNum, Ctx, BType)
    end.

%% @doc Checks whether the given bucket type "exists" (for use in
%% resource_exists callback).
bucket_type_exists(<<"default">>) -> true;
bucket_type_exists(Type) ->
    riak_core_bucket_type:get(Type) /= undefined.

%% Construct a {Type, Bucket} tuple, if not working with the default bucket
maybe_bucket_type(undefined, B) ->
    B;
maybe_bucket_type(<<"default">>, B) ->
    B;
maybe_bucket_type(T, B) ->
    {T, B}.

%% @doc Maps an HTTP method to an internal permission
%%-spec method_to_perm(atom()) -> string().
method_to_perm('POST') ->
    "riak_kv.put";
method_to_perm('PUT') ->
    "riak_kv.put";
method_to_perm('HEAD') ->
    "riak_kv.get";
method_to_perm('GET') ->
    "riak_kv.get";
method_to_perm('DELETE') ->
    "riak_kv.delete".

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

erlify_property_check_valid_test() ->
    ?assertEqual({datatype, integer},
                 erlify_bucket_prop({?JSON_DATATYPE, <<"integer">>})),

    ?assertEqual({linkfun, {modfun, erlang, halt}},
                 erlify_bucket_prop({?JSON_LINKFUN, {struct, [{?JSON_MOD, <<"erlang">>},
                                                              {?JSON_FUN, <<"halt">>}]}})),
    ?assertEqual({linkfun, {jsfun, <<"js_never_dies">>}},
                 erlify_bucket_prop({?JSON_LINKFUN, {struct, [{?JSON_JSFUN, <<"js_never_dies">>}]}})),

    ?assertEqual({linkfun, {jsanon, {<<"face">>, <<"book">>}}},
                 erlify_bucket_prop({?JSON_LINKFUN, {struct, [{?JSON_JSANON, {struct, [{?JSON_JSBUCKET, <<"face">>},
                                                                                       {?JSON_JSKEY, <<"book">>}]}}]}})),
    ?assertEqual({chash_keyfun, {re, run}},
                 erlify_bucket_prop({?JSON_CHASH, {struct, [{?JSON_MOD, <<"re">>},
                                                            {?JSON_FUN, <<"run">>}]}})).

erlify_property_check_exceptions_test() ->
    ?assertThrow({bad_datatype, 42},
                 erlify_bucket_prop({?JSON_DATATYPE, 42})),
    ?assertThrow({bad_datatype, <<"tatadype">>},
                 erlify_bucket_prop({?JSON_DATATYPE, <<"tatadype">>})),

    ?assertThrow({bad_linkfun_modfun, {<<"nomod">>, <<"nofun">>}},
                 erlify_bucket_prop({?JSON_LINKFUN, {struct, [{?JSON_MOD, <<"nomod">>},
                                                              {?JSON_FUN, <<"nofun">>}]}})),
    ?assertThrow({bad_linkfun_modfun, {<<"nomod">>, 368}},
                 erlify_bucket_prop({?JSON_LINKFUN, {struct, [{?JSON_MOD, <<"nomod">>},
                                                              {?JSON_FUN, 368}]}})),
    ?assertThrow({bad_linkfun_modfun, 635},
                 erlify_bucket_prop({?JSON_LINKFUN, {struct, [{?JSON_JSFUN, 635}]}})),

    ?assertThrow({bad_linkfun_bkey, {nobinarybucket, 89}},
                 erlify_bucket_prop({?JSON_LINKFUN, {struct, [{?JSON_JSANON, {struct, [{?JSON_JSBUCKET, nobinarybucket},
                                                                                       {?JSON_JSKEY, 89}]}}]}})),
    ?assertThrow({bad_chash_keyfun, {<<"nomod">>, <<"nofun">>}},
                 erlify_bucket_prop({?JSON_CHASH, {struct, [{?JSON_MOD, <<"nomod">>},
                                                            {?JSON_FUN, <<"nofun">>}]}})).
allowable_origin_riak() -> {http, "localhost"}.
allowable_origin_none_registered_test() ->
    register_allowable_origins(""),
    ?assertEqual(false,
                 allowable_origin(allowable_origin_riak(), {http, "notlocalhost"})).

allowable_origin_not_registered_test() ->
    register_allowable_origins("http://basho.com"),
    ?assertEqual(false,
                 allowable_origin(allowable_origin_riak(), {http, "notlocalhost"})).

allowable_origin_single_registered_test() ->
    register_allowable_origins("https://notlocalhost"),
    ?assertEqual(true,
                 allowable_origin(allowable_origin_riak(), {https, "notlocalhost"})).

allowable_origin_multi_registered_test() ->
    register_allowable_origins("http://basho.com,https://notlocalhost"),
    ?assertEqual(true,
                 allowable_origin(allowable_origin_riak(), {https, "notlocalhost"})).

allowable_origin_wildcard_registered_test() ->
    register_allowable_origins("*"),
    ?assertEqual(true,
                 allowable_origin(allowable_origin_riak(), {https, "notlocalhost"})).
-endif.
