%% -------------------------------------------------------------------
%%
%% riak_kv_wm_utils: Common functions used by riak_kv_wm_* modules.
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
         accept_value/2,
         any_to_list/1,
         any_to_bool/1,
         is_forbidden/1,
         jsonify_bucket_prop/1,
         erlify_bucket_prop/1,
         ensure_bucket_type/3,
         bucket_type_exists/1,
         maybe_bucket_type/2,
         method_to_perm/1,
         maybe_parse_table_def/2
        ]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

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

-spec accept_value(string(), binary()) -> term().
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

is_forbidden(RD) ->
    is_null_origin(RD) or
    (app_helper:get_env(riak_kv,secure_referer_check,true) and not is_valid_referer(RD)).

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
            lager:debug("WM referrer not origin.  Origin ~p != Referer ~p\n", [OriginTuple, RefererTuple]),
            false
    end.

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
jsonify_bucket_prop({Prop, Value}) ->
    {atom_to_binary(Prop, utf8), Value}.

-spec erlify_bucket_prop({Property::binary(), jsonpropvalue()}) ->
          {Property::atom(), erlpropvalue()}.
%% @doc The reverse of jsonify_bucket_prop/1.  Converts JSON representation
%%      of bucket properties to their Erlang form.
erlify_bucket_prop({?JSON_DATATYPE, Type}) when is_binary(Type) ->
    {datatype, binary_to_existing_atom(Type, utf8)};
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
erlify_bucket_prop({<<"ddl">>, Value}) ->
    {ddl, Value};
erlify_bucket_prop({Prop, Value}) ->
    {list_to_existing_atom(binary_to_list(Prop)), Value}.

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

%% Given bucket properties, transform the `<<"table_def">>' property if it
%% exists to riak_ql DDL and store it under the `<<"ddl">>' key, table_def
%% is removed.  
-spec maybe_parse_table_def(BucketType :: binary(),
                            Props :: list(proplists:property())) -> 
        {ok, Props2 :: [proplists:property()]} | {error, any()}.
maybe_parse_table_def(BucketType, Props) ->
    case lists:keytake(<<"table_def">>, 1, Props) of
        false ->
            {ok, Props};
        {value, {<<"table_def">>, TableDef}, PropsNoDef} ->
            case riak_ql_parser:parse(riak_ql_lexer:get_tokens(binary_to_list(TableDef))) of
                {ok, DDL} ->
                    ok = assert_type_and_table_name_same(BucketType, DDL),
                    ok = try_compile_ddl(DDL),
                    ok = assert_write_once_not_false(PropsNoDef),
                    ok = assert_partition_key_length(DDL),
                    ok = assert_primary_and_local_keys_match(DDL),
                    apply_timeseries_bucket_props(DDL, PropsNoDef);
                {error, _} = E ->
                    E
            end
    end.

%%
-spec apply_timeseries_bucket_props(DDL::#ddl_v1{},
                                    Props1::[proplists:property()]) -> 
        {ok, Props2::[proplists:property()]}.
apply_timeseries_bucket_props(DDL, Props1) ->
    Props2 = lists:keystore(
        <<"write_once">>, 1, Props1, {<<"write_once">>, true}),
    {ok, [{<<"ddl">>, DDL} | Props2]}.

%% Time series must always use write_once so throw an error if the write_once
%% property is ever set to false. This prevents a user thinking they have
%% disabled write_once when it has been set to true internally.
assert_write_once_not_false(Props) ->
    case lists:keyfind(<<"write_once">>, 1, Props) of
        {<<"write_once">>, false} ->
            throw({error,
                "Error, the bucket type could not be the created. "
                "The write_once property had a value of false but must be true "
                "or left blank\n"});
        _ ->
            ok
    end.

%%
assert_type_and_table_name_same(BucketType, #ddl_v1{table = BucketType}) ->
    ok;
assert_type_and_table_name_same(BucketType1, #ddl_v1{table = BucketType2}) ->
    throw({error,
        "Error, the bucket type could not be the created. The bucket type and table name must be the same~n"
        "    bucket type was: " ++ binary_to_list(BucketType1) ++ "\n"
        "    table name was:  " ++ binary_to_list(BucketType2) ++ "\n"}).

%% @doc Verify that the primary key has three components
%%      and the first element is a quantum
assert_partition_key_length(#ddl_v1{partition_key = {key_v1, Key}}) when length(Key) == 3 ->
    assert_third_param_is_quantum(lists:nth(3, Key));
assert_partition_key_length(_DDL) ->
    throw({error, {primary_key, "Primary key is too short"}}).

%% @doc Verify that the first element of the primary key is a quantum
assert_third_param_is_quantum(#hash_fn_v1{mod=riak_ql_quanta, fn=quantum}) ->
    ok;
assert_third_param_is_quantum(_KeyComponent) ->
    throw({error, {primary_key, "Third element of primary key must be a quantum"}}).

%% @doc Verify primary key and local partition have the same elements
assert_primary_and_local_keys_match(#ddl_v1{partition_key = #key_v1{ast = Primary}, local_key = #key_v1{ast = Local}}) ->
    PrimaryList = [query_field_name(F) || F <- Primary],
    LocalList = [query_field_name(F) || F <- Local],
    case PrimaryList == LocalList of
        true -> ok;
        _Else ->
            throw({error, {primary_key, "Local key does not match primary key"}})
    end.

%% Pull the name out of the appropriate record
query_field_name(#hash_fn_v1{args = Args}) ->
    Param = lists:keyfind(param_v1, 1, Args),
    query_field_name(Param);
query_field_name(#param_v1{name = Field}) ->
    Field.

%% Attempt to compile the DDL but don't do anything with the output, this is
%% catch failures as early as possible. Also the error messages are easy to
%% return at this point.
try_compile_ddl(DDL) ->
    {_, AST} = riak_ql_ddl_compiler:compile(DDL),
    {ok, _, _} = compile:forms(AST),
    ok.
