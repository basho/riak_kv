%% -------------------------------------------------------------------
%%
%% riak_kv_wm_utils: Common functions used by riak_kv_wm_* modules.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
         vclock_header/1,
         encode_vclock/1,
         format_links/3,
         encode_value/1,
         accept_value/2,
         any_to_list/1,
         any_to_bool/1
        ]).

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

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


%% @spec default_encodings() -> [{Encoding::string(), Producer::function()}]
%% @doc The default encodings available: identity and gzip.
default_encodings() ->
    [{"identity", fun(X) -> X end},
     {"gzip", fun(X) -> zlib:gzip(X) end}].

%% @spec multipart_encode_body(string(), binary(), {dict(), binary()}) -> iolist()
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

%% @spec vclock_header(riak_object()) -> {Name::string(), Value::string()}
%% @doc Transform the Erlang representation of the document's vclock
%%      into something suitable for an HTTP header
vclock_header(Doc) ->
    {?HEAD_VCLOCK,
        encode_vclock(riak_object:vclock(Doc))}.

encode_vclock(VClock) ->
    binary_to_list(base64:encode(zlib:zip(term_to_binary(VClock)))).

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
            2 -> 
                io_lib:format("</buckets/~s>; rel=\"~s\"",
                              [Bucket1, Tag1])
        end,
    format_links(Rest, Prefix, APIVersion, [{?HEAD_LINK, Val}|Acc]);
format_links([{Bucket, Key, Tag}|Rest], Prefix, APIVersion, Acc) ->
    Bucket1 = mochiweb_util:quote_plus(Bucket),
    Key1 = mochiweb_util:quote_plus(Key),
    Tag1 = mochiweb_util:quote_plus(Tag),
    Val = 
        case APIVersion of 
            1 ->
                io_lib:format("</~s/~s/~s>; riaktag=\"~s\"",
                              [Prefix, Bucket1, Key1, Tag1]);
            2 ->
                io_lib:format("</buckets/~s/keys/~s>; riaktag=\"~s\"",
                          [Bucket1, Key1, Tag1])
        end,
    format_links(Rest, Prefix, APIVersion, [{?HEAD_LINK, Val}|Acc]);
format_links([], _Prefix, _APIVersion, Acc) ->
    Acc.

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
