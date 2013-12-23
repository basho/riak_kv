%% -------------------------------------------------------------------
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

%% @doc JSON encoding/decoding utilities for riak_object


-module(riak_object_json).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include("riak_kv_wm_raw.hrl").
-include("riak_object.hrl").

-export([encode/1,decode/1]).

%% @doc Converts a riak_object into its JSON equivalent
-spec encode(riak_object:riak_object()) -> {struct, list(any())}.
encode(Obj) ->
    {_,Vclock} = riak_object:vclock_header(Obj),
    {struct, [{<<"bucket_type">>, riak_object:type(Obj)},
              {<<"bucket">>, riak_object:bucket_only(Obj)},
              {<<"key">>, riak_object:key(Obj)},
              {<<"vclock">>, list_to_binary(Vclock)},
              {<<"values">>,
               [{struct,
                 [{<<"metadata">>, jsonify_metadata(MD)},
                  {<<"data">>, V}]}
                || {MD, V} <- riak_object:get_contents(Obj)
               ]}]}.

-spec decode(any()) -> riak_object:riak_object().
decode({struct, Obj}) ->
    decode(Obj);
decode(Obj) ->
    BucketType = proplists:get_value(<<"bucket_type">>, Obj),
    Bucket0 = proplists:get_value(<<"bucket">>, Obj),
    Bucket = case BucketType of
        null ->
            Bucket0;
        undefined ->
            Bucket0;
        <<"default">> ->
            Bucket0;
        _ ->
            {BucketType, Bucket0}
    end,
    Key = proplists:get_value(<<"key">>, Obj),
    VClock0 = proplists:get_value(<<"vclock">>, Obj),
    VClock = riak_object:decode_vclock(base64:decode(VClock0)),
    [{struct, Values}] = proplists:get_value(<<"values">>, Obj),
    RObj0 = riak_object:new(Bucket, Key, <<"">>),
    RObj1 = riak_object:set_vclock(RObj0, VClock),
    riak_object:set_contents(RObj1, dejsonify_values(Values, [])).

jsonify_metadata(MD) ->
    L = [jsonify_pair(Pair) || {Key,_}=Pair <- dict:to_list(MD),
                               Key /= ?DOT],
    {struct, L}.

-spec jsonify_pair({term(), term()}) -> {term(), term()}.
jsonify_pair({LastMod, Now={_,_,_}}) ->
    %% convert Now to JS-readable time string
    {LastMod, list_to_binary(
                httpd_util:rfc1123_date(
                  calendar:now_to_local_time(Now)))};
jsonify_pair({?MD_USERMETA, []}) ->
    %% When the user metadata is empty, it should still be a struct
    {?MD_USERMETA, {struct, []}};
jsonify_pair({?MD_LINKS, Links}) ->
    {?MD_LINKS, [ [B, K, T] || {{B, K}, T} <- Links ]};
jsonify_pair({Name, List=[_|_]}) ->
    {Name, jsonify_metadata_list(List)};
jsonify_pair({Name, Value}) ->
    {Name, Value}.

%% @doc convert strings to binaries, and proplists to JSON objects
jsonify_metadata_list([]) -> [];
jsonify_metadata_list(List) ->
    Classifier = fun({Key,_}, Type) when (is_binary(Key) orelse is_list(Key)),
                                         Type /= array, Type /= string ->
                         struct;
                    (C, Type) when is_integer(C), C >= 0, C < 256,
                                   Type /= array, Type /= struct ->
                         string;
                    (_, _) ->
                         array
                 end,
    case lists:foldl(Classifier, undefined, List) of
        struct -> {struct, jsonify_proplist(List)};
        string -> list_to_binary(List);
        array -> List
    end.

%% @doc converts a proplist with potentially multiple values for the
%%    same key into a JSON object with single or multi-valued keys.
jsonify_proplist([]) -> [];
jsonify_proplist(List) ->
    dict:to_list(lists:foldl(fun({Key, Value}, Dict) ->
                                     JSONKey = if is_list(Key) -> list_to_binary(Key);
                                                  true -> Key
                                               end,
                                     JSONVal = if is_list(Value) -> jsonify_metadata_list(Value);
                                                  true -> Value
                                               end,
                                     case dict:find(JSONKey, Dict) of
                                         {ok, ListVal} when is_list(ListVal) ->
                                             dict:append(JSONKey, JSONVal, Dict);
                                         {ok, Other} ->
                                             dict:store(JSONKey, [Other,JSONVal], Dict);
                                         _ ->
                                             dict:store(JSONKey, JSONVal, Dict)
                                     end
                             end, dict:new(), List)).

dejsonify_values([], Accum) ->
    lists:reverse(Accum);
dejsonify_values([{<<"metadata">>, {struct, MD0}},
                  {<<"data">>, D}|T], Accum) ->
    Converter = fun({Key, Val}) ->
                        case Key of
                            ?MD_LINKS ->
                                {Key, [{{B, K}, Tag} || [B, K, Tag] <- Val]};
                            ?MD_LASTMOD ->
                                {Key, os:timestamp()};
                            _ ->
                                {Key, if
                                          is_binary(Val) ->
                                              binary_to_list(Val);
                                          true ->
                                              dejsonify_meta_value(Val)
                                      end}
                        end
                end,
    MD = dict:from_list([Converter(KV) || KV <- MD0]),
    dejsonify_values(T, [{MD, D}|Accum]).

%% @doc convert structs back into proplists
dejsonify_meta_value({struct, PList}) ->
    lists:foldl(fun({Key, List}, Acc) when is_list(List) ->
                        %% This reverses the {k,v},{k,v2} pattern that
                        %% is possible in multi-valued indexes.
                        [{Key, dejsonify_meta_value(L)} || L <- List] ++ Acc;
                    ({Key, V}, Acc) ->
                        [{Key, dejsonify_meta_value(V)}|Acc]
                end, [], PList);
dejsonify_meta_value(Value) -> Value.

-ifdef(TEST).

jsonify_multivalued_indexes_test() ->
    Indexes = [{<<"test_bin">>, <<"one">>},
               {<<"test_bin">>, <<"two">>},
               {<<"test2_int">>, 4}],
    ?assertEqual({struct, [{<<"test2_int">>,4},{<<"test_bin">>,[<<"one">>,<<"two">>]}]},
                 jsonify_metadata_list(Indexes)).


jsonify_round_trip_test() ->
    Links = [{{<<"b">>,<<"k2">>},<<"tag">>},
             {{<<"b2">>,<<"k2">>},<<"tag2">>}],
    Indexes = [{<<"test_bin">>, <<"one">>},
               {<<"test_bin">>, <<"two">>},
               {<<"test2_int">>, 4}],
    Meta = [{<<"foo">>, <<"bar">>}, {<<"baz">>, <<"quux">>}],
    MD = dict:from_list([{?MD_USERMETA, Meta},
                         {?MD_CTYPE, "application/json"},
                         {?MD_INDEX, Indexes},
                         {?MD_LINKS, Links}]),
    [begin
            O = riak_object:new(B, K, V, MD),
            O2 = decode(encode(O)),
            ?assertEqual(riak_object:bucket(O), riak_object:bucket(O2)),
            ?assertEqual(riak_object:key(O), riak_object:key(O2)),
            ?assert(vclock:equal(riak_object:vclock(O), riak_object:vclock(O2))),
            ?assertEqual(lists:sort(Meta),
                         lists:sort(dict:fetch(?MD_USERMETA,
                                               riak_object:get_metadata(O2)))),
            ?assertEqual(Links, dict:fetch(?MD_LINKS, riak_object:get_metadata(O2))),
            ?assertEqual(lists:sort(Indexes), lists:sort(riak_object:index_data(O2))),
            ?assertEqual(riak_object:get_contents(O), riak_object:get_contents(O2))
        end || {B, K, V} <- [{<<"b">>, <<"k">>, <<"{\"a\":1}">>},
                             {{<<"t">>, <<"b">>}, <<"k2">>, <<"{\"a\":2}">>}]].

-endif.
