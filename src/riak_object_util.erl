%% -------------------------------------------------------------------
%%
%% riak_object_util: Utilites for dealing with riak_objects
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

%% @doc Utilites for dealing with riak_objects
-module(riak_object_util).
-export([to_json/1, from_json/1]).

%% @spec to_json(riak_object()) -> {struct, list(any())}
%% @doc Converts a riak_object into its JSON equivalent
to_json(Obj) ->
    {_,Vclock} = riak_kv_wm_raw:vclock_header(Obj),
    {struct, [{<<"bucket">>, riak_object:bucket(Obj)},
              {<<"key">>, riak_object:key(Obj)},
              {<<"vclock">>, list_to_binary(Vclock)},
              {<<"values">>,
               [{struct,
                 [{<<"metadata">>, jsonify_metadata(MD)},
                  {<<"data">>, V}]}
                || {MD, V} <- riak_object:get_contents(Obj)
                      ]}]}.

-spec from_json(any()) -> riak_object:riak_object().
from_json({struct, Obj}) ->
    from_json(Obj);
from_json(Obj) ->
    Bucket = proplists:get_value(<<"bucket">>, Obj),
    Key = proplists:get_value(<<"key">>, Obj),
    VClock0 = proplists:get_value(<<"vclock">>, Obj),
    VClock = binary_to_term(zlib:unzip(base64:decode(VClock0))),
    [{struct, Values}] = proplists:get_value(<<"values">>, Obj),
    RObj0 = riak_object:new(Bucket, Key, <<"">>),
    RObj1 = riak_object:set_vclock(RObj0, VClock),
    riak_object:set_contents(RObj1, dejsonify_values(Values, [])).

jsonify_metadata(MD) ->
    MDJS = fun({LastMod, Now={_,_,_}}) ->
                   % convert Now to JS-readable time string
                   {LastMod, list_to_binary(
                               httpd_util:rfc1123_date(
                                 calendar:now_to_local_time(Now)))};
              ({<<"Links">>, Links}) ->
                   {<<"Links">>, [ [B, K, T] || {{B, K}, T} <- Links ]};
              ({Name, List=[_|_]}) ->
                   {Name, jsonify_metadata_list(List)};
              ({Name, Value}) ->
                   {Name, Value}
           end,
    {struct, lists:map(MDJS, dict:to_list(MD))}.

%% @doc convert strings to binaries, and proplists to JSON objects
jsonify_metadata_list([]) -> [];
jsonify_metadata_list(List) ->
    Classifier = fun({Key,_}, Type) when (is_binary(Key) orelse is_list(Key)),
                                         Type /= array, Type /= string ->
                         struct;
                    (C, Type) when is_integer(C), C >= 0, C =< 256,
                                   Type /= array, Type /= struct ->
                         string;
                    (_, _) ->
                         array
                 end,
    case lists:foldl(Classifier, undefined, List) of
        struct -> {struct, [ {if is_list(Key) -> list_to_binary(Key);
                                 true         -> Key
                              end,
                              if is_list(Value) -> jsonify_metadata_list(Value);
                                 true           -> Value
                              end}
                             || {Key, Value} <- List]};
        string -> list_to_binary(List);
        array -> List
    end.

dejsonify_values([], Accum) ->
    lists:reverse(Accum);
dejsonify_values([{<<"metadata">>, {struct, MD0}},
                   {<<"data">>, D}|T], Accum) ->
    Converter = fun({Key, Val}) ->
                        case Key of
                            <<"Links">> ->
                                {Key, [{{B, K}, Tag} || [B, K, Tag] <- Val]};
                            <<"X-Riak-Last-Modified">> ->
                                {Key, erlang:now()};
                            _ ->
                                {Key, if
                                          is_binary(Val) ->
                                              binary_to_list(Val);
                                          true ->
                                              Val
                                      end}
                        end
                end,
    MD = dict:from_list([Converter(KV) || KV <- MD0]),
    dejsonify_values(T, [{MD, D}|Accum]).

