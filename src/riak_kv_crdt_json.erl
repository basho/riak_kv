%% -------------------------------------------------------------------
%%
%% riak_kv_crdt_json: Codec routines for CRDTs in JSON
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
-module(riak_kv_crdt_json).
-export([update_request_from_json/3, fetch_response_to_json/4]).
-compile([{inline, [bad_op/2, bad_field/1]}]).

-define(FIELD_PATTERN, "^(.*)_(counter|set|register|flag|map)$").

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("riak_kv_types.hrl").
-endif.

-export_type([context/0]).

%% Mostly copied from riak_pb_dt_codec
%% Value types
-opaque context() :: binary().
-type map_field() :: {binary(), embedded_type()}.
-type embedded_type() :: counter | set | register | flag | map.
-type toplevel_type() :: counter | set | map.
-type type_mappings() :: [{embedded_type(), module()}].

%% Operations
-type counter_op() :: increment | decrement | {increment | decrement, integer()}.
-type simple_set_op() :: {add, binary()} | {remove, binary()} | {add_all, [binary()]} | {remove_all, [binary()]}.
-type set_op() :: simple_set_op() | {update, [simple_set_op()]}.
-type flag_op() :: enable | disable.
-type register_op() :: {assign, binary()}.
-type simple_map_op() :: {add, map_field()} | {remove, map_field()} | {update, map_field(), embedded_type_op()}.
-type map_op() :: simple_map_op() | {update, [simple_map_op()]}.
-type embedded_type_op() :: counter_op() | set_op() | register_op() | flag_op() | map_op().
-type toplevel_op() :: counter_op() | set_op() | map_op().
-type update() :: {toplevel_type(), toplevel_op(), context()}.

%% @doc Encodes a fetch response as a JSON struct, ready for
%% serialization with mochijson2.
-spec fetch_response_to_json(toplevel_type(), term(), context(), type_mappings()) -> mochijson2:json_object().
fetch_response_to_json(Type, Value, Context, Mods) ->
    {struct, [{<<"type">>, atom_to_binary(Type, utf8)},
              {<<"value">>, value_to_json(Type, Value, Mods)}] ++
         [ {<<"context">>, base64:encode(Context)} || Context /= undefined, Context /= <<>> ]}.

%% @doc Decodes a JSON value into an update operation.
-spec update_request_from_json(toplevel_type(), mochijson2:json_term(), type_mappings()) -> update().
update_request_from_json(Type, JSON0, Mods) ->
    {JSON, Context} = extract_context(JSON0),
    {Type, op_from_json(Type, JSON, Mods), Context}.

%% NB we assume that the internal format is well-formed and don't guard it.
value_to_json(counter, Int, _) -> Int;
value_to_json(set, List, _) -> List;
value_to_json(flag, Bool, _) -> Bool;
value_to_json(register, Bin, _) -> Bin;
value_to_json(map, Pairs, Mods) ->
    {struct,
     [ begin
           JSONField = {_Name,Type} = field_to_type(Key, Mods),
           {field_to_json(JSONField), value_to_json(Type, Value, Mods)}
       end || {Key, Value} <- Pairs ]}.

-spec extract_context(mochijson2:json_value()) -> {mochijson2:json_value(), undefined | context()}.
extract_context({struct, Fields0}=JSON) ->
    case lists:keytake(<<"context">>, 1, Fields0) of
        {value, {<<"context">>, Ctx}, Fields} ->
            {{struct, Fields}, decode_context(Ctx)};
        false ->
            {JSON, undefined}
    end;
extract_context(JSON) ->
    %% We will allow non-object requests for counters, since they only
    %% have 'increment' ops after all. {'increment':Int} is acceptable
    %% as well.
    {JSON, undefined}.

-spec decode_context(base64:ascii_binary()) -> context().
decode_context(Bin) when is_binary(Bin) ->
    try
        base64:decode(Bin)
    catch
        exit:_BadMatch ->
            throw({invalid_context, Bin})
    end.

-spec field_to_mod({binary(), toplevel_type()}, type_mappings()) -> {binary(), module()}.
field_to_mod({Name, Type}=Field, Mods) ->
    case lists:keyfind(Type, 1, Mods) of
        false ->
            Field;
        {Type, Mod} ->
            {Name, Mod}
    end.

-spec field_to_type({binary(), module()}, type_mappings()) -> {binary(), toplevel_type()}.
field_to_type({Name, Mod}=Field, Mods) ->
    case lists:keyfind(Mod, 2, Mods) of
        false ->
            Field;
        {Type, Mod} ->
            {Name, Type}
    end.

-spec field_to_json(map_field()) -> binary().
field_to_json({Name, Type}) when is_binary(Name), is_atom(Type) ->
    BinType = atom_to_binary(Type, utf8),
    <<Name/bytes, $_, BinType/bytes>>.

-spec field_from_json(binary()) -> map_field().
field_from_json(Bin) when is_binary(Bin) ->
    case re:run(Bin, ?FIELD_PATTERN, [anchored, {capture, all_but_first, binary}]) of
        {match, [Name, BinType]} ->
            {Name, binary_to_existing_atom(BinType, utf8)};
        _ ->
            bad_field(Bin)
    end.

-spec op_from_json(embedded_type(), mochijson2:json_term(), type_mappings()) -> embedded_type_op().
op_from_json(flag, Op, _Mods) -> flag_op_from_json(Op);
op_from_json(register, Op, _Mods) -> register_op_from_json(Op);
op_from_json(counter, Op, _Mods) -> counter_op_from_json(Op);
op_from_json(set, Op, _Mods) -> set_op_from_json(Op);
op_from_json(map, Op, Mods) -> map_op_from_json(Op, Mods).

%% Map: {"update":{Field:Op, ...}}
-spec map_op_from_json(mochijson2:json_object(), type_mappings()) -> map_op().
map_op_from_json({struct, Ops0}=InOp, Mods) ->
    Ops = lists:keymap(fun(<<"update">>) -> update;
                          (<<"add">>) -> add;
                          (<<"remove">>) -> remove;
                          (Op) -> bad_op(map, Op)
                       end, 1, Ops0),
    {struct, Updates} = proplists:get_value(update, Ops, {struct, []}),
    Adds = case proplists:get_value(add, Ops, []) of
               AddBin when is_binary(AddBin) ->
                   [AddBin];
               AddList when is_list(AddList) -> AddList;
               _ -> bad_op(map, InOp)
           end,
    Removes = case proplists:get_value(remove, Ops, []) of
               RemoveBin when is_binary(RemoveBin) ->
                   [RemoveBin];
               RemoveList when is_list(RemoveList) -> RemoveList;
               _ -> bad_op(map, InOp)
           end,
    {update, [ {remove, field_to_mod(field_from_json(Field), Mods)} || Field <- Removes ] ++
             [ {add, field_to_mod(field_from_json(Field), Mods)} || Field <- Adds ] ++
         [ map_update_op_from_json(Update, Mods) || Update <- Updates ]};
map_op_from_json(Op, _Mods) ->
    bad_op(map, Op).


-spec map_update_op_from_json({mochijson2:json_string(), mochijson2:json_term()}, type_mappings()) ->
                                     {update, map_field(), embedded_type_op()}.
map_update_op_from_json({JSONField, Op}, Mods) ->
    Field = {_Name, Type} = field_from_json(JSONField),
    {update, field_to_mod(Field, Mods), op_from_json(Type, Op, Mods)}.

-spec flag_op_from_json(binary()) -> flag_op().
flag_op_from_json(<<"enable">>) -> enable;
flag_op_from_json(<<"disable">>) -> disable;
flag_op_from_json(Op) -> bad_op(flag, Op).

-spec register_op_from_json(mochijson2:json_object()) -> register_op().
register_op_from_json({struct, [{<<"assign">>, Value}]}) when is_binary(Value) -> {assign, Value};
register_op_from_json(Value) when is_binary(Value) -> {assign, Value};
register_op_from_json(Op) -> bad_op(register, Op).

-spec counter_op_from_json(mochijson2:json_term()) -> counter_op().
counter_op_from_json(Int) when is_integer(Int), Int >= 0 -> {increment, Int};
counter_op_from_json(Int) when is_integer(Int), Int < 0 -> {decrement, -Int};
counter_op_from_json(<<"increment">>) -> increment;
counter_op_from_json(<<"decrement">>) -> decrement;
counter_op_from_json({struct, [{<<"increment">>,Int}]}) when is_integer(Int) -> {increment, Int};
counter_op_from_json({struct, [{<<"decrement">>,Int}]}) when is_integer(Int) -> {decrement, Int};
counter_op_from_json(Op) -> bad_op(counter, Op).


-spec set_op_from_json(mochijson2:json_term() |
                       {mochijson2:json_string(), mochijson2:json_term()}) ->
                              set_op().
set_op_from_json({struct, Ops}) when is_list(Ops) ->
    try
        {update, [ set_op_from_json(Op) || Op <- Ops]}
    catch
        throw:{invalid_operation, {set, _}} ->
            bad_op(set, {struct, Ops})
    end;
set_op_from_json({<<"add">>, Bin}) when is_binary(Bin) -> {add, Bin};
set_op_from_json({<<"remove">>, Bin}) when is_binary(Bin) -> {remove, Bin};
set_op_from_json({Verb, BinList}=Op) when is_list(BinList), (Verb == <<"add_all">> orelse
                                                             Verb == <<"remove_all">>)->
    case check_set_members(BinList) of
        true ->
            {binary_to_atom(Verb, utf8), BinList};
        false ->
            bad_op(set, Op)
    end;
set_op_from_json({<<"update">>, {struct, Ops}}) when is_list(Ops) ->
    {update, [ set_op_from_json(Op) || Op <- Ops]};
set_op_from_json({<<"update">>, Ops}) when is_list(Ops) ->
    {update, [ set_op_from_json(Op) || Op <- Ops]};
set_op_from_json(Op) -> bad_op(set, Op).

-spec check_set_members([term()]) -> boolean().
check_set_members(BinList) ->
    lists:all(fun erlang:is_binary/1, BinList).

-spec bad_op(atom(), term()) -> no_return().
bad_op(Type, Op) ->
    throw({invalid_operation, {Type, Op}}).

-spec bad_field(binary()) -> no_return().
bad_field(Bin) ->
    throw({invalid_field_name, Bin}).

-ifdef(TEST).

encode_fetch_response_test_() ->
    [
     {"encode counter",
      fun() ->
              {ok, Counter} = ?COUNTER_TYPE:update({increment, 5}, a, ?COUNTER_TYPE:new()),
              ?assertEqual({struct, [{<<"type">>,<<"counter">>},
                                     {<<"value">>, 5}]},
                           fetch_response_to_json(counter, ?COUNTER_TYPE:value(Counter), undefined, ?MOD_MAP)),
              ?assertEqual({struct, [{<<"type">>,<<"counter">>},
                                     {<<"value">>, 5}]},
                           fetch_response_to_json(counter, ?COUNTER_TYPE:value(Counter), <<>>, ?MOD_MAP))
      end},
     {"encode set",
      fun() ->
              {ok, Set} = ?SET_TYPE:update({add_all, [<<"a">>, <<"b">>, <<"c">>]}, a, ?SET_TYPE:new()),
              ?assertEqual({struct, [{<<"type">>, <<"set">>},
                                     {<<"value">>, [<<"a">>,<<"b">>,<<"c">>]}]},
                           fetch_response_to_json(set, ?SET_TYPE:value(Set), undefined, ?MOD_MAP)),
              ?assertMatch({struct, [_Type, _Value, {<<"context">>, Bin}]} when is_binary(Bin),
                           fetch_response_to_json(set, ?SET_TYPE:value(Set),
                                                  ?SET_TYPE:to_binary(?SET_TYPE:precondition_context(Set)),
                                                  ?MOD_MAP))
      end},
     {"encode map",
      fun() ->
              {ok, Map} = ?MAP_TYPE:update({update,
                                            [
                                             {update, {<<"a">>, ?SET_TYPE},{add_all, [<<"a">>, <<"b">>, <<"c">>]}},
                                             {update, {<<"b">>, ?FLAG_TYPE}, enable},
                                             {update, {<<"c">>, ?REG_TYPE}, {assign, <<"sean">>}},
                                             {update, {<<"d">>, ?MAP_TYPE},
                                              {update, [{update, {<<"e">>, ?COUNTER_TYPE}, {increment,5}}]}}
                                            ]}, a, ?MAP_TYPE:new()),
              ?assertEqual({struct, [
                                     {<<"type">>, <<"map">>},
                                     {<<"value">>,
                                      {struct,
                                       [ % NB inverse order from the order of update-application
                                        {<<"d_map">>, {struct, [{<<"e_counter">>, 5}]}},
                                        {<<"c_register">>, <<"sean">>},
                                        {<<"b_flag">>, true},
                                        {<<"a_set">>, [<<"a">>, <<"b">>, <<"c">>]}]}}
                                     ]},
                           fetch_response_to_json(map, ?MAP_TYPE:value(Map), undefined, ?MOD_MAP)
                           ),
              ?assertMatch({struct, [_Type, _Value, {<<"context">>, Bin}]} when is_binary(Bin),
                           fetch_response_to_json(map, ?MAP_TYPE:value(Map),
                                                  ?MAP_TYPE:to_binary(?MAP_TYPE:precondition_context(Map)),
                                                  ?MOD_MAP))
      end}
    ].

decode_update_request_test_() ->
    [
     {"decode counter ops",
      fun() ->
              ?assertEqual({counter, increment, undefined},
                           update_request_from_json(counter, <<"increment">>, ?MOD_MAP)),
              ?assertEqual({counter, decrement, undefined},
                           update_request_from_json(counter, <<"decrement">>, ?MOD_MAP)),
              ?assertEqual({counter, {increment, 10}, undefined},
                           update_request_from_json(counter, 10, ?MOD_MAP)),
              ?assertEqual({counter, {decrement, 7}, undefined},
                           update_request_from_json(counter, -7, ?MOD_MAP)),
              ?assertEqual({counter, {decrement, 7}, undefined},
                           update_request_from_json(counter, {struct, [{<<"decrement">>, 7}]}, ?MOD_MAP)),
              ?assertEqual({counter, {increment, 10}, undefined},
                           update_request_from_json(counter, {struct, [{<<"increment">>, 10}]}, ?MOD_MAP)),
              %% Increment argument must be integer
              ?assertThrow({invalid_operation, {counter, _}},
                           update_request_from_json(counter, {struct, [{<<"increment">>, true}]}, ?MOD_MAP)),
              %% Only one operation allowed, increment or decrement
              ?assertThrow({invalid_operation, {counter, _}},
                           update_request_from_json(counter, {struct, [{<<"increment">>, 5},{<<"decrement">>, 10}]}, ?MOD_MAP)),
              %% Must have an operation, empty object invalid
              ?assertThrow({invalid_operation, {counter, _}},
                           update_request_from_json(counter, {struct, []}, ?MOD_MAP)),
              %% Empty list is an invalid op
              ?assertThrow({invalid_operation, {counter, _}},
                           update_request_from_json(counter, [], ?MOD_MAP))
      end},
     {"decode set ops",
      fun() ->
              %% All single mutations
              ?assertEqual({set, {update, [{add, <<"foo">>}]}, undefined},
                           update_request_from_json(set, {struct, [{<<"add">>, <<"foo">>}]}, ?MOD_MAP)),
              ?assertEqual({set, {update, [{add_all, [<<"foo">>, <<"bar">>]}]}, undefined},
                           update_request_from_json(set, {struct, [{<<"add_all">>, [<<"foo">>,<<"bar">>]}]}, ?MOD_MAP)),
              ?assertEqual({set, {update, [{remove, <<"foo">>}]}, undefined},
                           update_request_from_json(set, {struct, [{<<"remove">>, <<"foo">>}]}, ?MOD_MAP)),
              ?assertEqual({set, {update, [{remove_all, [<<"foo">>,<<"bar">>]}]}, undefined},
                           update_request_from_json(set, {struct, [{<<"remove_all">>, [<<"foo">>,<<"bar">>]}]}, ?MOD_MAP)),
              %% Multiple ops may be passed at once
              ?assertEqual({set, {update, [{add, <<"foo">>}, {remove_all, [<<"baz">>,<<"quux">>]}]}, undefined},
                           update_request_from_json(set, {struct, [{<<"add">>, <<"foo">>}, {<<"remove_all">>, [<<"baz">>,<<"quux">>]}]}, ?MOD_MAP)),
              %% All members must be binaries
              ?assertThrow({invalid_operation, {set, _}},
                           update_request_from_json(set, {struct, [{<<"add">>, <<"foo">>}, {<<"remove_all">>, [<<"bar">>,true]}]}, ?MOD_MAP)),
              %% Only valid operations are add/remove/add_all/remove_all
              ?assertThrow({invalid_operation, {set, _}},
                           update_request_from_json(set, {struct, [{<<"increment">>, 5}]}, ?MOD_MAP)),
              %% Context should be extracted properly
              {ok, Set} = ?SET_TYPE:update({add_all, [<<"a">>, <<"b">>, <<"c">>]}, a, ?SET_TYPE:new()),
              BinContext = ?SET_TYPE:to_binary(?SET_TYPE:precondition_context(Set)),
              {struct, [_Type, _Value, {<<"context">>, JSONCtx}]} = fetch_response_to_json(set, ?SET_TYPE:value(Set),
                                                                                       BinContext,
                                                                                       ?MOD_MAP),
              ?assertMatch({set, {update, [{remove, <<"a">>}]}, BinContext},
                           update_request_from_json(set, {struct, [{<<"remove">>, <<"a">>}, {<<"context">>, JSONCtx}]}, ?MOD_MAP))
      end},
     {"decode map ops",
      fun() ->
              %% Simple ops
              ?assertEqual({map, {update, [{add, {<<"a">>, ?COUNTER_TYPE}}]}, undefined},
                           update_request_from_json(map, {struct, [{<<"add">>, <<"a_counter">>}]}, ?MOD_MAP)),
              ?assertEqual({map, {update, [{add, {<<"a">>, ?COUNTER_TYPE}}, {add, {<<"a">>, ?SET_TYPE}}]}, undefined},
                           update_request_from_json(map, {struct, [{<<"add">>, [<<"a_counter">>, <<"a_set">>]}]}, ?MOD_MAP)),
              ?assertEqual({map, {update, [{remove, {<<"a">>, ?COUNTER_TYPE}}]}, undefined},
                           update_request_from_json(map, {struct, [{<<"remove">>, <<"a_counter">>}]}, ?MOD_MAP)),
              ?assertEqual({map, {update, [{remove, {<<"a">>, ?COUNTER_TYPE}}, {remove, {<<"a">>, ?SET_TYPE}}]}, undefined},
                           update_request_from_json(map, {struct, [{<<"remove">>, [<<"a_counter">>, <<"a_set">>]}]}, ?MOD_MAP)),
              %% Removes get ordered before adds, multiple ops extracted
              ?assertEqual({map, {update, [{remove, {<<"a">>, ?COUNTER_TYPE}}, {add, {<<"a">>, ?SET_TYPE}}]}, undefined},
                           update_request_from_json(map, {struct, [{<<"add">>, [<<"a_set">>]}, {<<"remove">>, [<<"a_counter">>]}]}, ?MOD_MAP)),
              %% Nested updates
              ?assertEqual({map, {update, [{update, {<<"a">>, ?COUNTER_TYPE}, increment}]}, undefined},
                           update_request_from_json(map, {struct, [{<<"update">>, {struct, [{<<"a_counter">>, <<"increment">>}]}}]}, ?MOD_MAP)),

              ?assertEqual({map, {update, [{update, {<<"b">>, ?MAP_TYPE}, 
                                            {update, [{update, {<<"c">>, ?REG_TYPE}, {assign, <<"foo">>}}]}}]}, undefined},
                           update_request_from_json(map, {struct, [{<<"update">>, 
                                                                    {struct, [{<<"b_map">>, 
                                                                               {struct, [{<<"update">>, 
                                                                                          {struct, [{<<"c_register">>, <<"foo">>}]}}]}}]}}]}, 
                                                    ?MOD_MAP)),
              ?assertEqual({map, {update, [{update, {<<"b">>, ?MAP_TYPE}, 
                                            {update, [{update, {<<"c">>, ?FLAG_TYPE}, enable}]}}]}, undefined},
                           update_request_from_json(map, {struct, [{<<"update">>, 
                                                                    {struct, [{<<"b_map">>, 
                                                                               {struct, [{<<"update">>, 
                                                                                          {struct, [{<<"c_flag">>, <<"enable">>}]}}]}}]}}]}, 
                                                    ?MOD_MAP)),

              %% Extract context
              {ok, Map} = ?MAP_TYPE:update({update,
                                            [
                                             {update, {<<"a">>, ?SET_TYPE},{add_all, [<<"a">>, <<"b">>, <<"c">>]}},
                                             {update, {<<"b">>, ?FLAG_TYPE}, enable},
                                             {update, {<<"c">>, ?REG_TYPE}, {assign, <<"sean">>}},
                                             {update, {<<"d">>, ?MAP_TYPE},
                                              {update, [{update, {<<"e">>, ?COUNTER_TYPE}, {increment,5}}]}}
                                            ]}, a, ?MAP_TYPE:new()),
              BinContext = ?MAP_TYPE:to_binary(?MAP_TYPE:precondition_context(Map)),
              {struct, [_Type, _Value, {<<"context">>, JSONCtx}]} = fetch_response_to_json(set, ?MAP_TYPE:value(Map),
                                                                                           BinContext,
                                                                                           ?MOD_MAP),
              ?assertMatch({map, {update, [{remove, {<<"a">>, ?SET_TYPE}}]}, BinContext},
                           update_request_from_json(map, {struct, [{<<"remove">>, <<"a_set">>}, {<<"context">>, JSONCtx}]}, ?MOD_MAP)),
                           
              %% Invalid field names
              ?assertThrow({invalid_field_name, <<"a_hash">>}, update_request_from_json(map, {struct, [{<<"add">>, <<"a_hash">>}]}, ?MOD_MAP)),
              ?assertThrow({invalid_field_name, <<"foo">>}, update_request_from_json(map, {struct, [{<<"remove">>, <<"foo">>}]}, ?MOD_MAP)),
              ?assertThrow({invalid_field_name, <<"b_blob">>}, 
                           update_request_from_json(map, {struct, 
                                                          [{<<"update">>, 
                                                            {struct, [{<<"a_map">>, 
                                                                       {struct, [{<<"remove">>, <<"b_blob">>}]}}]}}]}, ?MOD_MAP)),
              %% Invalid operations for maps
              ?assertThrow({invalid_operation, {map, _}},
                           update_request_from_json(map, <<"increment">>, ?MOD_MAP)),
              ?assertThrow({invalid_operation, {map, _}},
                           update_request_from_json(map, {struct, [{<<"increment">>, 5}]}, ?MOD_MAP)),
              ?assertThrow({invalid_operation, {map, _}},
                           update_request_from_json(map, {struct, [{<<"add_all">>, [<<"foo">>]}]}, ?MOD_MAP)),
              %% Invalid operations for nested types
              ?assertThrow({invalid_operation, {counter, _}},
                           update_request_from_json(map, 
                                                    {struct, [{<<"update">>, {struct, [{<<"a_counter">>, <<"poke">>}]}}]}, ?MOD_MAP)),
              ?assertThrow({invalid_operation, {flag, _}},
                           update_request_from_json(map, 
                                                    {struct, [{<<"update">>, {struct, [{<<"a_flag">>, 5}]}}]}, ?MOD_MAP)),
              ?assertThrow({invalid_operation, {register, _}},
                           update_request_from_json(map, 
                                                    {struct, [{<<"update">>, {struct, [{<<"a_register">>, true}]}}]}, ?MOD_MAP)),
              ?assertThrow({invalid_operation, {set, _}},
                           update_request_from_json(map, 
                                                    {struct, [{<<"update">>, {struct, [{<<"a_set">>, {struct, [{<<"delete">>, <<"bar">>}]}}]}}]}, ?MOD_MAP)),
              ?assertThrow({invalid_operation, {map, _}},
                           update_request_from_json(map, 
                                                    {struct, [{<<"update">>, {struct, [{<<"a_map">>, {struct, [{<<"delete">>, <<"bar">>}]}}]}}]}, ?MOD_MAP))

      end}
    ].
-endif.
