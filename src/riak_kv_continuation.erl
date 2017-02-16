%% -------------------------------------------------------------------
%%
%% riak_kv_continuation: creating and decoding continuation tokens
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Central module for creating and decoding continuation tokens.

-module(riak_kv_continuation).

-export([make_token/1,
         make_token/2,
         make_token/3,
         decode_token/1]).
-export_type([token/0]).

-opaque token() :: binary().

%% @doc Make a continuation token for the given `Term'.
-spec make_token(term()) -> token().
make_token(Term) ->
    base64:encode(term_to_binary(Term)).

%% @doc Make a continuation token if and only if the length of the list of
%% `Results' is greater than `MaxResults'. If so, the item in the list at
%% position `MaxResults' is used as the term to make the continuation token.
%% If not, `undefined' is returned.
%% <p>
%% For greater control over the value used to make the continuation token, see
%% `make_token/3', which allows for passing a function to transform the value
%% used to make the token.
-spec make_token(Results::list(), MaxResults::non_neg_integer()) -> token() | undefined.
make_token(Results, MaxResults) when is_list(Results),
                                     is_integer(MaxResults),
                                     MaxResults >= 0 ->
    make_token(Results, MaxResults, fun(X) -> X end).

%% @doc Make a continuation token if and only if the length of the list of
%% `Results' is greater than `MaxResults'. If so, the item in the list at
%% position `MaxResults' is used as the term to make the continuation token,
%% after first being passed to the given `Fun' to allow for transforming the
%% value. If not, `undefined' is returned.
-spec make_token(Results::list(), MaxResults::non_neg_integer(), fun((any()) -> any())) ->
    token() | undefined.
make_token(_Results, 0, _Fun) ->
    undefined;
make_token(Results, MaxResults, Fun) when is_list(Results),
                                          is_integer(MaxResults),
                                          MaxResults >= 0 ->
    case length(Results) > MaxResults of
        true ->
            LastItem = Fun(lists:last(lists:sublist(Results, MaxResults))),
            make_token(LastItem);
        false ->
            undefined
    end.

%% @doc Decode the given continuation `Token' and return the original Erlang
%% term that was used to generate the continuation.
-spec decode_token(token() | undefined) -> term().
decode_token(undefined) ->
    undefined;
decode_token(Token) ->
    binary_to_term(base64:decode(Token)).

%% ====================
%% TESTS
%% ====================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

make_token_from_binary_test() ->
    Value = <<"foo/bar/baz">>,
    Token = make_token(Value),
    ?assertEqual(Value, decode_token(Token)).

make_token_from_tuple_test() ->
    Value = {{<<"type">>, <<"bucket">>}, <<"foo/bar/baz">>},
    Token = make_token(Value),
    ?assertEqual(Value, decode_token(Token)).

make_token_from_results_list_test() ->
    Results = [a, b, c, d, e],
    ?assertEqual(a, decode_token(make_token(Results, 1))),
    ?assertEqual(b, decode_token(make_token(Results, 2))),
    ?assertEqual(c, decode_token(make_token(Results, 3))),
    ?assertEqual(d, decode_token(make_token(Results, 4))),
    ?assertEqual(undefined, decode_token(make_token(Results, 5))),
    ?assertEqual(undefined, decode_token(make_token(Results, 6))).

make_token_from_results_list_with_fun_test() ->
    Results = [{a, 1}, {b, 2}, {c, 3}, {d, 4}, {e, 5}],
    Fun = fun({Key, _}) -> Key end,
    ?assertEqual(a, decode_token(make_token(Results, 1, Fun))),
    ?assertEqual(b, decode_token(make_token(Results, 2, Fun))),
    ?assertEqual(c, decode_token(make_token(Results, 3, Fun))),
    ?assertEqual(d, decode_token(make_token(Results, 4, Fun))),
    ?assertEqual(undefined, decode_token(make_token(Results, 5, Fun))),
    ?assertEqual(undefined, decode_token(make_token(Results, 6, Fun))).

-endif.
