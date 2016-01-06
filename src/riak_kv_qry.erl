%%-------------------------------------------------------------------
%%
%% riak_kv_qry: Riak SQL API
%%
%% Copyright (C) 2015 Basho Technologies, Inc. All rights reserved
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
%%-------------------------------------------------------------------

%% @doc API endpoints for the Riak SQL.  Functions in this module
%%      prepare and validate raw queries, pass them to riak_kv_qry_queue

-module(riak_kv_qry).

-export([
         submit/2
        ]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-spec submit(string() | ?SQL_SELECT{} | #riak_sql_describe_v1{}, #ddl_v1{}) ->
    {ok, any()} | {error, any()}.
%% @doc Parse, validate against DDL, and submit a query for execution.
%%      To get the results of running the query, use fetch/1.
submit(SQLString, DDL) when is_list(SQLString) ->
    Lexed = riak_ql_lexer:get_tokens(SQLString),
    case riak_ql_parser:parse(Lexed) of
        {error, _Reason} = Error ->
            Error;
        {ok, SQL} ->
            submit(SQL, DDL)
    end;

submit(#riak_sql_describe_v1{}, DDL) ->
    describe_table_columns(DDL);

submit(SQL = ?SQL_SELECT{}, DDL) ->
    maybe_submit_to_queue(SQL, DDL).

%% ---------------------
%% local functions

-spec describe_table_columns(#ddl_v1{}) ->
                                    {ok, [[binary() | boolean() | integer() | undefined]]}.
describe_table_columns(#ddl_v1{fields = FieldSpecs,
                               partition_key = #key_v1{ast = PKSpec},
                               local_key     = #key_v1{ast = LKSpec}}) ->
    {ok,
     [[Name, list_to_binary(atom_to_list(Type)), Nullable,
       column_pk_position_or_blank(Name, PKSpec),
       column_lk_position_or_blank(Name, LKSpec)]
      || #riak_field_v1{name = Name,
                        type = Type,
                        optional = Nullable} <- FieldSpecs]}.

%% the following two functions are identical, for the way fields and
%% keys are represented as of 2015-12-18; duplication here is a hint
%% of things to come.
-spec column_pk_position_or_blank(binary(), [#param_v1{}]) -> integer() | undefined.
column_pk_position_or_blank(Col, KSpec) ->
    count_to_position(Col, KSpec, 1).

-spec column_lk_position_or_blank(binary(), [#param_v1{}]) -> integer() | undefined.
column_lk_position_or_blank(Col, KSpec) ->
    count_to_position(Col, KSpec, 1).

count_to_position(_, [], _) ->
    undefined;
count_to_position(Col, [#param_v1{name = [Col]} | _], Pos) ->
    Pos;
count_to_position(Col, [#hash_fn_v1{args = [#param_v1{name = [Col]} | _]} | _], Pos) ->
    Pos;
count_to_position(Col, [_ | Rest], Pos) ->
    count_to_position(Col, Rest, Pos + 1).


maybe_submit_to_queue(SQL, #ddl_v1{table = BucketType} = DDL) ->
    Mod = riak_ql_ddl:make_module_name(BucketType),
    case riak_ql_ddl:is_query_valid(Mod, DDL, SQL) of
        true ->
            case riak_kv_qry_compiler:compile(DDL, SQL) of
                {error,_} = Error ->
                    Error;
                {ok, Queries} ->
                    maybe_await_query_results(
                        riak_kv_qry_queue:put_on_queue(self(), Queries, DDL))
            end;
        {false, Errors} ->
            {error, {invalid_query, format_query_syntax_errors(Errors)}}
    end.

maybe_await_query_results({error,_} = Error) ->
    Error;
maybe_await_query_results(_) ->
    Timeout = app_helper:get_env(riak_kv, timeseries_query_timeout_ms),

    % we can't use a gen_server call here because the reply needs to be
    % from an fsm but one is not assigned if the query is queued.
    receive
        Result ->
            Result
    after
        Timeout ->
            {error, qry_worker_timeout}
    end.

%% Format the multiple syntax errors into a multiline error
%% message.
format_query_syntax_errors(Errors) ->
    iolist_to_binary(
        [["\n", riak_ql_ddl:syntax_error_to_msg(E)] || E <- Errors]).

%%%===================================================================
%%% Unit tests
%%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

describe_table_columns_test() ->
    {ok, DDL} =
        riak_ql_parser:parse(
          riak_ql_lexer:get_tokens(
            "CREATE TABLE fafa ("
            " f varchar   not null,"
            " s varchar   not null,"
            " t timestamp not null,"
            " w sint64    not null,"
            " p double,"
            " PRIMARY KEY ((f, s, quantum(t, 15, m)), "
            " f, s, t))")),
    ?assertEqual(
       describe_table_columns(DDL),
       {ok, [[<<"f">>, <<"varchar">>,   false, 1,  1],
             [<<"s">>, <<"varchar">>,   false, 2,  2],
             [<<"t">>, <<"timestamp">>, false, 3,  3],
             [<<"w">>, <<"sint64">>, false, undefined, undefined],
             [<<"p">>, <<"double">>, true,  undefined, undefined]]}).

-endif.
