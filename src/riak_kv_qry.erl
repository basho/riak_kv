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

-spec submit(string() | #riak_sql_v1{}, #ddl_v1{}) ->
    {ok, [{Key::binary(), riak_pb_ts_codec:ldbvalue()}]} | {error, any()}.
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
submit(SQL, DDL) ->
    maybe_submit_to_queue(SQL, DDL).

maybe_submit_to_queue(SQL, #ddl_v1{table = BucketType} = DDL) ->
    Mod = riak_ql_ddl:make_module_name(BucketType),
    case riak_ql_ddl:is_query_valid(Mod, DDL, SQL) of
        true ->
            case riak_kv_qry_compiler:compile(DDL, SQL) of
                {error,_} = Error ->
                    Error;
                Queries when is_list(Queries) ->
                    maybe_await_query_results(
                        riak_kv_qry_queue:put_on_queue(self(), Queries, DDL))
            end;
        {false, Errors} ->
            {error, {invalid_query, format_query_syntax_errors(Errors)}}
    end.

%%
maybe_await_query_results({error,_} = Error) ->
    Error;
maybe_await_query_results(_) ->
    % we can't use a gen_server call here because the reply needs to be
    % from an fsm but one is not assigned if the query is queued.
    receive
        Result ->
            Result
    after
        10000 ->
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

%% specific unit tests are in riak_kv_qry_{worker,queue}.erl

-endif.
