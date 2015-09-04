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
         submit/2,
         fetch/1
        ]).

-include("riak_kv_qry_queue.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-spec submit(string(), #ddl_v1{}) -> {ok, query_id()} | {error, atom()}.
%% @doc Parse, validate against DDL, and submit a query for execution.
%%      To get the results of running the query, use fetch/1.
submit(Query, DDL) ->
    Lexed = riak_ql_lexer:get_tokens(Query),
    case riak_ql_parser:parse(Lexed) of
        {error, _Reason} = Error ->
            Error;
        {ok, SQL} ->
            case riak_ql_ddl:is_query_valid(DDL, SQL) of
                true ->
                    riak_kv_qry_queue:put_on_queue(SQL, DDL);
                _ ->
                    {error, malformed_query}
            end
    end.


-spec fetch(query_id()) -> list() | {error, atom()}.
%% @doc Fetch the results of execution of a previously submitted
%%      query.
fetch(QId) ->
    riak_kv_qry_queue:fetch(QId).


%%%===================================================================
%%% Unit tests
%%%===================================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
