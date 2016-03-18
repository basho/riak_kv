%% -------------------------------------------------------------------
%%
%% riak_kv_wm_timeseries_query: Webmachine resource for riak TS query call.
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Resource for Riak TS operations over HTTP.
%%
%% ```
%% POST   /ts/v1/query?query="query string"   execute SQL query
%% '''
%%
%% Response is a JSON containing data rows with column headers.
%%

-module(riak_kv_wm_timeseries_query).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         is_authorized/2,
         malformed_request/2,
         forbidden/2,
         allowed_methods/2,
         resource_exists/2,
         post_is_create/2,
         process_post/2,
         content_types_accepted/2,
         content_types_provided/2,
         encodings_provided/2
        ]).

-export([produce_doc_body/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_wm_raw.hrl").
-include("riak_kv_ts.hrl").

-record(ctx, {
          table :: 'undefined' | binary(),
          mod   :: 'undefined' | module(),
          method  :: atom(),
          timeout,      %% integer() - passed-in timeout value in ms
          security,     %% security context
          sql_type,
          compiled_query :: undefined | #ddl_v1{} | #riak_sql_describe_v1{} | #riak_select_v1{},
          result         :: undefined | ok | {Headers::[binary()], Rows::[ts_rec()]} |
                            [{entry, proplists:proplist()}]
         }).

-define(DEFAULT_TIMEOUT, 60000).
-define(TABLE_ACTIVATE_WAIT, 30).   %% wait until table's bucket type is activated

-type cb_rv_spec(T) :: {T, #wm_reqdata{}, #ctx{}}.
-type halt() :: {'halt', 200..599} | {'error' , term()}.
-type ts_rec() :: [riak_pb_ts_codec:ldbvalue()].


-spec init(proplists:proplist()) -> {ok, #ctx{}}.
init(_Props) ->
    {ok, #ctx{}}.

-spec service_available(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
%% @doc Determine whether or not a connection to Riak
%%      can be established.
service_available(RD, Ctx) ->
    case init:get_status() of
        {started, _} ->
            {true, RD, Ctx};
        Status ->
            Resp = riak_kv_wm_ts_util:set_error_message("Unable to connect to Riak: ~p",
                                                        [Status], RD),
            {false, Resp, Ctx}
    end.

-spec allowed_methods(#wm_reqdata{}, #ctx{}) -> cb_rv_spec([atom()]).
allowed_methods(RD, Ctx) ->
    {['POST'], RD, Ctx}.

-spec malformed_request(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
malformed_request(RD, Ctx) ->
    try
        {SqlType, SQL} = query_from_request(RD),
        checkpoint("malformed_request SqlType=~p, SQL=~p", [SqlType, SQL]),
        Table = table_from_sql(SQL),
        Mod = riak_ql_ddl:make_module_name(Table),
        {false, RD, Ctx#ctx{sql_type=SqlType,
                            compiled_query=SQL,
                            table=Table,
                            mod=Mod}}
    catch
        throw:{query, Reason} ->
            lager:log(info, self(), "try in malformed_request backfired: ~p", [Reason]),
            Response = riak_kv_wm_ts_util:set_error_message("bad query: ~p", [Reason], RD),
            {true, Response, Ctx}
    end.

-spec is_authorized(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()|string()|halt()).
is_authorized(RD, #ctx{sql_type=SqlType, table=Table}=Ctx) ->
    Call = call_from_sql_type(SqlType),
    lager:log(info, self(), "is_authorized type:~p", [SqlType]),
    case riak_kv_wm_ts_util:authorize(Call, Table, RD) of
        ok ->
            {true, RD, Ctx};
        {error, ErrorMsg} ->
            ErrorStr = lists:flatten(io_lib:format("~p", [ErrorMsg])),
            {ErrorStr, RD, Ctx};
        {insecure, Halt, Resp} ->
            {Halt, Resp, Ctx}
    end.

-spec forbidden(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
forbidden(RD, Ctx) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
        false ->
            %% depends on query type, we will check this later; pass
            %% for now
            {false, RD, Ctx}
    end.





-spec content_types_provided(#wm_reqdata{}, #ctx{}) ->
                                    cb_rv_spec([{ContentType::string(), Producer::atom()}]).
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_doc_body}], RD, Ctx}.


-spec encodings_provided(#wm_reqdata{}, #ctx{}) ->
                                cb_rv_spec([{Encoding::string(), Producer::function()}]).
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.


-spec content_types_accepted(#wm_reqdata{}, #ctx{}) ->
                                    cb_rv_spec([{ContentType::string(), Acceptor::atom()}]).
content_types_accepted(RD, Ctx) ->
%% @todo: if we end up without a body in the request this function should be deleted.
    {[], RD, Ctx}.


-spec resource_exists(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
resource_exists(RD, #ctx{sql_type=ddl}=Ctx) ->
    {true, RD, Ctx};
resource_exists(RD, #ctx{sql_type=Type,
                         mod=Mod}=Ctx) when Type == describe;
                                                Type == select    ->
    Res = riak_kv_wm_ts_util:table_module_exists(Mod),
    {Res, RD, Ctx};
resource_exists(RD, Ctx) ->
    lager:log(info, self(), "resource_exists default case Ctx=~p", [Ctx]),
    {false, RD, Ctx}.

-spec post_is_create(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

-spec process_post(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
process_post(RD, #ctx{sql_type=ddl, compiled_query=SQL}=Ctx) ->
    case create_table(SQL) of
        ok ->
            Result =  [{success, true}],  %% represents ok
            Json = to_json(Result),
            {true, wrq:append_to_response_body(Json, RD), Ctx};
        {error, Reason} ->
            Resp = riak_kv_wm_ts_util:set_error_message("query error: ~p",
                                                        [Reason],
                                                        RD),
            {{halt, 500}, Resp, Ctx}
    end;
process_post(RD, #ctx{sql_type=describe,
                      compiled_query=SQL,
                      mod=Mod}=Ctx) ->
    DDL = Mod:get_ddl(), %% might be faster to store this earlier on
    case riak_kv_ts_api:query(SQL, DDL) of
        {ok, Data} ->
            ColumnNames = [<<"Column">>, <<"Type">>, <<"Is Null">>,
                           <<"Primary Key">>, <<"Local Key">>],
            Json = to_json({ColumnNames, Data}),
            {true, wrq:append_to_response_body(Json, RD), Ctx};
        {error, Reason} ->
            Resp = riak_kv_wm_ts_util:set_error_message(
                     "describe failed: ~p", [Reason], RD),
            {{halt, 500}, Resp, Ctx}
    end;
process_post(RD, #ctx{sql_type=select,
                      compiled_query=SQL,
                      mod=Mod}=Ctx) ->
    DDL = Mod:get_ddl(), %% might be faster to store this earlier on
    case riak_kv_ts_api:query(SQL, DDL) of
        {ok, Data} ->
            {ColumnNames, _ColumnTypes, Rows} = Data,
            Json = to_json({ColumnNames, Rows}),
            {true, wrq:append_to_response_body(Json, RD), Ctx};
        %% the following timeouts are known and distinguished:
        {error, qry_worker_timeout} ->
            %% the eleveldb process didn't send us any response after
            %% 10 sec (hardcoded in riak_kv_qry), and probably died
            Resp = riak_kv_wm_ts_util:set_error_message(
                     "qry_worker_timeout", [], RD),
            {false, Resp, Ctx};
        {error, backend_timeout} ->
            %% the eleveldb process did manage to send us a timeout
            %% response
            Resp = riak_kv_wm_ts_util:set_error_message(
                     "backend_timeout", [], RD),
            {false, Resp, Ctx};
        {error, Reason} ->
            Resp = riak_kv_wm_ts_util:set_error_message(
                     "select query execution error: ~p", [Reason], RD),
            {false, Resp, Ctx}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
query_from_request(RD) ->
    QueryStr = query_string_from_request(RD),
    lager:log(info, self(), "query_from_request: ~p", [QueryStr]),
    compile_query(QueryStr).

query_string_from_request(RD) ->
    case wrq:get_qs_value("query", RD) of
        undefined ->
            throw({query, "no query key in query string"});
        Str ->
            Str
    end.

compile_query(QueryStr) ->
    case riak_ql_parser:ql_parse(
           riak_ql_lexer:get_tokens(QueryStr)) of
        {error, Reason} ->
            ErrorMsg = lists:flatten(io_lib:format("parse error: ~p", [Reason])),
            throw({query, ErrorMsg});
        {ddl, _ } = Res ->
            Res;
        {Type, Compiled} when Type==select; Type==describe ->
            {ok, SQL} =  riak_kv_ts_util:build_sql_record(
                           Type, Compiled, undefined),
            {Type, SQL}
    end.

%% @todo: should really be in riak_ql somewhere
table_from_sql(#ddl_v1{table=Table})                    -> Table;
table_from_sql(#riak_select_v1{'FROM'=Table})               -> Table;
table_from_sql(#riak_sql_describe_v1{'DESCRIBE'=Table}) -> Table.

call_from_sql_type(ddl)      -> query_create_table;
call_from_sql_type(select)   -> query_select;
call_from_sql_type(describe) -> query_describe.

create_table(DDL = #ddl_v1{table = Table}) ->
    %% would be better to use a function to get the table out.
    {ok, Props1} = riak_kv_ts_util:apply_timeseries_bucket_props(DDL, []),
    Props2 = [riak_kv_wm_utils:erlify_bucket_prop(P) || P <- Props1],
    case riak_core_bucket_type:create(Table, Props2) of
        ok ->
            wait_until_active(Table, ?TABLE_ACTIVATE_WAIT);
        {error, Reason} ->
            {error,{table_create_fail, Table, Reason}}
    end.

wait_until_active(Table, 0) ->
    {error, {table_activate_fail, Table}};
wait_until_active(Table, Seconds) ->
    case riak_core_bucket_type:activate(Table) of
        ok ->
            ok;
        {error, not_ready} ->
            timer:sleep(1000),
            wait_until_active(Table, Seconds - 1);
        {error, undefined} ->
            %% this is inconceivable because create(Table) has
            %% just succeeded, so it's here mostly to pacify
            %% the dialyzer (and of course, for the odd chance
            %% of Erlang imps crashing nodes between create
            %% and activate calls)
            {error, {table_created_missing, Table}}
    end.

-spec produce_doc_body(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(iolist()).
produce_doc_body(RD, Ctx = #ctx{result = {Columns, Rows}}) ->
    {mochijson2:encode(
       {struct, [{<<"columns">>, Columns},
                 {<<"rows">>, Rows}]}),
     RD, Ctx}.

to_json({Columns, Rows}) when is_list(Columns), is_list(Rows) ->
    mochijson2:encode(
      {struct, [{<<"columns">>, Columns},
                {<<"rows">>, Rows}]});
to_json(Other) ->
    mochijson2:encode(Other).

checkpoint(Format, Args) ->
    lager:log(info, self(), Format, Args).
