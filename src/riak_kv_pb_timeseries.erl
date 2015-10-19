%% -------------------------------------------------------------------
%%
%% riak_kv_pb_timeseries.erl: Riak TS protobuf callbacks
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%% @doc Callbacks for TS protobuf messages [codes 90..93]

-module(riak_kv_pb_timeseries).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-include("riak_kv_wm_raw.hrl").
-include("riak_kv_qry_queue.hrl").  %% for query_id().

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-record(state, {}).

%% these error codes are obviously local to this module,
%% until a central place for error numbers is defined
-define(E_SUBMIT,   1).
-define(E_FETCH,    2).
-define(E_IRREG,    3).
-define(E_PUT,      4).
-define(E_NOCREATE, 5).
-define(E_NOT_TS_TYPE, 6).
-define(E_MISSING_TYPE, 7).
-define(E_MISSING_TS_MODULE, 8).

-define(FETCH_RETRIES, 10).  %% TODO make it configurable in tsqueryreq


-spec init() -> any().
init() ->
    #state{}.


-spec decode(integer(), binary()) ->
    {ok, #ddl_v1{} | #riak_sql_v1{} | #tsputreq{},
        {PermSpec::string(), Table::binary()}} |
    {error,_}.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #tsqueryreq{query = Q}->
            case decode_query(Q) of
                {ok, DecodedQuery} ->
                    PermAndTarget = decode_query_permissions(DecodedQuery),
                    {ok, DecodedQuery, PermAndTarget};
                {error, Error} ->
                    {error, decoder_parse_error_resp(Error)}
            end;
        #tsputreq{table = Table} ->
            {ok, Msg, {"riak_kv.ts_put", Table}}
    end.


-spec encode(tuple()) -> {ok, iolist()}.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.


-spec process(atom() | #ddl_v1{} | #riak_sql_v1{} | #tsputreq{}, #state{}) ->
                     {reply, #tsqueryresp{} | #rpberrorresp{}, #state{}}.
%% @ignore CREATE TABLE
process(#ddl_v1{}, State) ->
    %% {module, Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    %% and what do we do with this DDL?
    %% isn't bucket creation (primarily) effected via bucket activation (via riak-admin)?
    {reply, make_rpberrresp(?E_NOCREATE,
                            "CREATE TABLE not supported via client interface;"
                            " use riak-admin command instead"),
     State};

%% @ignore INSERT (as if)
process(#tsputreq{rows = []}, State) ->
    {reply, #tsputresp{}, State};
process(#tsputreq{table = Bucket, columns = _Columns, rows = Rows}, State) ->
    Mod = riak_ql_ddl:make_module_name(Bucket),
    Data = riak_pb_ts_codec:decode_rows(Rows),
    %% validate only the first row as we trust the client to send us
    %% perfectly uniform data wrt types and order
    case (catch Mod:validate_obj(hd(Data))) of
        true ->
            %% however, prevent bad data to crash us
            try
                case put_data(Data, Bucket, Mod) of
                    0 ->
                        {reply, #tsputresp{}, State};
                    ErrorCount ->
                        EPutMessage = io_lib:format("Failed to put ~b record(s)", [ErrorCount]),
                        {reply, make_rpberrresp(?E_PUT, EPutMessage), State}
                end
            catch
                Class:Exception ->
                    lager:error("error: ~p:~p~n~p", [Class,Exception,erlang:get_stacktrace()]),
                    Error = make_rpberrresp(?E_IRREG, to_string({Class, Exception})),
                    {reply, Error, State}
            end;
        false ->
            {reply, make_rpberrresp(?E_IRREG, "Invalid data"), State};
        {_, {undef, _}} ->
            BucketProps = riak_core_bucket:get_bucket(Bucket),
            {reply, missing_helper_module(Bucket, BucketProps), State}
    end;

%% @ignore SELECT
process(SQL = #riak_sql_v1{'FROM' = Bucket}, State) ->
    Mod = riak_ql_ddl:make_module_name(Bucket),
    case (catch Mod:get_ddl()) of
        {_, {undef, _}} ->
            BucketProps = riak_core_bucket:get_bucket(Bucket),
            {reply, missing_helper_module(Bucket, BucketProps), State};
        DDL ->
            submit_query(DDL, Mod, SQL, State)
    end.

%%
submit_query(DDL, Mod, SQL, State) ->
    case riak_kv_qry:submit(SQL, DDL) of
        {ok, QId} ->
            case fetch_with_patience(QId, ?FETCH_RETRIES) of
                {ok, Data} ->
                    {reply, make_tsqueryresp(Data, Mod), State};
                {error, Reason} ->
                    {reply, make_rpberrresp(?E_FETCH, to_string(Reason)), State}
            end;
        {error, {E, Reason}} when is_atom(E), is_binary(Reason) ->
            ErrorMessage = lists:flatten(io_lib:format("~p: ~s", [E, Reason])),
            {reply, make_rpberrresp(?E_SUBMIT, ErrorMessage), State};
        {error, Reason} ->
            {reply, make_rpberrresp(?E_SUBMIT, to_string(Reason)), State}
    end.


%% TODO: implement
process_stream(_, _, State)->
    {ignore, State}.

-spec decode_query(Query::#tsinterpolation{}) ->
    {error, _} | {ok, #ddl_v1{} | #riak_sql_v1{}}.
decode_query(#tsinterpolation{ base = BaseQuery }) ->
    Lexed = riak_ql_lexer:get_tokens(binary_to_list(BaseQuery)),
    riak_ql_parser:parse(Lexed).

decoder_parse_error_resp({Token, riak_ql_parser, _}) ->
    flat_format("Unexpected token '~s'", [Token]);
decoder_parse_error_resp(Error) ->
    Error.

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

%% ---------------------------------------------------
%% local functions
%% ---------------------------------------------------

-spec make_rpberrresp(integer(), string()) -> #rpberrorresp{}.
make_rpberrresp(Code, Message) ->
    #rpberrorresp{errcode = Code,
                  errmsg = lists:flatten(Message)}.


-spec decode_query_permissions(#ddl_v1{} | #riak_sql_v1{}) -> {string(), binary()}.
decode_query_permissions(#ddl_v1{bucket=NewBucket}) ->
    {"riak_kv.ts_create_table", NewBucket};
decode_query_permissions(#riak_sql_v1{'FROM'=Bucket}) ->
    {"riak_kv.ts_query", Bucket}.

%%
-spec missing_helper_module(Bucket::binary(),
                            BucketProps::{error,any()} | [proplists:property()]) -> #rpberrorresp{}.
missing_helper_module(Bucket, {error, _}) ->
    missing_type_response(Bucket);
missing_helper_module(Bucket, BucketProps) when is_binary(Bucket), is_list(BucketProps) ->
    case lists:keymember(ddl, 1, BucketProps) of
        true  -> missing_table_module_response(Bucket);
        false -> not_timeseries_type_response(Bucket)
    end.

%%
-spec missing_type_response(Bucket::binary()) -> #rpberrorresp{}.
missing_type_response(Bucket) ->
    make_rpberrresp(
        ?E_MISSING_TYPE,
        io_lib:format("Bucket type ~s is missing.", [Bucket])).

%%
-spec not_timeseries_type_response(Bucket::binary()) -> #rpberrorresp{}.
not_timeseries_type_response(Bucket) ->
    make_rpberrresp(
        ?E_NOT_TS_TYPE,
        io_lib:format("Attempt Time Series operation on non Time Series bucket ~s.", [Bucket])).

-spec missing_table_module_response(Bucket::binary()) -> #rpberrorresp{}.
missing_table_module_response(Bucket) ->
    make_rpberrresp(
        ?E_MISSING_TS_MODULE,
        io_lib:format("The compiled module for Time Series bucket ~s cannot be loaded.", [Bucket])).

to_string(X) ->
    io_lib:format("~p", [X]).

%% ---------------------------------------------------
% functions supporting INSERT


-spec put_data([riak_pb_ts_codec:tsrow()], binary(), module()) -> integer().
%% @ignore return count of records we failed to put
put_data(Data, Table, Mod) ->
    DDL = Mod:get_ddl(),
    lists:foldl(
      fun(Raw, ErrorsCnt) ->
              Obj = Mod:add_column_info(Raw),

              PK  = eleveldb_ts:encode_key(
                      riak_ql_ddl:get_partition_key(DDL, Raw)),
              LK  = eleveldb_ts:encode_key(
                      riak_ql_ddl:get_local_key(DDL, Raw)),

              %% Bucket needs to be in duplicate, see riak_kv_qry_coverage_plan:create_plan
              RObj0 = riak_object:new({Table, Table}, PK, Obj),
              MD = riak_object:get_update_metadata(RObj0),
              MD1 = dict:store(?MD_TS_LOCAL_KEY, LK, MD),
	      MD2 = dict:store(?MD_DDL_VERSION, ?DDL_VERSION, MD1),
              RObj = riak_object:update_metadata(RObj0, MD2),

              case riak_client:put(RObj, {riak_client, [node(), undefined]}) of
                  {error, _Why} ->
                      ErrorsCnt + 1;
                  _Ok ->
                      ErrorsCnt
              end
      end,
      0, Data).


%% ---------------------------------------------------
% functions supporting SELECT

-spec fetch_with_patience(query_id(), non_neg_integer()) ->
                                 {ok, [{Key::binary(), riak_pb_ts_codec:ldbvalue()}]} |
                                 {error, atom()}.
fetch_with_patience(QId, 0) ->
    lager:info("Query results on qid ~p not available after ~b secs\n", [QId, ?FETCH_RETRIES]),
    {ok, []};
fetch_with_patience(QId, N) ->
    case riak_kv_qry_queue:fetch(QId) of
        {error, in_progress} ->
            timer:sleep(1000),
            fetch_with_patience(QId, N-1);
        Result ->
            Result
    end.

-spec make_tsqueryresp([{binary(), term()}], module()) -> #tsqueryresp{}.
make_tsqueryresp([], _Fun) ->
    #tsqueryresp{columns = [], rows = []};
make_tsqueryresp(Rows, Module) ->
    %% as returned by fetch, we have in Rows a sequence of KV pairs,
    %% making records concatenated in a flat list
    ColumnNames = get_column_names(Rows),
    ColumnTypes =
        lists:map(
          fun(C) ->
                  %% make column a single-element list, as
                  %% encode_field_type requires
                  riak_pb_ts_codec:encode_field_type(Module:get_field_type([C]))
          end, ColumnNames),
    Records = assemble_records(Rows, length(ColumnNames)),
    JustRows = lists:map(
                 fun(Rec) -> [C || {_K, C} <- Rec] end,
                 Records),
    #tsqueryresp{columns = [#tscolumndescription{name = Name, type = Type}
                            || {Name, Type} <- lists:zip(ColumnNames, ColumnTypes)],
                 rows = riak_pb_ts_codec:encode_rows(JustRows)}.

get_column_names([{C1, _} | MoreRecords]) ->
    {RestOfColumns, _DiscardedValues} =
        lists:unzip(
          lists:takewhile(
            fun({Cn, _}) -> C1 /= Cn end,
            MoreRecords)),
    [C1|RestOfColumns].

assemble_records(Rows, RecordSize) ->
    assemble_records_(Rows, RecordSize, []).
%% should we protect against incomplete records?
assemble_records_([], _, Acc) ->
    Acc;
assemble_records_(RR, RSize, Acc) ->
    Remaining = lists:nthtail(RSize, RR),
    assemble_records_(
      Remaining, RSize, [lists:sublist(RR, RSize) | Acc]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

missing_helper_module_missing_type_test() ->
    ?assertMatch(
        #rpberrorresp{errcode = ?E_MISSING_TYPE },
        missing_helper_module(<<"mytype">>, {error, any})
    ).

missing_helper_module_not_ts_type_test() ->
    ?assertMatch(
        #rpberrorresp{errcode = ?E_NOT_TS_TYPE },
        missing_helper_module(<<"mytype">>, []) % no ddl property
    ).

%% if the bucket properties exist and they contain a ddl property then
%% the bucket properties are in the correct state but the module is still
%% missing.
missing_helper_module_test() ->
    ?assertMatch(
        #rpberrorresp{errcode = ?E_MISSING_TS_MODULE },
        missing_helper_module(<<"mytype">>, [{ddl, #ddl_v1{}}])
    ).

-endif.
