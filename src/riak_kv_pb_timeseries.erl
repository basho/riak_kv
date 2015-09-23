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

-include("src/riak_kv_wm_raw.hrl").  %% for ?MD_LI_IDX
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").
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

-define(FETCH_RETRIES, 10).  %% TODO make it configurable in tsqueryreq


-spec init() -> any().
init() ->
    #state{}.


-spec decode(integer(), binary()) ->
                    {ok, #ddl_v1{} | #riak_sql_v1{} | #tsputreq{},
                     {PermSpec::string(), Table::binary()}}.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #tsqueryreq{query = Q}->
            DecodedQuery = decode_query(Q),
            PermAndTarget = decode_query_permissions(DecodedQuery),
            {ok, DecodedQuery, PermAndTarget};
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
process(#tsputreq{table = Table, columns = _Columns, rows = Rows}, State) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    Data = riak_pb_ts_codec:decode_rows(Rows),
    %% validate only the first row as we trust the client to send us
    %% perfectly uniform data wrt types and order
    case Mod:validate_obj(hd(Data)) of
        true ->
            %% however, prevent bad data to crash us
            try
                case put_data(Data, Table, Mod) of
                    0 ->
                        {reply, #tsputresp{}, State};
                    ErrorCount ->
                        {reply, make_rpberrresp(
                                  ?E_PUT, io_lib:format("Failed to put ~b record(s)", [ErrorCount])),
                         State}
                end
            catch
                Class:Exception ->
                    {reply, make_rpberrresp(?E_IRREG, {Class, Exception})}
            end;
        false ->
                {reply, make_rpberrresp(?E_IRREG, "Invalid data")}
    end;

%% @ignore SELECT
process(SQL = #riak_sql_v1{'FROM' = Bucket}, State) ->
    Mod = riak_ql_ddl:make_module_name(Bucket),
    DDL = Mod:get_ddl(),
    case riak_kv_qry:submit(SQL, DDL) of
        {ok, QId} ->
            case fetch_with_patience(QId, ?FETCH_RETRIES) of
                {ok, Data} ->
                    {reply, make_tsqueryresp(Data, fun Mod:get_field_type/1), State};
                {error, Reason} ->
                    {reply, make_rpberrresp(?E_FETCH, Reason), State}
            end;
        {error, Reason} ->
            {reply, make_rpberrresp(?E_SUBMIT, Reason), State}
    end.


%% TODO: implement
process_stream(_, _, State)->
    {ignore, State}.

decode_query(Query) ->
    case Query of
        #tsinterpolation{base=BaseQuery, interpolations=_Interpolations} ->
            Lexed = riak_ql_lexer:get_tokens(binary_to_list(BaseQuery)),
            {ok, Parsed} = riak_ql_parser:parse(Lexed),
            Parsed
    end.


%% ---------------------------------------------------
%% local functions
%% ---------------------------------------------------

-spec make_rpberrresp(integer(), term()) -> #rpberrorresp{}.
make_rpberrresp(Code, Reason) ->
    #rpberrorresp{errcode = Code,
                  errmsg = lists:flatten(
                             io_lib:format("~p", [Reason]))}.


-spec decode_query_permissions(#ddl_v1{} | #riak_sql_v1{}) -> {string(), binary()}.
decode_query_permissions(#ddl_v1{bucket=NewBucket}) ->
    {"riak_kv.ts_create_table", NewBucket};
decode_query_permissions(#riak_sql_v1{'FROM'=Bucket}) ->
    {"riak_kv.ts_query", Bucket}.


%% ---------------------------------------------------
% functions supporting INSERT


-spec put_data(list(list(riak_pb_ts_codec:ldbvalue())), binary(), module()) -> integer().
%% @ignore return count of records we failed to put
put_data(Data, Table, Mod) ->
    DDL = Mod:get_ddl(),
    lists:foldl(
      fun(Raw, ErrorsCnt) ->
              Obj_ = Mod:add_column_info(Raw),
              Obj  = eleveldb_ts:encode_record(Obj_),

              PK_  = riak_ql_ddl:get_partition_key(DDL, Raw),
              PK   = eleveldb_ts:encode_key(PK_),
              LK_  = riak_ql_ddl:get_local_key(DDL, Raw),
              LK   = eleveldb_ts:encode_key(LK_),

              RObj0 = riak_object:new(Table, PK, Obj),
              MD_ = riak_object:get_update_metadata(RObj0),
              MD  = dict:store(?MD_LI_IDX, LK, MD_),
              RObj = riak_object:update_metadata(RObj0, MD),

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
                                 {ok, [[{Key::binary(), riak_pb_ts_codec:ldbvalue()}]]} |
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


-spec make_tsqueryresp([[{binary(), term()}]], fun()) ->
                              #tsqueryresp{}.
make_tsqueryresp([], _Fun) ->
    #tsqueryresp{columns = [], rows = []};
make_tsqueryresp(Rows, GetFieldTypeF) ->
    %% get types and take the ordering of columns in the batches
    ARecord = hd(Rows),
    %% plain column names need to be represented as a single-element list
    ColumnNames = [[C] || {C, _V} <- ARecord],
    ColumnTypes = lists:map(fun(C) -> riak_pb_ts_codec:encode_field_type(GetFieldTypeF(C)) end, ColumnNames),
    %% drop column names in row data
    JustRows =
        lists:map(
          fun(Row) -> [C || {_K, C} <- Row] end,
          Rows),
    #tsqueryresp{columns = [#tscolumndescription{name = Name, type = Type}
                            || {Name, Type} <- lists:zip(ColumnNames, ColumnTypes)],
                 rows = riak_pb_ts_codec:encode_rows(JustRows)}.
