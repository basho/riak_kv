%% -------------------------------------------------------------------
%%
%% riak_kv_wm_ts_util: utility functions for riak_kv_wm_timeseries* resources.
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
-module(riak_kv_wm_ts_util).


-export([authorize/3,
         current_api_version/0,
         current_api_version_string/0,
         extract_api_version/1,
         handle_error/3,
         is_supported_api_version/1,
         local_key/1,
         local_key_fields_and_types/1,
         set_json_response/2,
         set_text_resp_header/2,
         table_from_request/1,
         table_module_exists/1,
         utf8_to_binary/1]).


-include_lib("webmachine/include/webmachine.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-spec current_api_version() -> non_neg_integer().
current_api_version() ->
    1.
-spec current_api_version_string() -> string().
current_api_version_string() ->
    "v1".

-spec extract_api_version(#wm_reqdata{} | string()) -> integer() | undefined.
extract_api_version(RD = #wm_reqdata{}) ->
    extract_api_version(
      wrq:path_info(api_version, RD));
extract_api_version("v1") ->
    1;
extract_api_version(_) ->
    undefined.

is_supported_api_version(Ver) when is_integer(Ver) ->
    Ver =< current_api_version();
is_supported_api_version(_) ->
    false.



%% @private
table_from_request(RD) ->
    utf8_to_binary(
      mochiweb_util:unquote(
        wrq:path_info(table, RD))).

%% move to util module.
utf8_to_binary(S) ->
    unicode:characters_to_binary(S, utf8, utf8).

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

set_text_resp_header(IoList, RD) ->
       wrq:set_resp_header(
       "Content-Type", "text/plain", wrq:append_to_response_body(IoList, RD)).

set_json_response(Json, RD) ->
     wrq:set_resp_header("Content-Type", "application/json",
                         wrq:append_to_response_body(Json, RD)).


authorize(Call, Table, RD) ->
     case riak_api_web_security:is_authorized(RD) of
        false ->
             {error, "Basic realm=\"Riak\""};
        {true, undefined} -> %% @todo: why is this returned during testing?
             ok;
        {true, SecContext} ->
            case riak_core_security:check_permission(
                   {riak_kv_ts_api:api_call_to_perm(Call), Table}, SecContext) of
                 {false, Error, _} ->
                    {error, utf8_to_binary(Error)};
                 _ ->
                    ok
            end;
        insecure ->
             insecure
     end.

%% @todo: this should be in riak_ql_ddl and should probably check deeper.
-spec table_module_exists(module()) -> boolean().
table_module_exists(Mod) ->
    try Mod:get_ddl() of
        _DDL -> %#ddl_v1{} ->
            true
    catch
        _:_ ->
            false
    end.


local_key(Mod) ->
    ddl_local_key(Mod:get_ddl()).

%% this should be in the DDL helper module.
-spec ddl_local_key(?DDL{}) -> [binary()].
ddl_local_key(?DDL{local_key = #key_v1{ast = Ast}}) ->
    [ param_name(P) || P <- Ast].

param_name(?SQL_PARAM{name=[Name]}) ->
    Name.

local_key_fields_and_types(Mod) ->
    LK = local_key(Mod),
    Types = [Mod:get_field_type([F]) || F <- LK ],
    LKStr = [ binary_to_list(F) || F <- LK ],
    lists:zip(LKStr, Types).


error_out(Type, Fmt, Args, RD, Ctx) ->
    {Type,
     wrq:set_resp_header(
       "Content-Type", "text/plain", wrq:append_to_response_body(
                                       flat_format(Fmt, Args), RD)),
     Ctx}.

-spec handle_error(atom()|tuple(), #wm_reqdata{}, Ctx::tuple()) ->
                          {tuple(), #wm_reqdata{}, Ctx::tuple()}.
handle_error(Error, RD, Ctx) ->
    case Error of
        {not_ready, State} ->
            error_out(false,
                      "Not ready (~s)", [State], RD, Ctx);
        insecure_connection ->
            error_out({halt, 426},
                      "Security is enabled and Riak does not"
                      " accept credentials over HTTP. Try HTTPS instead.", [], RD, Ctx);
        {unsupported_version, BadVersion} ->
            error_out({halt, 412},
                      "Unsupported API version ~s", [BadVersion], RD, Ctx);
        {not_permitted, Table, ErrMsg} ->
            error_out({halt, 401},
                      "Access to table \"~ts\" not allowed (~s)", [Table, ErrMsg], RD, Ctx);
        {malformed_request, Method} ->
            error_out({halt, 400},
                      "Malformed ~s request", [Method], RD, Ctx);
        {url_key_bad_method, Method} ->
            error_out({halt, 400},
                      "Inappropriate ~s request", [Method], RD, Ctx);
        {bad_parameter, Param} ->
            error_out({halt, 400},
                      "Bad value for parameter \"~s\"", [Param], RD, Ctx);
        {no_such_table, Table} ->
            error_out({halt, 404},
                      "Table \"~ts\" does not exist", [Table], RD, Ctx);
        {table_exists, Table} ->
            error_out({halt, 409},
                      "Table \"~ts\" already exists", [Table], RD, Ctx);
        {failed_some_puts, NoOfFailures, Table} ->
            error_out({halt, 400},
                      "Failed to put ~b records to table \"~ts\"", [NoOfFailures, Table], RD, Ctx);
        {invalid_data, BadRowIdxs} ->
            error_out({halt, 400},
                      "Invalid record #~s", [hd(BadRowIdxs)], RD, Ctx);
        invalid_json ->
            error_out({halt, 400},
                      "Invalid json in body", [], RD, Ctx);
        {key_element_count_mismatch, Got, Need} ->
            error_out({halt, 400},
                      "Incorrect number of elements (~b) for key of length ~b", [Need, Got], RD, Ctx);
        {bad_field, Table, Field} ->
            error_out({halt, 400},
                      "Table \"~ts\" has no field named \"~s\"", [Table, Field], RD, Ctx);
        {missing_field, Table, Field} ->
            error_out({halt, 400},
                      "Missing field \"~s\" for key in table \"~ts\"", [Field, Table], RD, Ctx);
        {bad_value, Table, {Field, Type}} ->
            error_out({halt, 400},
                      "Bad value for field \"~s\" of type ~s in table \"~ts\"",
                      [Field, Type, Table], RD, Ctx);
        {url_malformed_key_path, NPathElements, KeyLength, Table} ->
            error_out({halt, 400},
                      "Need ~b field/value pairs for key in table \"~ts\", got ~b path elements",
                      [KeyLength, Table, NPathElements], RD, Ctx);
        url_has_not_all_lk_fields ->
            error_out({halt, 400},
                      "Not all key-constituent fields given on URL", [], RD, Ctx);
        notfound ->
            error_out({halt, 404},
                      "Key not found", [], RD, Ctx);
        no_query_in_body ->
            error_out({halt, 400},
                      "No query in request body", [], RD, Ctx);
        {query_parse_error, Details} ->
            error_out({halt, 400},
                      "Query error: ~s", [Details], RD, Ctx);
        {query_compile_error, Details} ->
            error_out({halt, 400},
                      "~s", [Details], RD, Ctx);
        {table_create_fail, Table, Reason} ->
            error_out({halt, 500},
                      "Failed to create table \"~ts\": ~p", [Table, Reason], RD, Ctx);
        {table_activate_fail, Table} ->
            error_out({halt, 500},
                      "Failed to activate bucket type of table \"~ts\"", [Table], RD, Ctx);
        {table_created_missing, Table} ->
            error_out({halt, 500},
                      "Table \"~ts\" has been created but disappeared", [Table], RD, Ctx);
        {query_exec_error, Type, Table, {_Kind, Explained}} ->
            error_out({halt, 500},
                      "Execution of ~s query failed on table \"~ts\" (~s)",
                      [Type, Table, Explained], RD, Ctx);
        {query_exec_error, Type, Table, ErrMsg} ->
            error_out({halt, 500},
                      "Execution of ~s query failed on table \"~ts\" (~p)",
                      [Type, Table, ErrMsg], RD, Ctx);
        query_worker_timeout ->
            error_out({halt, 503},
                      "Query worker timeout", [], RD, Ctx);
        backend_timeout ->
            error_out({halt, 503},
                      "Storage backend timeout", [], RD, Ctx);
        {parameter_error, Message} ->
            error_out({halt, 400},
                      "~s", [Message], RD, Ctx);
        {riak_error, Detailed} ->
            error_out({halt, 500},
                      "Internal riak error: ~p", [Detailed], RD, Ctx)
    end.
