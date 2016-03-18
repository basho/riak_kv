%% -------------------------------------------------------------------
%%
%% riak_kv_wm_timeseries_listkeys: Webmachine resource for riak TS
%%                                  streaming operations.
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
%% GET    /ts/v1/table/Table/list_keys
%% '''
%%
%% Response is HTML URLs for the entries in the table.
%%

-module(riak_kv_wm_timeseries_listkeys).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         allowed_methods/2,
         is_authorized/2,
         forbidden/2,
         resource_exists/2,
         content_types_provided/2,
         encodings_provided/2]).

%% webmachine body-producing functions
-export([produce_doc_body/2]).

-include("riak_kv_wm_raw.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ctx, {riak,
              security,
              client,
              table    :: undefined | binary(),
              mod :: module()
             }).

-type cb_rv_spec(T) :: {T, #wm_reqdata{}, #ctx{}}.

-define(DEFAULT_TIMEOUT, 60000).

-spec init(proplists:proplist()) -> {ok, #ctx{}}.
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{riak = proplists:get_value(riak, Props)}}.

-spec service_available(#wm_reqdata{}, #ctx{}) ->
    {boolean(), #wm_reqdata{}, #ctx{}}.
%% @doc Determine whether or not a connection to Riak
%%      can be established.
service_available(RD, Ctx = #ctx{riak = RiakProps}) ->
    case riak_kv_wm_utils:get_riak_client(
           RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            Table = riak_kv_wm_ts_util:table_from_request(RD),
            Mod = riak_ql_ddl:make_module_name(Table),
            {true, RD,
             Ctx#ctx{client = C,
                     table = Table,
                     mod = Mod}};
        {error, Reason} ->
            Resp = riak_kv_wm_ts_util:set_error_message("Unable to connect to Riak: ~p", [Reason], RD),
            {false, Resp, Ctx}
    end.

is_authorized(RD, #ctx{table=Table}=Ctx) ->
    case riak_kv_wm_ts_util:authorize(listkeys, Table, RD) of
        ok ->
            {true, RD, Ctx};
        {error, ErrorMsg} ->
            {ErrorMsg, RD, Ctx};
        {insecure, Halt, Resp} ->
            {Halt, Resp, Ctx}
    end.

-spec forbidden(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
forbidden(RD, Ctx) ->
   Result = riak_kv_wm_utils:is_forbidden(RD),
    {Result, RD, Ctx}.

-spec allowed_methods(#wm_reqdata{}, #ctx{}) -> cb_rv_spec([atom()]).
%% @doc Get the list of methods this resource supports.
allowed_methods(RD, Ctx) ->
    {['GET'], RD, Ctx}.

-spec resource_exists(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
resource_exists(RD, #ctx{mod=Mod,
                         table=Table} = Ctx) ->
    case riak_kv_wm_ts_util:table_module_exists(Mod) of
        true ->
            {true, RD, Ctx};
        false ->
            Resp = riak_kv_wm_ts_util:set_error_message(
                     "table ~p does not exist", [Table], RD),
            {false, Resp, Ctx}
    end.

-spec encodings_provided(#wm_reqdata{}, #ctx{}) ->
                                cb_rv_spec([{Encoding::string(), Producer::function()}]).
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available.
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.

-spec content_types_provided(#wm_reqdata{}, #ctx{}) ->
                                    cb_rv_spec([{ContentType::string(), Producer::atom()}]).
%% @doc List the content types available for representing this resource.
content_types_provided(RD, Ctx) ->
      {[{"application/json", produce_doc_body}], RD, Ctx}.

produce_doc_body(RD, Ctx = #ctx{table = Table, mod=Mod,
                                client = Client}) ->
    {ok, ReqId} = riak_client:stream_list_keys(
                    {Table, Table}, undefined, Client),
    {{halt, 200}, wrq:set_resp_body({stream, prepare_stream(ReqId, Table, Mod)}, RD), Ctx}.

prepare_stream(ReqId, Table, Mod) ->
    {<<"<html>">>, fun() -> stream_keys(ReqId, Table, Mod) end}.

stream_keys(ReqId, Table, Mod) ->
    receive
        %% skip empty shipments
        {ReqId, {keys, []}} ->
            stream_keys(ReqId, Table, Mod);
        {ReqId, From, {keys, []}} ->
            _ = riak_kv_keys_fsm:ack_keys(From),
            stream_keys(ReqId, Table, Mod);
        {ReqId, From, {keys, Keys}} ->
            _ = riak_kv_keys_fsm:ack_keys(From),
            {ts_keys_to_json(Keys, Table, Mod), fun() -> stream_keys(ReqId, Table, Mod) end};
        {ReqId, {keys, Keys}} ->
            {ts_keys_to_json(Keys, Table, Mod), fun() -> stream_keys(ReqId, Table, Mod) end};
        {ReqId, done} ->
            {<<"</html>">>, done};
        {ReqId, {error, timeout}} ->
            {mochijson2:encode({struct, [{error, timeout}]}), done};
        _Weird ->
            %% @todo: should we log this?
            stream_keys(ReqId, Table, Mod)
    end.

ts_keys_to_json(EncodedKeys, Table, Mod) ->
    BaseUrl = base_url(Table),
    Keys = decode_keys(EncodedKeys),
    KeyTypes = riak_kv_wm_ts_util:local_key_fields_and_types(Mod),
    URLs = [format_url(BaseUrl, KeyTypes, Key)
            || Key <- Keys],
    %% Dialyzer does not like the list_comprehension, if you want to avoid the
    %% dialyzer error you have to write it like this:
    %% URLs = lists:map(fun(Key) ->
    %%                          format_url(BaseUrl, KeyTypes, Key)
    %%                  end,
    %%                  Keys),
    JsonList = [ mochijson2:encode([{url, URL}]) || URL <- URLs],
    list_to_binary(lists:flatten(JsonList)).


format_url(BaseUrl, KeyTypes, Key) ->
    list_to_binary(
      io_lib:format("~s~s", [BaseUrl, key_to_string(Key, KeyTypes)])).

decode_keys(Keys) ->
    [tuple_to_list(sext:decode(A))
     || A <- Keys, A /= []].

key_to_string([], []) ->
    "";
key_to_string([Key|Keys], [{Field, Type}|KeyTypes]) ->
    Field ++ "/" ++ value_to_url_string(Key, Type) ++ "/" ++ key_to_string(Keys, KeyTypes).

value_to_url_string(V, varchar) ->
    binary_to_list(V);
value_to_url_string(V, timestamp) ->
    erlang:integer_to_list(V).

base_url(Table) ->
    {ok, [{Server, Port}]} = application:get_env(riak_api, http),
    lists:flatten(io_lib:format("http://~s:~B/ts/v1/tables/~s/keys/",
                                [Server, Port, Table])).
