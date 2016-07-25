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
         encodings_provided/2
        ]).

%% webmachine body-producing functions
-export([produce_doc_body/2]).

-include("riak_kv_wm_raw.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ctx,
        {
          api_version :: undefined | integer(),
          riak,
          security,
          table    :: undefined | binary(),
          mod :: module()
        }).

-type cb_rv_spec(T) :: {T, #wm_reqdata{}, #ctx{}}.

-spec init(proplists:proplist()) -> {ok, #ctx{}}.
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{riak = proplists:get_value(riak, Props)}}.

-spec service_available(#wm_reqdata{}, #ctx{}) ->
    {boolean(), #wm_reqdata{}, #ctx{}}.
%% @doc Determine whether or not a connection to Riak
%%      can be established.
service_available(RD, Ctx) ->
    ApiVersion = riak_kv_wm_ts_util:extract_api_version(RD),
    case {riak_kv_wm_ts_util:is_supported_api_version(ApiVersion),
          init:get_status()} of
        {true, {started, _}} ->
            Table = riak_kv_wm_ts_util:table_from_request(RD),
            Mod = riak_ql_ddl:make_module_name(Table),
            {true, RD,
             Ctx#ctx{table = Table,
                     mod = Mod}};
        {false, {started, _}} ->
            riak_kv_wm_ts_util:handle_error({unsupported_version, ApiVersion}, RD, Ctx);
        {_, {InternalStatus, _}} ->
            riak_kv_wm_ts_util:handle_error({not_ready, InternalStatus}, RD, Ctx)
    end.

is_authorized(RD, #ctx{table = Table} = Ctx) ->
    case riak_kv_wm_ts_util:authorize(list_keys, Table, RD) of
        ok ->
            {true, RD, Ctx};
        {error, ErrorMsg} ->
            riak_kv_wm_ts_util:handle_error({not_permitted, Table, ErrorMsg}, RD, Ctx);
        insecure ->
            riak_kv_wm_ts_util:handle_error(insecure_connection, RD, Ctx)
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
resource_exists(RD, #ctx{mod = Mod, table = Table} = Ctx) ->
    case riak_kv_wm_ts_util:table_module_exists(Mod) of
        true ->
            {true, RD, Ctx};
        false ->
            riak_kv_wm_ts_util:handle_error({no_such_table, Table}, RD, Ctx)
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
      {[{"text/plain", produce_doc_body}], RD, Ctx}.

produce_doc_body(RD, Ctx = #ctx{table = Table, mod = Mod}) ->
    {ok, ReqId} = riak_client:stream_list_keys(
                    {Table, Table}, undefined, {riak_client, [node(), undefined]}),
    {{stream, {[], fun() -> stream_keys(ReqId, Table, Mod) end}}, RD, Ctx}.

stream_keys(ReqId, Table, Mod) ->
    receive
        %% skip empty shipments
        {ReqId, {keys, []}} ->
            stream_keys(ReqId, Table, Mod);
        {ReqId, From, {keys, []}} ->
            _ = riak_kv_keys_fsm:ack_keys(From),
            stream_keys(ReqId, Table, Mod);
        {ReqId, From, {keys, Keys}} when is_list(Keys) ->
            _ = riak_kv_keys_fsm:ack_keys(From),
            {ts_keys_to_body(Keys, Table, Mod), fun() -> stream_keys(ReqId, Table, Mod) end};
        {ReqId, {keys, Keys}} when is_list(Keys) ->
            {ts_keys_to_body(Keys, Table, Mod), fun() -> stream_keys(ReqId, Table, Mod) end};
        {ReqId, done} ->
            {<<>>, done};
        {ReqId, {error, timeout}} ->
            {mochijson2:encode({struct, [{error, timeout}]}), done};
        Weird ->
            lager:warning("Unexpected message while waiting for list_keys batch with ReqId ~p, Table ~s: ~p", [ReqId, Table, Weird]),
            stream_keys(ReqId, Table, Mod)
    end.

ts_keys_to_body(Keys, Table, Mod) ->
    BaseUrl = base_url(Table),
    KeyTypes = riak_kv_wm_ts_util:local_key_fields_and_types(Mod),
    URLs =
        lists:map(
          fun(Key) when is_binary(Key) ->
                  format_url(BaseUrl, KeyTypes, tuple_to_list(sext:decode(Key)));
             (Key) ->
                  %% this clause is to please dialyzer (dialyzer is
                  %% within his rights as we have no way to 'declare'
                  %% the type of Key)
                  format_url(BaseUrl, KeyTypes, tuple_to_list(Key))
          end,
          Keys),
    iolist_to_binary(URLs).


format_url(BaseUrl, KeyTypes, Key) ->
    iolist_to_binary([BaseUrl, key_to_string(lists:zip(Key, KeyTypes)), $\n]).

key_to_string(KFTypes) ->
    string:join(
      [[Field, $/, http_uri:encode(value_to_url_string(Key, Type))]
       || {Key, {Field, Type}} <- KFTypes],
      "/").

value_to_url_string(V, varchar) ->
    binary_to_list(V);
value_to_url_string(V, sint64) ->
    integer_to_list(V);
value_to_url_string(V, timestamp) ->
    integer_to_list(V).

base_url(Table) ->
    {ok, [{Server, Port}]} = application:get_env(riak_api, http),
    lists:flatten(
      io_lib:format(
        "http://~s:~B/ts/~s/tables/~s/keys/",
        [Server, Port, riak_kv_wm_ts_util:current_api_version_string(), Table])).

%%%
%%% TESTS
%%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

percent_twenty_test() ->
    Expected = <<"http://server/ke/forty%2Btwo/mu/spa%200%20aces\n">>,
    Got = format_url(
            "http://server/",
            [{"ke", varchar}, {"mu", varchar}],
            [<<"forty+two">>, <<"spa 0 aces">>]),
    ?assertEqual(Expected, Got).

-endif.
