%% -------------------------------------------------------------------
%%
%% riak_mapred_term: Term parsing for mapreduce
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Term parsing for mapreduce

-module(riak_kv_mapred_term).

-export([parse_request/1]).

-define(DEFAULT_TIMEOUT, 60000).

%%
%% Parse a map/reduce request encoded as a property list
%% [{'inputs', Inputs},
%%  {'query', Query},
%%  {'timeout', Timeout}].
%%
parse_request(BinReq) ->
    try
        Req = binary_to_term(BinReq),

        Timeout = proplists:get_value(timeout, Req, ?DEFAULT_TIMEOUT),
        Inputs = proplists:get_value(inputs, Req, undefined),
        Query = proplists:get_value('query', Req, undefined),

        case parse_inputs(Inputs) of
            {ok, Inputs1} ->
                case parse_query(Query) of
                    {ok, Query1} ->
                        {ok, Inputs1, Query1, Timeout};
                    {error, Reason} ->
                        {error, {'query', Reason}}
                end;
            {error, Reason} ->
                {error, {'inputs', Reason}}
        end
    catch
        _:Error ->
            {error, Error}
    end.

%% Return ok if inputs are valid, {error, Reason} if not
%% @type mapred_inputs() = [bucket_key()]
%%                        |bucket()
%%                        |{bucket(), list()}
%%                        |{modfun, atom(), atom(), list()}
parse_inputs(Bucket) when is_binary(Bucket) ->
    {ok, Bucket};
parse_inputs(Targets) when is_list(Targets) ->
    case valid_input_targets(Targets) of
        ok -> {ok, Targets};
        {error, Reason} -> {error, Reason}
    end;
parse_inputs(Inputs = {modfun, Module, Function, _Options})
  when is_atom(Module), is_atom(Function) ->
    {ok, Inputs};
parse_inputs({index, Bucket, Index, Key}) ->
    case riak_index:parse_fields([{Index, Key}]) of
        {ok, [{Index1, Key1}]} ->
            {ok, {index, Bucket, Index1, Key1}};
        {error, Reasons} ->
            {error, Reasons}
    end;
parse_inputs({index, Bucket, Index, StartKey, EndKey}) ->
    case riak_index:parse_fields([{Index, StartKey}, {Index, EndKey}]) of
        {ok, [{Index1, Key1}]} ->
            {ok, {index, Bucket, Index1, Key1}};
        {ok, [{Index1, StartKey1}, {Index1, EndKey1}]} ->
            {ok, {index, Bucket, Index1, StartKey1, EndKey1}};
        {error, Reasons} ->
            {error, Reasons}
    end;
parse_inputs(Inputs = {search, _Bucket, _Query}) ->
    {ok, Inputs};
parse_inputs(Inputs = {search, _Bucket, _Query, _Filter}) ->
    {ok, Inputs};
parse_inputs(Inputs = {Bucket, Filters}) when is_binary(Bucket), is_list(Filters) ->
    {ok, Inputs};
parse_inputs(Invalid) ->
    {error, {"Inputs must be a binary bucket, a tuple of bucket and key-filters, a list of target tuples, or a search, index, or modfun tuple:", Invalid}}.

%% @type bucket_key() = {binary(), binary()}
%%                     |{{binary(), binary()}, term()}
valid_input_targets([]) ->
    ok;
valid_input_targets([{B,K}|Rest]) when is_binary(B), is_binary(K) ->
    valid_input_targets(Rest);
valid_input_targets([{{B,K},_KeyData}|Rest]) when is_binary(B), is_binary(K) ->
    valid_input_targets(Rest);
valid_input_targets(Invalid) ->
    {error, {"Inputs target tuples must be {B,K} or {{B,K},KeyData}:", Invalid}}.

%% Return ok if query are valid, {error, Reason} if not.  Not very strong validation
%% done here as endpoints and riak_kv_mrc_pipe will check this.
parse_query(Query) when is_list(Query) ->
    {ok, Query};
parse_query(Invalid) ->
    {error, {"Query takes a list of step tuples", Invalid}}.
