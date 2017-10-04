%% -------------------------------------------------------------------
%%
%% riak_foldobject_fsm: Manage secondary index queries.
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

%% @doc The fold object fsm manages the execution of pre-defined object folds.
%%
%%      The fsm creates a plan to achieve coverage
%%      of the cluster using the minimum
%%      possible number of VNodes, sends object fold
%%      commands to each of those VNodes, and compiles the
%%      responses.
%%
%%      The number of VNodes required for full
%%      coverage is based on the number
%%      of partitions, the number of available physical
%%      nodes, and the bucket n_val.
%%
%%      The folds may have options such as:
%%      - fold_heads; the fold will work effectively on just the head of the 
%%      object, so backends that support the fold_heads capability can 
%%      optimise processing of this fold.


-module(riak_kv_mapfold_fsm).

-behaviour(riak_core_coverage_fsm).

-include_lib("riak_kv_vnode.hrl").

-export([init/2,
         plan/2,
         process_results/2,
         finish/2,
         decode_options/1]).

-ifdef(TEST).
-export([generate_options/2]).
-endif.

-define(UNDEFINED_INPUT, <<"undefined">>).

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

-record(state, {from :: from(),
                acc,
                merge_fun,
                sample = false :: boolean()}).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

    
%% @doc 
%% Return a tuple containing the ModFun to call per vnode, the number of 
%% primary preflist vnodes the operation should cover, the service to use to 
%% check for available nodes,and the registered name to use to access the 
%% vnode master process.
%%
%% Bucket - the bucket over which the fold will be run
%% Query - the range and type of query (e.g. {index, Field, ST, ET} or 
%% {object, SK, EK})
%% FoldMod - a module containing the necessary functions for the fold
%% FilterList - a list or none, if a list it is a list of interesting matches
%% to be used by the item filter function in the FoldMod to filter out results
%% (e.g. a list of segments)
%% Options - options relevant to the functions in the FoldMod, and capabilites
%% in the backend
%% Timeout - to timeout the query 
init(From={_, _, _}, 
        [Bucket, Query, FoldMod, FoldOpts, Timeout]) ->
    % Get the bucket n_val for use in creating a coverage plan
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),
    
    % Construct the object folding request 
    OptsList = FoldMod:valid_options(),
    Opts = [{bucket, Bucket}|generate_options(OptsList, FoldOpts)],

    QueryOpts = FoldMod:generate_queryoptions(Opts),
    InitAcc = FoldMod:generate_acc(Opts),
    MapFoldFun = FoldMod:generate_objectfold(Opts),
    CapabilityNeeds = FoldMod:state_needs(Opts), 
    lager:info("Received mapfold coverage query ~w for bucket ~w foldfun ~w" 
                ++ " and fold options ~w", 
                    [Query, Bucket, FoldMod, FoldOpts]),
    Req = ?KV_MAPFOLD_REQ{bucket = Bucket,
                            type = object,
                            qry = Query,
                            query_opts = QueryOpts,
                            fold_fun = MapFoldFun,
                            init_acc = InitAcc,
                            needs = CapabilityNeeds},

    % Make the merge fun
    MergeFun = FoldMod:generate_mergefun(FoldOpts),
    
    {Req, all, NVal, 1, riak_kv, riak_kv_vnode_master, Timeout,
     #state{from=From, acc=InitAcc, merge_fun=MergeFun}}.

%% @doc
%% Need to do something about recognising the sample case in plan/2
%% However, maybe cannot do in callback as we don't know about which of the
%% coverage vnodes are filtered (so the sample may be 1, 2 or n partitions).
%% This probably therefore needs to happen in the behaviour.  Perhaps return
%% something of other than 'ok'.
plan(_CoverageVnodes, State = #state{sample=true}) ->
    % filter to a single partition, or perhaps to a single vnode that isn't 
    % filtered
    {filter, State}; 
plan(_CoverageVnodes, State) ->
    {ok, State}.


process_results({error, Reason}, _State) ->
    lager:warning("Failure to process fold results due to ~w", [Reason]),
    {error, Reason};
process_results(Results, State) ->
    % Results are received as a one-off for each vnode in this case, and so 
    % once results are merged work is always done.
    Acc = State#state.acc,
    MergeFun = State#state.merge_fun,
    UpdAcc = MergeFun(Acc, Results),
    {done, State#state{acc = UpdAcc}}.

%% Once the coverage FSM has received done for all vnodes (as an output from
%% process_results), then it will call finish(clean, State) and so the results
%% can be sent to the client, and the FSM can be stopped. 
finish({error, Error}, State=#state{from={raw, ReqId, ClientPid}}) ->
    % Notify the requesting client that an error
    % occurred or the timeout has elapsed.
    lager:warning("Failure to finish process fold due to ~w", [Error]),
    ClientPid ! {ReqId, {error, Error}},
    {stop, normal, State};
finish(clean, State=#state{from={raw, ReqId, ClientPid}}) ->
    % The client doesn't expect results in increments only the final result, 
    % so no need for a seperate send of a 'done' message
    ClientPid ! {ReqId, {results, State#state.acc}},
    {stop, normal, State}.

-spec decode_options(list()) -> list().
%% @doc
%% Options are receieved as a JSON document like -
%% '[{"tree_size": "xsmall"}, {"check_presence": "true"}]'
%% this is then base64 encoded.
%% Decode this to a key list in erlang where the key is an atom
decode_options(EncodedOpts) ->
    {struct, Opts} = mochijson2:decode(base64:decode(EncodedOpts)),
    ConvertKeyFun = 
            fun({K, V}) -> 
                {list_to_atom(binary_to_list(K)), V} 
            end,
    lists:map(ConvertKeyFun, Opts).

%% ===================================================================
%% Internal functions
%% ===================================================================


generate_options(ExpectedOpts, SubmittedOpts) ->
    FindExpectedOptionsFun =
        fun({OptionName, ConvertTermFun, ValidateTermFun, Default}, OptsAcc) ->
            BinTerm = 
                binary_to_list(
                    proplists:get_value(OptionName, 
                                            SubmittedOpts, 
                                            ?UNDEFINED_INPUT)),
            try ValidateTermFun(ConvertTermFun(BinTerm)) of
                true ->
                    [{OptionName, ConvertTermFun(BinTerm)}| OptsAcc];
                false ->
                    [{OptionName, Default}| OptsAcc]
            catch
                error:_Error ->
                    [{OptionName, Default}| OptsAcc]
            end
        end,
    lists:foldl(FindExpectedOptionsFun, [], ExpectedOpts).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

options_test() ->
    OptsList = [{tree_size, 
                        fun list_to_atom/1, 
                        fun leveled_tictac:valid_size/1, 
                        small},
                {check_presence, 
                        fun list_to_atom/1, 
                        fun is_boolean/1, 
                        false},
                {limit,
                        fun list_to_integer/1,
                        fun is_integer/1,
                        infinity}],
    FoldOpts1 = [{tree_size, <<"xxsmall">>}, {check_presence, <<"true">>}],
    ?assertMatch(
        [{limit, infinity}, {check_presence, true}, {tree_size, xxsmall}],
            generate_options(OptsList, FoldOpts1)),
    FoldOpts2 = [{nonce_sense, <<"nonsense">>}, {limit, <<"nonsense">>}],
    ?assertMatch(
        [{limit, infinity}, {check_presence, false}, {tree_size, small}],
            generate_options(OptsList, FoldOpts2)).


decode_http_options_test() ->
    Opts = 
        "eyJjaGVja19wcmVzZW5jZSI6ICJ0cnVlIiwgInRyZWVfc2l6ZSI6ICJ4c21hbGwifQ==",
    DecodedOpts = decode_options(Opts),
    io:format("Decoded options of ~w", [DecodedOpts]),
    ?assertMatch({check_presence, <<"true">>}, % will still be encoded as bin 
                    lists:keyfind(check_presence, 1, DecodedOpts)).


-endif.

