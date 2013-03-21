%% -------------------------------------------------------------------
%%
%% riak_buckets_fsm: listing of buckets
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc The buckets fsm manages the listing of buckets.

-module(riak_kv_buckets_fsm).

-behaviour(riak_core_coverage_fsm).

-include_lib("riak_kv_vnode.hrl").

-export([init/2,
         process_results/2,
         finish/2]).

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

-record(state, {buckets=sets:new() :: set(),
                from :: from(),
                stream=false :: boolean()}).

-include("riak_kv_dtrace.hrl").

%% @doc Return a tuple containing the ModFun to call per vnode,
%% the number of primary preflist vnodes the operation
%% should cover, the service to use to check for available nodes,
%% and the registered name to use to access the vnode master process.
init(From, [_, _]=Args) ->
    init(From, Args ++ [false]);
init(From={_, _, ClientPid}, [ItemFilter, Timeout, Stream]) ->
    ClientNode = atom_to_list(node(ClientPid)),
    PidStr = pid_to_list(ClientPid),
    FilterX = if ItemFilter == none -> 0;
                 true               -> 1
              end,
    %% "other" is a legacy term from when MapReduce used this FSM (in
    %% which case, the string "mapred" would appear
    ?DTRACE(?C_BUCKETS_INIT, [2, FilterX],
            [<<"other">>, ClientNode, PidStr]),
    %% Construct the bucket listing request
    Req = ?KV_LISTBUCKETS_REQ{item_filter=ItemFilter},
    {Req, allup, 1, 1, riak_kv, riak_kv_vnode_master, Timeout,
     #state{from=From, stream=Stream}}.

process_results(done, StateData) ->
    {done, StateData};
process_results(Buckets,
                StateData=#state{buckets=BucketAcc, from=From, stream=true}) ->
    ?DTRACE(?C_BUCKETS_PROCESS_RESULTS, [length(Buckets)], []),
    BucketsToSend = [ B  || B <- Buckets,
                             not sets:is_element(B, BucketAcc) ],
    if 
        BucketsToSend =/= [] ->
            reply({buckets_stream, BucketsToSend}, From);
        true -> ok
    end,
    {ok, StateData#state{buckets=accumulate(Buckets, BucketAcc)}};
process_results(Buckets,
                StateData=#state{buckets=BucketAcc, stream=false}) ->
    ?DTRACE(?C_BUCKETS_PROCESS_RESULTS, [length(Buckets)], []),
    {ok, StateData#state{buckets=accumulate(Buckets, BucketAcc)}};
process_results({error, Reason}, _State) ->
    ?DTRACE(?C_BUCKETS_PROCESS_RESULTS, [-1], []),
    {error, Reason}.

finish({error, Error},
       StateData=#state{from=From}) ->
    ?DTRACE(?C_BUCKETS_FINISH, [-1], []),
    %% Notify the requesting client that an error
    %% occurred or the timeout has elapsed.
    reply(Error, From),
    {stop, normal, StateData};
finish(clean, StateData=#state{from=From, stream=true}) ->
    reply({buckets_stream, done}, From),
    ?DTRACE(?C_BUCKETS_FINISH, [0], []),
    {stop, normal, StateData};    
finish(clean,
       StateData=#state{buckets=Buckets,
                        from=From,
                        stream=false}) ->
    reply({buckets, sets:to_list(Buckets)}, From),
    ?DTRACE(?C_BUCKETS_FINISH, [0], []),
    {stop, normal, StateData}.

reply(Msg, {raw, ReqId, ClientPid}) ->
    ClientPid ! {ReqId, Msg}.

accumulate(Entries, Set) ->
    sets:union(sets:from_list(Entries),Set).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

bucket_stream_test_() ->
    {setup,
     riak_kv_test_util:common_setup("bucket_stream_test",
                                    fun(load) ->
                                            application:set_env(riak_kv, storage_backend, riak_kv_memory_backend);
                                       (_) -> ok end),
     riak_kv_test_util:common_cleanup("bucket_stream_test"),
     [{"streaming buckets sends multiple replies",
       fun() ->
               %% Insert some objects in different buckets
               {ok, C} = riak:local_client(),
               [ begin
                     Obj = riak_object:new(<<I:32/big>>, <<"bucket_stream_test">>, <<"test">>),
                     C:put(Obj, [{dw, 3}])
                 end || I <- lists:seq(1,2500) ],
               Client = self(),
               ReqId = erlang:phash2({self(), os:timestamp()}),
               Start = os:timestamp(),
               riak_kv_buckets_fsm_sup:start_buckets_fsm(node(), [{raw, ReqId, Client}, [none, infinity, true]]),
               {Buckets, Messages} = receive_bucket_stream(ReqId, Start),
               ?assertEqual(lists:sort(Buckets), [ <<I:32/big>> || I <- lists:seq(1,2500) ]),
               ?assertNotEqual(length(Messages), 1)
       end}]}.

receive_bucket_stream(ReqId, Start) ->
    receive_bucket_stream(ReqId, Start, [], []).

receive_bucket_stream(ReqId, Start, Buckets, Messages) ->
    receive
        {ReqId, {buckets_stream, done}=Msg} ->
            lager:debug("Bucket stream done! ~p", [timer:now_diff(os:timestamp(), Start)]),
            {Buckets, lists:reverse([Msg|Messages])};
        {ReqId, {buckets_stream, NewBuckets}=Msg} ->
            lager:debug("Bucket stream received: ~p", [Msg]),
            receive_bucket_stream(ReqId, Start, Buckets ++ NewBuckets, [Msg|Messages]);
        {ReqId, {error, Reason}} ->
            %% We errored! this is bad in this functional test
            lager:error("Bucket stream failed! ~p", [Reason]),
            ?assert(Reason =/= Reason)
    end.

-endif.
