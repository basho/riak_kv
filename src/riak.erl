%% -------------------------------------------------------------------
%%
%% Riak: A lightweight, decentralized key-value store.
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

-module(riak).
-author('Andy Gross <andy@basho.com>').
-author('Justin Sheehy <justin@basho.com>').
-author('Bryan Fink <bryan@basho.com>').
-export([stop/0, stop/1]).
-export([get_app_env/0, get_app_env/1,get_app_env/2]).
-export([client_connect/1,client_connect/2,
         client_test/1,
         local_client/0,local_client/1,
         join/1]).
-export([code_hash/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% @spec stop() -> ok
%% @doc Stop the riak application and the calling process.
stop() -> stop("riak stop requested").
stop(Reason) ->
    % we never do an application:stop because that makes it very hard
    %  to really halt the runtime, which is what we need here.
    lager:notice("~p",[Reason]),
    init:stop().
    
%% @spec get_app_env() -> [{Key :: atom(), Value :: term()}]
%% @doc Retrieve all values set in riak's configuration file.
%%      Returns a list of Key/Value pairs.
get_app_env() -> 
    application:get_all_env(riak) ++ init:get_arguments().   

%% @spec get_app_env(Opt :: atom()) -> term()
%% @doc The official way to get the values set in riak's configuration file.
%%      Will return the undefined atom if that option is unset.
get_app_env(Opt) -> get_app_env(Opt, undefined).

%% @spec get_app_env(Opt :: atom(), Default :: term()) -> term()
%% @doc The official way to get the values set in riak's configuration file.
%%      Will return Default if that option is unset.
get_app_env(Opt, Default) ->
    case application:get_env(riak, Opt) of
        {ok, Val} -> Val;
    _ ->
        case init:get_argument(Opt) of
            {ok, [[Val | _]]} -> Val;
            error       -> Default
        end
    end.

%% @spec local_client() -> {ok, Client :: riak_client()}
%% @equiv local_client(undefined)
local_client() ->
    local_client(undefined).

%% @spec local_client(binary()|undefined) -> {ok, Client :: riak_client()}
%% @doc When you want a client for use on a running Riak node.
%%      ClientId should be a 32-bit binary.  If it is not, a
%%      32-bit binary will be created from ClientId by phash2/1.
%%      If ClientId is the atom 'undefined', a random ClientId will
%%      be chosen.
local_client(ClientId) ->
    client_connect(node(), ClientId).

%% @spec client_connect(Node :: node())
%%        -> {ok, Client :: riak_client()} | {error, timeout}
%% @equiv client_connect(Node, undefined)
client_connect(Node) -> 
    client_connect(Node, undefined).

%% @spec client_connect(node(), binary()|undefined)
%%         -> {ok, Client :: riak_client} | {error, timeout}
%% @doc The usual way to get a client.  Timeout often means either a bad
%%      cookie or a poorly-connected distributed erlang network.
%%      ClientId should be a 32-bit binary.  If it is not, a
%%      32-bit binary will be created from ClientId by phash2/1.
%%      If ClientId is the atom 'undefined', a random ClientId will
%%      be chosen.
client_connect(Node, ClientId= <<_:32>>) ->
    % Make sure we can reach this node...
    case net_adm:ping(Node) of
        pang -> {error, {could_not_reach_node, Node}};
        pong -> 
            %% Check if the original client id based vclocks
            %% or the new vnode based vclocks should be used.
            %% N.B. all nodes must be upgraded to 1.0 before
            %% this can be enabled.
            case vnode_vclocks(Node) of
                {badrpc, _Reason} ->
                    {error, {could_not_reach_node, Node}};
                true ->
                    {ok, riak_client:new(Node, undefined)};
                _ ->
                    {ok, riak_client:new(Node, ClientId)}
            end
    end;
client_connect(Node, undefined) ->
    client_connect(Node, riak_core_util:mkclientid(Node));
client_connect(Node, Other) ->
    client_connect(Node, <<(erlang:phash2(Other)):32>>).

vnode_vclocks(Node) ->
    case rpc:call(Node, riak_core_capability, get,
                  [{riak_kv, vnode_vclocks}]) of
        {badrpc, {'EXIT', {undef, _}}} ->
            rpc:call(Node, app_helper, get_env,
                     [riak_kv, vnode_vclocks, false]);
        Result ->
            Result
    end.

join(Node) ->    
    riak_core:join(Node).

code_hash() ->
    {ok, AllMods0} = application:get_key(riak, modules),
    AllMods = lists:sort(AllMods0),
    <<MD5Sum:128>> = erlang:md5_final(
                       lists:foldl(
                         fun(D, C) -> erlang:md5_update(C, D) end, 
                         erlang:md5_init(),
                         [C || {_, C, _} <- [code:get_object_code(M) || M <- AllMods]]
                        )),
    riak_core_util:integer_to_list(MD5Sum, 62).

%%
%% @doc Validate that a specified node is accessible and functional.
%%

-define(CLIENT_TEST_BUCKET, <<"__riak_client_test__">>).
-define(CLIENT_TEST_KEY, <<"key1">>).
-define(CLIENT_TEST_KEY_LIST, <<"list">>).

client_test(cluster) ->
    %Create client, grab ring and n_value for all test operations
    {ok, Client} = riak:local_client(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    NValue = proplists:get_value(n_val,riak_core_bucket:get_bucket(?CLIENT_TEST_BUCKET, Ring)),
    %Get a list of {Preflist, {Bucket, Key}} that map to all preflists in the ring
    BKeyList = get_test_key_list(Ring, Client),
    client_test_phase_loop(BKeyList, Client, NValue);
client_test(NodeStr) when is_list(NodeStr) ->
    client_test(riak_core_util:str_to_node(NodeStr));
client_test(Node) ->
    case net_adm:ping(Node) of
        pong ->
            case client_connect(Node) of
                {ok, Client} ->
                    case client_test_phase1(Client,{?CLIENT_TEST_BUCKET, ?CLIENT_TEST_KEY},1) of
                        ok ->
                            io:format("Successfully completed 1 read/write cycle to ~p\n", [Node]),
                            ok;
                        error ->
                            error
                    end;
                Error ->
                    io:format("Error creating client connection to ~s: ~p",
                              [Node, Error]),
                    error
            end;
        pang ->
            io:format("Node ~p is not reachable from ~p.", [Node, node()]),
            error
    end.


%%
%% Internal functions for testing a Riak node through single read/write cycle
%%

create_test_key_list([], Keylist, _Ring) ->
    Keylist;

create_test_key_list([Pref|T], Keylist0, Ring) ->
    BKey = client_test_key_finder(Pref, Ring, 0),
    Keylist = [BKey|Keylist0],
    create_test_key_list(T,Keylist,Ring).

get_test_key_list(Ring, Client) ->
    NValue = proplists:get_value(n_val,riak_core_bucket:get_bucket(?CLIENT_TEST_BUCKET, Ring)),
    case Client:get(?CLIENT_TEST_BUCKET, ?CLIENT_TEST_KEY_LIST) of
        {ok, Object} ->
            %TODO: This should check if the ring size has changed with a simple length check
            riak_object:get_value(Object);
        {error, notfound} ->
            BKeylist = create_test_key_list(riak_core_ring:all_preflists(Ring, NValue), [], Ring),
            case Client:put(riak_object:new(?CLIENT_TEST_BUCKET, ?CLIENT_TEST_KEY_LIST, BKeylist)) of 
                ok ->
                    BKeylist;
                Error ->
                    io:format("Failed to write test value: ~p", [Error])
            end;
        Error ->
            io:format("Error during riak-admin test client operations: ~p", [Error]) 
    end.

client_test_key_finder(Pref, Ring, Iteration) ->
    % TODO: make this less expensive, takes many many iterations to find a good key

    % Make a bucket key. Attempt determinism using an iterator
    BKey = {?CLIENT_TEST_BUCKET,list_to_binary(integer_to_list(Iteration))},
    DocIdx = riak_core_util:chash_key(BKey),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    NValue = proplists:get_value(n_val,riak_core_bucket:get_bucket(?CLIENT_TEST_BUCKET, Ring)),
    Preflist2 = riak_core_apl:get_apl_ann(DocIdx, NValue, Ring, UpNodes),
    Preflist = [Index || {{Index, _Node}, _Type} <- Preflist2],
    F = fun(Elem) ->
            lists:member(Elem, [PartID || {PartID,_node} <- Pref]) 
    end,
    case lists:all(F, Preflist)  of
        true ->
            {Preflist,BKey};
        false ->
            client_test_key_finder(Pref, Ring, Iteration+1)
    end.

%Loop for a get/put/get cycle on a list of keys
client_test_phase_loop([],_Client,_NValue) ->
    io:format("Successfully completed read/write cycle to all preflists.~n"),
    ok;
client_test_phase_loop([{Preflist,{Bucket,Key}}|Tail],Client,NValue) ->
    case client_test_phase1(Client,{Bucket,Key},NValue) of
        ok ->
            client_test_phase_loop(Tail,Client,NValue);
        error ->
            io:format("Failed read/write cycle to preflist: ~p\n", [Preflist]),
            error
    end.

client_test_phase1(Client, {Bucket,Key}, NValue) ->
    case Client:get(Bucket, Key) of
        {ok, Object} ->
            client_test_phase2(Client, Object, NValue);
        {error, notfound} ->
            client_test_phase2(Client, riak_object:new(Bucket, Key, undefined),NValue);
        Error ->
            io:format("Failed to read test value: ~p", [Error]),
            error
    end.

client_test_phase2(Client, Object0, NValue) ->
    Now = calendar:universal_time(),
    Object = riak_object:update_value(Object0, Now),
    case Client:put(Object, NValue) of
        ok ->
            client_test_phase3(Client, Now, {riak_object:bucket(Object),riak_object:key(Object)});
        Error ->
            io:format("Failed to write test value: ~p", [Error]),
            error
    end.

client_test_phase3(Client, WrittenValue, {Bucket,Key}) ->
    case Client:get(Bucket, Key, 1) of
        {ok, Object} ->
            case lists:member(WrittenValue, riak_object:get_values(Object)) of
                true ->
                    ok;
                false ->
                    io:format(
                        "Failed to find test value in list of objects."
                        " Expected: ~p Actual: ~p",
                        [WrittenValue, riak_object:get_values(Object)]),
                    error
            end;
        Error ->
            io:format("Failed to read test value: ~p", [Error]),
            error
    end.
