-module(riak_repl_pb_get).

-include("riak_repl.hrl").

-include_lib("riak_repl_pb_api/include/riak_repl_pb.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-export([client_cluster_names_12/0, client_cluster_names_13/0]).

-import(riak_pb_kv_codec, [decode_quorum/1]).

-define(PB_MSG_PROXY_GET, 128).
-define(PB_MSG_GET_CLUSTER_ID, 129).
-define(PB_MSG_RESP_CLUSTER_ID, 130).

-record(state, {
        client,    % local client
        repl_modes,
        cluster_id
    }).

%% @doc init/0 callback. Returns the service internal start
%% state.
-spec init() -> any().
init() ->
    {ok, C} = riak:local_client(),
    lager:debug("Riak repl pb get init"),

    % get the current repl modes and stash them in the state
    % I suppose riak_repl_pb_get would need to be restarted if these values
    % changed
    {ok, Ring0} = riak_core_ring_manager:get_my_ring(),
    Ring = riak_repl_ring:ensure_config(Ring0),
    ClusterID = riak_core_ring:cluster_name(Ring),
    Modes = riak_repl_ring:get_modes(Ring),

    #state{client=C, repl_modes=Modes, cluster_id=ClusterID}.


%% @doc decode/2 callback. Decodes an incoming message.
decode(?PB_MSG_PROXY_GET, Bin) ->
    {ok, riak_repl_pb:decode_rpbreplgetreq(Bin)};
decode(?PB_MSG_GET_CLUSTER_ID, <<>>) ->
    {ok, riak_repl_pb:decode_rpbreplgetclusteridreq(<<>>)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(#rpbgetresp{} = Msg) ->
    {ok, riak_pb_codec:encode(Msg)};
encode(#rpbreplgetclusteridresp{} = Msg) ->
    {ok,
        [?PB_MSG_RESP_CLUSTER_ID|riak_repl_pb:encode_rpbreplgetclusteridresp(Msg)]}.

%% Process Protocol Buffer Requests
%%
%% @doc Return Cluster Id of the local cluster
process(#rpbreplgetclusteridreq{}, State) ->
    %% get cluster id from local ring manager and format as string
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ClusterId = lists:flatten(
        io_lib:format("~p", [riak_core_ring:cluster_name(Ring)])),
    lager:debug("Repl PB: returning cluster id ~p", [ClusterId]),
    {reply, #rpbreplgetclusteridresp{cluster_id = ClusterId}, State};
%% @doc Return Key/Value pair, derived from the KV version
process(#rpbreplgetreq{bucket=B, key=K, r=R0, pr=PR0, notfound_ok=NFOk,
                       basic_quorum=BQ, if_modified=VClock,
                       head=Head, deletedvclock=DeletedVClock,
                       cluster_id=CName0,
                       n_val=N_val, sloppy_quorum=SloppyQuorum},
        #state{client=C} = State) ->
    R = decode_quorum(R0),
    PR = decode_quorum(PR0),

    % check in ring metadata to see if there is a mapped cluser for this cluster_id
    % to use, in case the named one has been disabled
    
    case riak_core_metadata:get({<<"replication">>, <<"cluster-mapping">>}, CName0) of
        undefined -> 
            lager:info("Using non-mapped cluster_id: ~s", [CName0]),
            CName = CName0;
        MappedToClusterId ->
            lager:info("Using mapped cluster_id: ~s", [MappedToClusterId]),
            CName = MappedToClusterId
    end,
    lager:info("doing replicated GET using cluster id ~p", [CName]),
    
    GetOptions = make_option(deletedvclock, DeletedVClock) ++
        make_option(r, R) ++
        make_option(pr, PR) ++
        make_option(notfound_ok, NFOk) ++
        make_option(basic_quorum, BQ) ++
        make_option(n_val, N_val) ++
        make_option(sloppy_quorum, SloppyQuorum),
    case C:get(B, K, GetOptions) of
        {ok, O} ->
            make_object_response(O, VClock, Head, State);
        {error, {deleted, TombstoneVClock}} ->
            %% Found a tombstone - return its vector clock so it can
            %% be properly overwritten
            {reply, #rpbgetresp{vclock = pbify_rpbvc(TombstoneVClock)}, State};
        {error, notfound} ->
            %% find connection by cluster_id
            lager:debug("CName = ~p", [ CName ]),
            Modes = State#state.repl_modes,
            Repl12Enabled = riak_repl_util:mode_12_enabled(Modes),
            Repl13Enabled = riak_repl_util:mode_13_enabled(Modes),
            Result12 =
                case Repl12Enabled of
                    true ->
                        CNames12 = get_client_cluster_names_12(),
                        lager:debug("CNames12 = ~p", [ CNames12 ]),
                        proxy_get_12(CName, CNames12, B, K, GetOptions);
                    false ->
                        notconnected
                end,
            Result13 =
                case Repl13Enabled of
                    true ->
                        CNames13 = get_client_cluster_names_13(),
                        lager:debug("CNames13 = ~p", [ CNames13 ]),
                        proxy_get_13(State, CName, CNames13, B, K, GetOptions);
                    false ->
                        notconnected
                end,
            lager:debug("Result12 = ~p", [ Result12 ]),
            lager:debug("Result13 = ~p", [ Result13 ]),
            Result =
                case {Result12, Result13} of
                    {notconnected, Value} -> Value;
                    {Value, notconnected} -> Value;
                    {_, Value} ->
                        lager:warning("proxy_get received a result from multiple versions of replication for cluster ~p", [CName]),
                        Value %% default to 1.3 if both are valid
                end,
            case Result of
                notconnected ->
                    lager:info("not connected to cluster ~p", [CName]),
                    %% not connected to that cluster, return notfound
                    {reply, #rpbgetresp{}, State};
                {ok, O} ->
                    spawn(riak_repl_util, do_repl_put, [O]),
                    make_object_response(O, VClock, Head, State);
                {error, {deleted, TombstoneVClock}} ->
                    %% Found a tombstone - return its vector clock so
                    %% it can be properly overwritten
                    {reply, #rpbgetresp{vclock =
                                            pbify_rpbvc(TombstoneVClock)}, State};
                {error, notfound} ->
                    {reply, #rpbgetresp{}, State};
                {error, Reason} ->
                    {error, {format,Reason}, State}
            end;
        {error, Reason} ->
            {error, {format,Reason}, State}
    end.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.

proxy_get_12(CName, CNames, B, K, GetOptions) ->
    case lists:keyfind(CName, 2, CNames) of
        false ->
            notconnected;
        {ClientPid, _ClusterID} ->
            lager:debug("Using 1.2 proxy_get (A)"),
            riak_repl_tcp_client:proxy_get(ClientPid, B, K,
                                           GetOptions)
    end.

proxy_get_13(State, CName, CNames, B, K, GetOptions) ->
    case lists:keyfind(CName, 2, CNames) of
        false ->
            notconnected;
        {_ClientPid, _ClusterID, ClusterName} ->
            case lists:member(mode_repl13, State#state.repl_modes) of
                true ->
                    Leader = riak_core_cluster_mgr:get_leader(),
                    ProxyForCluster = riak_repl_util:make_pg_proxy_name(ClusterName),
                    try gen_server:call({ProxyForCluster, Leader},
                                        {proxy_get, B, K, GetOptions},
                                        ?LONG_TIMEOUT) of
                        Result ->
                            Result
                    catch
                        _:Error ->
                            lager:debug("proxy_get_13 failed: ~p",
                                        [Error]),
                            notconnected
                    end;
                false ->
                    notconnected
            end
    end.


%%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%%

%% return a key/value tuple that we can ++ to other options so long as the
%% value is not default or undefined -- those values are pulled from the
%% bucket by the get/put FSMs.
make_option(_, undefined) ->
    [];
make_option(_, default) ->
    [];
make_option(K, V) ->
    [{K, V}].

%% Convert a vector clock to erlang
erlify_rpbvc(undefined) ->
    vclock:fresh();
erlify_rpbvc(<<>>) ->
    vclock:fresh();
erlify_rpbvc(PbVc) ->
    binary_to_term(zlib:unzip(PbVc)).

%% Convert a vector clock to protocol buffers
pbify_rpbvc(Vc) ->
    zlib:zip(term_to_binary(Vc)).


make_object_response(O, VClock, Head, State) ->
    case erlify_rpbvc(VClock) == riak_object:vclock(O) of
        true ->
            {reply, #rpbgetresp{unchanged = true}, State};
        _ ->
            Contents = riak_object:get_contents(O),
            PbContent = case Head of
                true ->
                    %% Remove all the 'value' fields from the contents
                    %% This is a rough equivalent of a REST HEAD
                    %% request
                    BlankContents = [{MD, <<>>} || {MD, _} <- Contents],
                    riak_pb_kv_codec:encode_contents(BlankContents);
                _ ->
                    riak_pb_kv_codec:encode_contents(Contents)
            end,
            {reply, #rpbgetresp{content = PbContent,
                    vclock = pbify_rpbvc(riak_object:vclock(O))}, State}
    end.

%% proxy_get for 1.2 repl

get_client_cluster_names_12() ->
    {CNames, _BadNodes} = rpc:multicall(riak_core_node_watcher:nodes(riak_kv),
        riak_repl_pb_get, client_cluster_names_12, []),
    lists:flatten(CNames).

client_cluster_name_12(Client) ->
    catch(riak_repl_tcp_client:cluster_name(Client)).

client_cluster_names_12() ->
    [{P, client_cluster_name_12(P)} || {_,P,_,_} <-
        supervisor:which_children(riak_repl_client_sup), P /= undefined].

%% proxy_get for 1.3 repl
get_client_cluster_names_13() ->
    {CInfo, _BadNodes} = rpc:multicall(riak_core_node_watcher:nodes(riak_repl),
        riak_repl_pb_get, client_cluster_names_13, []),
    lists:flatten(CInfo).

client_cluster_name_13(Pid) ->
    catch(riak_repl2_pg_block_requester:provider_cluster_info(Pid)).

client_cluster_names_13() ->
    [{P, ClusterID, ClusterName}
     || {_,P,_,_} <- supervisor:which_children(riak_repl2_pg_block_requester_sup),
        P /= undefined,
        {ClusterID, ClusterName} <- [client_cluster_name_13(P)]
    ].

    
