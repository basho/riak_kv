%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_ensemble_console).

-include_lib("riak_ensemble/include/riak_ensemble_types.hrl").

-export([ensemble_overview/0,
         ensemble_detail/1]).

-compile(export_all).
-compile(nowarn_export_all).

-type ensembles() :: [{ensemble_id(), ensemble_info()}].
-type quorums() :: orddict(ensemble_id(), {leader_id(), boolean(), [peer_id()]}).
-type counts() :: orddict(node(), pos_integer()).
-type labels() :: [{pos_integer(), peer_id()}].
-type names() :: [{peer_id(), string()}].

-record(details, {enabled     :: boolean(),
                  active      :: boolean(),
                  validation  :: strong | weak,
                  metadata    :: async  | sync,
                  ensembles   :: ensembles(),
                  quorums     :: quorums(),
                  peer_info   :: orddict(peer_id(), peer_info()) | undefined,
                  nodes       :: [node()],
                  ring_ready  :: boolean()
                 }).

%%%===================================================================
%%% API
%%%===================================================================

-spec ensemble_overview() -> ok.
ensemble_overview() ->
    Details = get_quorums(get_details()),
    ensemble_overview(Details).

ensemble_overview(Details) ->
    print_overview(Details),
    print_ensembles(Details),
    ok.

-spec ensemble_detail(any()) -> ok.
ensemble_detail("root") ->
    ensemble_detail(root);
ensemble_detail(root) ->
    ensemble_detail(1);
ensemble_detail(N) ->
    Details1 = get_details(),
    case lookup_ensemble(N, Details1) of
        undefined ->
            io:format("No such ensemble: ~p~n", [N]);
        Ensemble ->
            Details2 = get_quorums(Ensemble, Details1),
            Details3 = get_peer_info(N, Details2),
            print_detail(N, Details3)
    end,
    ok.

%%%===================================================================

print_overview(#details{enabled=Enabled,
                        active=Active,
                        validation=Validation,
                        metadata=Metadata,
                        nodes=Nodes,
                        ring_ready=RingReady}) ->
    NumNodes = length(Nodes),
    ValidationMsg = case Validation of
                        strong ->
                            "strong (trusted majority required)";
                        weak ->
                            "weak (simple majority required)"
                    end,
    MetadataMsg = case Metadata of
                      sync ->
                          "guaranteed replication (synchronous)";
                      async ->
                          "best-effort replication (asynchronous)"
                  end,
    io:format("~s~n", [string:centre(" Consensus System ", 79, $=)]),
    io:format("Enabled:     ~s~n"
              "Active:      ~s~n"
              "Ring Ready:  ~s~n"
              "Validation:  ~s~n"
              "Metadata:    ~s~n~n",
              [Enabled, Active, RingReady, ValidationMsg, MetadataMsg]),
    if Enabled == false ->
            io:format("Note: The consensus subsystem is not enabled.~n~n");
       (Active == false) and (NumNodes < 3) ->
            io:format(cluster_warning());
       true ->
            ok
    end,
    ok.

cluster_warning() ->
    ("Note: The consensus subsystem will not be activated until there are more~n"
     "      than three nodes in this cluster.~n~n").

%%%===================================================================

print_ensembles(#details{ensembles=[]}) ->
    io:format("~s~n", [string:centre(" Ensembles ", 79, $=)]),
    io:format("There are no active ensembles.~n");
print_ensembles(#details{ensembles=L3, quorums=AllOnline}) ->
    io:format("~s~n", [string:centre(" Ensembles ", 79, $=)]),
    io:format(" Ensemble    ~9s    ~9s    Leader~n",
              [align(9,"Quorum"), align(9,"Nodes")]),
    io:format("~s~n", [string:chars($-, 79)]),
    print_ensembles(L3, 1, AllOnline).

-spec print_ensembles(ensembles(), pos_integer(), quorums()) -> ok.
print_ensembles([], _, _) ->
    ok;
print_ensembles([{Ens, Info}|T], N, AllOnline) ->
    #ensemble_info{views=Views} = Info,
    Names = peer_names(Views),
    {Leader, _, Online} = orddict:fetch(Ens, AllOnline),
    Label = case Ens of
                root -> "root";
                _    -> N
            end,
    print_ensemble_view(Views, Label, Leader, Names, Online, true),
    print_ensembles(T, N+1, AllOnline).

-spec print_ensemble_view(views(), string() | pos_integer(),
                          leader_id(), names(), [peer_id()], boolean()) -> ok.
print_ensemble_view([], _, _, _, _, _) ->
    ok;
print_ensemble_view([View|T], Label, Leader, Names, Online, First) ->
    LName = orddict:fetch(Leader, Names),
    Nodes = lists:usort([Node || {_,Node} <- View]),
    PeersOnline = ordsets:intersection(ordsets:from_list(View),
                                       ordsets:from_list(Online)),
    NodesOnline = ordsets:intersection(ordsets:from_list(Nodes),
                                       ordsets:from_list([node()|nodes()])),
    NumPeers = length(View),
    NumNodes = length(Nodes),
    NumOnline = length(PeersOnline),
    NumNodesOn = length(NodesOnline),

    case First of
        true ->
            io:format(" ~8s    ~9s    ~9s    ~s~n",
                      [align(8, Label),
                       align(9, io_lib:format("~3b / ~-3b *", [NumOnline, NumPeers])),
                       align(9, io_lib:format("~3b / ~-3b", [NumNodesOn, NumNodes])),
                       LName]);
        false ->
            io:format(" ~8s    ~9s    ~9s~n",
                      ["",
                       align(9, io_lib:format("~3b / ~-3b *", [NumOnline, NumPeers])),
                       align(9, io_lib:format("~3b / ~-3b", [NumNodesOn, NumNodes]))])
    end,
    print_ensemble_view(T, Label, Leader, Names, Online, false).

%%%===================================================================

print_detail(N, #details{ensembles=L3, quorums=Quorums, peer_info=PeerInfo}) ->
    {Id, Info} = lists:nth(N, L3),
    Views = Info#ensemble_info.views,
    {Counts, Labels} = label_peers(Views),
    Names = peer_names(Counts, Labels),

    {LeaderId, Ready, _} = orddict:fetch(Id, Quorums),
    Leader = orddict:fetch(LeaderId, Names),

    Peers = [format_info(Label, Peer, PeerInfo) || {Label, Peer} <- Labels],

    Header = string:centre(" Ensemble #" ++ integer_to_list(N) ++ " ", 79, $=),
    io:format("~s~n", [Header]),
    io:format("Id:           ~p~n"
              "Leader:       ~s~n"
              "Leader ready: ~p~n~n",
              [Id, Leader, Ready]),
    io:format("~s~n", [string:centre(" Peers ", 79, $=)]),
    io:format(" Peer  Status     Trusted          Epoch         Node~n"),
    print_detail_view(Views, Peers),
    ok.

print_detail_view([], _) ->
    ok;
print_detail_view([View|T], Peers) ->
    io:format("~s~n", [string:chars($-,79)]),
    [begin
         io:format(" ~4s  ~9s  ~7s  ~20s  ~s~n",
                   [string:centre(integer_to_list(Label), 4),
                    string:centre(State, 9),
                    string:centre(Trusted, 7),
                    string:centre(Epoch, 20),
                    Node])
     end || {Label, Peer={_, Node}, State, Trusted, Epoch} <- Peers,
            lists:member(Peer, View)],
    print_detail_view(T, Peers).

-spec format_info(Label, Peer, PeerInfo) -> Output when
      Label    :: pos_integer(),
      Peer     :: peer_id(),
      PeerInfo :: orddict(peer_id(), peer_info()),
      Output   :: {Label, Peer, string(), string(), string()}.
format_info(Label, Peer, PeerInfo) ->
    case orddict:fetch(Peer, PeerInfo) of
        nodedown ->
            {Label, Peer, "(offline)", "--", "--"};
        undefined ->
            {Label, Peer, "--", "--", "--"};
        {State, Trusted, Epoch} ->
            Trusted2 = case Trusted of
                           true  -> "yes";
                           false -> "no"
                       end,
            State2 = atom_to_list(State),
            Epoch2 = integer_to_list(Epoch),
            {Label, Peer, State2, Trusted2, Epoch2}
    end.

%%%===================================================================
%%% Private (state-dependent / side-effecting)
%%%===================================================================

get_details() ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Enabled = riak_core_sup:ensembles_enabled(),
    Ensembles = case Enabled of
                    true -> ordered_ensembles();
                    _ -> []
                end,
    Validation = case riak_ensemble_config:tree_validation() of
                     false ->
                         weak;
                     _ ->
                         strong
                 end,
    Metadata = case riak_ensemble_config:synchronous_tree_updates() of
                   true ->
                       sync;
                   _ ->
                       async
               end,
    #details{enabled     = Enabled,
             active      = riak_ensemble_manager:enabled(),
             validation  = Validation,
             metadata    = Metadata,
             ensembles   = Ensembles,
             quorums     = [],
             nodes       = riak_core_ring:ready_members(Ring),
             ring_ready  = riak_core_ring:ring_ready(Ring)}.

-spec ordered_ensembles() -> [{ensemble_id(), ensemble_info()}].
ordered_ensembles() ->
    Ensembles = all_ensembles(),
    L1 = [case Ens of
              root ->
                  {-1, Ens, Info};
              _ ->
                  MaxPeers = lists:max(num_peers(Info)),
                  {MaxPeers, Ens, Info}
          end || {Ens, Info} <- Ensembles],
    L2 = lists:sort(L1),
    L3 = [{Ens, Info} || {_, Ens, Info} <- L2],
    L3.

-spec all_ensembles() -> orddict(ensemble_id(), ensemble_info()).
all_ensembles() ->
    CS = riak_ensemble_manager:get_cluster_state(),
    riak_ensemble_state:ensembles(CS).

get_quorums(Details=#details{ensembles=Ensembles}) ->
    Quorums = riak_core_util:pmap(fun({Ens,_}) ->
                                          ping_quorum(Ens)
                                  end, Ensembles),
    Details#details{quorums=lists:sort(Quorums)}.

get_quorums(Ensemble, Details) ->
    Quorums = [ping_quorum(Ensemble)],
    Details#details{quorums=Quorums}.

-spec ping_quorum(ensemble_id()) -> {ensemble_id(), {leader_id(), boolean(), [peer_id()]}}.
ping_quorum(Ens) ->
    case riak_ensemble_peer:ping_quorum(Ens, 10000) of
        timeout ->
            {Ens, {undefined, false, []}};
        {Leader, Ready, Peers} ->
            {Ens, {Leader, Ready, Peers}}
    end.

get_peer_info(N, Details=#details{ensembles=Ensembles}) ->
    {Ens, Info} = lists:nth(N, Ensembles),
    Peers = lists:usort(lists:flatten(Info#ensemble_info.views)),
    F = fun(Peer) ->
                {Peer, riak_ensemble_manager:get_peer_info(Ens, Peer)}
        end,
    AllInfo = riak_core_util:pmap(F, Peers),
    Details#details{peer_info=AllInfo}.

%%%===================================================================
%%% Private (pure)
%%%===================================================================

lookup_ensemble(N, #details{ensembles=Ensembles}) ->
    try
        {Ensemble, _} = lists:nth(N, Ensembles),
        Ensemble
    catch _:_ ->
            undefined
    end.

-spec num_peers(#ensemble_info{views :: views()}) -> [non_neg_integer()].
num_peers(#ensemble_info{views=Views}) ->
    [length(View) || View <- Views].

-spec align(pos_integer(), integer()|string()) -> string().
align(Len, Int) when is_integer(Int) ->
    string:centre(integer_to_list(Int), Len);
align(Len, L) when is_list(L) ->
    string:centre(lists:flatten(L), Len).

-spec label_peers(views()) -> {counts(), labels()}.
label_peers(Views) ->
    Members = lists:flatmap(fun(View) ->
                                    lists:sort(View)
                            end, Views),
    label_peers(Members, 1, [], [], []).

-spec label_peers(Members, Label, Seen, Counts, Labels) -> {Counts, Labels} when
      Members :: [peer_id()],
      Label   :: pos_integer(),
      Seen    :: [peer_id()],
      Counts  :: counts(),
      Labels  :: labels().
label_peers([], _Label, _Seen, Counts, Labels) ->
    {Counts, lists:reverse(Labels)};
label_peers([Peer|Peers], Label, Seen, Counts, Labels) ->
    {_Id, Node} = Peer,
    case lists:member(Peer, Seen) of
        true ->
            label_peers(Peers, Label, Seen, Counts, Labels);
        false ->
            Counts2 = orddict:update_counter(Node, 1, Counts),
            Labels2 = [{Label, Peer}|Labels],
            label_peers(Peers, Label + 1, [Peer|Seen], Counts2, Labels2)
    end.

-spec peer_names(views()) -> names().
peer_names(Views) ->
    {Counts, Labels} = label_peers(Views),
    peer_names(Counts, Labels).

-spec peer_names(counts(), labels()) -> names().
peer_names(Counts, Labels) ->
    Names = [case orddict:fetch(Node, Counts) of
                 1 ->
                     {Peer, atom_to_list(Node)};
                 _ ->
                     Name = lists:flatten(io_lib:format("~s (~b)", [Node, Label])),
                     {Peer, Name}
             end || {Label, Peer={_, Node}} <- Labels],
    orddict:store(undefined, "--", lists:sort(Names)).

%%%===================================================================
%%% Forced output for manual testing / eye-balling
%%%===================================================================

print_variations() ->
    Details = #details{enabled     = false,
                       active      = false,
                       validation  = strong,
                       metadata    = async,
                       ensembles   = [],
                       quorums     = [],
                       ring_ready  = true,
                       nodes       = ['dev1@127.0.0.1']},
    %% Not enabled in config
    io:format("(*** Ensemble system not configured ***)~n~n"),
    ensemble_overview(Details),
    io:format("~n~n"),

    %% Enabled but two few nodes
    io:format("(*** Enabled but too few nodes ***)~n~n"),
    Details2 = Details#details{enabled=true},
    ensemble_overview(Details2),
    io:format("~n~n"),

    %% Enabled, enough nodes, but not yet active
    Details4 = Details2#details{nodes=['dev1@127.0.0.1',
                                       'dev2@127.0.0.1',
                                       'dev3@127.0.0.1']},
    io:format("(*** All good, waiting on activation ***)~n~n"),
    ensemble_overview(Details4),
    io:format("~n~n"),

    %% Active and ready, but no ensembles
    io:format("(*** Active but no ensembles ***)~n~n"),
    Details5 = Details4#details{active=true},
    ensemble_overview(Details5),
    io:format("~n~n"),

    %% Standard output
    io:format("(*** Standard output ***)~n~n"),
    Dev1 = 'dev1@127.0.0.1',
    Dev2 = 'dev2@127.0.0.1',
    Dev3 = 'dev3@127.0.0.1',
    Dev4 = 'dev4@127.0.0.1',
    Ensembles = [{test,
                  #ensemble_info{views = [[{test, Dev1},
                                            {test, Dev2},
                                            {test, Dev3}]],
                                    vsn = {0, 0},
                                    seq = {0, 0}}
                 },
                 {test2,
                  #ensemble_info{views = [[{test2, Dev1},
                                            {test2, Dev2},
                                            {test2, Dev3}]],
                                    vsn = {0, 0},
                                    seq = {0, 0}}
                 }],
    Quorums = [{test, {{test, Dev1},
                       true,
                       [{test, Dev1},
                        {test, Dev2}]}},
               {test2, {undefined, false, []}}],
    Details6 = Details5#details{ensembles=Ensembles, quorums=Quorums},
    ensemble_overview(Details6),
    io:format("~n~n"),

    %% Multiple views
    io:format("(*** Standard output w/ multiple views ***)~n~n"),
    Ensembles2 = [{test,
                   #ensemble_info{views  = [[{test, Dev1},
                                             {test, Dev2},
                                             {test, Dev3}],
                                            [{test, Dev1},
                                             {test, Dev2},
                                             {test, Dev4}]],
                                    vsn = {0, 0},
                                    seq = {0, 0}}
                  },
                  {test2,
                   #ensemble_info{views  = [[{test2, Dev1},
                                             {test2, Dev2},
                                             {test2, Dev3}]],
                                    vsn = {0, 0},
                                    seq = {0, 0}}
                  }],
    Details7 = Details5#details{ensembles=Ensembles2, quorums=Quorums},
    ensemble_overview(Details7),
    io:format("~n~n"),

    %% Standard ensemble output
    io:format("(*** Standard ensemble output ***)~n~n"),
    PeerInfo = [{{test, Dev1}, {leading, true, 1}},
                {{test, Dev2}, {following, true, 1}},
                {{test, Dev3}, {following, true, 1}}],
    Details8 = Details6#details{peer_info=PeerInfo},
    print_detail(1, Details8),
    io:format("~n~n"),

    %% Ensemble output with offline node
    io:format("(*** Ensemble output w/ offline node ***)~n~n"),
    PeerInfo2 = [{{test, Dev1}, {leading, true, 1}},
                 {{test, Dev2}, nodedown},
                 {{test, Dev3}, {following, true, 1}}],
    Details9 = Details6#details{peer_info=PeerInfo2},
    print_detail(1, Details9),
    io:format("~n~n"),

    %% Ensemble output with unknown info
    io:format("(*** Ensemble output w/ unknown info ***)~n~n"),
    PeerInfo3 = [{{test, Dev1}, {leading, true, 1}},
                 {{test, Dev2}, undefined},
                 {{test, Dev3}, {following, true, 1}}],
    Details10 = Details6#details{peer_info=PeerInfo3},
    print_detail(1, Details10),
    io:format("~n~n"),

    %% Ensemble output with multiple views
    io:format("(*** Standard ensemble output w/ multiple views ***)~n~n"),
    PeerInfo4 = [{{test, Dev1}, {leading, true, 1}},
                 {{test, Dev2}, {following, true, 1}},
                 {{test, Dev3}, {following, true, 1}},
                 {{test, Dev4}, {following, true, 1}}],
    Details11 = Details7#details{peer_info=PeerInfo4},
    print_detail(1, Details11),
    io:format("~n~n"),
    ok.
