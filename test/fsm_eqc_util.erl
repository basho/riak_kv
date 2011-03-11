-module(fsm_eqc_util).
-compile([export_all]).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").

not_empty(G) ->
    ?SUCHTHAT(X, G, X /= [] andalso X /= <<>>).

longer_list(K, G) ->
    ?SIZED(Size, resize(trunc(K*Size), list(resize(Size, G)))).

node_status() ->
    frequency([{1, ?SHRINK(down, [up])},
               {9, up}]).

%% Make sure at least one node is up - code in riak_kv_util makes
%% some assumptions that the node the get FSM is running on is
%% in the cluster causing problems if it isn't.
at_least_one_up(G) ->
    ?SUCHTHAT(X, G, lists:member(up, X)).

num_partitions() ->
    %% TODO: use some unfortunate parition counts (1, 50, etc.)
    % elements([4, 16, 64]).
    ?LET(N, choose(0, 6), pow(2, N)).

largenat() ->
    ?LET(X, largeint(), abs(X)).

bkey() ->
    %%TODO: "make this nastier"
    {binary(6),  %% bucket
     binary(6)}. %% key

vclock() ->
    ?LET(VclockSym, vclock_sym(), eval(VclockSym)).

vclock_sym() ->
    ?LAZY(
       oneof([
              {call, vclock, fresh, []},
              ?LETSHRINK([Clock], [vclock_sym()],
                         {call, ?MODULE, increment,
                          [noshrink(binary(4)), nat(), Clock]})
              ])).

increment(Actor, Count, Vclock) ->
    lists:foldl(
      fun vclock:increment/2,
      Vclock,
      lists:duplicate(Count, Actor)).

riak_object() ->
    ?LET({{Bucket, Key}, Vclock, Value},
         {bkey(), vclock(), binary()},
         riak_object:set_vclock(
           riak_object:new(Bucket, Key, Value),
           Vclock)).

maybe_tombstone() ->
    weighted_default({2, notombstone}, {1, tombstone}).

lineage() ->
    elements([current, ancestor, brother, sister, otherbrother]).

partval() ->
    Shrink = fun(G) -> ?SHRINK(G, [{ok, current}]) end,
    frequency([{2,{ok, lineage()}},
               {1,Shrink(notfound)},
               {1,Shrink(timeout)},
               {1,Shrink(error)}]).

partvals() ->
    not_empty(fsm_eqc_util:longer_list(2, partval())).

%% Generate 5 riak objects with the same bkey
%% 
riak_objects() ->
    ?LET({{Bucket,Key},AncestorVclock,Tombstones}, 
         {noshrink(bkey()),vclock(),vector(5, maybe_tombstone())},
    begin
        BrotherVclock  = vclock:increment(<<"bro!">>, AncestorVclock),
        OtherBroVclock = vclock:increment(<<"bro2">>, AncestorVclock),
        SisterVclock   = vclock:increment(<<"sis!">>, AncestorVclock),
        CurrentVclock  = vclock:merge([BrotherVclock,SisterVclock,OtherBroVclock]),
        Clocks = [{ancestor, AncestorVclock, <<"ancestor">>},
                  {brother,  BrotherVclock, <<"brother">>},
                  {sister,   SisterVclock, <<"sister">>},
                  {otherbrother, OtherBroVclock, <<"otherbrother">>},
                  {current,  CurrentVclock, <<"current">>}],
        [ {Lineage, build_riak_obj(Bucket, Key, Vclock, Value, Tombstone)}
            || {{Lineage, Vclock, Value}, Tombstone} <- lists:zip(Clocks, Tombstones) ]
    end).

build_riak_obj(B,K,Vc,Val,notombstone) ->
    riak_object:set_contents(
        riak_object:set_vclock(
            riak_object:new(B,K,Val),
                Vc),
        [{dict:from_list([{<<"X-Riak-Last-Modified">>,now()}]), Val}]);
build_riak_obj(B,K,Vc,Val,tombstone) ->
    Obj = build_riak_obj(B,K,Vc,Val,notombstone),
    add_tombstone(Obj).

add_tombstone(Obj) ->
    [{M,V}] = riak_object:get_contents(Obj),
    NewM = dict:store(<<"X-Riak-Deleted">>, true, M),
    riak_object:set_contents(Obj, [{NewM, V}]).
                

some_up_node_status(NumNodes) ->
  at_least_one_up(longer_list(NumNodes, node_status())).


pow(_, 0) -> 1;
pow(A, N) -> A * pow(A, N - 1).

make_power_of_two(Q) -> make_power_of_two(Q, 1).

make_power_of_two(Q, P) when P >= Q -> P;
make_power_of_two(Q, P) -> make_power_of_two(Q, P*2).

cycle(N, Xs=[_|_]) when N >= 0 ->
    cycle(Xs, N, Xs).

cycle(_Zs, 0, _Xs) ->
    [];
cycle(Zs, N, [X|Xs]) ->
    [X|cycle(Zs, N - 1, Xs)];
cycle(Zs, N, []) ->
    cycle(Zs, N, Zs).

start_mock_servers() ->
    case whereis(riak_kv_vnode_master) of
        undefined -> ok;
        Pid       ->
            unlink(Pid),
            exit(Pid, shutdown),
            riak_kv_test_util:wait_for_pid(Pid)
    end,
    get_fsm_qc_vnode_master:start_link(),
    ok = application:load(riak_core),
    application:start(crypto),
    riak_core_ring_events:start_link(),
    riak_core_node_watcher_events:start_link(),
    riak_core_node_watcher:start_link(),
    riak_core_node_watcher:service_up(riak_kv, self()),
    ok.



reassign_nodes(Status, Ring) ->
    Ids = [ I || {I, _} <- riak_core_ring:all_owners(Ring) ],
    lists:foldl(
        fun({down, Id}, R) ->
                riak_core_ring:transfer_node(Id, 'notanode@localhost', R);
           (_, R) -> R
        end, Ring, lists:zip(Status, Ids)).


wait_for_req_id(ReqId) ->
    receive
        {ReqId, {ok, Reply1}} ->
            {ok, Reply1};
        {ReqId, Error1} ->
            Error1;
        Anything1 ->
            {anything, Anything1}
    after 400 ->
            timeout
    end.

-endif. % EQC
