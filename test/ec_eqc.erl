-module(ec_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

%%====================================================================
%% eunit test 
%%====================================================================

eqc_test_() ->
    {timeout, 300000, ?_assertEqual(true, quickcheck(numtests(1000, ?QC_OUT(prop()))))}.

%% TODO: 
%% Change put to use per-node vclock.
%% Convert put core to require response from the coordinating node.


%% FSM scheduler EQC test.

test() ->
    test(100).

test(N) ->
    quickcheck(numtests(N, prop())).

check() ->
    check(prop(), current_counterexample()).

-define(B, <<"b">>).
-define(K, <<"k">>).

%%-define(FINALDBG(Fmt,Args), io:format("= " ++ Fmt, Args)).
-define(FINALDBG(_Fmt,_Args), ok).

%% Client requests must be processed in sequence.
%% Client also gets to have it's own state.

-record(client, { %% pri/id must be first two fields for next to work correctly
          cid,    %% Client id
          reqs,   %% Requests to execute [#req{}]
          clntst %% Client state
         }). 

-record(req, {    %% Request
          pri,    %% Priority for next request (undefined if req active)
          rid,    %% Request id
          op}).     %% Operation

-record(proc, {
          name,    %% Term to reference this process in from / to for messages
          handler, %% Handler function for messages
          procst   %% Process state
         }).

-record(msg, {
          pri = 0, %% Priority
          from,    %% Sender
          to,      %% Receipient
          c}).     %% Contents

-record(params, {m, %% Number of nodes in the cluster
                 n,
                 r,
                 w,
                 dw}).

-record(state, {verbose = false,
                step = 1, 
                params,
                next_rid = 1,
                clients = [],     %% [#client]
                procs = [],       %% {Name, State}
                msgs = [],
                history = []}).

-record(kvvnodest, {owner,
                    start,
                    object}).

nni() ->
    ?LET(Xs, int(), abs(Xs)).

gen_pris() ->
    non_empty(resize(40, list(nni()))).

gen_pl_seed() -> %% Seed for preference lists -- used as downed nodes
    list(int()).

gen_params() ->
    #params{n = choose(0, 10),
            r = choose(1, 5),
            w = choose(1, 5)}.

gen_regular_req() ->
    ?SHRINK(elements([{get, gen_pl_seed()},
                      {update, gen_pl_seed(), gen_pl_seed()}]),
            [{update, gen_pl_seed(), gen_pl_seed()}]).

gen_handoff_req() ->
    %% Generate a handoff request for index/node
    elements([{trigger_handoff, int(), int()},
              {set_owner, int(), int()}]).

gen_regular_client() ->
    #client{reqs = non_empty(list(gen_regular_req()))}.

gen_handoff_client() ->
    #client{reqs = ?SHRINK(list(gen_handoff_req()), [])}.

gen_client_seeds() ->
    ?LET({Clients, Handoff}, 
         {non_empty(list(gen_regular_client())), gen_handoff_client()},
         [Handoff | Clients]).

prop() ->
    ?FORALL({Pris, ClientSeeds, ParamsSeed},
            {gen_pris(), gen_client_seeds(),gen_params()},
            begin
                %% io:format(user, "Pris: ~p\n", [Pris]),
                set_pri(Pris),
                Params = make_params(ParamsSeed),
                                                %NodeProcs = make_nodes(Q),
                VnodeProcs = make_vnodes(Params),
                Initial = #state{params = Params, 
                                 procs = VnodeProcs},
                Clients = make_clients(ClientSeeds, Params),
                Start = Initial#state{clients = Clients},
                %% io:format(user, "=== Start ===\nParams:\n~p\nClientSeeds\n~p\nState: ~p\n",
                %%           [ann_params(Params), ClientSeeds, Start]),
                case exec(Start) of
                    {ok, Final} ->
                        ?WHENFAIL(
                           begin
                               io:format(user, "Params:\n~p\nClients:\n~p\nFinal:\n~p\nHistory\n",
                                         [ann_params(Params),
                                          Clients,
                                          Final]),
                               pretty_history(Final#state.history)
                           end,
                           check_final(Final));
                    {What, Reason, Next, FailS} ->
                        io:format(user, "FAILED: ~p: ~p\nParams:\n~p\nNext: ~p\nStart:\n~p\n"
                                  "FailS:\n~p\n",
                                  [What, Reason, ann_params(Params), Next,
                                   ann_state(Start), ann_state(FailS)]),
                        false
                end
            end).

check_final(#state{history = H}) ->
    %% Go through the history and check for the expected values
    %% For each get track the must value(s)
    %% For each put move the must value(s) at get time to may and add the new must.
    Results = [R || R = {result, _, _, _} <- H],

    ?WHENFAIL(io:format(user, "Results:\n~p\n", [Results]),
              check_final(Results, [], [], [])).

%% Todo, stop overloading Result

check_final([{result, _, #req{op = {get, PL}}, Result}], Must, _May, _ClientView) ->
    {Values, _VClock} = case Result of
                            {ok, Obj} ->
                                {riak_object:get_values(Obj), riak_object:vclock(Obj)};
                            {error, _} ->
                                {[], []}
                        end,
    _Fallbacks = [{I,J} || {kv_vnode, I, J} <- PL, I /= J],
    ?FINALDBG("Final GOT VALUES: ~p VC: ~w Must: ~p May: ~p Fallbacks: ~w\n",
              [Values, _VClock, Must, _May, _Fallbacks]),
    ?WHENFAIL(io:format(user, "Must: ~p\nMay: ~p\nValues: ~p\n", [Must, _May, Values]),
              equals(Must -- Values, []));
%% conjunction([{must_leftover, equals(Must -- Values, [])},
%%              {must_may_leftover, equals(Values -- (Must ++ May), [])}]))

check_final([{result, Cid, #req{op = {get, PL}}, Result} | Results],
            Must, May, ClientViews) ->
    %% TODO: Check if _Result matches expected values
    {ValuesAtGet, _VClockAtGet} = case Result of
                                      {ok, Obj} ->
                                          {riak_object:get_values(Obj), riak_object:vclock(Obj)};
                                      {error, _} ->
                                          {[], []}
                                  end,
    _Fallbacks = [{I,J} || {kv_vnode, I, J} <- PL, I /= J],
    ?FINALDBG("Cid ~p GOT VALUES: ~p VC: ~w FALLBACKS: ~w\n",
              [Cid, ValuesAtGet, _VClockAtGet, _Fallbacks]),
    UpdClientViews = lists:keystore(Cid, 1, ClientViews, {Cid, ValuesAtGet}),
    check_final(Results, Must, May, UpdClientViews);
check_final([{result, Cid, #req{rid = _ReqId, op = {put, PL, V}}, 
              {Result, _PutObj, _UpdObj}} | Results],
            Must, May, ClientViews) ->
    {Cid, ValuesAtGet} = lists:keyfind(Cid, 1, ClientViews),
    ValNotInMay = not lists:member(V, May),
    _Fallbacks = [{I,J} || {kv_vnode, I, J} <- PL, I /= J],
    UpdMust = case Result of
                  ok when ValNotInMay -> %, Fallbacks == [] ->
                      %% This put could have already been overwritten
                      %% Only add to must if not in may
                      %% Only add if writing against primaries, otherwise all bets are off
                      %% until vclocks are changed
                      [V | lists:usort(Must -- ValuesAtGet)];
                  ok  ->
                      %% This value has already been overwritten
                      %% so the values that it overwrote should also be removed 
                      %% from must.
                      Must -- ValuesAtGet;

                  _ ->
                      %% Put failed, not sure what must be there for now
                      %% TODO: Work out what should be here.
                      []
              end,
    UpdMay = lists:usort(May ++ ValuesAtGet),
    ?FINALDBG("Cid ~p PUT: ~p RESPONSE: ~p VC: ~w OVER ~p MUST: ~p MAY: ~p FALLBACKS: ~w\n",
              [Cid, V, Result, riak_object:vclock(_PutObj), ValuesAtGet, UpdMust, UpdMay, _Fallbacks]),
    UpdClientViews = lists:keydelete(Cid, 1, ClientViews),
    check_final(Results, UpdMust, UpdMay, UpdClientViews);
check_final([_ | Results], Must, May, ClientViews) ->
    check_final(Results, Must, May, ClientViews).



exec(S) ->
    Next = next(S),
    try
        status(S, Next),
        case Next of
            done ->
                {ok, S};
            deliver_msg ->
                exec(inc_step(deliver_msg(S)));
            {client, Cid}->
                exec(inc_step(deliver_req(Cid, S)))
        end
    catch
        What:Reason ->
            {What, {Reason, erlang:get_stacktrace()}, Next, S}
    end.

status(#state{verbose = true} = S, Next) ->
    io:format(user, "--- Step ~p - ~p ---\n~p\n",
              [S#state.step, Next, ann_state(S)]);
status(_S, _Next) ->
    ok.

inc_step(#state{step = Step} = S) ->
    S#state{step = Step + 1}.

next(#state{clients = Clients, msgs = Msgs}=S) ->
    %% Work out which client ids are active
    ReadyCs = [{Pri, Cid} || #client{cid = Cid,
                                     reqs = [#req{pri = Pri, 
                                                  rid = undefined}|_]} <- Clients],
    {ClientPri, Cid} = case lists:sort(ReadyCs) of
                           [] ->
                               %% No clients ready
                               {undefined, undefined};
                           RCs -> % ready clients
                               hd(RCs)
                       end,
    MsgPri = case Msgs of
                 [] ->
                     undefined;
                 [Msg|_] ->
                     Msg#msg.pri
             end,
    if
        MsgPri == undefined andalso ClientPri == undefined ->
            case Clients of
                [] ->
                    done;
                _ -> % oh no, there was client work to do, but we've run out of msgs
                    throw({stalled, S})
            end;
        ClientPri == undefined orelse MsgPri =< ClientPri ->
            deliver_msg;

        true ->
            {client, Cid}
    end.

%% Deliver next client request for Cid
deliver_req(Cid, #state{next_rid = ReqId, clients = Clients, procs = Procs,
                        msgs = Msgs, params = Params} = S) ->
    C = get_client(Cid, Clients),
    Result = start_req(ReqId, C, Params),
    NewProcs = proplists:get_value(procs, Result, []),
    NewMsgs = proplists:get_value(msgs, Result, []),
    UpdC = proplists:get_value(updc, Result, C),
    add_msgs(NewMsgs, Msgs, 
             S#state{next_rid = ReqId + 1,
                     clients = update_client(UpdC, Clients),
                     procs = Procs ++ NewProcs}).

%% Deliver next pending msg
deliver_msg(#state{msgs = [#msg{to = {req, ReqId}} = Msg | Msgs],
                   clients = Clients,
                   history = H} = S) ->
    [C] = [C || #client{reqs = [#req{rid = ReqId0}|_]}=C <- Clients, ReqId0 == ReqId],
    Result = end_req(Msg, C),
    UpdC = next_req(proplists:get_value(updc, Result, C)),
    NewH = proplists:get_value(history, Result, []),
    UpdClients = case UpdC of
                     #client{reqs = [], cid = Cid} ->
                         delete_client(Cid, Clients);
                     _ ->
                         update_client(UpdC, Clients)
                 end,
    S#state{clients = UpdClients,
            msgs = Msgs,
            history = H ++ [{deliver_msg, Msg} | NewH]};

deliver_msg(#state{msgs = [#msg{to = To} = Msg | Msgs],
                   history = History,
                   procs = Procs} = S) ->
    P = get_proc(To, Procs),
    Handler = P#proc.handler,
    Result = ?MODULE:Handler(Msg, P),
    NewMsgs = proplists:get_value(msgs, Result, []),
    UpdP = proplists:get_value(updp, Result, P),
    add_msgs(NewMsgs, Msgs,
             S#state{procs = update_proc(UpdP, Procs),
                     history = History ++ [{deliver_msg, Msg}]}).

make_params(#params{n = NSeed, r = R, w = W} = P) ->
    %% Ensure R >= N, W >= N and R+W>N
    MinN = lists:max([R, W]),
    N = make_range(R + W - NSeed, MinN, R+W-1),
    %% Force N to be odd while testing
    {N1,R1} = case N rem 2 == 0 of
                  true ->
                      {N + 1, R + 1};
                  false ->
                      {N, R}
              end,
    P#params{n = N1, r = R1, m = N1, dw = W}. 
%% make_params(#params{} = P) ->
%%     P#params{n = 3, r = 2, m = 5, w = 2, dw = 2}.
%% make_params(#params{} = P) ->
%%     P#params{n = 1, r = 1, m = 2, w = 1, dw = 1}.


make_vnodes(#params{n = N, m = M}) ->
    [#proc{name={kv_vnode, I, J}, 
           handler=kv_vnode, 
           procst=#kvvnodest{owner = I}} || I <- lists:seq(1, N), J <- lists:seq(1, M)].

make_clients(ClientSeeds, Params) ->
    make_clients(ClientSeeds, 0, Params, []). %% 0 will be handoff req

make_clients([], _Cid, #params{n = N}, Acc) ->
    %% Make sure the last request is a get.
    HandoffReq = new_req(final_handoffs),
    GetReq = new_req({get, [{kv_vnode, I, I} || I <- lists:seq(1, N)]}),
    LastC = #client{cid = 1000000, reqs = [HandoffReq#req{pri = 1000001},
                                           GetReq#req{pri = 1000002}]},
    lists:reverse([LastC | Acc]);
make_clients([#client{reqs = ReqSeeds} = CS | CSs], Cid, Params, Acc) ->
    case make_reqs(ReqSeeds, Cid, Params) of
        [] ->% skip request-less clients
            make_clients(CSs, Cid + 1, Params, Acc); % increment Cid, is probably handoff proc
        Reqs ->
            C = CS#client{cid = Cid, reqs = Reqs},
            make_clients(CSs, Cid + 1, Params, [C | Acc])
    end.

make_reqs(ReqSeeds, Cid, Params) ->
    make_reqs(ReqSeeds, Cid, Params, 1, []).

make_reqs([], _Cid, _Params, _CReqNum, Acc) ->
    lists:reverse(Acc);
make_reqs([ReqSeed | ReqSeeds], Cid, Params, CReqNum, Acc) ->
    Reqs = make_req(ReqSeed, Cid, Params, CReqNum),
    make_reqs(ReqSeeds, Cid, Params, CReqNum + 1, lists:reverse(Reqs) ++ Acc).


%% Make a preference list for partitions 1..N from nodes 1..M.
%% Owner for partition P == node P.
%% Replace any downed nodes with fallbacks.  If all nodes are down, 
%% generate default preflist.
%% Pick the fallbacks by using non-primaries then fallback 
%%
make_pl(N, M, DownNodesSeed) ->
    %% Convert seed to 1..M
    DownNodes = [(abs(Seed - 1) rem M) + 1 || Seed <- DownNodesSeed],
    %% Generate list of primaries and fallbacks
    Primaries = lists:seq(1, N) -- DownNodes,
    NonPrimaries = lists:seq(N+1, M) -- DownNodes,
    Fallbacks = NonPrimaries ++ Primaries,
    %% Work out which partitions need fallbacks
    NeedFallbacks = lists:seq(1, N) -- Primaries,
    case Fallbacks == [] andalso NeedFallbacks /= [] of 
        true -> %% System unavailable, not a very interesting test so use a perfect one
            [{kv_vnode, Idx, Idx} || Idx <- lists:seq(1, N)];
        _ ->
            make_pl_add_fallbacks(NeedFallbacks,
                                  Fallbacks,
                                  [{kv_vnode, Primary, Primary} || Primary <- lists:reverse(Primaries)])
    end.

make_pl_add_fallbacks([], _, PL) ->
    lists:reverse(PL);
make_pl_add_fallbacks([Idx | NeedFallbacks], [Node | Fallbacks], PL) ->
    make_pl_add_fallbacks(NeedFallbacks, Fallbacks ++ [Node], [{kv_vnode, Idx, Node} | PL]).


%% %% Full EC test make_pl - create a pref list that shrinks to primaries as
%% %% seed shrinks to [0]
%% make_pl(N, M, PLSeed) ->
%%     make_pl(N, M, PLSeed, []).

%% make_pl(0, _M, _PLSeed, PL) ->
%%     PL;
%% make_pl(Idx, M, [PLSeedH|PLSeedT], PL) ->
%%     Node = (abs(Idx - 1 + PLSeedH) rem M) + 1, % Node between 1 and M
%%     make_pl(Idx - 1, M, PLSeedT ++ [PLSeedH], [{kv_vnode, Idx, Node} | PL]).


%% %% Use PLSeed to change one entry at random when N > 1
%% make_pl(N, M, PLSeed) ->
%%     PerfectPL = [{kv_vnode, Idx, Idx} || Idx <- lists:seq(1, N)],
%%     case hd(PLSeed) of
%%         0 ->
%%             PerfectPL;
%%         _IdxSeed when N == 1 ->
%%             PerfectPL;
%%         IdxSeed ->
%%             Idx = (abs(IdxSeed) rem N) + 1,
%%             PLSeedH = hd(tl(PLSeed) ++ [IdxSeed]),
%%             Node = (abs(Idx - 1 + PLSeedH) rem M) + 1,
%%             lists:keyreplace(Idx, 2, PerfectPL, {kv_vnode, Idx, Node})
%%     end.

%% Move the client on to the next request, setting priority if present
next_req(#client{reqs = [_|Reqs]} = C) ->
    C#client{reqs = Reqs}.

%% Get client by cid from list of clients - blow up if missing
get_client(Cid, Clients) ->
    case lists:keysearch(Cid, #client.cid, Clients) of
        {value, C} ->
            C;
        false ->
            throw({bad_cid, Cid, [Cid0 || #client{cid = Cid0} <- Clients]})
    end.

delete_client(Cid, Clients) ->
    lists:keydelete(Cid, #client.cid, Clients).

update_client(C, Clients) ->
    lists:keyreplace(C#client.cid, #client.cid, Clients, C).

%% Get proc by name - blow up if missing
get_proc(Name, Procs) ->
    case lists:keysearch(Name, #proc.name, Procs) of
        {value, Proc} ->
            Proc;
        false ->
            throw({bad_proc, Name, [N || #proc{name = N} <- Procs]})
    end.

%% Update a proc
update_proc(P, Procs) ->
    lists:keyreplace(P#proc.name, #proc.name, Procs, P).

%% Annotate a state record - returns a proplist by fieldname
ann_state(R) ->
    Elements = tuple_to_list(R),
    Type = hd(Elements),
    Fields = record_info(fields, state),
    {Type, lists:zip(Fields, tl(Elements))}.

%% Annotate a parameters record - returns a proplist by fieldname
ann_params(Params) ->
    Elements = tuple_to_list(Params),
    Type = hd(Elements),
    Fields = record_info(fields, params),
    {Type, lists:zip(Fields, tl(Elements))}.


%% Make a value between min and max inclusive using the seed
make_range(Seed, Min, Max) when Min =< Seed, Seed =< Max ->
    Seed;
make_range(Seed, Min, Max) ->
    Range = Max - Min + 1,
    Min + (abs(Seed) rem Range).


%% Create a request for a client
make_req({get, PLSeed}, _Cid, #params{n = N, m = M}, _CReqNum) ->
    [new_req({get, make_pl(N, M, PLSeed)})];
make_req({update, PLSeed1, PLSeed2}, Cid, #params{n = N, m = M}, CReqNum) ->
    %% For an update, issue a get then a put.
    %% TODO: Find a way to make the update priority different from the get
    Value = iolist_to_binary(io_lib:format("C~p-U~p", [Cid, CReqNum])),
    [new_req({get, make_pl(N, M, PLSeed1)}),
     new_req({put, make_pl(N, M, PLSeed2), Value})];
make_req({set_owner, IdxSeed, NodeSeed},
         _Cid, #params{n = N, m = M}, _CReqNum) ->
    Idx = make_range(IdxSeed, 1, N),
    Node = make_range(NodeSeed, 1, M),
    [new_req({set_owner, Idx, Node})];
make_req({trigger_handoff, IdxSeed, NodeSeed},
         _Cid, #params{n = N, m = M}, _CReqNum) ->
    Idx = make_range(IdxSeed, 1, N),
    Node = make_range(NodeSeed, 1, M),
    case Idx of
        Node ->
            []; %% Cannot handoff to self
        _ ->
            [new_req({trigger_handoff, Idx, Node})]
    end;
make_req(final_handoffs, _Cid, _Params, _CReqNum) ->
    [new_req(final_handoffs)].

new_req(Op) ->
    #req{pri = next_pri(), op = Op}.

new_msg(From, To, Contents) ->
    new_msg(From, To, Contents, next_pri()).

new_msg(From, To, Contents, Pri) ->
    #msg{from = From, to = To, c = Contents, pri = Pri}.

add_msgs(NewMsgs, Msgs, S) ->
    S#state{msgs = lists:keymerge(#msg.pri, Msgs, lists:keysort(#msg.pri, NewMsgs))}.

set_pri(Pris) ->
    put(pri, Pris).

next_pri() ->
    [Next|Rest] = get(pri),
    set_pri(Rest ++ [Next]),
    Next.

%% Start the next client request
start_req(ReqId, #client{reqs = [Req | Reqs]} = C, Params) ->
    UpdReq = Req#req{rid = ReqId},
    UpdC = C#client{reqs = [UpdReq | Reqs]},
    Result = client_req(UpdC, Params),
    %% If the result does not include an updated client record
    %% use the one with the reqs updated.  Any original
    %% updc entry will be never be retrieved.
    [{updc, proplists:get_value(updc, Result, UpdC)} | Result].


%% Start client requests - return a proplist of 
%% [{procs, NewProcs},
%%  {msgs, NewMsgs},
%%  {updc, UpdC}];
client_req(#client{reqs = [#req{op = {get, PL}, rid = ReqId} | _]}, Params) ->
    Proc = get_fsm_proc(ReqId, Params),
    NewProcs = [Proc],
    NewMsgs = [new_msg({req, ReqId}, Proc#proc.name, {get, PL})],
    [{procs, NewProcs},
     {msgs, NewMsgs}];
client_req(#client{reqs = [#req{op = {put, PL, UpdV}, rid = ReqId} | _],
                   cid = _Cid,  clntst = {#req{op={get, _PL}}, GetResult}},
           Params) ->
    Proc = put_fsm_proc(ReqId, Params),
    NewProcs = [Proc],
    UpdObj = riak_object:apply_updates(
               case GetResult of
                   {error, notfound} ->
                       riak_object:new(?B, ?K, UpdV, dict:from_list([{gen, ReqId}]));
                   {ok, Obj} ->
                       Gen = lists:max([dict:fetch(gen, MD0) ||
                                           MD0 <- riak_object:get_metadatas(Obj)]),
                       MD = hd(riak_object:get_metadatas(Obj)),
                       UpdMD = dict:store(gen, Gen, MD),
                       riak_object:update_metadata(riak_object:update_value(Obj, UpdV), UpdMD)
               end),
    NewMsgs = [new_msg({req, ReqId}, Proc#proc.name, {put, PL, UpdObj})],
    [{procs, NewProcs},
     {msgs, NewMsgs}];
client_req(#client{reqs = [#req{op = {set_owner, Idx, Node}, rid = ReqId} | _]},
           #params{m = M}) ->
    %% Send set owner messages to each node for the index - they may not be delivered
    %% at the same time so there will be periods of inconsistency
    SetOwnerMsgs = [new_msg(set_owner, {kv_vnode, Idx, J}, {set_owner, Node}) || 
                       J <- lists:seq(1, M)],
    ResponseMsg = new_msg(set_owner, {req, ReqId}, set_owner_scheduled),
    [{msgs, SetOwnerMsgs ++ [ResponseMsg]}];
client_req(#client{reqs = [#req{op = {trigger_handoff, Idx, Node}, rid = ReqId} | _]},
           _Params) ->
    %% Send handoff_to messages to each fallback vnode
    HandoffMsg = new_msg(handoff, {kv_vnode, Idx, Node}, handoff),
    ResponseMsg = new_msg(handoff, {req, ReqId}, handoff_scheduled),
    [{msgs, [HandoffMsg, ResponseMsg]}];
client_req(#client{reqs = [#req{op = final_handoffs, rid = ReqId} | _]},
           #params{n = N, m = M}) ->
    %% Reset the owner for partition I to node I.
    SetOwnerMsgs = [new_msg(final_handoff, {kv_vnode, I, J}, {set_owner, I}, 0) || 
                      I <- lists:seq(1, N),
                      J <- lists:seq(1, M)],
    %% Send handoff_to messages to each fallback vnode
    HandoffMsgs = [new_msg(final_handoff, {kv_vnode, I, J}, handoff, 0) || 
                      I <- lists:seq(1, N),
                      J <- lists:seq(1, M),
                      I /= J],
    ResponseMsg = new_msg(final_handoff, {req, ReqId}, final_handoffs_scheduled, 0),
    [{msgs, SetOwnerMsgs ++ HandoffMsgs ++ [ResponseMsg]}].

end_req(#msg{c = Result},
        #client{cid = Cid, reqs = [Req |_Reqs]} = C) ->
    [{history, [{result, Cid, Req, Result}]},
     {updc, C#client{clntst = {Req, Result}}},
     done].


%% Deliver a message to a process
%% node_proc(#msg{from = From, c = ping}, #proc{name = {node, _}=Name}) ->
%%     [{msgs, [#msg{from = Name, to = From, c = pong}]}].

-record(getfsmst, {pl,
                   reply_to,
                   responded = false,
                   getcore}).

get_fsm_proc(ReqId, #params{n = N, r = R}) ->
    FailThreshold = (N div 2) + 1,
    NotFoundOk = true,
    AllowMult = true,
    DeletedVclock = true,
    GetCore = riak_kv_get_core:init(N, R,
                                    0, %% SLF hack
                                    FailThreshold,
                                    NotFoundOk, AllowMult, DeletedVclock,
                                    [{Idx, primary} || Idx <- lists:seq(1, N)] %% SLF hack
                                   ),
    #proc{name = {get_fsm, ReqId}, handler = get_fsm,
          procst = #getfsmst{getcore = GetCore}}.

get_fsm(#msg{from = From, c = {get, PL}},
        #proc{name = {get_fsm, ReqId} = Name, procst = ProcSt} = P) ->
    %% Kick off requests to the vnodes
    [{msgs, [new_msg(Name, Vnode, {get, ReqId}) || Vnode <- PL]},
     {updp, P#proc{procst = ProcSt#getfsmst{pl = PL, reply_to = From}}}];
get_fsm(#msg{from = {kv_vnode, Idx, _}, c = {r, Result, Idx, _ReqId}},
        #proc{name = Name, procst = #getfsmst{pl = PL,
                                              reply_to = ReplyTo,
                                              responded = Responded,
                                              getcore = GetCore} = ProcSt} = P) ->
    UpdGetCore1 = riak_kv_get_core:add_result(Idx, Result, GetCore),
    {ReplyMsgs, UpdGetCore3, UpdResponded} =
        case riak_kv_get_core:enough(UpdGetCore1) of
            true when Responded == false ->
                %% Worry about read repairs later
                {Response, UpdGetCore2} = riak_kv_get_core:response(UpdGetCore1),
                {[new_msg(Name, ReplyTo, Response)], UpdGetCore2, true};
            _ ->
                %% Replied already or more to come, worry about timeout later.
                {[], UpdGetCore1, Responded}
        end,
    %% If all results then trigger any final actions
    {FinalMsgs, UpdGetCore} =
        case riak_kv_get_core:has_all_results(UpdGetCore3) of
            true ->
                {Final, UpdGetCore4} = riak_kv_get_core:final_action(UpdGetCore3),
                case Final of
                    %% Treat read repairs as handoff puts for now - want 
                    %% normal merging behavior.
                    {read_repair, RepairIdx, MObj} ->
                        {[new_msg(Name, To, {handoff_obj, MObj, read_repair}) ||
                             {kv_vnode, Idx0, _Node} = To <- PL,
                             lists:member(Idx0, RepairIdx)], 
                         UpdGetCore4};
                    _ -> %% Ignore deletes and nops for now
                        {[], UpdGetCore4}
                end;
            false ->
                {[], UpdGetCore3}
        end,
    [{msgs, ReplyMsgs ++ FinalMsgs},
     {updp, P#proc{procst = ProcSt#getfsmst{responded = UpdResponded,
                                            getcore = UpdGetCore}}}].



-record(putfsmst, {reply_to,
                   putobj,
                   updobj,
                   remotepl, %% Preference list for remove vnodes
                   responded = false,
                   putcore}).
put_fsm_proc(ReqId, #params{n = N, w = W, dw = DW}) ->
    AllowMult = true,
    ReturnBody = false,
    PutCore = riak_kv_put_core:init(N, W, DW,
                                    DW, %% SLF hack
                                    N-W+1,   % cannot ever get W replies
                                    N-DW+1,  % cannot ever get DW replies
                                    AllowMult,
                                    ReturnBody,
                                    [{Idx, primary} || Idx <- lists:seq(1, N)] %% SLF hack
),
    #proc{name = {put_fsm, ReqId}, handler = put_fsm, procst = #putfsmst{putcore = PutCore}}.

put_fsm(#msg{from = From, c = {put, PL, Obj}},
        #proc{name = {put_fsm, ReqId}= Name, procst = ProcSt} = P) ->
    Ts = ReqId, % re-use ReqId for a timestamp to make them unique
    %% Decide on the coordinating vnode and require that as part of the response.
    %% As indices are fixed, pick lowest index of primary node, falling back to 
    %% lowest index secondary node
    {kv_vnode, CoordIdx, Node} = hd(PL),

    %% TODO, find some way to make putcore wait on the response from the local vnode.
    %% Temporarily set dw to all.

    %% Kick off requests to the vnodes
    LocalMsg =  new_msg(Name, {kv_vnode, CoordIdx, Node}, {coord_put, ReqId, Obj, Ts}),
    RemotePL = case [Entry || {kv_vnode, Idx0, _Node0} = Entry <- PL, Idx0 /= CoordIdx] of
                   [] ->
                       undefined;
                   RemotePL0 ->
                       RemotePL0
               end,
    [{msgs, [LocalMsg]},
     {updp, P#proc{procst = ProcSt#putfsmst{remotepl = RemotePL,
                                            putobj = Obj,
                                            reply_to = From}}}];
%% Handle local vnode response 
put_fsm(#msg{from = {kv_vnode, _, _}, c = Result}, 
        #proc{name = {put_fsm, ReqId} = Name, 
              procst = #putfsmst{reply_to = ReplyTo,
                                 putobj = PutObj,
                                 remotepl = RemotePL,
                                 putcore = PutCore} = ProcSt} = P) when RemotePL /= undefined ->
    UpdPutCore = riak_kv_put_core:add_result(Result, PutCore),
    case Result of
        {fail, _, _} -> %% failed because out of date
            [{msgs, [new_msg(Name, ReplyTo, {{error, out_of_date}, PutObj})]},
             {updp, P#proc{procst = ProcSt#putfsmst{responded = true, putcore = UpdPutCore}}}];
        {w, _, _} -> % W from coord, no excitement.
            [{updp, P#proc{procst = ProcSt#putfsmst{putcore = UpdPutCore}}}];
        {dw, _, _, UpdObj} -> % DW from coord, green light for the rest of the put
            RemoteMsgs = [new_msg(Name, Vnode, {put, ReqId, UpdObj}) || Vnode <- RemotePL],
            [{msgs, RemoteMsgs},
             {updp, P#proc{procst = ProcSt#putfsmst{putcore = UpdPutCore,
                                                    updobj = UpdObj,
                                                    remotepl = undefined}}}]
    end;
%% Handle results from non-coordinator
put_fsm(#msg{from = {kv_vnode, _, _}, c = Result},
        #proc{name = Name, procst = #putfsmst{reply_to = ReplyTo,
                                              putobj = PutObj,
                                              updobj = UpdObj,
                                              responded = Responded,
                                              putcore = PutCore} = ProcSt} = P) ->
    UpdPutCore = riak_kv_put_core:add_result(Result, PutCore),
    case riak_kv_put_core:enough(UpdPutCore) of
        true when Responded == false ->
            %% Worry about read repairs later
            {Response, UpdPutCore2} = riak_kv_put_core:response(UpdPutCore),
            [{msgs, [new_msg(Name, ReplyTo, {Response, PutObj, UpdObj})]},
             {updp, P#proc{procst = ProcSt#putfsmst{responded = true,
                                                    putcore = UpdPutCore2}}}];
        _ ->
            %% Already responded or more to come, worry about timeout later.
            [{updp, P#proc{procst = ProcSt#putfsmst{putcore = UpdPutCore}}}]
    end.

kv_vnode(#msg{from = From, c = {get, ReqId}},
         #proc{name = {kv_vnode, Idx, _Node} = Name,
               procst = #kvvnodest{object = CurObj}}) ->
    Result = case CurObj of
                 undefined ->
                     {error, notfound};
                 _ ->
                     {ok, CurObj}
             end,
    [{msgs, [new_msg(Name, From, {r, Result, Idx, ReqId})]}];
kv_vnode(#msg{from = From, c = {coord_put, ReqId, NewObj, Timestamp}},
         #proc{name = {kv_vnode, Idx, Node} = Name, 
               procst = #kvvnodest{start = Start0, object = CurObj} = PS} = P) ->
    %% Change start when data written for the first time
    Start = case CurObj of
                undefined ->
                    ReqId;
                _ ->
                    Start0
            end,
    [Pri1, Pri2] = lists:sort([next_pri(), next_pri()]),
    WMsg = new_msg(Name, From, {w, Idx, ReqId}, Pri1),
    NodeId = {vc, Start, Node},
    NewObj1 = riak_object:increment_vclock(NewObj, NodeId, ReqId),
    UpdObj = coord_put_merge(CurObj, NewObj1, NodeId, Timestamp),
    DWMsg = new_msg(Name, From, {dw, Idx, ReqId, UpdObj}, Pri2), % ignore returnbody for now
    [{msgs, [WMsg, DWMsg]},
     {updp, P#proc{procst = PS#kvvnodest{start = Start, object = UpdObj}}}];
kv_vnode(#msg{from = From, c = {put, ReqId, NewObj}},
         #proc{name = {kv_vnode, Idx, _Node} = Name, 
               procst = #kvvnodest{object = CurObj} = PS} = P) ->
    [Pri1, Pri2] = lists:sort([next_pri(), next_pri()]),
    WMsg = new_msg(Name, From, {w, Idx, ReqId}, Pri1),
    case syntactic_put_merge(CurObj, NewObj) of
        keep ->
            FailMsg = new_msg(Name, From, {fail, Idx, ReqId}, Pri2),
            [{msgs, [WMsg, FailMsg]}];
        UpdObj ->
            DWMsg = new_msg(Name, From, {dw, Idx, ReqId}, Pri2), % ignore returnbody for now
            [{msgs, [WMsg, DWMsg]},
             {updp, P#proc{procst = PS#kvvnodest{object = UpdObj}}}]
    end;
kv_vnode(#msg{c = {set_owner, Owner}},
         #proc{procst = PS} = P) ->
    [{updp, P#proc{procst = PS#kvvnodest{owner = Owner}}}];
kv_vnode(#msg{from = From, c = handoff},
         #proc{name = {kv_vnode, Idx, Node} = Name, 
               procst = #kvvnodest{owner = Owner, object = CurObj}}) ->
    %% Send current object back to the owner if there is one
    NewMsgs = case {CurObj, Owner} of
                  {undefined, _} -> %% Nothing to handoff
                      [];
                  {_, Node} -> %% Do not handoff to self
                      [];
                  _ ->
                      To = {kv_vnode, Idx, Owner},
                      Reason = case From of
                                   final_handoff ->
                                       final_handoff;
                                   _ ->
                                       handoff
                               end,
                      [new_msg(Name, To, {handoff_obj, CurObj, Reason})]
              end,
    [{msgs, NewMsgs}];
kv_vnode(#msg{from = From, c = {handoff_obj, HandoffObj, Reason}},
         #proc{name = Name, procst = #kvvnodest{object = CurObj} = PS} = P) ->
    %% Simulate a do_diffobj_put
    UpdObj = case syntactic_put_merge(CurObj, HandoffObj) of
                 keep ->
                     CurObj;
                 UpdObj0 ->
                     UpdObj0
             end,
    AckMsg = case Reason of
                 handoff ->
                     [new_msg(Name, From, {ack_handoff, HandoffObj})];
                 _ ->
                     []
             end,

    %% Real syntactic put merge checks allow_mult here and applies, testing with 
    %% allow_mult is true so leave object
    [{msgs, AckMsg},
     {updp, P#proc{procst = PS#kvvnodest{object = UpdObj}}}];
kv_vnode(#msg{c = {ack_handoff, HandoffObj}},
         #proc{name = {kv_vnode, _Idx, Node},
               procst = #kvvnodest{owner = Owner, object = CurObj} = PS} = P) ->
    case {HandoffObj == CurObj, Owner == Node} of 
        %% If ack is for current object and this node is not the owner
        {true, false} ->
            [{updp, P#proc{procst = PS#kvvnodest{object = undefined}}}];
        _ ->
            []
    end.



syntactic_put_merge(CurObj, UpdObj) ->
    case CurObj of
        undefined ->
            UpdObj;
        _ ->
            case riak_object:ancestors([CurObj, UpdObj]) of
                [UpdObj] ->
                    keep;
                [CurObj] ->
                    UpdObj;
                _ ->
                    riak_object:merge(CurObj, UpdObj)
            end
    end.


coord_put_merge(undefined, UpdObj, _NodeId, _Timestamp) ->
    UpdObj;
coord_put_merge(CurObj, UpdObj, NodeId, Timestamp) ->
    %% Make sure UpdObj descends from CurObj and that NodeId is greater
    CurVC = riak_object:vclock(CurObj),
    UpdVC = riak_object:vclock(UpdObj),

    %% Valid coord put replacing current object
    case get_counter(NodeId, UpdVC) > get_counter(NodeId, CurVC) andalso
        vclock:descends(CurVC, UpdVC) == false andalso 
        vclock:descends(UpdVC, CurVC) == true of
        true ->
            UpdObj;
        false ->
            riak_object:increment_vclock(riak_object:merge(CurObj, UpdObj),NodeId, Timestamp)
    end.


get_counter(Id, VC) ->            
    case lists:keyfind(Id, 1, VC) of
        false ->
            0;
        {Counter, _TS} ->
            Counter
    end.


pretty_history([]) ->
    ok;
pretty_history([Event | Rest]) ->
    pretty_history_event(Event),
    pretty_history(Rest).

pretty_history_event({deliver_msg, #msg{to = {kv_vnode, Idx, N}, from = From, c = C}}) ->
    case C of
        {get, ReqId} ->
            io:format(user, "Vnode Req {~p,~p} from=~p get reqId=~p\n", [Idx, N, From, ReqId]);
        {coord_put, ReqId, Obj, NodeId, Ts} ->
            io:format(user, "Vnode Req {~p,~p} from=~p coord_put reqId=~p v=~p nid=~p ts=~p\n",
                      [Idx, N, From, ReqId, riak_object:get_values(Obj), NodeId, Ts]);
        _ ->
            io:format(user, "Vnode Req {~p,~p} from=~p C=~p)\n", [Idx, N, From, C])
    end;
pretty_history_event({deliver_msg, #msg{from = {kv_vnode, Idx, N}, to = To, c = C}}) ->
    case C of
        {w,_,_} ->
            io:format(user, "Vnode Res {~p,~p} to=~p put W\n",
                      [Idx, N, To]);
        {dw,_,_,Obj} ->
            io:format(user, "Vnode Res {~p,~p} to=~p put DW v=~p\n",
                      [Idx, N, To, riak_object:get_values(Obj)]);
        _ ->
            io:format(user, "Vnode Res {~p,~p} to=~p C=~p)\n", [Idx, N, To, C])
    end;
pretty_history_event({deliver_msg, #msg{to = {req, _ReqId}}}) ->
    ok; % The result should output instead
pretty_history_event({deliver_msg, #msg{}} = Msg) ->
    io:format(user, "Msg: ~p\n", [Msg]);
pretty_history_event({result, Cid, {req, Seq, _, {get,PL}}, Result}) ->
    case Result of
        {ok, Obj} ->
            io:format(user, "C~p get~p(~p) -> ~p\n", [Cid, Seq, PL, riak_object:get_values(Obj)]);
        ER ->
            io:format(user, "C~p get~p(~p) -> ~p\n", [Cid, Seq, PL, ER])
    end;
pretty_history_event({result, Cid, {req, Seq, _, {put,PL,Val}}, Result}) ->
    case Result of
        {ok, Obj, _} ->
            io:format(user, "C~p put~p(~p, ~p) -> ~p\n", [Cid, Seq, PL, Val, riak_object:get_values(Obj)]);
        ER ->
            io:format(user, "C~p put~p(~p, ~p) -> ~p\n", [Cid, Seq, PL, Val, ER])
    end;
pretty_history_event({result, Cid, Req, Result}) ->
    io:format(user, "Result: Cid=~p Req=~p Result=~p\n", [Cid, Req, Result]);
pretty_history_event(Event) ->
    io:format(user, "Event: ~p\n", [Event]).

-endif.
