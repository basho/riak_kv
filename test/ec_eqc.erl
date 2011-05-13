-module(ec_eqc).
-include_lib("eqc/include/eqc.hrl").
-compile([export_all]).

%% FSM scheduler EQC test.

test() ->
    test(100).

test(N) ->
    quickcheck(numtests(N, prop())).

check() ->
    check(prop(), current_counterexample()).

-define(B, <<"b">>).
-define(K, <<"k">>).

-define(FINALDBG(Fmt,Args), io:format("XXX" ++ Fmt, Args)).
%-define(FINALDBG(_Fmt,_Args), ok).

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

nni() ->
    ?LET(Xs, int(), abs(Xs)).

gen_pris() ->
    non_empty(resize(40, list(nni()))).

gen_pl_seed() -> %% Seed for preference lists
    non_empty(list(int())).
    
gen_params() ->
    #params{n = choose(0, 10),
            r = choose(1, 5),
            w = choose(1, 5)}.
            
gen_req() ->
    elements([{get, gen_pl_seed()},
              {update, gen_pl_seed(), gen_pl_seed()}]).

gen_client_seeds() ->
    non_empty(list(#client{reqs = non_empty(list(gen_req()))})).

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
                case exec(Start) of
                    {ok, Final} ->
                        ?WHENFAIL(
                           begin
                               io:format("Params:\n~p\nClients:\n~p\nFinal:\n~p\nHistory:\n~p\n",
                                         [ann_params(Params),
                                          Clients,
                                          Final,
                                          Final#state.history])
                           end,
                           check_final(Final));
                    {What, Reason, Next, FailS} ->
                        io:format(user, "FAILED: ~p: ~p\nNext: ~p\nStart:\n~p\n"
                                  "FailS:\n~p\n",
                                  [What, Reason, Next, ann_state(Start), ann_state(FailS)]),
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

check_final([{result, _, #req{op = {get, _PL}}, Result}], Must, _May, _ClientView) ->
    Values = case Result of
                 {error, notfound} ->
                     [];
                 {ok, Obj} ->
                     riak_object:get_values(Obj)
             end,
    ?FINALDBG("Final GOT VALUES: ~p Must: ~p May: ~p\n", [Values, Must, _May]),
    ?WHENFAIL(io:format(user, "Must: ~p\nMay: ~p\nValues: ~p\n", [Must, _May, Values]),
              equals(Must -- Values, []));
              %% conjunction([{must_leftover, equals(Must -- Values, [])},
              %%              {must_may_leftover, equals(Values -- (Must ++ May), [])}]))

check_final([{result, Cid, #req{op = {get, _PL}}, Result} | Results],
            Must, May, ClientViews) ->
    %% TODO: Check if _Result matches expected values
    ValuesAtGet = case Result of
                      {ok, Obj} ->
                          riak_object:get_values(Obj);
                      {error, _} ->
                          []
                  end,
    ?FINALDBG("Cid ~p GOT VALUES: ~p\n", [Cid, ValuesAtGet]),
    UpdClientViews = lists:keystore(Cid, 1, ClientViews, {Cid, ValuesAtGet}),
    check_final(Results, Must, May, UpdClientViews);
check_final([{result, Cid, #req{rid = ReqId, op = {put, _PL}}, Result} | Results],
            Must, May, ClientViews) ->
    V = <<ReqId:32>>,
    {Cid, ValuesAtGet} = lists:keyfind(Cid, 1, ClientViews),
    ValNotInMay = not lists:member(V, May),
    UpdMust = case Result of
                  ok when ValNotInMay ->
                      %% This put could have already been overwritten
                      %% Only add to must if not in may
                      [V | lists:usort(Must -- ValuesAtGet)];
                  _  ->
                      %% This value has already been overwritten
                                %% so the values that it overwrote should also be removed 
                      %% from must.
                      Must -- ValuesAtGet
              end,
    UpdMay = lists:usort(May ++ ValuesAtGet),
    ?FINALDBG("Cid ~p PUT ~p OVER VALUES: ~p  MUST: ~p MAY: ~p\n",
             [Cid, V, ValuesAtGet, UpdMust, UpdMay]),
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
            {What, Reason, Next, S}
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
    P#params{n = N, m = N, dw = W}. 

make_vnodes(#params{n = N, m = M}) ->
    [#proc{name={kv_vnode, I, J}, handler=kv_vnode, procst=undefined} || I <- lists:seq(1, N),
                                                                         J <- lists:seq(1, M)].

make_clients(ClientSeeds, Params) ->
    make_clients(ClientSeeds, 1, Params, []).

make_clients([], _Cid, #params{n = N}, Acc) ->
    %% Make sure the last request is a get.
    HandoffReq = new_req(handoff_fallbacks),
    GetReq = new_req({get, [{kv_vnode, I, I} || I <- lists:seq(1, N)]}),
    LastC = #client{cid = 1000000, reqs = [HandoffReq#req{pri = 1000001},
                                           GetReq#req{pri = 1000002}]},
    lists:reverse([LastC | Acc]);
make_clients([#client{reqs = ReqSeeds} = CS | CSs], Cid, Params, Acc) ->
    Reqs = make_reqs(ReqSeeds, Params),
    C = CS#client{cid = Cid, reqs = Reqs},
    make_clients(CSs, Cid + 1, Params, [C | Acc]).
    
make_reqs(ReqSeeds, Params) ->
    make_reqs(ReqSeeds, Params, []).

make_reqs([], _Params, Acc) ->
    lists:reverse(Acc);
make_reqs([ReqSeed | ReqSeeds], Params, Acc) ->
    Reqs = make_req(ReqSeed, Params),
    make_reqs(ReqSeeds, Params, lists:reverse(Reqs) ++ Acc).

%% Full EC test make_pl - create a pref list that shrinks to primaries as
%% seed shrinks to [0]
%% make_pl(N, M, PLSeed) ->
%%     make_pl(N, M, PLSeed, []).

%% make_pl(0, _M, _PLSeed, PL) ->
%%     PL;
%% make_pl(Idx, M, [PLSeedH|PLSeedT], PL) ->
%%     Node = (abs(Idx - 1 + PLSeedH) rem M) + 1, % Node between 1 and M
%%     make_pl(Idx - 1, M, PLSeedT ++ [PLSeedH], [{kv_vnode, Idx, Node} | PL]).


%% Use PLSeed to change one entry at random when N > 1
make_pl(N, M, PLSeed) ->
    PerfectPL = [{kv_vnode, Idx, Idx} || Idx <- lists:seq(1, N)],
    case hd(PLSeed) of
        0 ->
            PerfectPL;
        _IdxSeed when N == 1 ->
            PerfectPL;
        IdxSeed ->
            Idx = (abs(IdxSeed) rem N) + 1,
            PLSeedH = hd(tl(PLSeed) ++ [IdxSeed]),
            Node = (abs(Idx - 1 + PLSeedH) rem M) + 1,
            lists:keyreplace(Idx, 2, PerfectPL, {kv_vnode, Idx, Node})
    end.
        
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
make_req({get, PLSeed}, #params{n = N, m = M}) ->
    [new_req({get, make_pl(N, M, PLSeed)})];
make_req({update, PLSeed1, PLSeed2}, #params{n = N, m = M}) ->
    %% For an update, issue a get then a put.
    %% TODO: Find a way to make the update priority different from the get
    [new_req({get, make_pl(N, M, PLSeed1)}),
     new_req({put, make_pl(N, M, PLSeed2)})];
make_req(handoff_fallbacks, _Params) ->
    [new_req(handoff_fallbacks)].

new_req(Op) ->
    #req{pri = next_pri(), op = Op}.

new_msg(From, To, Contents) ->
    #msg{pri = next_pri(), from = From, to = To, c = Contents}.

add_msgs(NewMsgs, Msgs, S) ->
    S#state{msgs = lists:merge(Msgs, lists:sort(NewMsgs))}.

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
client_req(#client{reqs = [#req{op = {put, PL}, rid = ReqId} | _],
                   cid = Cid,  clntst = {#req{op={get, _PL}}, GetResult}},
           Params) ->
    Proc = put_fsm_proc(ReqId, Params),
    NewProcs = [Proc],
    UpdV = <<ReqId:32>>,
    UpdObj = case GetResult of
                 {error, notfound} ->
                     riak_object:new(?B, ?K, UpdV);
                 {ok, Obj} ->
                     riak_object:apply_updates(riak_object:update_value(Obj, UpdV))
             end,
    UpdObj2 = riak_object:increment_vclock(UpdObj, <<Cid:32>>),
    NewMsgs = [new_msg({req, ReqId}, Proc#proc.name, {put, PL, UpdObj2})],
    [{procs, NewProcs},
     {msgs, NewMsgs}];
client_req(#client{reqs = [#req{op = handoff_fallbacks, rid = ReqId} | _]},
           #params{n = N, m = M}) ->
    %% Send handoff_to messages to each fallback vnode
    HandoffMsgs = [new_msg(undefined, {kv_vnode, I, J}, {handoff_to, {kv_vnode, I, I}}) || 
                      I <- lists:seq(1, N),
                      J <- lists:seq(1, M),
                      I /= J],
    ResponseMsg = new_msg(undefined, {req, ReqId}, handoffs_scheduled),
    [{msgs, HandoffMsgs ++ [ResponseMsg]}].

end_req(#msg{c = Result},
        #client{cid = Cid, reqs = [Req |_Reqs]} = C) ->
    [{history, [{result, Cid, Req, Result}]},
     {updc, C#client{clntst = {Req, Result}}},
     done].


%% Deliver a message to a process
%% node_proc(#msg{from = From, c = ping}, #proc{name = {node, _}=Name}) ->
%%     [{msgs, [#msg{from = Name, to = From, c = pong}]}].
               
-record(getfsmst, {reply_to,
                   responded = false,
                   getcore}).

get_fsm_proc(ReqId, #params{n = N, r = R}) ->
    FailThreshold = (N div 2) + 1,
    NotFoundOk = true,
    AllowMult = true,
    GetCore = riak_kv_get_core:init(N, R, FailThreshold, NotFoundOk, AllowMult),
    #proc{name = {get_fsm, ReqId}, handler = get_fsm,
          procst = #getfsmst{getcore = GetCore}}.

get_fsm(#msg{from = From, c = {get, PL}},
        #proc{name = {get_fsm, ReqId} = Name, procst = ProcSt} = P) ->
    %% Kick off requests to the vnodes
    [{msgs, [new_msg(Name, Vnode, {get, ReqId}) || Vnode <- PL]},
     {updp, P#proc{procst = ProcSt#getfsmst{reply_to = From}}}];
get_fsm(#msg{from = {kv_vnode, Idx, _}, c = {r, Result, Idx, _ReqId}},
        #proc{name = Name, procst = #getfsmst{reply_to = ReplyTo,
                                              responded = Responded,
                                              getcore = GetCore} = ProcSt} = P) ->
    UpdGetCore = riak_kv_get_core:add_result(Idx, Result, GetCore),
    case riak_kv_get_core:enough(UpdGetCore) of
        true when Responded == false ->
            %% Worry about read repairs later
            {Response, UpdGetCore2} = riak_kv_get_core:response(UpdGetCore),
            [{msgs, [new_msg(Name, ReplyTo, Response)]},
             {updp, P#proc{procst = ProcSt#getfsmst{responded = true, 
                                                    getcore = UpdGetCore2}}}];
        _ ->
            %% Replied already or more to come, worry about timeout later.
            [{updp, P#proc{procst = ProcSt#getfsmst{getcore = UpdGetCore}}}]
    end.

-record(putfsmst, {reply_to,
                   responded = false,
                   putcore}).
put_fsm_proc(ReqId, #params{n = N, w = W, dw = DW}) ->
    AllowMult = true,
    ReturnBody = false,
    PutCore = riak_kv_put_core:init(N, W, DW, 
                                    N-W+1,   % cannot ever get W replies
                                    N-DW+1,  % cannot ever get DW replies
                                    AllowMult,
                                    ReturnBody),
    #proc{name = {put_fsm, ReqId}, handler = put_fsm, procst = #putfsmst{putcore = PutCore}}.

put_fsm(#msg{from = From, c = {put, PL, Obj}},
        #proc{name = {put_fsm, ReqId}= Name, procst = ProcSt} = P) ->
    Ts = ReqId, % re-use ReqId for a timestamp to make them unique
    %% Kick off requests to the vnodes
    [{msgs, [new_msg(Name, Vnode, {put, Obj, ReqId, Ts}) || Vnode <- PL]},
     {updp, P#proc{procst = ProcSt#putfsmst{reply_to = From}}}];
put_fsm(#msg{from = {kv_vnode, _, _}, c = Result},
        #proc{name = Name, procst = #putfsmst{reply_to = ReplyTo,
                                              responded = Responded,
                                              putcore = PutCore} = ProcSt} = P) ->
    UpdPutCore = riak_kv_put_core:add_result(Result, PutCore),
    case riak_kv_put_core:enough(UpdPutCore) of
        true when Responded == false ->
            %% Worry about read repairs later
            {Response, UpdPutCore2} = riak_kv_put_core:response(UpdPutCore),
            [{msgs, [new_msg(Name, ReplyTo, Response)]},
             {updp, P#proc{procst = ProcSt#putfsmst{responded = true,
                                                    putcore = UpdPutCore2}}}];
        _ ->
            %% Already responded or more to come, worry about timeout later.
            [{updp, P#proc{procst = ProcSt#putfsmst{putcore = UpdPutCore}}}]
    end.

kv_vnode(#msg{from = From, c = {get, ReqId}},
         #proc{name = {kv_vnode, Idx, _Node} = Name, procst = CurObj}) ->
    Result = case CurObj of
                 undefined ->
                     {error, notfound};
                 _ ->
                     {ok, CurObj}
             end,
    [{msgs, [new_msg(Name, From, {r, Result, Idx, ReqId})]}];
kv_vnode(#msg{from = From, c = {put, NewObj, ReqId, Ts}},
         #proc{name = {kv_vnode, Idx, _Node} = Name, procst = CurObj} = P) ->
    UpdObj = syntactic_put_merge(CurObj, NewObj, ReqId, Ts),
    WMsg = new_msg(Name, From, {w, Idx, ReqId}),
    DWMsg = new_msg(Name, From, {dw, Idx, ReqId}), % ignore returnbody for now
    [{msgs, [WMsg, DWMsg]},
     {updp, P#proc{procst = UpdObj}}];
kv_vnode(#msg{from = From, c = {handoff_to, To}},
         #proc{procst = CurObj}) ->
    %% Send current object to node if there is one
    %% TODO: Schedule message to make this vnode reset itself if unchanged
    NewMsgs = case CurObj of
                  undefined ->
                      [];
                  _ ->
                      [new_msg(From, To, {handoff_obj, CurObj})]
              end,
    [{msgs, NewMsgs}];
kv_vnode(#msg{c = {handoff_obj, HandoffObj}},
         #proc{procst = CurObj} = P) ->
    %% Simulate a do_diffobj_put
    ReqId = erlang:phash2(erlang:now()),
    Ts = vclock:timestamp(),
    UpdObj = syntactic_put_merge(CurObj, HandoffObj, ReqId, Ts),
    %% Real syntactic put merge checks allow_mult here and applies, testing with 
    %% allow_mult is true so leave object
    [{updp, P#proc{procst = UpdObj}}].


syntactic_put_merge(CurObj, UpdObj, ReqId, Ts) ->
    case CurObj of
        undefined ->
            UpdObj;
        _ ->
            ResObj = riak_object:syntactic_merge(CurObj, UpdObj, ReqId, Ts),
            case riak_object:vclock(ResObj) =:= riak_object:vclock(CurObj) of
                true -> CurObj; %% {oldobj, ResObj};
                false -> ResObj %% {newobj, ResObj}
            end
    end.
