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

%% Client requests must be processed in sequence.
%% Client also gets to have it's own state.

-record(client, { %% pri/id must be first two fields for next to work correctly
          pri,    %% Priority for next request (undefined if req active)
          cid,    %% Client id
          rid,    %% request id
          reqs,   %% Requests to execute - {Pri,Req}
          clntst %% Client state
         }). 
                 

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

-record(params, {q,
                 n,
                 r,
                 w}).

-record(state, {verbose = false,
                step = 1, 
                params,
                next_rid = 1,
                clients = [],     %% [#client]
                procs = [],       %% {Name, State}
                pending_msgs = [],
                history = []}).

nni() ->
    ?LET(Xs, int(), abs(Xs)).

gen_params() ->
    #params{n = choose(0, 10),
            r = choose(1, 5),
            w = choose(1, 5)}.
            
gen_req() ->
    elements([get, update]).

gen_client_seeds() ->
    non_empty(list(#client{reqs = non_empty(list({nni(), gen_req()}))})).

prop() ->
    ?FORALL({ClientSeeds,ParamsSeed},{gen_client_seeds(),gen_params()},
            begin
                Params = make_params(ParamsSeed),
                io:format(user, "Params = ~p\n", [Params]),
                %NodeProcs = make_nodes(Q),
                VnodeProcs = make_vnodes(Params),
                Initial = #state{params = Params, 
                                 procs = VnodeProcs},
                Clients = make_clients(ClientSeeds, Params),
                Start = Initial#state{clients = Clients},
                case exec(Start) of
                    {ok, _Final} ->
                        io:format("Params:\n~p\nHistory:\n~p\n",
                                  [_Final#state.params,
                                   _Final#state.history]),
                        true;
                    {What, Reason, Next, FailS} ->
                        io:format(user, "FAILED: ~p: ~p\nNext: ~p\nInitial:\n~p\n"
                                  "FailS:\n~p\n",
                                  [What, Reason, Next, Initial, FailS]),
                        false
                end
            end).

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
                exec(inc_step(client_req(Cid, S)))
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

next(#state{clients = Clients, pending_msgs = PendingMsgs}) ->
    %% Work out which client ids are active
    {ClientPri, Cid} = case lists:sort([C || C <- Clients, C#client.pri /= undefined]) of
                           [] ->
                               %% No clients ready
                               {undefined, undefined};
                           RCs -> % ready clients
                               C = hd(RCs),
                               {C#client.pri, C#client.cid}
                       end,
    MsgPri = case PendingMsgs of
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
                    stalled
            end;
        ClientPri == undefined orelse MsgPri =< ClientPri ->
            deliver_msg;
        
        true ->
            {client, Cid}
    end.

%% Deliver next client request for Cid
client_req(Cid, #state{next_rid = ReqId, clients = Clients, procs = Procs,
                       pending_msgs = PendingMsgs} = S) ->
    C = get_client(Cid, Clients),
    true = (C#client.pri /= undefined), % paranoia - make sure this was scheduled right
    Result = start_req({req, ReqId}, C),
    NewProcs = proplists:get_value(procs, Result, []),
    NewMsgs = proplists:get_value(msgs, Result, []),
    UpdC = proplists:get_value(updc, Result, C),
    S#state{next_rid = ReqId + 1,
            clients = update_client(UpdC#client{rid = ReqId, pri = undefined}, Clients),
            pending_msgs = PendingMsgs ++ NewMsgs,
            procs = Procs ++ NewProcs}.

%% Deliver next pending msg
deliver_msg(#state{pending_msgs = [#msg{to = {req, ReqId}} = Msg | Msgs],
                   clients = Clients,
                   history = H} = S) ->
    [C] = [C || #client{rid = ReqId0}=C <- Clients, ReqId0 == ReqId],
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
            pending_msgs = Msgs,
            history = H ++ NewH};

deliver_msg(#state{pending_msgs = [#msg{to = To} = Msg | Msgs],
                   procs = Procs} = S) ->
    P = get_proc(To, Procs),
    Handler = P#proc.handler,
    Result = ?MODULE:Handler(Msg, P),
    NewMsgs = proplists:get_value(msgs, Result, []),
    UpdP = proplists:get_value(updp, Result, P),
    S#state{pending_msgs = Msgs ++ NewMsgs,
            procs = update_proc(UpdP, Procs)}.

make_params(#params{n = NSeed, r = R, w = W} = P) ->
    MaxN = lists:max([R, W, R+W-1]),
    P#params{n = make_range(R + W - NSeed, 1, MaxN)}. % Ensure R+W>N

%% make_nodes(Q) ->
%%     [#proc{name={node, I}, procst=nostate} || I <- lists:seq(1, Q)].

make_vnodes(#params{n = N}) ->
    [#proc{name={kv_vnode, I, I}, handler=kv_vnode, procst=[]} || I <- lists:seq(1, N)].


make_clients(ClientSeeds, Params) ->
    make_clients(ClientSeeds, 1, Params, []).

make_clients([], _Cid, _Params, Acc) ->
    lists:reverse(Acc);
make_clients([#client{reqs = ReqSeeds} = CS | CSs], Cid, Params, Acc) ->
    Reqs = make_reqs(ReqSeeds, Params),
    [{Pri, _}|_] = Reqs,
    C = CS#client{cid = Cid, pri = Pri, reqs = Reqs},
    make_clients(CSs, Cid + 1, Params, [C | Acc]).
    
make_reqs(ReqSeeds, Params) ->
    make_reqs(ReqSeeds, Params, []).

make_reqs([], _Params, Acc) ->
    lists:reverse(Acc);
make_reqs([ReqSeed | ReqSeeds], Params, Acc) ->
    Reqs = make_req(ReqSeed, Params),
    make_reqs(ReqSeeds, Params, lists:reverse(Reqs) ++ Acc).


%% Move the client on to the next request, setting priority if present
next_req(#client{reqs = [_|Reqs]} = C) ->
    case Reqs of
        [] ->
            C#client{reqs = []};
        [{Pri,_} | _] ->
            C#client{pri = Pri, reqs = Reqs}
    end.

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
    

%% Make a value between min and max inclusive using the seed
make_range(Seed, Min, Max) when Min =< Seed, Seed =< Max ->
    Seed;
make_range(Seed, Min, Max) ->
    Range = Max - Min + 1,
    Min + (abs(Seed) rem Range).


%% Create a request for a client
make_req({Pri, {ping, {node, NodeSeed}}}, #params{q = Q}) ->
    [{Pri, {ping, {node, make_range(NodeSeed, 1, Q)}}}];

make_req({Pri, get}, #params{n = N}) ->
    [{Pri, {get, [{kv_vnode, I, I} || I <- lists:seq(1, N)]}}];
make_req({Pri, update}, #params{n = N}) ->
    %% For an update, issue a get then a put.
    %% TODO: Find a way to make the update priority different from the get
    [{Pri, {get, [{kv_vnode, I, I} || I <- lists:seq(1, N)]}},
     {Pri, {put, [{kv_vnode, I, I} || I <- lists:seq(1, N)]}}].



%% Start the next client request
%% start_req(Name, #client{reqs = [{_Pri, {ping, NodeName}} | _Reqs]} = C) ->
%%     NewMsgs = [#msg{from = Name, to = NodeName, c = ping}],
%%     {NewMsgs, C#client{clntst = {pinged, NodeName}}}.

start_req({req, ReqId} = Name, #client{reqs = [{_Pri, {get, PL}} | _Reqs]} = C) ->
    Proc = get_fsm_proc(ReqId),
    NewProcs = [Proc],
    NewMsgs = [#msg{from = Name, to = Proc#proc.name, c = {get, PL}}],
    UpdC = C#client{clntst = {get, ReqId}},
    [{procs, NewProcs},
     {msgs, NewMsgs},
     {updc, UpdC}];
start_req({req, ReqId} = Name, #client{reqs = [{_Pri, {put, PL}} | _Reqs],
                                       cid = Cid,
                                       clntst = {{get, _PL}, GetResult}} = C) ->
    Proc = put_fsm_proc(ReqId),
    NewProcs = [Proc],
    UpdObj = case GetResult of
                 {error, notfound} ->
                     {obj, Cid, ReqId};
                 {ok, _Obj} ->
                     {obj, Cid, ReqId} %%  Will be based on Obj
             end,
    NewMsgs = [#msg{from = Name, to = Proc#proc.name, c = {put, PL, UpdObj}}],
    UpdC = C#client{clntst = {get, ReqId}},
    [{procs, NewProcs},
     {msgs, NewMsgs},
     {updc, UpdC}].
 
%% end_req(#msg{from = Name, c = pong},
%%         #client{cid = Cid, reqs = [{_Pri, {ping, Name}}|_Reqs]}) ->
%%     [{history, [{Cid, ponged, Name}]},
%%      done].
end_req(#msg{c = R},
        #client{cid = Cid, reqs = [{_Pri, Req}|_Reqs]} = C) ->
    [{history, [{Cid, Req, R}]},
     {updc, C#client{clntst = {Req, R}}},
     done].


%% Deliver a message to a process
%% node_proc(#msg{from = From, c = ping}, #proc{name = {node, _}=Name}) ->
%%     [{msgs, [#msg{from = Name, to = From, c = pong}]}].
               

kv_vnode(#msg{from = From, c = get}, #proc{name = Name}) ->
    [{msgs, [#msg{from = Name, to = From, c = {error, notfound}}]}];
kv_vnode(#msg{from = From, c = put}, #proc{name = Name}) ->
    [{msgs, [#msg{from = Name, to = From, c = {error, fail}}]}].

-record(getfsmst, {reply_to,
                   expect, %% Number of replies expected
                   replies}).

get_fsm_proc(ReqId) ->
    #proc{name = {get_fsm, ReqId}, handler = get_fsm, procst = #getfsmst{}}.

get_fsm(#msg{from = From, c = {get, PL}},
        #proc{name = Name, procst = ProcSt} = P) ->
    %% Kick off requests to the vnodes
    [{msgs, [#msg{from = Name, to = Vnode, c = get} || Vnode <- PL]},
     {updp, P#proc{procst = ProcSt#getfsmst{reply_to = From, expect = length(PL)}}}];
get_fsm(#msg{from = {kv_vnode, _, _}},
        #proc{name = Name, procst = #getfsmst{expect = 1, reply_to = ReplyTo}}) ->
    %% Final expected response from vnodes
    %% TODO: Find a way to mark the process as stopped, can just build up for now.
    [{msgs, [#msg{from = Name, to = ReplyTo, c = {error, notfound}}]}];
get_fsm(#msg{from = {kv_vnode, _, _}},
        #proc{procst = #getfsmst{expect = Expect} = ProcSt} = P) ->
    %% Response from vnode
    [{updp, P#proc{procst = ProcSt#getfsmst{expect = Expect - 1}}}].
    
-record(putfsmst, {reply_to,
                   expect, %% Number of replies expected
                   replies}).

put_fsm_proc(ReqId) ->
    #proc{name = {put_fsm, ReqId}, handler = put_fsm, procst = #putfsmst{}}.

put_fsm(#msg{from = From, c = {put, PL, _Obj}},
        #proc{name = Name, procst = ProcSt} = P) ->
    %% Kick off requests to the vnodes
    [{msgs, [#msg{from = Name, to = Vnode, c = put} || Vnode <- PL]},
     {updp, P#proc{procst = ProcSt#putfsmst{reply_to = From, expect = length(PL)}}}];
put_fsm(#msg{from = {kv_vnode, _, _}},
        #proc{name = Name, procst = #putfsmst{expect = 1, reply_to = ReplyTo}}) ->
    %% Final expected response from vnodes
    %% TODO: Find a way to mark the process as stopped, can just build up for now.
    [{msgs, [#msg{from = Name, to = ReplyTo, c = {error, notfound}}]}];
put_fsm(#msg{from = {kv_vnode, _, _}},
        #proc{procst = #putfsmst{expect = Expect} = ProcSt} = P) ->
    %% Response from vnode
    [{updp, P#proc{procst = ProcSt#putfsmst{expect = Expect - 1}}}].
    
  
