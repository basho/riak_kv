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
                msgs = [],
                history = []}).

nni() ->
    ?LET(Xs, int(), abs(Xs)).

gen_pris() ->
    non_empty(resize(40, list(nni()))).

gen_params() ->
    #params{n = choose(0, 10),
            r = choose(1, 5),
            w = choose(1, 5)}.
            
gen_req() ->
    elements([get, update]).

gen_client_seeds() ->
    non_empty(list(#client{reqs = non_empty(list(gen_req()))})).

prop() ->
    ?FORALL({Pris, ClientSeeds, ParamsSeed},
            {gen_pris(), gen_client_seeds(),gen_params()},
            begin
                io:format(user, "Pris: ~p\n", [Pris]),
                set_pri(Pris),
                Params = make_params(ParamsSeed),
                %NodeProcs = make_nodes(Q),
                VnodeProcs = make_vnodes(Params),
                Initial = #state{params = Params, 
                                 procs = VnodeProcs},
                Clients = make_clients(ClientSeeds, Params),
                Start = Initial#state{clients = Clients},
                case exec(Start) of
                    {ok, _Final} ->
                        io:format("Params:\n~p\nClients:\n~p\nHistory:\n~p\n",
                                  [Params,
                                   Clients,
                                   _Final#state.history]),
                        true;
                    {What, Reason, Next, FailS} ->
                        io:format(user, "FAILED: ~p: ~p\nNext: ~p\nStart:\n~p\n"
                                  "FailS:\n~p\n",
                                  [What, Reason, Next, ann_state(Start), ann_state(FailS)]),
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
client_req(Cid, #state{next_rid = ReqId, clients = Clients, procs = Procs,
                       msgs = Msgs} = S) ->
    C = get_client(Cid, Clients),
    Result = start_req(ReqId, C),
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
    MinN = lists:max([R, W]),
    P#params{n = make_range(R + W - NSeed, MinN, R+W-1)}. % Ensure R >= N, W >= N and R+W>N

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
    C = CS#client{cid = Cid, reqs = Reqs},
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
    

%% Make a value between min and max inclusive using the seed
make_range(Seed, Min, Max) when Min =< Seed, Seed =< Max ->
    Seed;
make_range(Seed, Min, Max) ->
    Range = Max - Min + 1,
    Min + (abs(Seed) rem Range).


%% Create a request for a client
make_req(get, #params{n = N}) ->
    [new_req({get, [{kv_vnode, I, I} || I <- lists:seq(1, N)]})];
make_req(update, #params{n = N}) ->
    %% For an update, issue a get then a put.
    %% TODO: Find a way to make the update priority different from the get
    [new_req({get, [{kv_vnode, I, I} || I <- lists:seq(1, N)]}),
     new_req({put, [{kv_vnode, I, I} || I <- lists:seq(1, N)]})].

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
start_req(ReqId, #client{reqs = [Req | Reqs]} = C) ->
    UpdReq = Req#req{rid = ReqId},
    UpdC = C#client{reqs = [UpdReq | Reqs]},
    Result = client_req(UpdC),
    %% If the result does not include an updated client record
    %% use the one with the reqs updated.  Any original
    %% updc entry will be never be retrieved.
    [{updc, proplists:get_value(updc, Result, UpdC)} | Result].
    

%% Start client requests - return a proplist of 
%% [{procs, NewProcs},
%%  {msgs, NewMsgs},
%%  {updc, UpdC}];
client_req(#client{reqs = [#req{op = {get, PL}, rid = ReqId} | _]}) ->
    Proc = get_fsm_proc(ReqId),
    NewProcs = [Proc],
    NewMsgs = [new_msg({req, ReqId}, Proc#proc.name, {get, PL})],
    [{procs, NewProcs},
     {msgs, NewMsgs}];
client_req(#client{reqs = [#req{op = {put, PL}, rid = ReqId} | _],
                   cid = Cid,  clntst = {#req{op={get, _PL}}, GetResult}}) ->
    Proc = put_fsm_proc(ReqId),
    NewProcs = [Proc],
    UpdObj = case GetResult of
                 {error, notfound} ->
                     {obj, Cid, ReqId};
                 {ok, _Obj} ->
                     {obj, Cid, ReqId} %%  Will be based on Obj
             end,
    NewMsgs = [new_msg({req, ReqId}, Proc#proc.name, {put, PL, UpdObj})],
    [{procs, NewProcs},
     {msgs, NewMsgs}].

end_req(#msg{c = Result},
        #client{cid = Cid, reqs = [Req |_Reqs]} = C) ->
    [{history, [{Cid, Req, Result}]},
     {updc, C#client{clntst = {Req, Result}}},
     done].


%% Deliver a message to a process
%% node_proc(#msg{from = From, c = ping}, #proc{name = {node, _}=Name}) ->
%%     [{msgs, [#msg{from = Name, to = From, c = pong}]}].
               

kv_vnode(#msg{from = From, c = get}, #proc{name = Name}) ->
    [{msgs, [new_msg(Name, From, {error, notfound})]}];
kv_vnode(#msg{from = From, c = put}, #proc{name = Name}) ->
    [{msgs, [new_msg(Name, From, {error, fail})]}].

-record(getfsmst, {reply_to,
                   expect, %% Number of replies expected
                   replies}).

get_fsm_proc(ReqId) ->
    #proc{name = {get_fsm, ReqId}, handler = get_fsm, procst = #getfsmst{}}.

get_fsm(#msg{from = From, c = {get, PL}},
        #proc{name = Name, procst = ProcSt} = P) ->
    %% Kick off requests to the vnodes
    [{msgs, [new_msg(Name, Vnode, get) || Vnode <- PL]},
     {updp, P#proc{procst = ProcSt#getfsmst{reply_to = From, expect = length(PL)}}}];
get_fsm(#msg{from = {kv_vnode, _, _}},
        #proc{name = Name, procst = #getfsmst{expect = 1, reply_to = ReplyTo}}) ->
    %% Final expected response from vnodes
    %% TODO: Find a way to mark the process as stopped, can just build up for now.
    [{msgs, [new_msg(Name, ReplyTo, {error, notfound})]}];
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
    [{msgs, [new_msg(Name, Vnode, put) || Vnode <- PL]},
     {updp, P#proc{procst = ProcSt#putfsmst{reply_to = From, expect = length(PL)}}}];
put_fsm(#msg{from = {kv_vnode, _, _}},
        #proc{name = Name, procst = #putfsmst{expect = 1, reply_to = ReplyTo}}) ->
    %% Final expected response from vnodes
    %% TODO: Find a way to mark the process as stopped, can just build up for now.
    [{msgs, [new_msg(Name, ReplyTo, {error, notfound})]}];
put_fsm(#msg{from = {kv_vnode, _, _}},
        #proc{procst = #putfsmst{expect = Expect} = ProcSt} = P) ->
    %% Response from vnode
    [{updp, P#proc{procst = ProcSt#putfsmst{expect = Expect - 1}}}].
    
  
