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
          name,   %% term to reference this process in from / to for messages
          procst  %% Process state
          }).
        
-record(msg, {
          pri = 0, %% Priority
          from,    %% Sender
          to,      %% Receipient
          c}).     %% Contents

-record(params, {q}).

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

gen_req() ->
    {ping, {node, nni()}}.

gen_client_seeds() ->
    non_empty(list(#client{reqs = non_empty(list({nni(), gen_req()}))})).

prop() ->
    ?FORALL({ClientSeeds,QSeed},{gen_client_seeds(),choose(0, 10)},
            begin
                Q = 1 + abs(QSeed),
                Params = #params{q = Q},
                NodeProcs = make_nodes(Q),
                Initial = #state{params = Params, 
                                 procs = NodeProcs},
                Clients = make_clients(ClientSeeds, Params),
                Start = Initial#state{clients = Clients},
                case exec(Start) of
                    {ok, Final} ->
                        true;
                    {What, Reason, Next, FailS} ->
                        io:format(user, "FAILED: ~p: ~p\nNext: ~p\nInitial:\n~p\n"
                                  "FailS:\n~p\n",
                                  [What, Reason, Initial, Next, FailS]),
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
              [S#state.step, Next, S]);
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
client_req(Cid, #state{next_rid = ReqId, clients = Clients, pending_msgs = PendingMsgs} = S) ->
    C = get_client(Cid, Clients),
    true = (C#client.pri /= undefined), % paranoia - make sure this was scheduled right
    {NewMsgs, UpdC} = start_req({req, ReqId}, C),
    S#state{next_rid = ReqId + 1,
            clients = update_client(UpdC#client{rid= ReqId, pri = undefined}, Clients),
            pending_msgs = PendingMsgs ++ NewMsgs}.

%% Deliver next pending msg
deliver_msg(#state{pending_msgs = [#msg{to = {req, ReqId}} = Msg | Msgs],
                   clients = Clients,
                   history = H} = S) ->
    [C] = [C || #client{rid = ReqId0}=C <- Clients, ReqId0 == ReqId],
    Result = end_req(Msg, C),
    UpdC = next_req(C),
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
    Proc = get_proc(To, Procs),
    %% Deliver message - as a result, the process could do any/all of
    %% create new processes
    %% stop itself
    %% send a message to another process 
    %% complete a request
    %%            
    Result = deliver(Msg, Proc),
    NewMsgs = proplists:get_value(msgs, Result, []),
    S#state{pending_msgs = Msgs ++ NewMsgs}.

make_nodes(Q) ->
    [#proc{name={node, I}, procst=nostate} || I <- lists:seq(1, Q)].

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
    Req = make_req(ReqSeed, Params),
    make_reqs(ReqSeeds, Params, [Req | Acc]).

make_req({Pri, {ping, {node, NodeSeed}}}, #params{q = Q}) ->
    {Pri, {ping, {node, make_range(NodeSeed, 1, Q)}}}.

%% Move the client on to the next request, setting priority if present
next_req(#client{reqs = [_|Reqs]} = C) ->
    case Reqs of
        [] ->
            C#client{reqs = []};
        [{Pri,_} | _] ->
            C#client{pri = Pri, reqs = Reqs}
    end.
    

%% Deliver a message to a process
deliver(#msg{from = From, c = ping}, #proc{name = {node, _}=Name}) ->
    [{msgs, [#msg{from = Name, to = From, c = pong}]}].
               
    
%% Start the next client request
start_req(Name, #client{reqs = [{_Pri, {ping, NodeName}} | _Reqs]} = C) ->
    NewMsgs = [#msg{from = Name, to = NodeName, c = ping}],
    {NewMsgs, C#client{clntst = {pinged, NodeName}}}.

end_req(#msg{from = Name, c = pong},
        #client{cid = Cid, reqs = [{_Pri, {ping, Name}}|_Reqs]}) ->
    [{history, [{Cid, ponged, Name}]},
     done].

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


%% Make a value between min and max inclusive using the seed
make_range(Seed, Min, Max) when Min =< Seed, Seed =< Max ->
    Seed;
make_range(Seed, Min, Max) ->
    Range = Max - Min + 1,
    Min + (abs(Seed) rem Range).
