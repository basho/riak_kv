
%% Set up mock server like get FSM?  
%% Re-use preferencelist / ring set up code
%% Initial data (per-vnode)
%%   - notfound
%%   - ancestor
%%   - sibling
%%   - descendant

%% Hooks
%%   - precommit/postcommit
%%   - crash
%%   - java / erlang?

%% Set responses for vnodes
%%   none
%%   just w
%%   w, then dw

%% Options:
%%   update_last_modified
%%   returnbody

%% Things to check
%%   - last modified time
%%   - optionally return object

-module(put_fsm_eqc).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_kv_vnode.hrl").

-compile(export_all).
-define(RING_KEY, riak_ring).
-define(DEFAULT_BUCKET_PROPS,
        [{allow_mult, false},
         {chash_keyfun, {riak_core_util, chash_std_keyfun}}]).

eqc_test_() ->
    {spawn,
    {timeout, 20, ?_test(
        begin
            fsm_eqc_util:start_mock_servers(),
            ?assert(test(30))
        end)
    }}.

test() ->
    test(100).

test(N) ->
    quickcheck(numtests(N, prop_basic_put())).

check() ->
    check(prop_basic_put(), current_counterexample()).

prop_basic_put() ->
    %% ?FORALL({WSeed,DWSeed},
    ?FORALL({WSeed,DWSeed,NQdiff,
             Objects,ReqId,PartVals,NodeStatus0}, 
            {fsm_eqc_util:largenat(),fsm_eqc_util:largenat(),choose(0,4096),
             fsm_eqc_util:riak_objects(), noshrink(largeint()),
             fsm_eqc_util:partvals(),fsm_eqc_util:some_up_node_status(10)},
    begin
        N = length(PartVals),
        W = (WSeed rem N) + 1,
        DW = (DWSeed rem W) + 1,
        Options = [],

        Q = fsm_eqc_util:make_power_of_two(N + NQdiff),
        NodeStatus = fsm_eqc_util:cycle(Q, NodeStatus0),

        Ring = fsm_eqc_util:reassign_nodes(NodeStatus,
                                           riak_core_ring:fresh(Q, node())),

        ok = gen_server:call(riak_kv_vnode_master,
                             {set_data, Objects, PartVals}),

        mochiglobal:put(?RING_KEY, Ring),

        application:set_env(riak_core,
                            default_bucket_props,
                            [{n_val, N}
                             |?DEFAULT_BUCKET_PROPS]),

        [{_,Object}|_] = Objects,

        {ok, PutPid} = riak_kv_put_fsm:start(ReqId,
                                             Object,
                                             W,
                                             DW,
                                             200,
                                             self(),
                                             Options),
        ok = riak_kv_test_util:wait_for_pid(PutPid),
        Res = fsm_eqc_util:wait_for_req_id(ReqId),

        %% Expected = expect(N, W, DW, NodeStatus),
        %% Res = ok,
        Expected = expect(PartVals, NodeStatus, N, W, DW),
        IsExpected = (Res == Expected),
        ?WHENFAIL(
           begin
               io:format(user, "Q: ~p N: ~p W:~p DW: ~p\n", [Q, N, W, DW]),
               io:format(user, "NodeStatus: ~p\n", [NodeStatus]),
               io:format(user, "Expected: ~p Res: ~p\n", [Expected, Res])
           end,
           conjunction([{result,IsExpected}]))
           %% conjunction(
           %%   [{result, Res == Expected}]))

      %       [{result, equals(Res, Expected)}]))

    end).

%% %% Calculate an expected value - have to do recursively so 
%% %% you can decide on failure cases for N=2, if 
%% expect(PartVals, N, W, DW) ->
%%     expect(PartVals, N, W, DW, 0, 0, 0).

%% expect([], _N, _W, _DW, _NumW, _NumDW, _NumFail) ->
%%     {error, timeout};
%% expect([{ok,_}|Rest], N, W, DW, NumW, NumDW, NumFail) ->
%%     case NumW >= W andalso NumDW >= DW of
%%         true ->
%%             ok;
%%         _ ->
%%             expect(Rest, N, W, DW, NumW + 1, NumDW + 1, NumFail)
%%     end;
%% expect([notfound|Rest], N, W, DW, NumW, NumDW, NumFail) ->
%%     case NumW >= W andalso NumDW >= DW of
%%         true ->
%%             ok;
%%         _ ->
%%             expect(Rest, N, W, DW, NumW + 1, NumDW + 1, NumFail)
%%     end;
%% expect([error|Rest], N, W, DW, NumW, NumDW, NumFail) ->
%%     case NumFail > N - max(DW, W) of
%%         true ->
%%             ok;
%%         _ ->
%%             expect(Rest, N, W, DW, NumW + 1, NumDW, NumFail + 1)
%%     end;
%% expect([timeout|Rest], N, W, DW, NumW, NumDW, NumFail) ->
%%     expect(Rest, N, W, DW, NumW, NumDW, NumFail).


expect([], _NoteStatus, _N, _W, _DW) ->
    {error, timeout};
expect(PartVals, NodeStatus, N, W, DW) ->
    %% Count non-timeout/errors, check > W and DW
    MaxFails = N - max(DW, W),

    {WReply, DWReply, FailReply} = Counters = 
        lists:foldl(fun({_PartVal, down}, Counters0) ->
                            Counters0;
                       ({PartVal, _Status}, {WReply0, DWReply0, FailReply0}) -> 
                            case PartVal of
                                {ok, _} ->
                                    {WReply0+1, DWReply0+1, FailReply0};
                                notfound ->
                                    {WReply0+1, DWReply0+1, FailReply0};
                                error ->
                                    {WReply0+1, DWReply0, FailReply0+1};
                                timeout ->
                                    {WReply0, DWReply0, FailReply0}
                                        
                            end
                    end, {0, 0, 0}, lists:zip(lists:sublist(PartVals, N),
                                              lists:sublist(NodeStatus, N))),
    io:format(user, "Counters=~p\n", [Counters]),
    if
        WReply >= W andalso DWReply >= DW ->
            ok;
        FailReply > MaxFails ->
            {error, too_many_fails};
        true ->
            {error, timeout}
    end.
    
-endif. % EQC
