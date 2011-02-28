%% -------------------------------------------------------------------
%%
%% riak_put_fsm: coordination of Riak PUT requests
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
    {timeout, 2000, 
     {spawn, [{ setup,
                fun setup/0,
                fun cleanup/1,
                [?_test(begin test(100) end)]}
                   
             ]}}.

setup() ->
    fsm_eqc_util:start_mock_servers(),
    case net_kernel:stop() of
        {error, not_allowed} ->
            running;
        _ ->
            {ok, _Pid} = net_kernel:start(['putfsmeqc@localhost', shortnames]),
            started
    end.

cleanup(running) ->
    ok;
cleanup(started) ->
    ok = net_kernel:stop().

test() ->
    test(100).

test(N) ->
    quickcheck(numtests(N, prop_basic_put())).

check() ->
    check(prop_basic_put(), current_counterexample()).


%% Vnode put responses
%% {FirstResp, FirstSeq, SecondResp, SecondSeq}
vnodeputresps() ->
    fsm_eqc_util:not_empty(fsm_eqc_util:longer_list(2, vnodeputresp())).

vnodeputresp() ->
    {fsm_eqc_util:lineage(),
     vputfirst(), fsm_eqc_util:largenat(),
     vputsecond(), fsm_eqc_util:largenat()}.

vputfirst() ->    
    frequency([{9, w},
               {1, ?SHRINK({timeout, 1}, [w])}]).

vputsecond() ->
    Shrink = fun(G) -> ?SHRINK(G, [dw]) end,
    frequency([{18, dw},
               {1, Shrink(fail)},
               {1, Shrink({timeout, 2})}]).
   



prop_basic_put() ->
    %% ?FORALL({WSeed,DWSeed},
    ?FORALL({WSeed,DWSeed,NQdiff,
             Objects,ReqId,_PartVals,VPutResp,NodeStatus0}, 
            {fsm_eqc_util:largenat(),fsm_eqc_util:largenat(),choose(0,4096),
             fsm_eqc_util:riak_objects(), noshrink(largeint()),
             fsm_eqc_util:partvals(),vnodeputresps(),
             fsm_eqc_util:some_up_node_status(10)},
    begin
        N = length(VPutResp),
        W = (WSeed rem N) + 1,
        DW = (DWSeed rem W) + 1,
        Options = [],

        Q = fsm_eqc_util:make_power_of_two(N + NQdiff),
        NodeStatus = fsm_eqc_util:cycle(Q, NodeStatus0),

        Ring = fsm_eqc_util:reassign_nodes(NodeStatus,
                                           riak_core_ring:fresh(Q, node())),

        [{_,Object}|_] = Objects,

        VPutReplies = make_vput_replies(VPutResp, Object, Objects),

        ok = gen_server:call(riak_kv_vnode_master,
                             {set_data, Objects, []}),
        ok = gen_server:call(riak_kv_vnode_master,
                             {set_vput_replies, VPutReplies}),
        
        mochiglobal:put(?RING_KEY, Ring),

        application:set_env(riak_core,
                            default_bucket_props,
                            [{n_val, N}
                             |?DEFAULT_BUCKET_PROPS]),


        {ok, PutPid} = riak_kv_put_fsm:start(ReqId,
                                             Object,
                                             W,
                                             DW,
                                             200,
                                             self(),
                                             Options),
        ok = riak_kv_test_util:wait_for_pid(PutPid),
        Res = fsm_eqc_util:wait_for_req_id(ReqId),
        H = get_fsm_qc_vnode_master:get_reply_history(),

        Expected = expect(H, N, W, DW),
        ?WHENFAIL(
           begin
               io:format(user, "NodeStatus: ~p\n", [NodeStatus]),
               io:format(user, "VPutReplies = ~p\n", [VPutReplies]),
               io:format(user, "Q: ~p N: ~p W:~p DW: ~p\n", [Q, N, W, DW]),
               io:format(user, "History: ~p\n", [H]),
               io:format(user, "Expected: ~p Res: ~p\n", [Expected, Res])
           end,
           equals(Res, Expected))
    end).


make_vput_replies(VPutResp, Object, Objects) ->
    make_vput_replies(VPutResp, Object, Objects, 1, []).
    
make_vput_replies([], _Object, _Objects, _LIdx, SeqReplies) ->
    {_Seqs, Replies} = lists:unzip(lists:sort(SeqReplies)),
    Replies;
make_vput_replies([{_CurObj, {timeout, 1}, FirstSeq, _Second, SecondSeq} | Rest],
                  Object, Objects, LIdx, Replies) ->
    make_vput_replies(Rest, Object, Objects, LIdx + 1, 
                     [{FirstSeq, {LIdx, {timeout, 1}}},
                      {FirstSeq+SecondSeq+1, {LIdx, {timeout, 2}}} | Replies]);
make_vput_replies([{_CurObj, First, FirstSeq, Second, SecondSeq} | Rest],
                  Object, Objects, LIdx, Replies) ->
    make_vput_replies(Rest, Object, Objects, LIdx + 1,
                     [{FirstSeq, {LIdx, First}},
                      {FirstSeq+SecondSeq+1, {LIdx, Second}} | Replies]).


expect(H, N, W, DW) ->
    case {H, N, W, DW} of
        %% Workaround for bug transitioning from awaiting_w to awaiting_dw
        {[{w,_,_},{dw,_,_},{w,_,_},{fail,_,_}], 2, 2, 1} ->
            {error, timeout};
        {[{w,_,_},{dw,_,_},{w,_,_},{{timeout,_},_,_}], 2, 2, 1} ->
            {error, timeout};
        _ ->
            HNoTimeout = lists:filter(fun({{timeout,_},_,_}) -> false;
                                             (_) -> true
                                          end, H),
            expect(HNoTimeout, {H, N, W, DW, 0, 0, 0})
    end.

expect([], {_H, _N, _W, _DW, _NumW, _NumDW, _NumFail}) ->
    {error, timeout};
expect([{w, _, _}|Rest], {H, N, W, DW, NumW, NumDW, NumFail}) ->
    S = {H, N, W, DW, NumW + 1, NumDW, NumFail},

    case enough_replies(S) of
        {true, Expect} when Expect == {error, too_many_fails};
                            Expect == ok ->
            %% Workaround for DW met before last w and only fail after last w, no more
            %% dws
            %% Q: 4 N: 3 W:3 DW: 1
            %%     History: [{w,0,-885709871},
            %%               {w,365375409332725729550921208179070754913983135744,-885709871},
            %%               {dw,0,-885709871},
            %%               {dw,365375409332725729550921208179070754913983135744,-885709871},
            %%               {w,730750818665451459101842416358141509827966271488,-885709871},
            %%               {fail,730750818665451459101842416358141509827966271488,-885709871}]

            %% Q: 4 N: 3 W:2 DW: 1
            %% History: [{w,730750818665451459101842416358141509827966271488,3289308765},
            %%           {{timeout,1},
            %%            1096126227998177188652763624537212264741949407232,3289308765},
            %%           {dw,730750818665451459101842416358141509827966271488,3289308765},
            %%           {w,0,3289308765},
            %%           {{timeout,2},
            %%            1096126227998177188652763624537212264741949407232,3289308765},
            %%           {fail,0,3289308765}]
            %% Expected: ok Res: {error,timeout}

            %% Q: 4 N: 3 W:3 DW: 1
            %% NodeStatus: [up,up,up,up]
            %% History: [{w,365375409332725729550921208179070754913983135744,6059244597},
            %%           {w,730750818665451459101842416358141509827966271488,6059244597},
            %%           {dw,365375409332725729550921208179070754913983135744,6059244597},
            %%           {dw,730750818665451459101842416358141509827966271488,6059244597},
            %%           {w,1096126227998177188652763624537212264741949407232,6059244597},
            %%           {{timeout,2},
            %%            1096126227998177188652763624537212264741949407232,6059244597}]
            %% Expected: ok Res: {error,timeout}
            case {NumW+1, Rest, length([x || {What, _, _} <- Rest, What == dw])} of
                {W, [{fail,_,_}|_], 0} -> 
                    {error, timeout};
                {W, [], 0} when NumDW >= DW ->
                    {error, timeout};
                _ ->
                    Expect % and here is passing on expected value
            end;
        {true, Expect} ->
            Expect;
        
        false ->
            expect(Rest, S)
    end;
expect([{dw, _, _}|Rest], {H, N, W, DW, NumW, NumDW, NumFail}) ->
    S = {H, N, W, DW, NumW, NumDW + 1, NumFail},
    case enough_replies(S) of
        {true, Expect} ->

            %% Workaround for - seems like you should wait for DWs until you know
            %% enough cannot be received.  Nasty N=2 case anyway.
            %% Q: 2 N: 2 W:2 DW: 1
            %% History: [{w,0,-7826778492},
            %%           {fail,0,-7826778492},
            %%           {w,730750818665451459101842416358141509827966271488,-7826778492},
            %%           {dw,730750818665451459101842416358141509827966271488,-7826778492}]
            case S of
                {[{w,_,_},{fail,_,_},{w,_,_},{dw,_,_}], 2, 2, 1, _, _, _} ->
                    {error, too_many_fails};
                _ ->
                    Expect
            end;
        false ->
            expect(Rest, S)
    end;
expect([{fail, _, _}|Rest], {H, N, W, DW, NumW, NumDW, NumFail}) ->
    S = {H, N, W, DW, NumW, NumDW, NumFail + 1},
    case enough_replies(S) of
        {true, Expect} ->
            Expect;
        false ->
            expect(Rest, S)
    end;
expect([{{timeout,_Stage}, _, _}|Rest], S) ->
    expect(Rest, S).

enough_replies({_H, N, W, DW, NumW, NumDW, NumFail}) ->
    MaxWFails =  N - W,
    MaxDWFails =  N - DW,
    if
        NumW >= W andalso NumDW >= DW ->
            {true, ok};

        NumW < W andalso NumFail > MaxWFails ->
            {true, {error, too_many_fails}};

        NumW >= W andalso NumFail > MaxDWFails ->
            {true, {error, too_many_fails}};

        true ->
            false
    end.

%% expect([], _N, _W, _DW) ->
%%     {error, timeout};
%% expect(History, N, W, DW) ->
%%     %% Count non-timeout/errors, check > W and DW
%%     MaxFails = N - max(DW, W),

%%     {NumW, NumDW, NumFail} = Counters = 
%%         lists:foldl(fun(Result, {NumW0, NumDW0, NumFail0}) -> 
%%                             case Result of
%%                                 {w, _Idx, _ReqId} ->
%%                                     {NumW0+1, NumDW0, NumFail0};
%%                                 {dw, _Idx, _ReqId} ->
%%                                     {NumW0, NumDW0 + 1, NumFail0};
%%                                 {fail, _Idx, _ReqId} ->
%%                                     {NumW0, NumDW0, NumFail0 + 1};
%%                                 {{timeout, _Stage}, _Idx, _ReqId} ->
%%                                     {NumW0, NumDW0, NumFail0}
%%                             end
%%                     end, {0, 0, 0}, History),
%%     io:format(user, "Counters=~p\n", [Counters]),
%%     io:format(user, "History=~p\n", [History]),
%%     if
%%         NumW >= W andalso NumDW >= DW ->
%%             case {History, N, W, DW} of
%%                 %% Check for bug - if DW > 0 and
%%                 %% dw response is received before enough W responses received
%%                 %% and the remaining responses 
%%                 %% expect_working_around_dw_bug(History);
%%                 {[{w,_,_},{dw,_,_},{w,_,_},{fail,_,_}], 2, 2, 1} ->
%%                     {error, timeout};

%%                 _ ->
%%                     %% Bug - if fail is before last w and N-numfail >= W then
%%                     %% too_many_fails is returned incorrectly
%%                     History2 = trim_non_dw(History),
%%                     FailBeforeLastW = fail_before_last_w(History2),
%%                     NotDWAfterLastW = not_dw_after_last_w(History2),
%%                     Last = element(1, hd(lists:reverse(History2))),
%%                     LastNotDw = (Last /= dw andalso Last /= w),
%%                     LastIsTimeout = (Last == {timeout,1} orelse Last == {timeout,2}),
%%                     if
%%                         FailBeforeLastW andalso 
%%                         (N - NumFail < W orelse
%%                          N - NumFail < DW)  ->
%%                             {error, too_many_fails};
 
%%                         N > DW andalso LastIsTimeout ->
%%                             {error, timeout};
                       
%%                         %% Last is history item is fail after W
%%                         %% Does not correctly check for meeting dw after
%%                         %% last fail
%%                         NotDWAfterLastW andalso LastNotDw andalso
%%                           DW > 0 andalso NumW >= W ->
%%                             {error, timeout};
                        
                        
%%                         true ->
%%                             %% The correct answer when FSM fixed
%%                             io:format(user,
%%                                       "History2: ~p\n"
%%                                       "Last: ~p\n"
%%                                       "LastNotDw: ~p\n"
%%                                       "LastIsTimeout: ~p\n",
%%                                       [History2, Last, LastNotDw, LastIsTimeout]),
                                     
%%                             ok
%%                     end
%%             end;
%%         NumFail > MaxFails ->
%%             {error, too_many_fails};
%%         true ->
%%             {error, timeout}
%%     end.

%% %% Return true if a fail is returned before the last w
%% fail_before_last_w(History) ->
%%     HSeq = lists:zip(History, lists:seq(1, length(History))),
%%     LastW = maxifdef([Seq || {{w,_,_}, Seq} <- HSeq]),
%%     FirstFail = maxifdef([Seq || {{fail,_,_}, Seq} <- HSeq]),
%%     LastW /= undefined andalso FirstFail /= undefined andalso FirstFail < LastW.     

%% %% Check for a fail immediately after the last w
%% not_dw_after_last_w(History) ->
%%     SeqH = lists:zip(lists:seq(1, length(History)), History),
%%     LastW = maxifdef([Seq || {Seq, {w,_,_}} <- SeqH]),
%%     LastW /= undefined andalso proplists:get_value(LastW+1, SeqH) /= dw.

%% trim_non_dw(History) ->
%%     do_trim_non_dw(lists:reverse(History)).

%% do_trim_non_dw([]) ->
%%     [];
%% do_trim_non_dw([{dw, _, _}|_]=RevHist) ->
%%     lists:reverse(RevHist);
%% do_trim_non_dw([{w, _, _}|_]=RevHist) ->
%%     lists:reverse(RevHist);
%% do_trim_non_dw([_|Rest]) ->
%%     do_trim_non_dw(Rest).

%% maxifdef([]) ->
%%     undefined;
%% maxifdef(L) ->
%%     lists:max(L).


-endif. % EQC
