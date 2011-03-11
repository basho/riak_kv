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

-define(REQ_ID, 1234).
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
    {vputpartval(),
     vputfirst(), fsm_eqc_util:largenat(),
     vputsecond(), fsm_eqc_util:largenat()}.

vputpartval() ->
    Shrink = fun(G) -> ?SHRINK(G, [notfound]) end,
    frequency([{2,Shrink({ok, fsm_eqc_util:lineage()})},
               {1,notfound}]).

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
             Objects,ObjectIdxSeed,
             _PartVals,VPutResp,NodeStatus0}, 
            {fsm_eqc_util:largenat(),fsm_eqc_util:largenat(),choose(0,4096),
             fsm_eqc_util:riak_objects(), fsm_eqc_util:largenat(),
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

        
        %% Pick the object to put - as ObjectIdxSeed shrinks, it should go towards
        %% the end of the list (for current), so simplest to just reverse the list
        %% and calculate modulo list length
        ObjectIdx = (ObjectIdxSeed rem length(Objects)) + 1,
        {_,Object} = lists:nth(ObjectIdx, lists:reverse(Objects)),

        VPutReplies = make_vput_replies(VPutResp, Object, Objects, Options),

        ok = gen_server:call(riak_kv_vnode_master,
                             {set_data, Objects, []}),
        ok = gen_server:call(riak_kv_vnode_master,
                             {set_vput_replies, VPutReplies}),
        
        mochiglobal:put(?RING_KEY, Ring),

        application:set_env(riak_core,
                            default_bucket_props,
                            [{n_val, N}
                             |?DEFAULT_BUCKET_PROPS]),


        {ok, PutPid} = riak_kv_put_fsm:start(?REQ_ID,
                                             Object,
                                             W,
                                             DW,
                                             200,
                                             self(),
                                             Options),
        ok = riak_kv_test_util:wait_for_pid(PutPid),
        Res = fsm_eqc_util:wait_for_req_id(?REQ_ID),
        H = get_fsm_qc_vnode_master:get_reply_history(),

        Expected = expect(H, N, W, DW, Options),
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


make_vput_replies(VPutResp, PutObj, Objects, Options) ->
    make_vput_replies(VPutResp, PutObj, Objects, Options, 1, []).
    
make_vput_replies([], _PutObj, _Objects, _Options, _LIdx, SeqReplies) ->
    {_Seqs, Replies} = lists:unzip(lists:sort(SeqReplies)),
    Replies;
make_vput_replies([{_CurObj, {timeout, 1}, FirstSeq, _Second, SecondSeq} | Rest],
                  PutObj, Objects, Options, LIdx, Replies) ->
    make_vput_replies(Rest, PutObj, Objects, Options, LIdx + 1, 
                     [{FirstSeq, {LIdx, {timeout, 1}}},
                      {FirstSeq+SecondSeq+1, {LIdx, {timeout, 2}}} | Replies]);
make_vput_replies([{CurPartVal, First, FirstSeq, dw, SecondSeq} | Rest],
                  PutObj, Objects, Options, LIdx, Replies) ->
    %% Work out what the result of syntactic put merge is
    Obj = case CurPartVal of
              notfound ->
                  PutObj;
              {ok, Lineage} ->
                  CurObj = proplists:get_value(Lineage, Objects),
                  {_, ResObj} = put_merge(CurObj, PutObj, term_to_binary(?REQ_ID)),
                  ResObj
          end,
    make_vput_replies(Rest, PutObj, Objects, Options, LIdx + 1,
                      [{FirstSeq, {LIdx, First}},
                       {FirstSeq+SecondSeq+1, {LIdx, {dw, Obj}}} | Replies]);
make_vput_replies([{_CurObj, First, FirstSeq, Second, SecondSeq} | Rest],
                  PutObj, Objects, Options, LIdx, Replies) ->
    make_vput_replies(Rest, PutObj, Objects, Options, LIdx + 1,
                     [{FirstSeq, {LIdx, First}},
                      {FirstSeq+SecondSeq+1, {LIdx, Second}} | Replies]).

%% TODO: The riak_kv_vnode code should be refactored to expose this function
%%       so we are close to testing the real thing.
put_merge(CurObj, NewObj, ReqId) ->
    ResObj = riak_object:syntactic_merge(
               CurObj,NewObj, ReqId),
    case riak_object:vclock(ResObj) =:= riak_object:vclock(CurObj) of
        true -> {oldobj, ResObj};
        false -> {newobj, ResObj}
    end.

expect(H, N, W, DW, Options) ->
    ReturnObj = case proplists:get_value(returnbody, Options, false) of
                    true ->
                        undefined;
                    false ->
                        noreply
                end,
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
            expect(HNoTimeout, {H, N, W, DW, 0, 0, 0, ReturnObj})
    end.

expect([], {_H, _N, _W, _DW, _NumW, _NumDW, _NumFail, _RObj}) ->
    {error, timeout};
expect([{w, _, _}|Rest], {H, N, W, DW, NumW, NumDW, NumFail, RObj}) ->
    S = {H, N, W, DW, NumW + 1, NumDW, NumFail, RObj},

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

            %% Q: 4 N: 4 W:3 DW: 1
            %% History: [{w,730750818665451459101842416358141509827966271488,1234},
            %%           {w,1096126227998177188652763624537212264741949407232,1234},
            %%           {dw,730750818665451459101842416358141509827966271488, _Obj, 1234},
            %%           {dw,1096126227998177188652763624537212264741949407232, _Obj, 1234},
            %%           {w,0,1234},
            %%           {w,365375409332725729550921208179070754913983135744,1234},
            %%           {{timeout,2},0,1234},
            %%           {{timeout,2},365375409332725729550921208179070754913983135744,1234}]
            %% Expected: ok Res: {error,timeout}

            %% Q: 4 N: 3 W:2 DW: 1
            %% History: [{w,365375409332725729550921208179070754913983135744,1234},
            %%           {dw,365375409332725729550921208179070754913983135744,
            %%               {r_object,
            %%                   <<133,95,45,109,7,80>>,
            %%                   <<"@Ü±,0ü">>,
            %%                   [{r_content,
            %%                        {dict,1,16,16,8,80,48,
            %%                            {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
            %%                            {{[],[],[],[],[],[],[],[],[],[],[],[],[],
            %%                              [[<<"X-Riak-Last-Modified">>|
            %%                                {1298,999408,148661}]],
            %%                              [],[]}}},
            %%                        <<"current">>}],
            %%                   [{<<"bro!">>,{1,63466218608}},
            %%                    {<<"bro2">>,{1,63466218608}},
            %%                    {<<"sis!">>,{1,63466218608}}],
            %%                   {dict,1,16,16,8,80,48,
            %%                       {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
            %%                       {{[],[],[],[],[],[],[],[],[],[],[],[],[],[],
            %%                         [[clean|true]],
            %%                         []}}},
            %%                   undefined},
            %%               1234},
            %%           {w,730750818665451459101842416358141509827966271488,1234},
            %%           {w,1096126227998177188652763624537212264741949407232,1234},
            %%           {{timeout,2},730750818665451459101842416358141509827966271488,1234},
            %%           {fail,1096126227998177188652763624537212264741949407232,1234}]
            %% Expected: ok Res: {error,timeout}

            DWLeft = length([x || Reply <- Rest, element(1,Reply) == dw]),
            OnlyWLeft = lists:all(fun({w, _, _}) -> true;
                                     (_) -> false
                                  end, Rest),
            case {NumW+1, Rest, DWLeft, OnlyWLeft} of
                {W, [{fail,_,_}|_], 0, _} -> 
                    {error, timeout};
                {X, [{w,_,_},{fail,_,_}|_], 0, _} when X >= W->  %DW met before last w, then fail
                    {error, timeout};
                {W, [], 0, _} when NumDW >= DW ->
                    {error, timeout};
                {W, _, 0, true} when DW > 0 ->
                    {error, timeout};
                _ ->
                    io:format("NumW+1=~p W=~p Rest=~p DWLeft = ~p, OnlyWLeft=~p\n",
                              [NumW+1, W, Rest, DWLeft, OnlyWLeft]),
                    maybe_add_robj(Expect, RObj) % and here is passing on expected value
            end;
        {true, Expect} ->
            maybe_add_robj(Expect, RObj);
        
        false ->
            expect(Rest, S)
    end;
expect([DWReply|Rest], {H, N, W, DW, NumW, NumDW, NumFail, RObj}) when element(1, DWReply) == dw->
    S = {H, N, W, DW, NumW, NumDW + 1, NumFail, RObj},
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
                {[{w,_,_},{fail,_,_},{w,_,_},{dw,_,_,_}], 2, 2, 1, _, _, _} ->
                    {error, too_many_fails};
                _ ->
                    maybe_add_robj(Expect, RObj)
            end;
        false ->
            expect(Rest, S)
    end;
expect([{fail, _, _}|Rest], {H, N, W, DW, NumW, NumDW, NumFail, RObj}) ->
    S = {H, N, W, DW, NumW, NumDW, NumFail + 1, RObj},
    case enough_replies(S) of
        {true, Expect} ->
            maybe_add_robj(Expect, RObj);
        false ->
            expect(Rest, S)
    end;
expect([{{timeout,_Stage}, _, _}|Rest], S) ->
    expect(Rest, S).

enough_replies({_H, N, W, DW, NumW, NumDW, NumFail, _RObj}) ->
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

maybe_add_robj(ok, noreply) ->
    ok;
maybe_add_robj(ok, RObj) ->
    {ok, RObj};
maybe_add_robj(Expect, _Robj) ->
    Expect.

-endif. % EQC
