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
%%
%% To run outside of eunit
%%
%% $ erl -name t -pa deps/*/{ebin,.eunit} .eunit
%% (t@jons-macpro.local)1> put_fsm_eqc:prepare().
%% (t@jons-macpro.local)2> put_fsm_eqc:test(100).
%% (t@jons-macpro.local)3> put_fsm_eqc:check().
%%
%% Remember, if the eunit test failed the current_counterexample file is under .eunit dir
%%
%% TODO: Would like to clean up the expecte result code and make it dependent
%%       on the generate vnode responses.  Hard because the update to the last modified
%%       time stamp breaks the pre-defined lineage code in fsm_eqc_util and the current
%%       last-w-does-not-check-dw bug makes reasoning hard. 
%%
-module(put_fsm_eqc).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_kv_vnode.hrl").
-include_lib("riak_kv_js_pools.hrl").
-include_lib("riak_kv/src/riak_kv_wm_raw.hrl").

-compile(export_all).
-export([postcommit_ok/1]).

-define(REQ_ID, 1234).
-define(DEFAULT_BUCKET_PROPS,
        [{chash_keyfun, {riak_core_util, chash_std_keyfun}}]).
-define(HOOK_SAYS_NO, <<"the hook says no">>).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

%%====================================================================
%% eunit test 
%%====================================================================

eqc_test_() ->
    {spawn, 
     [{setup,
       fun setup/0,
       fun cleanup/1,
       [%% Check networking/clients are set up 
        ?_assert(node() /= 'nonode@nohost'),
        ?_assertEqual(pong, net_adm:ping(node())),
        ?_assertEqual(pang, net_adm:ping('nonode@nohost')),
        ?_assertMatch({ok,_C}, riak:local_client()),
        %% Check javascript is working
        ?_assertMatch({ok, 123}, 
                      riak_kv_js_manager:blocking_dispatch(riak_kv_js_hook,
                                                           {{jsanon, 
                                                             <<"function() { return 123; }">>},
                                                            []}, 5)),
        %% Run the quickcheck tests
        {timeout, 60000, % do not trust the docs - timeout is in msec
         ?_assertEqual(true, quickcheck(numtests(250, ?QC_OUT(prop_basic_put()))))}
       ]
      }
     ]
    }.

setup() ->
    %% Start net_kernel - hopefully can remove this after FSM is purified..
    State = case net_kernel:stop() of
                {error, not_allowed} ->
                    running;
                _ ->
                    {ok, _Pid} = net_kernel:start(['putfsmeqc@localhost', shortnames]),
                    started
            end,
    %% Shut logging up - too noisy.
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "put_fsm_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "put_fsm_eqc.log"}),

    %% Start up mock servers and dependencies
    fsm_eqc_util:start_mock_servers(),
    start_javascript(),
    State.

cleanup(running) ->
    cleanup_javascript(),
    fsm_eqc_util:cleanup_mock_servers(),
    ok;
cleanup(started) ->
    ok = net_kernel:stop(),
    cleanup(running).

%%====================================================================
%% Shell helpers 
%%====================================================================

prepare() ->
    fsm_eqc_util:start_mock_servers(),
    start_javascript().
    
test() ->
    test(100).

test(N) ->
    quickcheck(numtests(N, prop_basic_put())).

check() ->
    check(prop_basic_put(), current_counterexample()).

%%====================================================================
%% Generators
%%====================================================================

%% Vnode put response. Responses are generated one per-vnode
%% and consisists of an initial first response {w} or timeout
%% and perhaps a second response {dw},{fail} or timeout.
%%
%% The sequence numbers let quickcheck re-order the messages so that
%% different vnode responses are interleaved.
%%
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
  
%% Put FSM options
%% 
option() ->
    frequency([{1, returnbody},
               {1, {returnbody, bool()}},
               {1, update_last_modified},
               {1, {update_last_modified, bool()}}]).

options() ->
    list(option()).

%%
%% Pre/postcommit hooks
%% 

precommit_hook() ->
    frequency([
               {5,  {erlang, precommit_noop}},
               {5,  {erlang, precommit_add_md}},
               {5,  {erlang, precommit_append_value}},
               {1,  {erlang, precommit_nonobj}},
               {1,  {erlang, precommit_fail}},
               {1,  {erlang, precommit_fail_reason}},
               {1,  {erlang, precommit_crash}},
               {1,  {erlang, precommit_undefined}},
               {5,  {js, precommit_noop}},
               {5,  {js, precommit_append_value}},
               {5,  {js, precommit_nonobj}},
               {1,  {js, precommit_fail}},
               {1,  {js, precommit_fail_reason}},
               {1,  {js, precommit_crash}},
               {1,  {js, precommit_undefined}}]).
                
               
precommit_hooks() ->
    frequency([{4, []},
               {1, list(precommit_hook())}]).

postcommit_hook() ->
    frequency([{9, {erlang, postcommit_ok}},
               {1,  {erlang, postcommit_crash}}]).
                
postcommit_hooks() ->
    frequency([{5, []},
               {1, list(postcommit_hook())}]).

%%====================================================================
%% Property
%%====================================================================

prop_basic_put() ->
    ?FORALL({WSeed, DWSeed, NQdiff,
             Objects, ObjectIdxSeed,
             VPutResp, NodeStatus0,
             Options, AllowMult, Precommit, Postcommit}, 
            {fsm_eqc_util:largenat(),fsm_eqc_util:largenat(),choose(0,4096),
             fsm_eqc_util:riak_objects(), fsm_eqc_util:largenat(),
             vnodeputresps(), fsm_eqc_util:some_up_node_status(10),
             options(),bool(), precommit_hooks(), postcommit_hooks()},
    begin
        N = length(VPutResp),
        W = (WSeed rem N) + 1, %% W from 1..N
        DW = DWSeed rem (W + 1), %% DW from 0..DW

        {Q, _Ring, NodeStatus} = fsm_eqc_util:mock_ring(N + NQdiff, NodeStatus0),

        %% Pick the object to put - as ObjectIdxSeed shrinks, it should go towards
        %% the end of the list (for current), so simplest to just reverse the list
        %% and calculate modulo list length
        ObjectIdx = (ObjectIdxSeed rem length(Objects)) + 1,
        {_PutLin,Object} = lists:nth(ObjectIdx, lists:reverse(Objects)),

        %% Work out how the vnodes should respond and in which order
        %% the messages shoudl be delivered.
        VPutReplies = make_vput_replies(VPutResp, Objects, Options),

        %% Prepare the mock vnode master
        ok = gen_server:call(riak_kv_vnode_master,
                             {set_data, Objects, []}),
        ok = gen_server:call(riak_kv_vnode_master,
                             {set_vput_replies, VPutReplies}),
        
        %% Transform the hook test atoms into the arcane hook config
        set_bucket_props(N, AllowMult, Precommit, Postcommit),

        %% Run the test and wait for all processes spawned by it to settle.
        {ok, PutPid} = riak_kv_put_fsm:start(?REQ_ID,
                                             Object,
                                             W,
                                             DW,
                                             200,
                                             self(),
                                             Options),
        ok = riak_kv_test_util:wait_for_pid(PutPid),
        ok = riak_kv_test_util:wait_for_children(PutPid),
        Res = fsm_eqc_util:wait_for_req_id(?REQ_ID),

        %% Get the history of what happened to the vnode master
        H = get_fsm_qc_vnode_master:get_reply_history(),
        PostCommits = get_fsm_qc_vnode_master:get_postcommits(),

        %% Work out the expected results.  Have to determine the effective dw
        %% the FSM would have used to know when it would have stopped processing responses
        %% and returned to the client.
        EffDW = get_effective_dw(DW, Options, Postcommit),
        ExpectObject = expect_object(H, W, EffDW, AllowMult),
        {Expected, ExpectedPostCommits} = expect(H, N, W, EffDW, Options,
                                                 Precommit, Postcommit, ExpectObject),
        ?WHENFAIL(
           begin
               io:format(user, "NodeStatus: ~p\n", [NodeStatus]),
               io:format(user, "VPutReplies = ~p\n", [VPutReplies]),
               io:format(user, "Q: ~p N: ~p W:~p DW: ~p EffDW: ~p\n",
                         [Q, N, W, DW, EffDW]),
               io:format(user, "Object: ~p\n", [Object]),
               io:format(user, "Expected Object: ~p\n", [ExpectObject]),
               %% io:format(user, "Expected Object Given Lineage: ~p\n", [ExpectObjectGivenLineage]),
               io:format(user, "History: ~p\n", [H]),
               io:format(user, "Expected: ~p Res: ~p\n", [Expected, Res]),
               io:format(user, "PostCommits: ~p Got: ~p\n", [ExpectedPostCommits, PostCommits])
           end,
           conjunction([{result, equals(Res, Expected)},
                        {postcommit, equals(PostCommits, ExpectedPostCommits)}]))
    end).

%% make_vput_replies - build the list of vnode replies to pass to the mock vnode master.
%%
%% If the first response is a timeout, no second response is sent.
%%
%% If the second response is a dw requests, lookup the object currently 
%% on the vnode so the vnode master can merge it.
%%
%% Pass on all other requests as they are.
%%
%% The generated sequence numbers are used to re-order the responses - the second
%% is added to the first to make sure the first response from a vnode comes first.
make_vput_replies(VPutResp, Objects, Options) ->
    make_vput_replies(VPutResp, Objects, Options, 1, []).
    
make_vput_replies([], _Objects, _Options, _LIdx, SeqReplies) ->
    {_Seqs, Replies} = lists:unzip(lists:sort(SeqReplies)),
    Replies;
make_vput_replies([{_CurObj, {timeout, 1}, FirstSeq, _Second, SecondSeq} | Rest],
                  Objects, Options, LIdx, Replies) ->
    make_vput_replies(Rest, Objects, Options, LIdx + 1, 
                     [{FirstSeq, {LIdx, {timeout, 1}}},
                      {FirstSeq+SecondSeq+1, {LIdx, {timeout, 2}}} | Replies]);
make_vput_replies([{CurPartVal, First, FirstSeq, dw, SecondSeq} | Rest],
                  Objects, Options, LIdx, Replies) ->
    %% Lookup the lineage for the current object and prepare it for
    %% merging in get_fsm_qc_vnode_master
    {Obj, CurLin} = case CurPartVal of
                        notfound ->
                            {notfound, notfound};
                        {ok, PartValLin} ->
                            {proplists:get_value(PartValLin, Objects), PartValLin}
                     end,
    make_vput_replies(Rest, Objects, Options, LIdx + 1,
                      [{FirstSeq, {LIdx, First}},
                       {FirstSeq+SecondSeq+1, {LIdx, {dw, Obj, CurLin}}} | Replies]);
make_vput_replies([{_CurObj, First, FirstSeq, Second, SecondSeq} | Rest],
                  Objects, Options, LIdx, Replies) ->
    make_vput_replies(Rest, Objects, Options, LIdx + 1,
                     [{FirstSeq, {LIdx, First}},
                      {FirstSeq+SecondSeq+1, {LIdx, Second}} | Replies]).


set_bucket_props(N, AllowMult, Precommit, Postcommit) ->
    RequestProps =  [{n_val, N},
                     {allow_mult, AllowMult}],
    ModDef = {<<"mod">>, atom_to_binary(?MODULE, latin1)},
    HookXform = fun({erlang, Hook}) ->
                        {struct, [ModDef,
                                   {<<"fun">>, atom_to_binary(Hook,latin1)}]};
                   ({js, Hook}) ->
                        {struct, [{<<"name">>, atom_to_binary(Hook,latin1)}]}
                end,
    PrecommitProps = case Precommit of
                         [] ->
                             [];
                         _ ->
                             [{precommit, [HookXform(H) || H <- Precommit]}]
                     end,
    PostcommitProps = case Postcommit of
                          [] ->
                              [];
                          _ ->
                              [{postcommit, [HookXform(H) || H <- Postcommit]}]
                      end,
    application:set_env(riak_core, default_bucket_props,
                        lists:flatten([RequestProps,
                                       PrecommitProps, PostcommitProps, 
                                       ?DEFAULT_BUCKET_PROPS])).

%%====================================================================
%% Expected Result Calculation
%%====================================================================

%% Work out the expected return value from the FSM and the expected postcommit log.
expect(H, N, W, EffDW, Options, Precommit, Postcommit, Object) ->
    ReturnObj = case proplists:get_value(returnbody, Options, false) of
                    true ->
                        Object;
                    false ->
                        noreply
                end,
    
    ExpectResult = case {H, N, W, EffDW} of
                       %% Workaround for bug transitioning from awaiting_w to awaiting_dw
                       {[{w,_,_},{dw,_,_},{w,_,_},{fail,_,_}], 2, 2, 1} ->
                           {error, timeout};
                       {[{w,_,_},{dw,_,_},{w,_,_},{{timeout,_},_,_}], 2, 2, 1} ->
                           {error, timeout};
                       _ ->
                           HNoTimeout = filter_timeouts(H),
                           expect(HNoTimeout, {H, N, W, EffDW, 0, 0, 0, ReturnObj, Precommit})
                   end,
    ExpectPostcommit = case {ExpectResult, Postcommit} of
                           {{error, _}, _} ->
                               [];
                           {timeout, _} ->
                               [];
                           {_, []} ->
                               [];
                           {_, _} ->
                               %% Postcommit should be called for each ok hook registered.
                               [Object || Hook <- Postcommit, Hook =:= {erlang, postcommit_ok}]
                       end,
    {ExpectResult, ExpectPostcommit}.

expect([], {_H, _N, _W, DW, _NumW, _NumDW, _NumFail, RObj, Precommit}) ->
    maybe_add_robj({error, timeout}, RObj, Precommit, DW);
expect([{w, _, _}|Rest], {H, N, W, DW, NumW, NumDW, NumFail, RObj, Precommit}) ->
    S = {H, N, W, DW, NumW + 1, NumDW, NumFail, RObj, Precommit},

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
            Expect2 = case {NumW+1, Rest, DWLeft, OnlyWLeft} of
                          {W, [{fail,_,_}|_], 0, _} when DW > 0-> 
                              {error, timeout};
                          %% DW met before last w, then fail
                          {X, [{w,_,_},{fail,_,_}|_], 0, _} when DW > 0, X >= W->  
                              {error, timeout};
                          {W, [], 0, _} when DW > 0, NumDW >= DW ->
                              {error, timeout};
                          {W, _, 0, true} when DW > 0 ->
                              {error, timeout};
                          _ ->
                              Expect
                      end,
            %% io:format("NumW+1=~p W=~p Rest=~p DWLeft = ~p, OnlyWLeft=~p\n",
            %%           [NumW+1, W, Rest, DWLeft, OnlyWLeft]),
            maybe_add_robj(Expect2, RObj, Precommit, DW);
        {true, Expect} ->
            maybe_add_robj(Expect, RObj, Precommit, DW);
        
        false ->
            expect(Rest, S)
    end;
expect([DWReply|Rest], {H, N, W, DW, NumW, NumDW, NumFail,
                        RObj, Precommit}) when element(1, DWReply) == dw->
    S = {H, N, W, DW, NumW, NumDW + 1, NumFail, RObj, Precommit},
    case enough_replies(S) of
        {true, Expect} ->

            %% Workaround: it seems like you should wait for DWs until you know
            %% enough cannot be received.  Nasty N=2 case anyway.
            %% Q: 2 N: 2 W:2 DW: 1
            %% History: [{w,0,-7826778492},
            %%           {fail,0,-7826778492},
            %%           {w,730750818665451459101842416358141509827966271488,-7826778492},
            %%           {dw,730750818665451459101842416358141509827966271488,-7826778492}]
            Expect2 = case S of
                {[{w,_,_},{fail,_,_},{w,_,_},{dw,_,_}], 2, 2, 1, _, _, _} ->
                    {error, too_many_fails};
                {[{w,_,_},{fail,_,_},{w,_,_},{dw,_,_,_}], 2, 2, 1, _, _, _} ->
                    {error, too_many_fails};
                _ ->
                    Expect
            end,
            maybe_add_robj(Expect2, RObj, Precommit, DW);
        false ->
            expect(Rest, S)
    end;
expect([{fail, _, _}|Rest], {H, N, W, DW, NumW, NumDW, NumFail, RObj, Precommit}) ->
    S = {H, N, W, DW, NumW, NumDW, NumFail + 1, RObj, Precommit},
    case enough_replies(S) of
        {true, Expect} ->
            maybe_add_robj(Expect, RObj, Precommit, DW);
        false ->
            expect(Rest, S)
    end;
expect([{{timeout,_Stage}, _, _}|Rest], S) ->
    expect(Rest, S).

expect_object(H, W, DW, AllowMult) ->
    expect_object(filter_timeouts(H), W, DW, AllowMult, 0, 0, []).

%% Once W and DW are met, reconcile the returned object
expect_object([], _W, _DW, _AllowMult, _NumW, _NumDW, _Objs) ->
    noreply;
expect_object([{w,_,_} | H], W, DW, AllowMult, NumW, NumDW, Objs) ->
    %% Bug in current impl waits for dw message to be received after
    %% NumW meets DW.
    %% case NumW+1 >= W andalso NumDW >= DW of
    %%     true ->
    %%         riak_object:reconcile(Objs, AllowMult);
    %%     false ->
    %%         expect_object(H, W, DW, AllowMult, NumW + 1, NumDW, Objs)
    %% end;
    expect_object(H, W, DW, AllowMult, NumW + 1, NumDW, Objs);
expect_object([{dw,_,Obj, _} | H], W, DW, AllowMult, NumW, NumDW, Objs) ->
    case NumW >= W andalso NumDW +1 >= DW of
        true ->
            riak_object:reconcile([Obj | Objs], AllowMult);
        false ->
            expect_object(H, W, DW, AllowMult, NumW, NumDW + 1, [Obj | Objs])
    end;
expect_object([_ | H], W, DW, AllowMult, NumW, NumDW, Objs) ->
    expect_object(H, W, DW, AllowMult, NumW, NumDW, Objs).

%% Work out what DW value is being effectively used by the FSM - anything
%% that needs a result - postcommit or returnbody requires a minimum DW of 1.
get_effective_dw(DW, Options, PostCommit) ->
    OptionsDW = case proplists:get_value(returnbody, Options, false) of
                     true ->
                         1;
                     false ->
                         0
                 end,
    PostCommitDW = case PostCommit of
                       [] ->
                           0;
                       _ ->
                           1
                   end,
    lists:max([DW, OptionsDW, PostCommitDW]).

%% Filter any timeouts from the history to make it easier to look
%% for events that immediately follow one another.
filter_timeouts(H) ->
    lists:filter(fun({{timeout,_},_,_}) -> false;
                    (_) -> true
                 end, H).

enough_replies({_H, N, W, DW, NumW, NumDW, NumFail, _RObj, _Precommit}) ->
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
 
maybe_add_robj(ok, RObj, Precommit, DW) ->
    case precommit_should_fail(Precommit, DW) of
        {true, Expect} ->
            Expect;
        
        false ->
            case RObj of
                noreply ->
                    ok;
                _ ->
                    {ok, RObj}
            end
    end;
maybe_add_robj({error, timeout}, _Robj, Precommit, DW) ->
    %% Catch cases where it would fail before sending to vnodes, otherwise
    %% timeout rather than receive an {error, too_many_fails} message - it 
    %% will never come if the put FSM times out.
    case precommit_should_fail(Precommit, DW) of
        {true, {error, precommit_fail}} ->
            {error, precommit_fail};
        {true, {error, {precommit_fail, Why}}} ->
            {error, {precommit_fail, Why}};
        {true, {error, _}} ->
            {error, timeout};
        {true, Other} ->
            Other;
        false ->
            {error, timeout}
    end;
maybe_add_robj(Expect, _Robj, _Precommit, _DW) ->
    Expect.

%%====================================================================
%% Expected Result Calculation
%%====================================================================

precommit_noop(Obj) -> % No-op precommit, no changed
    r_object = element(1, Obj),
    Obj.

precommit_add_md(Obj) ->
    r_object = element(1, Obj),
    MD = riak_object:get_metadata(Obj),
    UpdMD = dict:store(?MD_USERMETA, [{"X-Riak-Meta-PrecommitHook","was here"}], MD),
    riak_object:update_metadata(Obj, UpdMD).
    
precommit_append_value(Obj) ->
    r_object = element(1, Obj),
    Val = riak_object:get_value(Obj),
    UpdVal = <<Val/binary, "_precommit_hook_was_here">>,
    riak_object:update_value(Obj, UpdVal).

precommit_nonobj(Obj) -> % Non-riak object
    r_object = element(1, Obj),
    not_an_obj.

precommit_fail(Obj) -> % Pre-commit fails
    r_object = element(1, Obj),
    fail.

precommit_fail_reason(Obj) -> % Pre-commit fails
    r_object = element(1, Obj),
    {fail, ?HOOK_SAYS_NO}. % return binary so same tests can be used on javascript hooks

precommit_crash(_Obj) ->
    Ok = ok,
    Ok = precommit_crash.

postcommit_ok(Obj) ->
    get_fsm_qc_vnode_master:log_postcommit(Obj),
    ok.

postcommit_crash(_Obj) ->
    Ok = ok,
    Ok = postcommit_crash.

apply_precommit(Object, []) ->
    Object;
apply_precommit(Object, [{_, precommit_add_md} | Rest]) ->
    UpdObj = riak_object:apply_updates(precommit_add_md(Object)),
    apply_precommit(UpdObj, Rest);
apply_precommit(Object, [{_, precommit_append_value} | Rest]) ->
    UpdObj = riak_object:apply_updates(precommit_append_value(Object)),
    apply_precommit(UpdObj, Rest);
apply_precommit(Object, [_ | Rest]) ->
    apply_precommit(Object, Rest).

precommit_should_fail([], _DW) ->
    false;
precommit_should_fail([{_Lang,Hook} | _Rest], _DW) when Hook =:= precommit_fail;
                                                        Hook =:= precommit_crash;
                                                        Hook =:= precommit_undefined ->
    {true, {error, precommit_fail}};
precommit_should_fail([{_Lang,precommit_fail_reason}], _DW) ->
    {true, {error, {precommit_fail, ?HOOK_SAYS_NO}}};
precommit_should_fail([{js, precommit_nonobj} | _Rest], _DW) ->
    %% Javascript precommit returning a non-object crashes the JS VM.
    {true, timeout};
precommit_should_fail([{_Lang,Hook} | Rest], DW) when Hook =:= precommit_nonobj;
                                                      Hook =:= precommit_fail_reason ->
    %% Work around bug - no check for valid object on return from precommit hook.
    %% instead tries to use it anyway as a valid object and puts fail.
    case {DW, Rest} of 
        {0, []} ->
            false;
        {_, []} ->
            {true, {error, too_many_fails}};
        _ ->
            {true, crashfail_before_js(Rest)}
    end;
precommit_should_fail([_LangHook | Rest], DW) ->
    precommit_should_fail(Rest, DW).

%% Return true if there is a crash or fail that will prevent more precommit hooks
%% running before a javascript one is hit.
crashfail_before_js([]) ->
    {error, precommit_fail}; % for our purposes no js hook was hit, so kinda true.
crashfail_before_js([{js, _} | _Rest]) ->
    %% Javascript precommit with non-object crashes the VM and we get
    %% a timeout waiting for the VM to reply
    timeout;
crashfail_before_js([{erlang, _Hook} | _Rest]) ->
    %% All erlang test hooks check to see if valid object coming in
    {error, precommit_fail};
crashfail_before_js([_|Rest]) ->
    crashfail_before_js(Rest).

%%====================================================================
%% Javascript helpers 
%%====================================================================

start_javascript() ->
    application:stop(erlang_js),
    application:stop(sasl),
    application:start(sasl),
    application:load(erlang_js),
    application:start(erlang_js),
    %% Set up the test dir so erlang_js will find put_fsm_precommit.js
    TestDir = filename:join([filename:dirname(code:which(?MODULE)), "..", "test"]),
    application:set_env(riak_kv, js_source_dir, TestDir),
    {ok, _} = riak_kv_js_sup:start_link(),
    {ok, _} = riak_kv_js_manager:start_link(?JSPOOL_HOOK, 1),
    ok.

cleanup_javascript() ->
    application:stop(erlang_js),
    application:unload(erlang_js),
    application:stop(sasl).

-endif. % EQC

