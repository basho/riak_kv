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
%% Remember, if the eunit test failed the current_counterexample file
%% is under .eunit dir
%%
%% TODO: Would like to clean up the expecte result code and make it dependent
%%       on the generate vnode responses.  Hard because the update to the last
%%       modified time stamp breaks the pre-defined lineage code in
%%       fsm_eqc_util and the current last-w-does-not-check-dw bug makes
%%       reasoning hard.
%%
-module(put_fsm_eqc).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("riak_kv_vnode.hrl").
-include("riak_kv_js_pools.hrl").
-include("../src/riak_kv_wm_raw.hrl").

-compile(export_all).
-export([postcommit_ok/1]).

-define(REQ_ID, 1234).
-define(DEFAULT_BUCKET_PROPS,
        [{chash_keyfun, {riak_core_util, chash_std_keyfun}},
         {pw, 0},
         {w, quorum},
         {dw, quorum}]).
-define(HOOK_SAYS_NO, <<"the hook says no">>).
-define(LAGER_LOGFILE, "put_fsm_eqc_lager.log").
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
        {timeout, 60, [?_assertEqual(pang, net_adm:ping('nonode@nohost'))]},
        ?_assertMatch({ok,_C}, riak:local_client()),
        %% Check javascript is working
        ?_assertMatch({ok, 123}, 
                      riak_kv_js_manager:blocking_dispatch(riak_kv_js_hook,
                                                           {{jsanon, 
                                                             <<"function() { return 123; }">>},
                                                            []}, 5)),
        %% Run the quickcheck tests
        {timeout, 60000, % do not trust the docs - timeout is in msec
         ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(50, ?QC_OUT(prop_basic_put()))))}
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
                    %% Make sure epmd is started - will not be if erl -name has
                    %% not been run from the commandline.
                    os:cmd("epmd -daemon"),
                    timer:sleep(100),
                    TestNode = list_to_atom("putfsmeqc" ++ integer_to_list(element(3, now())) ++ integer_to_list(element(2, now()))),
                    {ok, _Pid} = net_kernel:start([TestNode, shortnames]),
                    started
            end,
    %% Shut logging up - too noisy.
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "put_fsm_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "put_fsm_eqc.log"}),

    application:stop(lager),
    application:load(lager),
    application:set_env(lager, handlers,
                        [{lager_file_backend, [{?LAGER_LOGFILE, info, 10485760,"$D0",5}]}]),
    ok = lager:start(),

    %% Start up mock servers and dependencies
    fsm_eqc_util:start_mock_servers(),
    start_javascript(),
    State.

cleanup(running) ->
    application:stop(lager),
    application:unload(lager),
    cleanup_javascript(),
    fsm_eqc_util:cleanup_mock_servers(),
    %% Cleanup the JS manager process
    %% since it tends to hang around too long.
    case whereis(?JSPOOL_HOOK) of
        undefined ->
            ignore;
        Pid ->
            exit(Pid, put_fsm_eqc_js_cleanup)
    end,
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
    State = setup(),
    try quickcheck(numtests(N, prop_basic_put()))
    after
        cleanup(State)
    end.

check() ->
    State = setup(),
    try check(prop_basic_put(), current_counterexample())
    after
        cleanup(State)
    end.


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
%% {notfound|{ok, lineage()}, FirstResp, FirstSeq, SecondResp, SecondSeq}

vnodeputresps() ->
    fsm_eqc_util:not_empty(fsm_eqc_util:longer_list(2, vnodeputresp())).

vnodeputresp() ->
    {vputpartval(),
     vputfirst(), fsm_eqc_util:largenat(),
     vputsecond(), fsm_eqc_util:largenat(),
     nodestatus()}.

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

nodestatus() ->
    ?SHRINK(frequency([{16, primary},
                       {4, fallback},
                       {0, down}]),
            [primary]).

%% Put FSM options
%% 
detail() ->
    timing.

details() ->
    list(detail()).

option() ->
    frequency([{1, returnbody},
               {1, {returnbody, bool()}},
               {1, update_last_modified},
               {1, {update_last_modified, bool()}},
               {1, details},
               {1, {details, details()}},
               {1, disable_hooks}]).
    
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
               {1,  {js, precommit_undefined}},
               {1,  {garbage, garbage}},
               {1,  {garbage, empty}}]).
                
               
precommit_hooks() ->
    frequency([{4, []},
               {1, list(precommit_hook())}]).

postcommit_hook() ->
    frequency([{9, {erlang, postcommit_ok}},
               {1,  {erlang, postcommit_crash}},
               {1,  {erlang, postcommit_fail}},
               {1,  {erlang, postcommit_fail_reason}},
               {1,  {garbage, garbage}}]).
 
postcommit_hooks() ->
    frequency([{5, []},
               {1, list(postcommit_hook())}]).

w_dw_seed() ->
    frequency([{10, quorum},
               {10, default},
               {10, missing},
               {10, one},
               {10, all},
               {10, fsm_eqc_util:largenat()},
               { 5, 0},
               { 1, garbage}]).

%%====================================================================
%% Property
%%====================================================================

prop_basic_put() ->
    ?FORALL({PWSeed, WSeed, DWSeed,
             Objects, ObjectIdxSeed,
             VPutResp,
             Options0, AllowMult, Precommit, Postcommit,
             LogLevel}, 
            {w_dw_seed(), w_dw_seed(), w_dw_seed(),
             fsm_eqc_util:riak_objects(), fsm_eqc_util:largenat(),
             vnodeputresps(),
             options(),bool(), precommit_hooks(), postcommit_hooks(),
             oneof([info, debug])},
    begin
        lager:set_loglevel(lager_file_backend, ?LAGER_LOGFILE, LogLevel),

        N = length(VPutResp),
        {PW, RealPW} = pw_val(N, 1000000, PWSeed),
        %% DW is always at least 1 because of coordinating puts
        {DW, RealDW}  = w_dw_val(N, 1, DWSeed),
        %% W can be 0
        {W, RealW0} = w_dw_val(N, 0, WSeed),

        %% {Q, _Ring, NodeStatus} = fsm_eqc_util:mock_ring(N + NQdiff, NodeStatus0), 

        %% Pick the object to put - as ObjectIdxSeed shrinks, it should go
        %% towards the end of the list (for current), so simplest to just
        %% reverse the list and calculate modulo list length
        ObjectIdx = (ObjectIdxSeed rem length(Objects)) + 1,
        {_PutLin,Object0} = lists:nth(ObjectIdx, lists:reverse(Objects)),
        Object = riak_object:update_value(Object0, riak_object:get_value(Object0)),

        %% Work out how the vnodes should respond and in which order
        %% the messages should be delivered.
        Options = fsm_eqc_util:make_options([{pw, PW}, {w, W}, {dw, DW},
                                             {timeout, 200} | Options0], []),
        VPutReplies = make_vput_replies(VPutResp, Objects, Options),
        PL2 = make_preflist2(VPutResp, 1, []),
        [{CoordPLEntry,_}|_] = PL2,

        %% Prepare the mock vnode master
        ok = fsm_eqc_vnode:set_data(Objects, []),
        ok = fsm_eqc_vnode:set_vput_replies(VPutReplies),
        
        %% Transform the hook test atoms into the arcane hook config
        BucketProps = make_bucket_props(N, AllowMult, Precommit, Postcommit),

        %% Needed for riak_kv_util get_default_rw_val
        application:set_env(riak_core,
                            default_bucket_props,
                            BucketProps),

        %% Run the test and wait for all processes spawned by it to settle.
        process_flag(trap_exit, true),
        {ok, PutPid} = riak_kv_put_fsm:test_link({raw, ?REQ_ID, self()},
                                                 Object,
                                                 Options,
                                                 [{starttime, riak_core_util:moment()},
                                                  {n, N},
                                                  {bucket_props, BucketProps},
                                                  {preflist2, PL2},
                                                  {coord_pl_entry, CoordPLEntry}]),
        ok = riak_kv_test_util:wait_for_pid(PutPid),
        ok = riak_kv_test_util:wait_for_children(PutPid),
        Res = fsm_eqc_util:wait_for_req_id(?REQ_ID, PutPid),
        receive % check for death during postcommit hooks
            {'EXIT', PutPid, Why} ->
                ?assertEqual(normal, Why)
        after
            0 ->
                ok
        end,
        process_flag(trap_exit, false),

        %% Get the history of what happened to the vnode master
        H = fsm_eqc_vnode:get_reply_history(),
        PostCommits = fsm_eqc_vnode:get_postcommits(),

        %% Work out the expected results.  Have to determine the effective dw
        %% the FSM would have used to know when it would have stopped processing responses
        %% and returned to the client.
        EffDW0 = get_effective_dw(RealDW, Options, Postcommit),

        EffDW = erlang:max(EffDW0, RealPW),
        RealW = erlang:max(EffDW, RealW0),

        ExpectObject = expect_object(H, RealW, EffDW, RealPW, AllowMult, PL2),
        {Expected, ExpectedPostCommits, ExpectedVnodePuts} = 
            expect(VPutResp, H, N, PW, RealPW, W, RealW, DW, EffDW, Options,
                   Precommit, Postcommit, ExpectObject, PL2),

        {RetResult, RetInfo} = case Res of 
                                   timeout ->
                                       {Res, undefined};
                                   ok ->
                                       {ok, undefined};
                                   {ok, Info} when is_list(Info) ->
                                       {ok, Info};
                                   {ok, _RetObj} ->
                                       {Res, undefined};
                                   {error, _Reason} ->
                                       {Res, undefined};
                                   {ok, RetObj, Info0} ->
                                       {{ok, RetObj}, Info0};
                                   {error, Reason, Info0} ->
                                       {{error, Reason}, Info0};
                                   {error, Reason, Info0, Info1} ->
                                       {{error, Reason, Info0, Info1}, undefined};
                                   {error, Reason, Info0, Info1, _Info2} ->
                                       {{error, Reason, Info0, Info1}, undefined}
                               end,

        ?WHENFAIL(
           begin
               io:format(user, "BucketProps: ~p\n", [BucketProps]),
               io:format(user, "VPutResp = ~p\n", [VPutResp]),
               io:format(user, "VPutReplies = ~p\n", [VPutReplies]),
               io:format(user, "PrefList2: ~p\n", [PL2]),
               io:format(user, "N: ~p PW: ~p RealPW: ~p W:~p RealW: ~p DW: ~p RealDW: ~p EffDW: ~p"
                         " Pid: ~p\n",
                         [N, PW, RealPW, W, RealW, DW, RealDW, EffDW, PutPid]),
               io:format(user, "Options: ~p\n", [Options]),
               io:format(user, "Precommit: ~p\n", [Precommit]),
               io:format(user, "Postcommit: ~p\n", [Postcommit]),
               io:format(user, "Object: ~p\n", [Object]),
               io:format(user, "Expected Object: ~p\n", [ExpectObject]),
               %% io:format(user, "Expected Object Given Lineage: ~p\n", [ExpectObjectGivenLineage]),
               io:format(user, "History: ~p\n", [H]),
               io:format(user, "Expected: ~p Res: ~p\n", [Expected, Res]),
               io:format(user, "PostCommits:\nExpected: ~p\nGot: ~p\n",
                         [ExpectedPostCommits, PostCommits])
           end,
           conjunction([{result, equals(RetResult, Expected)},
                        {details, check_details(RetInfo, Options)},
                        {postcommit, equals(PostCommits, ExpectedPostCommits)},
                        {puts_sent, check_puts_sent(ExpectedVnodePuts, H)},
                        {told_monitor, fsm_eqc_util:is_get_put_last_cast(put, PutPid)}]))
    end).

make_options([], Options) ->
    Options;
make_options([{_Name, missing} | Rest], Options) ->
    make_options(Rest, Options);
make_options([Option | Rest], Options) ->
    make_options(Rest, [Option | Options]).

%% make preflist2 from the vnode responses.
%% [{notfound|{ok, lineage()}, PrimaryFallback, Response, NodeStatus]
make_preflist2([], _Index, PL2) ->
    lists:reverse(PL2);
make_preflist2([{_PartVal, _First, _FirstSeq, _Sec, _SecSeq, down} | Rest], Index, PL2) ->
    make_preflist2(Rest, Index, PL2);
make_preflist2([{_PartVal, _First, _FirstSeq, _Sec, _SecSeq, NodeStatus} | Rest], Index, PL2) ->
    make_preflist2(Rest, Index + 1, 
                   [{{Index, whereis(fsm_eqc_vnode)}, NodeStatus} | PL2]).

%% make_vput_replies - build the list of vnode replies to pass to the mock
%% vnode master.
%%
%% If the first response is a timeout, no second response is sent.
%%
%% If the second response is a dw requests, lookup the object currently 
%% on the vnode so the vnode master can merge it.
%%
%% Pass on all other requests as they are.
%%
%% The generated sequence numbers are used to re-order the responses - the
%% second is added to the first to make sure the first response from a vnode
%% comes first.
make_vput_replies(VPutResp, Objects, Options) ->
    make_vput_replies(VPutResp, Objects, Options, 1, []).
    
make_vput_replies([], _Objects, _Options, _LIdx, SeqReplies) ->
    %% Randomize the order of replies - make sure the first vnode
    %% is handled before the others to simulate local/remote vnodes
    [{_Seq1, Local1}, {_Seq2, Local2} | SeqRemotes] = lists:reverse(SeqReplies),
    {_Seqs, Replies} = lists:unzip(lists:sort(SeqRemotes)),
    [Local1, Local2 | Replies];
make_vput_replies([{_CurObj, _First, _FirstSeq, _Second, _SecondSeq, down} | Rest],
                  Objects, Options, LIdx, Replies) ->
    make_vput_replies(Rest, Objects, Options, LIdx, Replies); 
make_vput_replies([{_CurObj, {timeout, 1}, FirstSeq, _Second, SecondSeq, _NodeStatus} | Rest],
                  Objects, Options, LIdx, Replies) ->
    make_vput_replies(Rest, Objects, Options, LIdx + 1, 
                     [{FirstSeq+SecondSeq+1, {LIdx, {timeout, 2}}}, 
                      {FirstSeq, {LIdx, {timeout, 1}}} | Replies]);
make_vput_replies([{CurPartVal, First, FirstSeq, dw, SecondSeq, _NodeStatus} | Rest],
                  Objects, Options, LIdx, Replies) ->
    %% Lookup the lineage for the current object and prepare it for
    %% merging in fsm_eqc_vnode
    {Obj, CurLin} = case CurPartVal of
                        notfound ->
                            {notfound, notfound};
                        {ok, PartValLin} ->
                            {proplists:get_value(PartValLin, Objects), PartValLin}
                     end,
    make_vput_replies(Rest, Objects, Options, LIdx + 1,
                      [{FirstSeq+SecondSeq+1, {LIdx, {dw, Obj, CurLin}}},
                       {FirstSeq, {LIdx, First}} | Replies]);
make_vput_replies([{_CurObj, First, FirstSeq, Second, SecondSeq, _NodeStatus} | Rest],
                  Objects, Options, LIdx, Replies) ->
    make_vput_replies(Rest, Objects, Options, LIdx + 1,
                     [{FirstSeq+SecondSeq+1, {LIdx, Second}},
                      {FirstSeq, {LIdx, First}} | Replies]).


make_bucket_props(N, AllowMult, Precommit, Postcommit) ->
    RequestProps =  [{n_val, N},
                     {allow_mult, AllowMult}],
    ModDef = {<<"mod">>, atom_to_binary(?MODULE, latin1)},
    HookXform = fun({erlang, Hook}) ->
                        {struct, [ModDef,
                                   {<<"fun">>, atom_to_binary(Hook,latin1)}]};
                   ({js, Hook}) ->
                        {struct, [{<<"name">>, atom_to_binary(Hook,latin1)}]};
                   ({garbage, garbage}) ->
                        not_a_hook_def;
                   ({garbage, empty}) ->
                        {struct, []}
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
    lists:flatten([RequestProps,
                   PrecommitProps, PostcommitProps, 
                   ?DEFAULT_BUCKET_PROPS]).

%%====================================================================
%% Expected Result Calculation
%%====================================================================

%% Work out the expected return value from the FSM and the expected postcommit log.
expect(VPutResp, H, N, PW, RealPW, W, RealW, DW, EffDW, Options, 
       Precommit, Postcommit, Object, PL2) ->
    ReturnObj = case proplists:get_value(returnbody, Options, false) of
                    true ->
                        Object;
                    false ->
                        noreply
                end,
    Disable = proplists:get_bool(disable_hooks, Options),
    NumPrimaries = length([xx || {_,primary} <- PL2]),
    UpNodes =  length(PL2),
    MinNodes = erlang:max(1, erlang:max(RealW, EffDW)),
    {ExpectResult, ExpectVnodePuts} = 
        if
            PW =:= garbage ->
                {{error, {pw_val_violation, garbage}}, 0};

            W =:= garbage ->
                {{error, {w_val_violation, garbage}}, 0};

            DW =:= garbage ->
                {{error, {dw_val_violation, garbage}}, 0};

            RealW > N orelse EffDW > N orelse RealPW > N  ->
                {{error, {n_val_violation, N}}, 0};

            RealPW > NumPrimaries ->
                {{error, {pw_val_unsatisfied, RealPW, NumPrimaries}}, 0};

            UpNodes < MinNodes ->
                {{error,{insufficient_vnodes,UpNodes,need,MinNodes}}, 0};
           
           true ->
                HNoTimeout = filter_timeouts(H),
                VPuts = case precommit_should_fail(Precommit, Options) of
                            false ->
                                %% If the first local W/DW times out, 
                                %% the remote ones will not be sent.
                                case hd(VPutResp) of
                                    {_,w,_,dw,_,_} ->
                                        N;
                                    _ ->
                                        1
                                end;
                            _ ->
                                0
                        end,
                case H of
                    [{w, _, _}, {fail, _, FailedReqId} | _] ->
                        {maybe_add_robj({error, FailedReqId}, ReturnObj, Precommit, Options), VPuts};
                    _ ->
                        
                        {expect(HNoTimeout, {H, N, RealW, EffDW, RealPW, 0, 0, 0, 0, ReturnObj, Precommit, PL2}, Options), 
                         VPuts}
                end
        end,
    ExpectPostcommit = case {ExpectResult, Postcommit} of
                           {{error, _}, _} ->
                               [];
                           {{error, _, _, _}, _} ->
                               [];
                           {timeout, _} ->
                               [];
                           {_, []} ->
                               [];
                           {_, _} when Disable ->
                               [];
                           {_, _} ->
                               %% Postcommit should be called for each ok hook registered.
                               [Object || Hook <- Postcommit, Hook =:= {erlang, postcommit_ok}]
                       end,
    {ExpectResult, ExpectPostcommit, ExpectVnodePuts}.

expect([], {_H, _N, _W, _DW, _PW, _NumW, _NumDW, _NumPW, _NumFail, RObj, Precommit, _PL}=S,
      Options) ->
    case enough_replies(S) of % this handles W=0 case
        {true, Expect} ->
            maybe_add_robj(Expect, RObj, Precommit, Options);
        false ->
            maybe_add_robj({error, timeout}, RObj, Precommit, Options)
    end;
expect([{w, _Idx, _}|Rest], {H, N, W, DW, PW, NumW, NumDW, NumPW,  NumFail, RObj, Precommit, PL},
      Options) ->
    S = {H, N, W, DW, PW, NumW + 1, NumDW, NumPW, NumFail, RObj, Precommit, PL},

    case enough_replies(S) of
        {true, Expect} ->
            maybe_add_robj(Expect, RObj, Precommit, Options);
        
        false ->
            expect(Rest, S, Options)
    end;
expect([DWReply|Rest], {H, N, W, DW, PW, NumW, NumDW, NumPW, NumFail, RObj, Precommit, PL},
       Options) when element(1, DWReply) == dw->
    Idx = element(2, DWReply),
    POK = primacy(Idx, PL),

    S = {H, N, W, DW, PW, NumW, NumDW + 1, NumPW+POK, NumFail, RObj, Precommit, PL},
    case enough_replies(S) of
        {true, Expect} ->
            maybe_add_robj(Expect, RObj, Precommit, Options);
        false ->
            expect(Rest, S, Options)
    end;
%% Fail on coordindating vnode - request fails
expect([{fail, {1, _}, _}|_Rest], {_H, _N, _W, _DW, _PW, _NumW, _NumDW, _NumPW, _NumFail, RObj, Precommit, _PL},
       Options) ->
    maybe_add_robj({error, too_many_fails}, RObj, Precommit, Options);
expect([{fail, _, _}|Rest], {H, N, W, DW, PW, NumW, NumDW, NumPW, NumFail, RObj, Precommit, PL},
       Options) ->
    S = {H, N, W, DW, PW, NumW, NumDW, NumPW, NumFail + 1, RObj, Precommit, PL},
    case enough_replies(S) of
        {true, Expect} ->
            maybe_add_robj(Expect, RObj, Precommit, Options);
        false ->
            expect(Rest, S, Options)
    end;
expect([{{timeout,_Stage}, _, _}|Rest], S, Options) ->
    expect(Rest, S, Options).

expect_object(H, W, DW, PW, AllowMult, PL) ->
    expect_object(filter_timeouts(H), W, DW, PW, AllowMult, PL, 0, 0, 0, []).

%% Once W and DW are met, reconcile the returned object
expect_object([], _W, _DW, _PW, _AllowMult, _PL, _NumW, _NumDW, _NumPW, _Objs) ->
    noreply;
expect_object([{w,_,_} | H], W, DW, PW, AllowMult, PL, NumW, NumDW, NumPW, Objs) ->
    case NumW+1 >= W andalso NumDW >= DW andalso
            Objs /= [] andalso NumPW >= PW of
        true ->
            riak_object:reconcile(Objs, AllowMult);
        false ->
            expect_object(H, W, DW, PW, AllowMult, PL, NumW + 1, NumDW, NumPW, Objs)
    end;
 %%   expect_object(H, W, DW, AllowMult, NumW + 1, NumDW, Objs);
expect_object([{dw,Idx,Obj, _} | H], W, DW,  PW, AllowMult, PL, NumW, NumDW, NumPW, Objs) ->
    POK = primacy(Idx, PL),
    case NumW >= W andalso NumDW +1 >= DW andalso NumPW+POK >= PW of
        true ->
            riak_object:reconcile([Obj | Objs], AllowMult);
        false ->
            expect_object(H, W, DW, PW, AllowMult, PL, NumW, NumDW + 1,
                NumPW+POK, [Obj | Objs])
    end;
expect_object([_ | H], W, DW, PW, AllowMult, PL, NumW, NumDW, NumPW, Objs) ->
    expect_object(H, W, DW, PW, AllowMult, PL, NumW, NumDW, NumPW, Objs).

%% Work out W and DW given a seed.
%% Generate a value from 0..N+1
w_dw_val(_N, _Min, garbage) ->
    {garbage, 1000000};
w_dw_val(N, Min, Seed) when is_number(Seed) ->
    Val = Seed rem (N + 2),
    {Val, erlang:max(1, erlang:max(Min, Val))};
w_dw_val(N, Min, Seed) ->
    Val = case Seed of
              one -> 1;
              all -> N;
              X when X == quorum; X == default; X == missing -> (N div 2) + 1;
              _ -> Seed
          end,
    {Seed, erlang:max(Min, Val)}.

%% Is the given index a primary index accoring to the preflist?
primacy(Idx, Preflist) ->
    case lists:keyfind(Idx, 1, [{Index, Primacy} ||
                                   {{Index, _Pid}, Primacy} <- Preflist]) of
        {Idx, primary} ->
            1;
        _ ->
            0
    end.

%% Work out W and DW given a seed.
%% Generate a value from 0..N+1
pw_val(_N, _Min, garbage) ->
    {garbage, 1000000};
pw_val(N, Min, Seed) when is_number(Seed) ->
    Val = Seed rem (N + 2),
    {Val, erlang:min(Min, Val)};
pw_val(N, Min, Seed) ->
    Val = case Seed of
              one -> 1;
              all -> N;
              quorum -> (N div 2) + 1;
              X when X == default; X == missing -> 0;
              _ -> Seed
          end,
    {Seed, erlang:min(Min, Val)}.
                  

%% Work out what DW value is being effectively used by the FSM - anything
%% that needs a result - postcommit or returnbody requires a minimum DW of 1.
get_effective_dw(error, _Options, _Postcommit) ->
    error;
get_effective_dw(DW, Options, PostCommit) ->
    OptionsDW = case proplists:get_value(returnbody, Options, false) of
                     true ->
                         1;
                     false ->
                         0
                 end,
    Disable = proplists:get_bool(disable_hooks, Options),
    PostCommitDW = case PostCommit of
                       [] ->
                           0;
                       _ when Disable ->
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

%% All quorum satisfied
enough_replies({_H, _N, W, DW, PW, NumW, NumDW, NumPW, _NumFail, _RObj, _Precommit, _PL}) when
      NumW >= W, NumDW >= DW, NumPW >= PW ->
    {true, ok};
%% Too many failures to reach PW & PW >= DW
enough_replies({_H, N, _W, DW, PW, _NumW, _NumDW, NumPW, NumFail, _RObj, _Precommit, _PL}) when
      NumFail > N - PW, PW >= DW ->
    {true, {error, {pw_val_unsatisfied, PW, NumPW}}};
%% Too many failures to reach DW
enough_replies({_H, N, _W, DW, _PW, _NumW, NumDW, _NumPW, NumFail, _RObj, _Precommit, _PL}) when
      NumFail > N - DW ->
    {true, {error, {dw_val_unsatisfied, DW, NumDW}}};
%% Got N responses, but not PW
enough_replies({_H, N, _W, _DW, PW, _NumW, NumDW, NumPW, NumFail, _RObj, _Precommit, _PL}) when
      NumFail + NumDW >= N, NumPW < PW ->
    {true, {error, {pw_val_unsatisfied, PW, NumPW}}};
enough_replies(_) ->
    false.

maybe_add_robj(ok, RObj, Precommit, Options) ->
    case precommit_should_fail(Precommit, Options) of
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
maybe_add_robj({error, timeout}, _Robj, Precommit, Options) ->
    %% Catch cases where it would fail before sending to vnodes, otherwise
    %% timeout rather than receive an {error, too_many_fails} message - it 
    %% will never come if the put FSM times out.
    case precommit_should_fail(Precommit, Options) of
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
maybe_add_robj(Expect, _Robj, _Precommit, _Options) ->
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
    {fail, ?HOOK_SAYS_NO}. % return binary so same tests can be used on
                           % javascript hooks

precommit_crash(_Obj) ->
    Ok = ok,
    Ok = precommit_crash.

postcommit_ok(Obj) ->
    fsm_eqc_vnode:log_postcommit(Obj),
    ok.

postcommit_crash(_Obj) ->
    Ok = ok,
    Ok = postcommit_crash.

postcommit_fail(_Obj) ->
    fail.

postcommit_fail_reason(_Obj) ->
    {fail, ?HOOK_SAYS_NO}.

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

precommit_should_fail(Precommit, Options) ->
    case proplists:get_bool(disable_hooks, Options) of
        true -> false;
        false -> precommit_should_fail(Precommit)
    end.

precommit_should_fail([]) ->
    false;
precommit_should_fail([{garbage, garbage} | _Rest]) ->
    {true, {error, {precommit_fail, {invalid_hook_def, not_a_hook_def}}}};
precommit_should_fail([{garbage, empty} | _Rest]) ->
    {true, {error, {precommit_fail, {invalid_hook_def, {struct, []}}}}};
precommit_should_fail([{erlang,precommit_undefined} | _Rest]) ->
    {true, {error, {precommit_fail, 
                    {hook_crashed,{put_fsm_eqc,precommit_undefined,error,undef}}}}};
precommit_should_fail([{erlang,precommit_crash} | _Rest]) ->
    {true, {error, {precommit_fail, 
                    {hook_crashed,{put_fsm_eqc,precommit_crash,
                                   error,{badmatch,precommit_crash}}}}}};
precommit_should_fail([{_Lang,Hook} | _Rest]) when Hook =:= precommit_fail;
                                                   Hook =:= precommit_crash;
                                                   Hook =:= precommit_undefined ->
    {true, {error, precommit_fail}};
precommit_should_fail([{_Lang,precommit_fail_reason}| _Rest]) ->
    {true, {error, {precommit_fail, ?HOOK_SAYS_NO}}};
precommit_should_fail([{Lang, precommit_nonobj} | _Rest]) ->
    %% Javascript precommit returning a non-object crashes the JS VM.
    Details = case Lang of
                  js ->
                      {<<"precommit_nonobj">>,<<"not_an_obj">>};
                  erlang ->
                      {?MODULE, precommit_nonobj, not_an_obj}
              end,
    {true, {error, {precommit_fail, {invalid_return, Details}}}};
precommit_should_fail([_LangHook | Rest]) ->
    precommit_should_fail(Rest).

check_details(Info, Options) ->
    case proplists:get_value(details, Options, false) of
        false ->
            equals(undefined, Info);
        [] ->
            equals(undefined, Info);
        _ -> % some info being returned is good enough for now
            true
    end.


%% Check a 'w' message was sent for each planned response.
%% i.e. make sure that the put FSM really sent N 
check_puts_sent(ExpectedPuts, VPutResp) ->
    Requests = [normal || {w,_,_} <- VPutResp] ++
        [timeout || {{timeout, 1},_,_} <- VPutResp],
    NumPutReqs = length(Requests),
    ?WHENFAIL(begin
                  io:format(user, "VPutResp:\n~p\n", [VPutResp]),
                  io:format(user, "Requests: ~p = ~p\n", [Requests, NumPutReqs])
              end,
              equals(ExpectedPuts, NumPutReqs)).

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
    application:stop(sasl).

-endif. % EQC

