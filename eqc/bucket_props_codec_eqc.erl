%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(bucket_props_codec_eqc).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

-define(QC_OUT(P), eqc:on_output(fun(F,TL) ->
                                         io:format(user, F, TL)
                                 end, P)).

%%====================================================================
%% Eunit integration
%%====================================================================
bucket_codec_test_() ->
    [{timeout, 10,
      ?_test(begin
                 eqc:quickcheck(?QC_OUT(eqc:testing_time(4, prop_codec())))
             end)}].

%%====================================================================
%% Properties
%%====================================================================
prop_codec() ->
    ?FORALL(Props, sortuniq(list(bucket_prop())),
            ?WHENFAIL(begin
                          io:format("Props: ~p~n~n", [Props])
                      end,
                      begin
                          Props2 = riak_pb_codec:decode_bucket_props(
                                     riak_pb:decode_msg(
                                       iolist_to_binary(riak_pb:encode_msg(
                                                          riak_pb_codec:encode_bucket_props(Props))),
                                       rpbbucketprops)),
                          Props =:= lists:sort(Props2)
                      end)).

%%====================================================================
%% Shell helpers
%%====================================================================
qc() ->
    qc(2000).

qc(NumTests) ->
    quickcheck(numtests(NumTests, prop_codec())).

check() ->
    eqc:check(prop_codec(), eqc:current_counterexample()).

%%====================================================================
%% Generators
%%====================================================================
bucket_prop() ->
    oneof([num(n_val),
           flag(allow_mult),
           flag(last_write_wins),
           commit(precommit),
           commit(postcommit),
           chash(),
           linkfun(),
           num(old_vclock),
           num(young_vclock),
           num(big_vclock),
           num(small_vclock),
           quorum(pr),
           quorum(r),
           quorum(w),
           quorum(pw),
           quorum(dw),
           quorum(rw),
           flag(basic_quorum),
           flag(notfound_ok),
           backend(),
           flag(search),
           repl(),
           yz_index(),
           datatype(),
           flag(consistent),
           flag(write_once)]).

sortuniq(Gen) ->
    ?LET(L, Gen, lists:ukeysort(1,L)).

flag(Prop) ->
    ?LET(B, bool(), {Prop, B}).

num(Prop) ->
    ?LET(N, nat(), {Prop, N}).

quorum(Prop) ->
    ?LET(V, oneof([one, quorum, all, default, nat()]), {Prop, V}).

backend() ->
    ?LET(B, non_empty(binary()), {backend, B}).

repl() ->
    ?LET(R, oneof([false, realtime, fullsync, true]), {repl, R}).

commit(Prop) ->
    ?LET(C, non_empty(list(commit_hook())), {Prop, C}).

commit_hook() ->
    ?LET(H, oneof([modfun_hook(), name_hook()]), {struct, H}).

modfun_hook() ->
    ?LET({M,F}, {non_empty(binary()), non_empty(binary())},
         [{<<"mod">>, M}, {<<"fun">>, F}]).

name_hook() ->
    ?LET(N, non_empty(binary()), [{<<"name">>, N}]).

chash() ->
    ?LET({M,F}, {atom(), atom()}, {chash_keyfun,{M,F}}).

atom() ->
    ?LET(B, non_empty(binary()), binary_to_atom(B, latin1)).

linkfun() ->
    ?LET({M,F}, {atom(), atom()}, {linkfun, {modfun, M, F}}).

datatype() ->
    {datatype, elements([counter, set, map, hll])}.

yz_index() ->
    {search_index, non_empty(binary())}.
-endif.
