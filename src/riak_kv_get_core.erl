%% -------------------------------------------------------------------
%%
%% riak_kv_get_core: Riak get logic
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
-module(riak_kv_get_core).
-export([init/6, add_result/3, result_shortcode/1, enough/1, response/1,
         has_all_results/1, final_action/1, info/1]).
-export_type([getcore/0, result/0, reply/0, final_action/0]).

-type result() :: {ok, riak_object:riak_object()} |
                  {error, notfound} | % for dialyzer
                  {error, any()}.
-type reply() :: {ok, riak_object:riak_object()} |
                 {error, notfound} |
                 {error, any()}.
-type final_action() :: nop |
                        {read_repair, [non_neg_integer()], riak_object:riak_object()} |
                        delete.
-type idxresult() :: {non_neg_integer(), result()}.

-record(getcore, {n :: pos_integer(),
                  r :: pos_integer(),
                  fail_threshold :: pos_integer(),
                  notfound_ok :: boolean(),
                  allow_mult :: boolean(),
                  deletedvclock :: boolean(),
                  results = [] :: [idxresult()],
                  merged :: {notfound | tombstone | ok,
                             riak_object:riak_object() | undefined},
                  num_ok = 0 :: non_neg_integer(),
                  num_notfound = 0 :: non_neg_integer(),
                  num_fail = 0 :: non_neg_integer()}).
-opaque getcore() :: #getcore{}.

%% ====================================================================
%% Public API
%% ====================================================================

%% Initialize a get and return an opaque get core context
-spec init(pos_integer(), pos_integer(), pos_integer(), boolean(), boolean(),
           boolean()) -> getcore().
init(N, R, FailThreshold, NotFoundOk, AllowMult, DeletedVClock) ->
    #getcore{n = N,
             r = R,
             fail_threshold = FailThreshold,
             notfound_ok = NotFoundOk,
             allow_mult = AllowMult,
             deletedvclock = DeletedVClock}.

%% Add a result for a vnode index
-spec add_result(non_neg_integer(), result(), getcore()) -> getcore().
add_result(Idx, Result, GetCore = #getcore{results = Results}) ->
    UpdResults = [{Idx, Result} | Results],
    case Result of
        {ok, _RObj} ->
            GetCore#getcore{results = UpdResults, merged = undefined,
                            num_ok = GetCore#getcore.num_ok + 1};
        {error, notfound} ->
            case GetCore#getcore.notfound_ok of
                true ->
                    GetCore#getcore{results = UpdResults, merged = undefined,
                                    num_ok = GetCore#getcore.num_ok + 1};
                _ ->
                    GetCore#getcore{results = UpdResults, merged = undefined,
                                    num_notfound = GetCore#getcore.num_notfound + 1}
            end;
        {error, _Reason} ->
            GetCore#getcore{results = UpdResults, merged = undefined,
                            num_fail = GetCore#getcore.num_fail + 1}
    end.

result_shortcode({ok, _RObj})       -> 1;
result_shortcode({error, notfound}) -> 0;
result_shortcode(_)                 -> -1.

%% Check if enough results have been added to respond 
-spec enough(getcore()) -> boolean().
enough(#getcore{r = R, num_ok = NumOk,
                num_notfound = NumNotFound,
                num_fail = NumFail,
                fail_threshold = FailThreshold}) ->
    if
        NumOk >= R ->
            true;
        NumNotFound + NumFail >= FailThreshold ->
            true;
        true ->
            false
    end.

%% Get success/fail response once enough results received
-spec response(getcore()) -> {reply(), getcore()}.
response(GetCore = #getcore{r = R, num_ok = NumOk, num_notfound = NumNotFound,
                            results = Results, allow_mult = AllowMult,
                            deletedvclock = DeletedVClock}) ->
    {ObjState, MObj} = Merged = merge(Results, AllowMult),
    Reply = case NumOk >= R of
                true ->
                    case ObjState of
                        ok ->
                            Merged; % {ok, MObj}
                        tombstone when DeletedVClock ->
                            {error, {deleted, riak_object:vclock(MObj)}};
                        _ -> % tombstone or notfound
                            {error, notfound}
                    end;
                false ->
                    DelObjs = length([xx || {_Idx, {ok, RObj}} <- Results,
                                            riak_kv_util:is_x_deleted(RObj)]),
                    Fails = [F || F = {_Idx, {error, Reason}} <- Results,
                                  Reason /= notfound],
                    fail_reply(R, NumOk, NumOk - DelObjs, 
                               NumNotFound + DelObjs, Fails)
            end,
    {Reply, GetCore#getcore{merged = Merged}}.

%% Check if all expected results have been added
-spec has_all_results(getcore()) -> boolean().
has_all_results(#getcore{n = N, num_ok = NOk, 
                         num_fail = NFail, num_notfound = NNF}) ->
    NOk + NFail + NNF >= N.

%% Decide on any post-response actions
%% nop - do nothing
%% {readrepair, Indices, MObj} - send read repairs iff any vnode has ancestor data 
%%                               (including tombstones)
%% delete - issue deletes if all vnodes returned tombstones.  This needs to be
%%          supplemented with a check that the vnodes were all primaries.
%% 
-spec final_action(getcore()) -> {final_action(), getcore()}.
final_action(GetCore = #getcore{n = N, merged = Merged0, results = Results,
                                allow_mult = AllowMult}) ->
    Merged = case Merged0 of
                 undefined ->
                     merge(Results, AllowMult);
                 _ ->
                     Merged0
             end,
    {ObjState, MObj} = Merged,
    ReadRepairs = case ObjState of
                      notfound ->
                          [];
                      _ -> % ok or tombstone
                          [Idx || {Idx, {ok, RObj}} <- Results, 
                                  strict_descendant(MObj, RObj)] ++
                              [Idx || {Idx, {error, notfound}} <- Results]
                  end,
    Action = case ReadRepairs of
                 [] when ObjState == tombstone ->
                     %% Allow delete if merge object is deleted,
                     %% there are no read repairs pending and
                     %% a value was received from all vnodes
                     case riak_kv_util:is_x_deleted(MObj) andalso 
                         length([xx || {_Idx, {ok, _RObj}} <- Results]) == N of
                         true ->
                             delete;
                         _ ->
                             nop
                     end;
                 [] ->
                     nop;
                 _ ->
                     {read_repair, ReadRepairs, MObj}
             end,
    {Action, GetCore#getcore{merged = Merged}}.

%% Return request info
-spec info(undefined | getcore()) -> [{vnode_oks, non_neg_integer()} | 
                                      {vnode_errors, [any()]}].
                  
info(undefined) -> 
    []; % make uninitialized case easier
info(#getcore{num_ok = NumOks, num_fail = NumFail, results = Results}) ->
    Oks = [{vnode_oks, NumOks}],
    case NumFail of
        0 ->
            Oks;
        _ ->
            Errors = [Reason || {_Idx, {error, Reason}} <- Results,
                                Reason /= undefined],
            [{vnode_errors, Errors} | Oks]
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

strict_descendant(O1, O2) ->
    vclock:descends(riak_object:vclock(O1),riak_object:vclock(O2)) andalso
    not vclock:descends(riak_object:vclock(O2),riak_object:vclock(O1)).
        
merge(Replies, AllowMult) ->
    RObjs = [RObj || {_I, {ok, RObj}} <- Replies],
    case RObjs of
        [] ->
            {notfound, undefined};
        _ ->
            Merged = riak_object:reconcile(RObjs, AllowMult), % include tombstones
            case riak_kv_util:is_x_deleted(Merged) of
                true ->
                    {tombstone, Merged};
                _ ->
                    {ok, Merged}
            end
    end.

fail_reply(_R, _NumR, 0, NumNotFound, []) when NumNotFound > 0 ->
    {error, notfound};
fail_reply(R, NumR, _NumNotDeleted, _NumNotFound, Fails) ->
    case [x || {_,{error, overload}} <- Fails] of
        [] ->
            {error, {r_val_unsatisfied, R,  NumR}};
        _->
            {error, overload}
    end.



