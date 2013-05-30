%% -------------------------------------------------------------------
%%
%% riak_util: functions that are useful throughout Riak
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


%% @doc Various functions that are useful throughout riak_kv.
-module(riak_kv_util).


-export([is_x_deleted/1,
         obj_not_deleted/1,
         try_cast/3,
         fallback/4,
         expand_value/3,
         expand_rw_value/4,
         normalize_rw_value/2,
         make_request/2,
         get_index_n/1,
         preflist_siblings/1,
         fix_incorrect_index_entries/1,
         fix_incorrect_index_entries/0,
         responsible_preflists/1,
         responsible_preflists/2,
         make_vtag/1,
         puts_active/0,
         exact_puts_active/0,
         gets_active/0,
         overload_reply/1]).

-include_lib("riak_kv_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type riak_core_ring() :: riak_core_ring:riak_core_ring().
-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @spec is_x_deleted(riak_object:riak_object()) -> boolean()
%% @doc 'true' if all contents of the input object are marked
%%      as deleted; 'false' otherwise
%% @equiv obj_not_deleted(Obj) == undefined
is_x_deleted(Obj) ->
    case obj_not_deleted(Obj) of
        undefined -> true;
        _ -> false
    end.

%% @spec obj_not_deleted(riak_object:riak_object()) ->
%%          undefined|riak_object:riak_object()
%% @doc Determine whether all contents of an object are marked as
%%      deleted.  Return is the atom 'undefined' if all contents
%%      are marked deleted, or the input Obj if any of them are not.
obj_not_deleted(Obj) ->
    case [{M, V} || {M, V} <- riak_object:get_contents(Obj),
                    dict:is_key(<<"X-Riak-Deleted">>, M) =:= false] of
        [] -> undefined;
        _ -> Obj
    end.

%% @spec try_cast(term(), [node()], [{Index :: term(), Node :: node()}]) ->
%%          {[{Index :: term(), Node :: node(), Node :: node()}],
%%           [{Index :: term(), Node :: node()}]}
%% @doc Cast {Cmd, {Index,Node}, Msg} at riak_kv_vnode_master on Node
%%      if Node is in UpNodes.  The list of successful casts is the
%%      first element of the return tuple, and the list of unavailable
%%      nodes is the second element.  Used in riak_kv_put_fsm and riak_kv_get_fsm.
try_cast(Msg, UpNodes, Targets) ->
    try_cast(Msg, UpNodes, Targets, [], []).
try_cast(_Msg, _UpNodes, [], Sent, Pangs) -> {Sent, Pangs};
try_cast(Msg, UpNodes, [{Index,Node}|Targets], Sent, Pangs) ->
    case lists:member(Node, UpNodes) of
        false ->
            try_cast(Msg, UpNodes, Targets, Sent, [{Index,Node}|Pangs]);
        true ->
            gen_server:cast({riak_kv_vnode_master, Node}, make_request(Msg, Index)),
            try_cast(Msg, UpNodes, Targets, [{Index,Node,Node}|Sent],Pangs)
    end.

%% @spec fallback(term(), term(), [{Index :: term(), Node :: node()}],
%%                [{any(), Fallback :: node()}]) ->
%%         [{Index :: term(), Node :: node(), Fallback :: node()}]
%% @doc Cast {Cmd, {Index,Node}, Msg} at a node in the Fallbacks list
%%      for each node in the Pangs list.  Pangs should have come
%%      from the second element of the response tuple of a call to
%%      try_cast/3.
%%      Used in riak_kv_put_fsm and riak_kv_get_fsm

fallback(Cmd, UpNodes, Pangs, Fallbacks) ->
    fallback(Cmd, UpNodes, Pangs, Fallbacks, []).
fallback(_Cmd, _UpNodes, [], _Fallbacks, Sent) -> Sent;
fallback(_Cmd, _UpNodes, _Pangs, [], Sent) -> Sent;
fallback(Cmd, UpNodes, [{Index,Node}|Pangs], [{_,FN}|Fallbacks], Sent) ->
    case lists:member(FN, UpNodes) of
        false -> fallback(Cmd, UpNodes, [{Index,Node}|Pangs], Fallbacks, Sent);
        true ->
            gen_server:cast({riak_kv_vnode_master, FN}, make_request(Cmd, Index)),
            fallback(Cmd, UpNodes, Pangs, Fallbacks, [{Index,Node,FN}|Sent])
    end.


-spec make_request(vnode_req(), partition()) -> #riak_vnode_req_v1{}.
make_request(Request, Index) ->
    riak_core_vnode_master:make_request(Request,
                                        {fsm, undefined, self()},
                                        Index).

get_bucket_option(Type, BucketProps) ->
    case proplists:get_value(Type, BucketProps, default) of
        default ->
            {ok, DefaultProps} = application:get_env(riak_core, default_bucket_props),
            proplists:get_value(Type, DefaultProps, error);
        Val -> Val
    end.

expand_value(Type, default, BucketProps) ->
    get_bucket_option(Type, BucketProps);
expand_value(_Type, Value, _BucketProps) ->
    Value.

expand_rw_value(Type, default, BucketProps, N) ->
    normalize_rw_value(get_bucket_option(Type, BucketProps), N);
expand_rw_value(_Type, Val, _BucketProps, N) ->
    normalize_rw_value(Val, N).

normalize_rw_value(RW, _N) when is_integer(RW) -> RW;
normalize_rw_value(RW, N) when is_binary(RW) ->
    try
        ExistingAtom = binary_to_existing_atom(RW, utf8),
        normalize_rw_value(ExistingAtom, N)
    catch _:badarg ->
        error
    end;
normalize_rw_value(one, _N) -> 1;
normalize_rw_value(quorum, N) -> erlang:trunc((N/2)+1);
normalize_rw_value(all, N) -> N;
normalize_rw_value(_, _) -> error.

%% ===================================================================
%% Preflist utility functions
%% ===================================================================

%% @doc Given a bucket/key, determine the associated preflist index_n.
-spec get_index_n({binary(), binary()}) -> index_n().
get_index_n({Bucket, Key}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    N = proplists:get_value(n_val, BucketProps),
    ChashKey = riak_core_util:chash_key({Bucket, Key}),
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    Index = chashbin:responsible_index(ChashKey, CHBin),
    {Index, N}.

%% @doc Given an index, determine all sibling indices that participate in one
%%      or more preflists with the specified index.
-spec preflist_siblings(index()) -> [index()].
preflist_siblings(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    preflist_siblings(Index, Ring).

%% @doc See {@link preflist_siblings/1}.
-spec preflist_siblings(index(), riak_core_ring()) -> [index()].
preflist_siblings(Index, Ring) ->
    MaxN = determine_max_n(Ring),
    preflist_siblings(Index, MaxN, Ring).

-spec preflist_siblings(index(), pos_integer(), riak_core_ring()) -> [index()].
preflist_siblings(Index, N, Ring) ->
    IndexBin = <<Index:160/integer>>,
    PL = riak_core_ring:preflist(IndexBin, Ring),
    Indices = [Idx || {Idx, _} <- PL],
    RevIndices = lists:reverse(Indices),
    {Succ, _} = lists:split(N-1, Indices),
    {Pred, _} = lists:split(N-1, tl(RevIndices)),
    lists:reverse(Pred) ++ Succ.

-spec responsible_preflists(index()) -> [index_n()].
responsible_preflists(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    responsible_preflists(Index, Ring).

-spec responsible_preflists(index(), riak_core_ring()) -> [index_n()].
responsible_preflists(Index, Ring) ->
    AllN = determine_all_n(Ring),
    responsible_preflists(Index, AllN, Ring).

-spec responsible_preflists(index(), [pos_integer(),...], riak_core_ring())
                           -> [index_n()].
responsible_preflists(Index, AllN, Ring) ->
    IndexBin = <<Index:160/integer>>,
    PL = riak_core_ring:preflist(IndexBin, Ring),
    Indices = [Idx || {Idx, _} <- PL],
    RevIndices = lists:reverse(Indices),
    lists:flatmap(fun(N) ->
                          responsible_preflists_n(RevIndices, N)
                  end, AllN).

-spec responsible_preflists_n([index()], pos_integer()) -> [index_n()].
responsible_preflists_n(RevIndices, N) ->
    {Pred, _} = lists:split(N, RevIndices),
    [{Idx, N} || Idx <- lists:reverse(Pred)].

-spec determine_max_n(riak_core_ring()) -> pos_integer().
determine_max_n(Ring) ->
    lists:max(determine_all_n(Ring)).

-spec determine_all_n(riak_core_ring()) -> [pos_integer(),...].
determine_all_n(Ring) ->
    Buckets = riak_core_ring:get_buckets(Ring),
    BucketProps = [riak_core_bucket:get_bucket(Bucket, Ring) || Bucket <- Buckets],
    Default = app_helper:get_env(riak_core, default_bucket_props),
    DefaultN = proplists:get_value(n_val, Default),
    AllN = lists:foldl(fun(Props, AllN) ->
                               N = proplists:get_value(n_val, Props),
                               ordsets:add_element(N, AllN)
                       end, [DefaultN], BucketProps),
    AllN.

fix_incorrect_index_entries() ->
    fix_incorrect_index_entries([]).

fix_incorrect_index_entries(Opts) when is_list(Opts) ->
    MaxN = proplists:get_value(concurrency, Opts, 2),
    ForUpgrade = not proplists:get_value(downgrade, Opts, false),
    BatchSize = proplists:get_value(batch_size, Opts, 100),
    lager:info("index reformat: starting with concurrency: ~p, batch size: ~p, for upgrade: ~p",
               [MaxN, BatchSize, ForUpgrade]),
    IdxList = [Idx || {riak_kv_vnode, Idx, _} <- riak_core_vnode_manager:all_vnodes()],
    FixOpts = [{batch_size, BatchSize}, {downgrade, not ForUpgrade}],
    F = fun(X) -> fix_incorrect_index_entries(X, FixOpts) end,
    Counts = riak_core_util:pmap(F, IdxList, MaxN),
    {SuccessCounts, IgnoredCounts, ErrorCounts} = lists:unzip3(Counts),
    SuccessTotal = lists:sum(SuccessCounts),
    IgnoredTotal = lists:sum(IgnoredCounts),
    ErrorTotal = lists:sum(ErrorCounts),
    case ErrorTotal of
        0 ->
            lager:info("index reformat: complete on all partitions. Fixed: ~p, Ignored: ~p",
                       [SuccessTotal, IgnoredTotal]);
        _ ->
            lager:info("index reformat: encountered ~p errors reformatting keys. Please re-run",
                       [ErrorTotal])
    end,
    {SuccessTotal, IgnoredTotal, ErrorTotal}.

fix_incorrect_index_entries(Idx, FixOpts) ->
    fix_incorrect_index_entries(Idx, fun fix_incorrect_index_entry/4, {0, 0, 0}, FixOpts).

fix_incorrect_index_entries(Idx, FixFun, Acc0, FixOpts) ->
    Ref = make_ref(),
    ForUpgrade = not proplists:get_value(downgrade, FixOpts, false),
    lager:info("index reformat: querying partition ~p for index entries to reformat", [Idx]),
    riak_core_vnode_master:command({Idx, node()},
                                   {get_index_entries, FixOpts},
                                   {raw, Ref, self()},
                                   riak_kv_vnode_master),
    case process_incorrect_index_entries(Ref, Idx, ForUpgrade, FixFun, Acc0) of
        ignore -> Acc0;
        {_,_,ErrorCount}=Res ->
            MarkRes = mark_indexes_reformatted(Idx, ErrorCount, ForUpgrade),
            case MarkRes of
                error ->
                    %% there was an error marking the partition as reformatted. treat this like
                    %% any other error (indicating the need to re-run reformatting)
                    {element(1, Res), element(2, Res), 1};
                _ -> Res
            end
    end.

fix_incorrect_index_entry(Idx, ForUpgrade, BadKeys, {Success, Ignore, Error}) ->
    Res = riak_core_vnode_master:sync_command({Idx, node()},
                                              {fix_incorrect_index_entry, BadKeys, ForUpgrade},
                                              riak_kv_vnode_master),
    case Res of
        ok ->
            {Success+1, Ignore, Error};
        ignore ->
            {Success, Ignore+1, Error};
        {error, _} ->
            {Success, Ignore, Error+1};
        {S, I, E} ->
            {Success+S, Ignore+I, Error+E}
    end.

%% needs to take an acc to count success/error/ignore
process_incorrect_index_entries(Ref, Idx, ForUpgrade, FixFun, {S, I, E} = Acc) ->
    receive
        {Ref, ignore} ->
            lager:info("index reformat: ignoring partition ~p", [Idx]),
            ignore;
        {Ref, done} ->
            lager:info("index reformat: finished with partition ~p, Fixed=~p, Ignored=~p, Errors=~p", [Idx, S, I, E]),
            Acc;
        {Ref, {Pid, BatchRef, Keys}} ->
            {NS, NI, NE} = NextAcc = FixFun(Idx, ForUpgrade, Keys, Acc),
            ReportN = 10000,
            case ((NS+NI+NE) div ReportN) /= ((S+I+E) div ReportN) of
               true ->
                    lager:info("index reformat: reformatting partition ~p, Fixed=~p, Ignore=~p, Error=~p", [Idx, NS, NI, NE]);
                false ->
                    ok
            end,
            ack_incorrect_keys(Pid, BatchRef),
            process_incorrect_index_entries(Ref, Idx, ForUpgrade, FixFun, NextAcc)
    end.

ack_incorrect_keys(Pid, Ref) ->
    Pid ! {ack_keys, Ref}.

mark_indexes_reformatted(Idx, 0, ForUpgrade) ->
    riak_core_vnode_master:sync_command({Idx, node()},
                                        {fix_incorrect_index_entry, {done, ForUpgrade}},
                                        riak_kv_vnode_master),
    lager:info("index reformat: marked partition ~p as fixed", [Idx]),
    ok;
mark_indexes_reformatted(_Idx, _ErrorCount, _ForUpgrade) ->
    undefined.

%% @Doc vtag creation function
-spec make_vtag(erlang:timestamp()) -> list().
make_vtag(Now) ->
    <<HashAsNum:128/integer>> = crypto:md5(term_to_binary({node(), Now})),
    riak_core_util:integer_to_list(HashAsNum,62).

overload_reply({raw, ReqId, Pid}) ->
    Pid ! {ReqId, {error, overload}};
overload_reply(_) ->
    ok.

puts_active() ->
    case whereis(riak_kv_put_fsm_sj) of
        undefined ->
            riak_kv_get_put_monitor:puts_active();
        _ ->
            sidejob_resource_stats:usage(riak_kv_put_fsm_sj)
    end.

exact_puts_active() ->
    case whereis(riak_kv_put_fsm_sj) of
        undefined ->
            riak_kv_get_put_monitor:puts_active();
        _ ->
            length(sidejob_supervisor:which_children(riak_kv_put_fsm_sj))
    end.

gets_active() ->
    case whereis(riak_kv_get_fsm_sj) of
        undefined ->
            riak_kv_get_put_monitor:gets_active();
        _ ->
            sidejob_resource_stats:usage(riak_kv_get_fsm_sj)
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

normalize_test() ->
    3 = normalize_rw_value(3, 3),
    1 = normalize_rw_value(one, 3),
    2 = normalize_rw_value(quorum, 3),
    3 = normalize_rw_value(all, 3),
    1 = normalize_rw_value(<<"one">>, 3),
    2 = normalize_rw_value(<<"quorum">>, 3),
    3 = normalize_rw_value(<<"all">>, 3),
    error = normalize_rw_value(garbage, 3),
    error = normalize_rw_value(<<"garbage">>, 3).


deleted_test() ->
    O = riak_object:new(<<"test">>, <<"k">>, "v"),
    false = is_x_deleted(O),
    MD = dict:new(),
    O1 = riak_object:apply_updates(
           riak_object:update_metadata(
             O, dict:store(<<"X-Riak-Deleted">>, true, MD))),
    true = is_x_deleted(O1).

make_vtag_test() ->
    crypto:start(),
    ?assertNot(make_vtag(now()) =:=
               make_vtag(now())).

-endif.
