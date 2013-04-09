-module(riak_kv_reformat).

-export([run/2]).

run(ObjectVsn, KillHandoffs) ->
    lager:info("Setting preferred object format to ~p", [ObjectVsn]),
    set_capabilities(ObjectVsn),
    lager:info("Preferred object format set to ~p", [ObjectVsn]),
    case KillHandoffs of
        true ->
            lager:info("killing any inbound and outbound handoffs", []);
        false ->
            lager:info("waiting on any in-flight inbound and outbound handoffs", [])
    end,
    kill_or_wait_on_handoffs(KillHandoffs, 0),

    %% migrate each running vnode
    Running = riak_core_vnode_manager:all_vnodes(riak_kv_vnode),
    Me = node(),
    Counts = riak_core_util:pmap(fun({riak_kv_vnode, Idx, _}) ->
                                         lager:debug("reformatting partition ~p on ~p",
                                                     [Idx, Me]),
                                         reformat_partition(Idx)
                                 end,
                                 Running),
    {SuccessCounts, IgnoredCounts, ErrorCounts} = lists:unzip3(Counts),
    SuccessTotal = lists:sum(SuccessCounts),
    IgnoredTotal = lists:sum(IgnoredCounts),
    ErrorTotal = lists:sum(ErrorCounts),
    lager:info("Completed reformating all partitions to ~p. Success: ~p. Ignored: ~p. Error: ~p",
               [ObjectVsn, SuccessTotal, IgnoredTotal, ErrorTotal]),
    if ErrorTotal > 0 ->
            lager:info("There were errors reformatting ~p keys. Re-run before dowgrading",
                       [ErrorTotal]);
       true -> ok
    end,
    {SuccessTotal, IgnoredTotal, ErrorTotal}.

%% set preferred object format to desired version. Although we could just
%% switch the preference order, removing other versions premptively
%% downgrades the whole cluster (after ring convergence) reducing the
%% amount of data needing to be reformatted on other nodes (under the
%% assumption those other nodes will be downgraded as well)
set_capabilities(Vsn) ->
    riak_core_capability:register({riak_kv, object_format},
                                  [Vsn],
                                  Vsn).


kill_or_wait_on_handoffs(true, _) ->
    riak_core_handoff_manager:kill_handoffs();
kill_or_wait_on_handoffs(false, CheckCount) ->
    case num_running_handoffs() of
        0 -> kill_or_wait_on_handoffs(true, CheckCount);
        N ->
            case CheckCount rem 10 of
                0 -> lager:info("~p handoffs still outstanding", [N]);
                _ -> ok
            end,
            timer:sleep(1000),
            kill_or_wait_on_handoffs(false, CheckCount+1)
    end.

reformat_partition(Idx) ->
    riak_kv_vnode:fold({Idx, node()},
                       fun(BKey,Value,Acc) -> reformat_object(Idx,BKey,Value,Acc) end,
                       {0,0,0}).

reformat_object(Idx, BKey, Value, {SuccessCount, IgnoredCount, ErrorCount}) ->
    case riak_object:binary_version(Value) of
        v0 -> {SuccessCount, IgnoredCount+1, ErrorCount};
        %% TODO: accumulate and handle errors
        _ ->
            case riak_kv_vnode:reformat_object(Idx, BKey) of
                ok -> {SuccessCount+1, IgnoredCount, ErrorCount};
                {error, not_found} -> {SuccessCount, IgnoredCount+1, ErrorCount};
                {error, _} -> {SuccessCount, IgnoredCount, ErrorCount+1}
            end
    end.

num_running_handoffs() ->
    Receivers=supervisor:count_children(riak_core_handoff_receiver_sup),
    Senders=supervisor:count_children(riak_core_handoff_sender_sup),
    ActiveReceivers=proplists:get_value(active,Receivers),
    ActiveSenders=proplists:get_value(active,Senders),
    ActiveSenders+ActiveReceivers.
