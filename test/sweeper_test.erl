%% @author mikael
%% @doc @todo Add description to sweeper_test.


-module(sweeper_test).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(TM, riak_kv_sweeper).


add_remove_participant_test() ->
    setup(),
    Module = riak_kv_index_hashtree,

    mock_riak_kv_vnode(),
    ?TM:add_sweep_participant(Module, [], []),
    ?TM:sweep({0, node()}),
    true = ?TM:remove_sweep_participant(Module),
    cleanup().

mock_riak_kv_vnode() ->
    meck:new(riak_kv_vnode, [passthrough]),
    meck:expect(riak_kv_vnode, sweep, fun(_Preflist, SweepParticipant) -> SweepParticipant end).

setup() ->
    riak_kv_sweeper:start_link().

cleanup() ->
    meck:unload(riak_kv_vnode),
    riak_kv_test_util:stop_process(?TM).

-endif.