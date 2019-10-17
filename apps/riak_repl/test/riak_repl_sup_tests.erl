-module(riak_repl_sup_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

% Why? Because it's nice to know that system can start.

can_start_test_() ->
    {spawn,
     [
      {setup,
      fun() ->
              error_logger:tty(false),
              %% core features that are needed
              {ok, _Eventer} = riak_core_ring_events:start_link(),
              {ok, _RingMgr} = riak_core_ring_manager:start_link(test),

              %% needed by the leaders
              meck:new(riak_core_node_watcher_events),
              meck:expect(riak_core_node_watcher_events,
                          add_sup_callback,
                          fun(_fn) ->
                                  ok
                          end),
              meck:new(riak_core_node_watcher),
              meck:expect(riak_core_node_watcher, nodes, fun(_) -> [node()] end),

              %% needed by repl itself
              application:start(ranch),
              application:set_env(riak_repl, data_root, ".")
      end,
      fun(_) ->
              process_flag(trap_exit, true),
              riak_core_ring_manager:stop(),
              catch exit(riak_core_ring_events, kill),
              application:stop(ranch),
              meck:unload(riak_core_node_watcher),
              meck:unload(riak_core_node_watcher_events),
              process_flag(trap_exit, false),
              ok
      end,
      fun(_) ->
              [
               fun() ->
                       {ok, Pid} = riak_repl_sup:start_link(),
                       timer:sleep(500),
                       ?assert(is_process_alive(Pid))
               end

              ]
      end
     }]}.

-endif.
