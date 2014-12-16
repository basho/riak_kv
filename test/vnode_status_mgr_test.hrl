%% The `counter_state' and `state' record fields were directly
%% extracted from `riak_kv_vnode'.
-record(counter_state, {
          %% The number of new epoch writes co-ordinated by this vnode
          %% What even is a "key epoch?" It is any time a key is
          %% (re)created. A new write, a write not yet coordinated by
          %% this vnode, a write where local state is unreadable.
          cnt = 0 :: non_neg_integer(),
          %% Counter leased up-to. For totally new state/id
          %% this will be that flush threshold See config value
          %% `{riak_kv, counter_lease_size}'
          lease = 0 :: non_neg_integer(),
          lease_size = 0 :: non_neg_integer(),
          %% Has a lease been requested but not granted yet
          leasing = false :: boolean()
         }).

-record(state, {vnodeid :: undefined | binary(),
                counter :: #counter_state{},
                status_mgr_pid :: pid()}).
-type state() :: #state{}.
-type driver_state() :: #state{}.
