-record(crdt, {mod, ctype, value}).
-record(crdt_op, {mod, op}).

-define(CRDT, #crdt).
-define(CRDT_OP, #crdt_op).

-define(COUNTER_TYPE, riak_dt_pncounter).
-define(COUNTER_TYPE(Val), #crdt{mod=riak_dt_pncounter, ctype="application/riak_counter", value=Val}).

-define(SET_TYPE, riak_dt_vvorset).
-define(SET_TYPE(Val), #crdt{mod=riak_dt_vvorset, ctype="application/riak_orset", value=Val}).

-define(LWW_TYPE, riak_dt_lwwreg).
-define(LWW_TYPE(Val), #crdt{mod=riak_dt_lwwreg, ctype="application/riak_lwwreg", value=Val}).

-define(MAP_TYPE, riak_dt_multi).
-define(MAP_TYPE(Val), #crdt{mod=riak_dt_multi, ctype="application/riak_map", value=Val}).

-define(KNOWN_TYPES, [?COUNTER_TYPE, ?SET_TYPE, ?LWW_TYPE, ?MAP_TYPE]).
