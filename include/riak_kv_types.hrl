-record(crdt, {mod, ctype, value}).
-record(crdt_op, {mod, op, ctx}).

-define(CRDT, #crdt).
-define(CRDT_OP, #crdt_op).

%% Top Level Key->Type Types
-define(COUNTER_TYPE, riak_dt_pncounter).
-define(COUNTER_TYPE(Val), #crdt{mod=?COUNTER_TYPE, ctype="application/riak_counter", value=Val}).

-define(SET_TYPE, riak_dt_vvorset).
-define(SET_TYPE(Val), #crdt{mod=?SET_TYPE, ctype="application/riak_set", value=Val}).

-define(MAP_TYPE, riak_dt_multi).
-define(MAP_TYPE(Val), #crdt{mod=?MAP_TYPE, ctype="application/riak_map", value=Val}).

%% Internal Only Key->Map->Field->Type types
-define(FLAG_TYPE, riak_dt_od_flag).
-define(LWW_TYPE, riak_dt_lwwreg).

-define(TOP_LEVEL_TYPES, [?COUNTER_TYPE, ?SET_TYPE, ?MAP_TYPE]).
