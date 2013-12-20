-record(crdt, {mod, ctype, value}).
-record(crdt_op, {mod, op, ctx}).

-define(CRDT, #crdt).
-define(CRDT_OP, #crdt_op).

%% Top Level Key->Type Types
-define(V1_COUNTER_TYPE, riak_kv_pncounter).
-define(V1_COUNTER_TYPE(Val), #crdt{mod=?V1_COUNTER_TYPE, ctype="application/riak_counter", value=Val}).
-define(COUNTER_TYPE, riak_dt_pncounter).
-define(COUNTER_TYPE(Val), #crdt{mod=?COUNTER_TYPE, ctype="application/riak_counter", value=Val}).

-define(SET_TYPE, riak_dt_orswot).
-define(SET_TYPE(Val), #crdt{mod=?SET_TYPE, ctype="application/riak_set", value=Val}).

-define(MAP_TYPE, riak_dt_map).
-define(MAP_TYPE(Val), #crdt{mod=?MAP_TYPE, ctype="application/riak_map", value=Val}).

%% Internal Only Key->Map->Field->Type types
-define(FLAG_TYPE, riak_dt_od_flag).
-define(REG_TYPE, riak_dt_lwwreg).


-define(V1_TOP_LEVEL_TYPES, [pncounter]).
-define(V2_TOP_LEVEL_TYPES, [?COUNTER_TYPE, ?SET_TYPE, ?MAP_TYPE]).
-define(TOP_LEVEL_TYPES, ?V1_TOP_LEVEL_TYPES ++ ?V2_TOP_LEVEL_TYPES).
-define(ALL_TYPES, ?TOP_LEVEL_TYPES ++ [?FLAG_TYPE, ?REG_TYPE]).
-define(MOD_MAP, [{map, ?MAP_TYPE}, {set, ?SET_TYPE},
                  {counter, ?COUNTER_TYPE}, {flag, ?FLAG_TYPE},
                  {register, ?REG_TYPE}]).

-define(DATATYPE_STATS_DEFAULTS, [actor_count]).

-type crdt() :: ?CRDT{}.
-type crdt_op() :: ?CRDT_OP{}.
