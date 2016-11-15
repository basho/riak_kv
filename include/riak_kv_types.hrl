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

-define(HLL_TYPE, riak_kv_hll).
-define(HLL_TYPE(Val), #crdt{mod=?HLL_TYPE,
                             ctype="application/riak_hll",
                             value=Val}).
-define(GSET_TYPE, riak_dt_gset).
-define(GSET_TYPE(Val), #crdt{mod=?GSET_TYPE, ctype="application/riak_gset", value=Val}).

-define(MAP_TYPE, riak_dt_map).
-define(MAP_TYPE(Val), #crdt{mod=?MAP_TYPE, ctype="application/riak_map", value=Val}).

%% Internal Only Key->Map->Field->Type types
-define(FLAG_TYPE, riak_dt_od_flag).
-define(REG_TYPE, riak_dt_lwwreg).
-define(EMCNTR_TYPE, riak_dt_emcntr).

-define(V1_TOP_LEVEL_TYPES, [pncounter]).
-define(V2_TOP_LEVEL_TYPES, [?COUNTER_TYPE, ?SET_TYPE, ?MAP_TYPE]).
-define(V3_TOP_LEVEL_TYPES, [?HLL_TYPE]).
-define(V4_TOP_LEVEL_TYPES, [?GSET_TYPE]).
-define(TOP_LEVEL_TYPES, ?V1_TOP_LEVEL_TYPES ++ ?V2_TOP_LEVEL_TYPES ++
            ?V3_TOP_LEVEL_TYPES ++ ?V4_TOP_LEVEL_TYPES).
-define(ALL_TYPES, ?TOP_LEVEL_TYPES ++ [?FLAG_TYPE, ?REG_TYPE]).
-define(EMBEDDED_TYPES, [{map, ?MAP_TYPE}, {set, ?SET_TYPE},
                         {counter, ?EMCNTR_TYPE}, {flag, ?FLAG_TYPE},
                         {register, ?REG_TYPE}]).


-define(MOD_MAP, [{map, ?MAP_TYPE}, {set, ?SET_TYPE},
                  {counter, ?COUNTER_TYPE}, {hll, ?HLL_TYPE}, {gset, ?GSET_TYPE}]).

-define(DATATYPE_STATS_DEFAULTS, [actor_count]).
-define(HLL_STATS, [bytes]).

%% These proplists represent the current versions of supported
%% datatypes. The naming `EN_DATATYPE_VERSIONS' means `Epoch' and
%% number. `N' is incremented when any new version of any datatype is
%% introduced, thus bumping the data type `Epoch'.
-define(E1_DATATYPE_VERSIONS, [{?COUNTER_TYPE, 2}]).
-define(E2_DATATYPE_VERSIONS, [{?MAP_TYPE, 2},
                               {?SET_TYPE, 2},
                               {?COUNTER_TYPE, 2}]).
-define(E3_DATATYPE_VERSIONS, ?E2_DATATYPE_VERSIONS ++ [{?HLL_TYPE, 1}]).
-define(E4_DATATYPE_VERSIONS, ?E2_DATATYPE_VERSIONS ++ [{?HLL_TYPE, 1}, {?GSET_TYPE, 2}]).

-type crdt() :: ?CRDT{}.
-type crdt_op() :: ?CRDT_OP{}.

%% Redis Default Yo Lolz, but a good default.
-define(HYPER_DEFAULT_PRECISION, 14).
