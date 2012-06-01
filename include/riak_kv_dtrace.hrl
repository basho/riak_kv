-include_lib("riak_core/include/riak_core_dtrace.hrl").

%% Main wrapper macro for DTrace/SystemTap probe annotations
%% NOTE: We assume there will be per-module dtrace_int() and dtrace() funcs!

-define(DTRACE(Category, Ints, Strings),
        dtrace_int(Category, Ints, Strings)).

%% Probe categories
-define(C_GET_FSM_INIT,               500).
-define(C_GET_FSM_PREPARE,            501).
-define(C_GET_FSM_VALIDATE,           502).
-define(C_GET_FSM_EXECUTE,            503).
-define(C_GET_FSM_PREFLIST,           504).
-define(C_GET_FSM_WAITING_R,          505).
-define(C_GET_FSM_WAITING_R_TIMEOUT,  506).
-define(C_GET_FSM_CLIENT_REPLY,       507).
-define(C_GET_FSM_FINALIZE,           508).
-define(C_GET_FSM_MAYBE_DELETE,       509).
-define(C_GET_FSM_RR,                 510).
-define(C_GET_FSM_WAITING_RR,         511).
-define(C_GET_FSM_WAITING_RR_TIMEOUT, 512).

-define(C_PUT_FSM_INIT,               520).
-define(C_PUT_FSM_PREPARE,            521).
-define(C_PUT_FSM_VALIDATE,           522).
-define(C_PUT_FSM_PRECOMMIT,          523).
-define(C_PUT_FSM_EXECUTE_LOCAL,      524).
-define(C_PUT_FSM_WAITING_LOCAL_VNODE, 525).
-define(C_PUT_FSM_EXECUTE_REMOTE,     526).
-define(C_PUT_FSM_WAITING_REMOTE_VNODE, 527).
-define(C_PUT_FSM_PROCESS_REPLY,      528).
-define(C_PUT_FSM_POSTCOMMIT,         529).
-define(C_PUT_FSM_FINISH,             530).
-define(C_PUT_FSM_DECODE_PRECOMMIT,   531).     % errors only
-define(C_PUT_FSM_DECODE_POSTCOMMIT,  532).     % errors only

-define(C_DELETE_INIT1,               535).
-define(C_DELETE_INIT2,               536).
-define(C_DELETE_REAPER_GET_DONE,     537).

-define(C_BUCKETS_INIT,               540).
-define(C_BUCKETS_PROCESS_RESULTS,    541).
-define(C_BUCKETS_FINISH,             542).

-define(C_KEYS_INIT,                  545).
-define(C_KEYS_PROCESS_RESULTS,       546).
-define(C_KEYS_FINISH,                547).
