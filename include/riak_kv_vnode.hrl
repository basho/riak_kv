-include_lib("riak_core/include/riak_core_vnode.hrl").


-record(riak_kv_w1c_put_reply_v1, {
    reply :: ok | {error, term()},
    type :: primary | fallback
}).

-record(riak_kv_head_req_v1, {
          bkey :: {binary(), binary()},
          req_id :: non_neg_integer()}).

%% this is a legacy request *potentially* handled via riak_core_vnode_master
%% we are not refactoring it because we think it likely should be deleted.
%% TODO: investigate whether it *can* be deleted
-record(riak_kv_listkeys_req_v2, {
          bucket :: binary()|'_'|tuple(),
          req_id :: non_neg_integer(),
          caller :: pid()}).

-record(riak_kv_mapfold_req_v1, {
            bucket :: binary() | tuple(),
            type :: key | object,
            qry :: riak_index:query_def(),
            query_opts = [] :: list(),
            fold_fun :: riak_kv_backend:fold_objects_fun(),
            init_acc :: any(),
            needs :: list(atom())
            }).



-define(KV_HEAD_REQ, #riak_kv_head_req_v1).
-define(KV_MAPFOLD_REQ, #riak_kv_mapfold_req_v1).
-define(KV_W1C_PUT_REPLY, #riak_kv_w1c_put_reply_v1).

%% @doc vnode_lock(PartitionIndex) is a kv per-vnode lock, used possibly,
%% by AAE tree rebuilds, fullsync, and handoff.
%% See @link riak_core_background_mgr:get_lock/1
-define(KV_VNODE_LOCK(Idx), {vnode_lock, Idx}).

-define(ENABLE_TICTACAAE, true).
-define(PARALLEL_AAEORDER, leveled_ko).
-define(REBUILD_SCHEDULE, {120, 14400}).
