-include_lib("riak_core/include/riak_core_vnode.hrl").


-record(riak_kv_w1c_put_reply_v1, {
    reply :: ok | {error, term()},
    type :: primary | fallback
}).

-record(riak_kv_w1c_batch_put_reply_v1, {
    reply :: ok | {error, term()},
    type :: primary | fallback
}).

%% this is a legacy request *potentially* handled via riak_core_vnode_master
%% we are not refactoring it because we think it likely should be deleted.
%% TODO: investigate whether it *can* be deleted
-record(riak_kv_listkeys_req_v2, {
          bucket :: binary()|'_'|tuple(),
          req_id :: non_neg_integer(),
          caller :: pid()}).

-define(KV_W1C_PUT_REPLY, #riak_kv_w1c_put_reply_v1).
-define(KV_W1C_BATCH_PUT_REPLY, #riak_kv_w1c_batch_put_reply_v1).

%% @doc vnode_lock(PartitionIndex) is a kv per-vnode lock, used possibly,
%% by AAE tree rebuilds, fullsync, and handoff.
%% See @link riak_core_background_mgr:get_lock/1
-define(KV_VNODE_LOCK(Idx), {vnode_lock, Idx}).
