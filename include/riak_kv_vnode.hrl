-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(riak_kv_put_req_v1, {
          bkey :: {binary(),binary()},
          object :: term(),
          req_id :: non_neg_integer(),
          start_time :: non_neg_integer(),
          options :: list()}).

-record(riak_kv_w1c_put_req_v1, {
    bkey :: {binary(),binary()},
    encoded_obj :: binary(),
    type :: primary | fallback
    % start_time :: non_neg_integer(), Jon to add?
}).

-record(riak_kv_w1c_put_reply_v1, {
    reply :: ok | {error, term()},
    type :: primary | fallback
}).

%% Currently only for timeseries batches
-record(riak_kv_w1c_batch_put_req_v1, {
    objs :: list({{binary(), binary()}, binary()}),
    type :: primary | fallback
}).

-record(riak_kv_w1c_batch_put_reply_v1, {
    reply :: ok | {error, term()},
    type :: primary | fallback
}).

-record(riak_kv_get_req_v1, {
          bkey :: {binary(), binary()},
          req_id :: non_neg_integer()}).

-record(riak_kv_listkeys_req_v2, {
          bucket :: binary()|'_'|tuple(),
          req_id :: non_neg_integer(),
          caller :: pid()}).

-record(riak_kv_listkeys_req_v3, {
          bucket :: binary() | tuple(),
          item_filter :: function()}).

%% same as _v3, but triggers ack-based backpressure
-record(riak_kv_listkeys_req_v4, {
          bucket :: binary() | tuple(),
          item_filter :: function()}).

-record(riak_kv_listbuckets_req_v1, {
          item_filter :: function()}).

-record(riak_kv_index_req_v1, {
          bucket :: binary() | tuple(),
          item_filter :: riak_kv_coverage_filter:filter(),
          qry :: riak_index:query_def()}).

%% same as _v1, but triggers ack-based backpressure
-record(riak_kv_index_req_v2, {
          bucket :: binary() | tuple(),
          item_filter :: riak_kv_coverage_filter:filter(),
          qry :: riak_index:query_def()}).

%% TODO get riak_ql types included properly somehow
-record(riak_kv_sql_select_req_v1, {
          bucket :: {binary(), binary()},
          qry}).

-record(riak_kv_vnode_status_req_v1, {}).

-record(riak_kv_delete_req_v1, {
          bkey :: {binary(), binary()},
          req_id :: non_neg_integer()}).

-record(riak_kv_map_req_v1, {
          bkey :: {binary(), binary()},
          qterm :: term(),
          keydata :: term(),
          from :: term()}).

-record(riak_kv_vclock_req_v1, {
          bkeys = [] :: [{binary(), binary()}]
         }).

-define(KV_PUT_REQ, #riak_kv_put_req_v1).
-define(KV_W1C_PUT_REQ, #riak_kv_w1c_put_req_v1).
-define(KV_W1C_PUT_REPLY, #riak_kv_w1c_put_reply_v1).
-define(KV_W1C_BATCH_PUT_REQ, #riak_kv_w1c_batch_put_req_v1).
-define(KV_W1C_BATCH_PUT_REPLY, #riak_kv_w1c_batch_put_reply_v1).
-define(KV_GET_REQ, #riak_kv_get_req_v1).
-define(KV_LISTBUCKETS_REQ, #riak_kv_listbuckets_req_v1).
-define(KV_LISTKEYS_REQ, #riak_kv_listkeys_req_v4).
-define(KV_INDEX_REQ, #riak_kv_index_req_v2).
-define(KV_VNODE_STATUS_REQ, #riak_kv_vnode_status_req_v1).
-define(KV_DELETE_REQ, #riak_kv_delete_req_v1).
-define(KV_MAP_REQ, #riak_kv_map_req_v1).
-define(KV_VCLOCK_REQ, #riak_kv_vclock_req_v1).

%% @doc vnode_lock(PartitionIndex) is a kv per-vnode lock, used possibly,
%% by AAE tree rebuilds, fullsync, and handoff.
%% See @link riak_core_background_mgr:get_lock/1
-define(KV_VNODE_LOCK(Idx), {vnode_lock, Idx}).
