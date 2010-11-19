-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(riak_kv_put_req_v1, {
          bkey :: {binary(),binary()},
          object :: term(),
          req_id :: non_neg_integer(),
          start_time :: non_neg_integer(),
          options :: list()}).

-record(riak_kv_get_req_v1, {
          bkey :: {binary(), binary()},
          req_id :: non_neg_integer()}).

-record(riak_kv_mget_req_v1, {
          bkeys :: list({binary(), binary()}),
          req_id :: non_neg_integer(),
          from :: term()}).

-record(riak_kv_listkeys_req_v1, {
          bucket :: binary(),
          req_id :: non_neg_integer()}).

-record(riak_kv_listkeys_req_v2, {
          bucket :: binary(),
          req_id :: non_neg_integer(),
          caller :: pid()}).

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
-define(KV_GET_REQ, #riak_kv_get_req_v1).
-define(KV_MGET_REQ, #riak_kv_mget_req_v1).
-define(KV_LISTKEYS_REQ, #riak_kv_listkeys_req_v2).
-define(KV_DELETE_REQ, #riak_kv_delete_req_v1).
-define(KV_MAP_REQ, #riak_kv_map_req_v1).
-define(KV_VCLOCK_REQ, #riak_kv_vclock_req_v1).
