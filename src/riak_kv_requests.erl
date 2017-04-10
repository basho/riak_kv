%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------


-module(riak_kv_requests).

%% API
-export([new_put_request/5,
         new_get_request/2,
         new_w1c_put_request/3,
         new_listkeys_request/3,
         new_list_group_request/2,
         new_listbuckets_request/1,
         new_index_request/4,
         new_vnode_status_request/0,
         new_delete_request/2,
         new_map_request/3,
         new_vclock_request/1,
         is_coordinated_put/1,
         get_bucket_key/1,
         get_bucket_keys/1,
         get_bucket/1,
         get_item_filter/1,
         get_group_params/1,
         get_ack_backpressure/1,
         get_query/1,
         get_object/1,
         get_encoded_obj/1,
         get_replica_type/1,
         set_object/2,
         get_request_id/1,
         get_start_time/1,
         get_options/1,
         remove_option/2,
         request_type/1]).

-export_type([put_request/0,
              get_request/0,
              w1c_put_request/0,
              listkeys_request/0,
              list_group_request/0,
              listbuckets_request/0,
              index_request/0,
              vnode_status_request/0,
              delete_request/0,
              map_request/0,
              vclock_request/0,
              request/0,
              request_type/0]).

-type bucket_key() :: {binary(),binary()}.
-type object() :: term().
-type request_id() :: non_neg_integer().
-type start_time() :: non_neg_integer().
-type request_options() :: [any()].
-type replica_type() :: primary | fallback.
-type encoded_obj() :: binary().
-type bucket() :: riak_core_bucket:bucket().
-type item_filter() :: function().
-type group_params() :: proplists:proplist().
-type coverage_filter() :: riak_kv_coverage_filter:filter().
-type query() :: riak_index:query_def().

-record(riak_kv_put_req_v1,
        { bkey :: bucket_key(),
          object :: object(),
          req_id :: request_id(),
          start_time :: start_time(),
          options :: request_options()}).

-record(riak_kv_get_req_v1, {
          bkey :: bucket_key(),
          req_id :: request_id()}).

-record(riak_kv_w1c_put_req_v1, {
    bkey :: bucket_key(),
    encoded_obj :: encoded_obj(),
    type :: replica_type()
    % start_time :: non_neg_integer(), Jon to add?
}).

-record(riak_kv_listkeys_req_v3, {
          bucket :: bucket(),
          item_filter :: item_filter()}).

%% same as _v3, but triggers ack-based backpressure (we switch on the record *name*)
-record(riak_kv_listkeys_req_v4, {
          bucket :: bucket(),
          item_filter :: item_filter()}).

-record(riak_kv_list_group_req_v1, {
    bucket :: bucket(),
    group_params:: group_params()}).

-record(riak_kv_listbuckets_req_v1, {
          item_filter :: item_filter()}).

-record(riak_kv_index_req_v1, {
          bucket :: bucket(),
          item_filter :: coverage_filter(),
          qry :: query()}).

%% same as _v1, but triggers ack-based backpressure
-record(riak_kv_index_req_v2, {
          bucket :: bucket(),
          item_filter :: coverage_filter(),
          qry :: riak_index:query_def()}).

-record(riak_kv_vnode_status_req_v1, {}).

-record(riak_kv_delete_req_v1, {
          bkey :: bucket_key(),
          req_id :: request_id()}).

-record(riak_kv_map_req_v1, {
          bkey :: bucket_key(),
          qterm :: term(),
          keydata :: term(),
          from :: term()}).

-record(riak_kv_vclock_req_v1, {bkeys = [] :: [bucket_key()]}).


-opaque put_request() :: #riak_kv_put_req_v1{}.
-opaque get_request() :: #riak_kv_get_req_v1{}.
-opaque w1c_put_request() :: #riak_kv_w1c_put_req_v1{}.
-opaque listbuckets_request() :: #riak_kv_listbuckets_req_v1{}.
-opaque listkeys_request() :: #riak_kv_listkeys_req_v3{} | #riak_kv_listkeys_req_v4{}.
-opaque list_group_request() :: #riak_kv_list_group_req_v1{}.
-opaque index_request() :: #riak_kv_index_req_v1{} | #riak_kv_index_req_v2{}.
-opaque vnode_status_request() :: #riak_kv_vnode_status_req_v1{}.
-opaque delete_request() :: #riak_kv_delete_req_v1{}.
-opaque map_request() :: #riak_kv_map_req_v1{}.
-opaque vclock_request() :: #riak_kv_vclock_req_v1{}.


-type request() :: put_request()
                 | get_request()
                 | w1c_put_request()
                 | listkeys_request()
                 | list_group_request()
                 | listbuckets_request()
                 | index_request()
                 | vnode_status_request()
                 | delete_request()
                 | map_request()
                 | vclock_request().

-type request_type() :: kv_put_request
                      | kv_get_request
                      | kv_w1c_put_request
                      | kv_listkeys_request
                      | kv_list_group_request
                      | kv_listbuckets_request
                      | kv_index_request
                      | kv_vnode_status_request
                      | kv_delete_request
                      | kv_map_request
                      | kv_vclock_request
                      | unknown.

-spec request_type(request()) -> request_type().
request_type(#riak_kv_put_req_v1{})          -> kv_put_request;
request_type(#riak_kv_get_req_v1{})          -> kv_get_request;
request_type(#riak_kv_w1c_put_req_v1{})      -> kv_w1c_put_request;
request_type(#riak_kv_listkeys_req_v3{})     -> kv_listkeys_request;
request_type(#riak_kv_listkeys_req_v4{})     -> kv_listkeys_request;
request_type(#riak_kv_list_group_req_v1{})   -> kv_list_group_request;
request_type(#riak_kv_listbuckets_req_v1{})  -> kv_listbuckets_request;
request_type(#riak_kv_index_req_v1{})        -> kv_index_request;
request_type(#riak_kv_index_req_v2{})        -> kv_index_request;
request_type(#riak_kv_vnode_status_req_v1{}) -> kv_vnode_status_request;
request_type(#riak_kv_delete_req_v1{})       -> kv_delete_request;
request_type(#riak_kv_map_req_v1{})          -> kv_map_request;
request_type(#riak_kv_vclock_req_v1{})       -> kv_vclock_request;
request_type(_)                              -> unknown.

-spec new_put_request(bucket_key(),
                      object(),
                      request_id(),
                      start_time(),
                      request_options()) -> put_request().
new_put_request(BKey, Object, ReqId, StartTime, Options) ->
    #riak_kv_put_req_v1{bkey = BKey,
                        object = Object,
                        req_id = ReqId,
                        start_time = StartTime,
                        options = Options}.

-spec new_get_request(bucket_key(), request_id()) -> get_request().
new_get_request(BKey, ReqId) ->
    #riak_kv_get_req_v1{bkey = BKey, req_id = ReqId}.

-spec new_w1c_put_request(bucket_key(), encoded_obj(), replica_type()) -> w1c_put_request().
new_w1c_put_request(BKey, EncodedObj, ReplicaType) ->
    #riak_kv_w1c_put_req_v1{bkey = BKey, encoded_obj = EncodedObj, type = ReplicaType}.

-spec new_listkeys_request(bucket(), item_filter(), UseAckBackpressure::boolean()) -> listkeys_request().
new_listkeys_request(Bucket, ItemFilter, true) ->
    #riak_kv_listkeys_req_v4{bucket=Bucket,
                             item_filter=ItemFilter};
new_listkeys_request(Bucket, ItemFilter, false) ->
    #riak_kv_listkeys_req_v3{bucket=Bucket,
                             item_filter=ItemFilter}.

new_list_group_request(Bucket, GroupParams) ->
    #riak_kv_list_group_req_v1{
        bucket=Bucket,
        group_params=GroupParams
    }.

-spec new_listbuckets_request(item_filter()) -> listbuckets_request().
new_listbuckets_request(ItemFilter) ->
    #riak_kv_listbuckets_req_v1{item_filter=ItemFilter}.

-spec new_index_request(bucket(),
                        coverage_filter(),
                        riak_index:query_def(),
                        UseAckBackpressure::boolean())
                       -> index_request().
new_index_request(Bucket, ItemFilter, Query, false) ->
    #riak_kv_index_req_v1{bucket=Bucket,
                         item_filter=ItemFilter,
                         qry=Query};
new_index_request(Bucket, ItemFilter, Query, true) ->
    #riak_kv_index_req_v2{bucket=Bucket,
                          item_filter=ItemFilter,
                          qry=Query}.

-spec new_vnode_status_request() -> vnode_status_request().
new_vnode_status_request() ->
    #riak_kv_vnode_status_req_v1{}.

-spec new_delete_request(bucket_key(), request_id()) -> delete_request().
new_delete_request(BKey, ReqID) ->
    #riak_kv_delete_req_v1{bkey=BKey, req_id=ReqID}.

-spec new_map_request(bucket_key(), term(), term()) -> map_request().
new_map_request(BKey, QTerm, KeyData) ->
    #riak_kv_map_req_v1{bkey=BKey, qterm=QTerm, keydata=KeyData}.

-spec new_vclock_request([bucket_key()]) -> vclock_request().
new_vclock_request(BKeys) ->
    #riak_kv_vclock_req_v1{bkeys=BKeys}.

-spec is_coordinated_put(put_request()) -> boolean().
is_coordinated_put(#riak_kv_put_req_v1{options=Options}) ->
    proplists:get_value(coord, Options, false).

get_bucket_key(#riak_kv_get_req_v1{bkey = BKey}) ->
    BKey;
get_bucket_key(#riak_kv_put_req_v1{bkey = BKey}) ->
    BKey;
get_bucket_key(#riak_kv_w1c_put_req_v1{bkey = BKey}) ->
    BKey;
get_bucket_key(#riak_kv_delete_req_v1{bkey = BKey}) ->
    BKey.

-spec get_bucket_keys(vclock_request()) -> [bucket_key()].
get_bucket_keys(#riak_kv_vclock_req_v1{bkeys = BKeys}) ->
    BKeys.

-spec get_bucket(request()) -> bucket().
get_bucket(#riak_kv_listkeys_req_v3{bucket = Bucket}) ->
    Bucket;
get_bucket(#riak_kv_listkeys_req_v4{bucket = Bucket}) ->
    Bucket;
get_bucket(#riak_kv_list_group_req_v1{bucket = Bucket}) ->
    Bucket;
get_bucket(#riak_kv_index_req_v1{bucket = Bucket}) ->
    Bucket;
get_bucket(#riak_kv_index_req_v2{bucket = Bucket}) ->
    Bucket.


-spec get_item_filter(request()) -> item_filter() | coverage_filter().
get_item_filter(#riak_kv_listkeys_req_v3{item_filter = ItemFilter}) ->
    ItemFilter;
get_item_filter(#riak_kv_listkeys_req_v4{item_filter = ItemFilter}) ->
    ItemFilter;
get_item_filter(#riak_kv_listbuckets_req_v1{item_filter = ItemFilter}) ->
    ItemFilter;
get_item_filter(#riak_kv_index_req_v1{item_filter = ItemFilter}) ->
    ItemFilter;
get_item_filter(#riak_kv_index_req_v2{item_filter = ItemFilter}) ->
    ItemFilter.

-spec get_group_params(request()) -> group_params().
get_group_params(#riak_kv_list_group_req_v1{
    group_params = GroupParams}) ->
    GroupParams.

-spec get_ack_backpressure(listkeys_request() | list_group_request()) ->
    UseAckBackpressure::boolean().
get_ack_backpressure(#riak_kv_listkeys_req_v3{}) ->
    false;
get_ack_backpressure(#riak_kv_listkeys_req_v4{}) ->
    true;
get_ack_backpressure(#riak_kv_list_group_req_v1{}) ->
    true;
get_ack_backpressure(#riak_kv_index_req_v1{}) ->
    false;
get_ack_backpressure(#riak_kv_index_req_v2{}) ->
    true.

-spec get_query(request()) -> query().
get_query(#riak_kv_index_req_v1{qry = Query}) ->
    Query;
get_query(#riak_kv_index_req_v2{qry = Query}) ->
    Query.

-spec get_encoded_obj(request()) -> encoded_obj().
get_encoded_obj(#riak_kv_w1c_put_req_v1{encoded_obj = EncodedObj}) ->
    EncodedObj.

get_object(#riak_kv_put_req_v1{object = Object}) ->
    Object.

-spec get_replica_type(request()) -> replica_type().
get_replica_type(#riak_kv_w1c_put_req_v1{type = Type}) ->
    Type.

-spec get_request_id(request()) -> request_id().
get_request_id(#riak_kv_put_req_v1{req_id = ReqId}) ->
    ReqId;
get_request_id(#riak_kv_get_req_v1{req_id = ReqId}) ->
    ReqId.

get_start_time(#riak_kv_put_req_v1{start_time = StartTime}) ->
    StartTime.

get_options(#riak_kv_put_req_v1{options = Options}) ->
    Options.

set_object(#riak_kv_put_req_v1{}=Req, Object) ->
    Req#riak_kv_put_req_v1{object = Object}.

remove_option(#riak_kv_put_req_v1{options = Options}=Req, Option) ->
    NewOptions = proplists:delete(Option, Options),
    Req#riak_kv_put_req_v1{options = NewOptions}.
