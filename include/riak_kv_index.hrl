%% Index query records
-record(riak_kv_index_v2, {
          start_key= <<>> :: binary(),
          filter_field :: binary() | undefined,
          start_term :: binary() | undefined, %% Note, in a $key query, start_key==start_term
          end_term :: binary() | undefined, %% Note, in an eq query, start==end
          return_terms=true :: boolean(), %% Note, should be false for an equals query
          start_inclusive=true :: boolean(),
          end_inclusive=true :: boolean(),
          return_body=false ::boolean() %% Note, only for riak cs bucket folds
         }).

-define(KV_INDEX_Q, #riak_kv_index_v2).
