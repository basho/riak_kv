-ifdef(namespaced_types).
-type riak_dt_dict() :: dict:dict().
-else.
-type riak_dt_dict() :: dict().
-endif.

-type riak_dt_set() :: ordsets:ordset(_).
