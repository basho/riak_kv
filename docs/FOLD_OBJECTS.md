# Extending Coverage Folds

## Background

This is work prompted initially by the need to support a coverage fold for [active-anti-entropy and multi-data-centre replication](https://github.com/martinsumner/leveled/blob/master/docs/ANTI_ENTROPY.md).  To support this feature there is a need to run a coverage fold across objects (or heads in the case of leveled), one that can be potentially throttled by using the core node_worker_pool rather than the vnode_worker_pool, can fold over indexes as well as keys/objects, and where the accumulator for the fold may not be a simple list but may be a more complex object.

There are three potential starting points in Riak for evolving such a feature:

- The `riak_core_coverage_fsm` behaviour, currently used by secondary index queries and list_keys queries;

- The `riak_pipe` framework written to support Map/Reduce operations in Riak KV (which is already optimised to re-use the coverage FSM when the Mp/Reduce operation is for a defined range on the special $bucket or $key indexes);

- The `riak_kv_sweeper` framework that is as yet unreleased, but is available on the Riak develop branch - which is design to allow multiple functions to be combined on the same fold, with concurrency management and throttling of folds in-built.

This attempt to support a coverage fold will be based on the `riak_core_coverage_fsm` behaviour as used by secondary index queries.

## Coverage Queries
