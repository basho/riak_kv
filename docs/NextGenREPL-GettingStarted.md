# Riak (NextGen) Replication - Getting started

The Riak NextGen replication is an alternative to the riak_repl replication solution, with these benefits:

- allows for replication between clusters with different ring-sizes and n-vals;

- provides very efficient reconciliation to confirm clusters are synchronised;

- efficient and fast resolution of small deltas between clusters;

- extensive configuration control over the behaviour of replication;

- a comprehensive set of operator tools to troubleshoot and resolve issues via `remote_console`.

The relative negatives of the Riak NextGen replication solution are:

- greater responsibility on the operator to ensure that the configuration supplied across the nodes provides sufficient capacity and resilience.

- slow to resolve very large deltas between clusters, operator intervention to use alternative tools may be required.

- relative little production testing when compared to `riak_repl`.

## Getting started

For this getting started, it is assumed that the setup involves:

- 2 x 8-node clusters where all data is nval=3

- A requirement to both real-time replicate and full-sync reconcile between clusters;

- Each cluster has o(1bn) keys;

- bi-directional replication required.

### Set the Delete mode

There are three possible delete modes for riak

- Timeout (default 3s);

- Keep;

- Immediate.

When running replication, it is strongly recommended to change from the default setting, and use to the delete mode of `keep`.  This needs to be added via a `riak_kv` section of the advanced.config file (there is no way of setting the delete mode via riak.conf).  Running `keep` will retain semi-permanent tombstones after deletion, that are important to avoid issues of object resurrection when running bi-directional replication between clusters.

When running Tictac AAE, the tombstones can now be reaped using the `reap_tomb` aae_fold query.  This allows for tombstones to be reaped after a long delay (e.g. 1 week).  Note though, that tombstone reaping needs to be conducted concurrently on replicating clusters, ideally with full-sync replication temporarily disabled.  There is no fully-automated replication-friendly way of reaping tombstones.

Running an alternative delete mode, is tested, and will work, but there will be a significantly increased probability of false-negative reconciliation events, that may consume resource on the cluster.

### Enable Tictac AAE

To use the full-sync mechanisms, and the operational tools then TictacAAE must be enabled:

```
tictacaae_active = active
```

This can be enabled, and the cluster run in 'parallel' mode - where backend other than leveled is used.  However, for optimal replication performance Tictac AAE is best run in `native` mode with a leveled backend.


### Configure real-time replication

The first stage is to enable real-time replication.  Each cluster will need to be configured as both a source and a sink for real-time replication -  a source being a potentially originator of an update to be replicated, the sink being the recipient cluster for a replicated update.

```
replrtq_enablesrc = enabled
replrtq_srcqueue = replq:any
```

This configuration enables the node to be a source for changes to replicate, and replicates `any` change to queue named `replq` - and this queue will need to be configured as the source for updates on the sink cluster.  The configuration is required on each and every node in the source cluster.

For more complicated configurations multiple queue names can be used, with different filters.

For the sink cluster, the following configuration is required:

```
replrtq_enablesink = enabled
replrtq_sinkqueue = replq
replrtq_sinkpeers = <ip_addr_node1>:8087:pb|<ip_addr_node2>:8087:pb|<ip_addr_node3>:8087:pb
replrtq_sinkworkers = 24
```

For each node in the source there should be multiple sink nodes that have that node configured as a `replrtq_sinkpeer`.  Configuration must ensure that another sink peer will take over when a sink node fails that is reading from a given source - there is no automative handoff of responsibility for another node.  Each sink node may be configured to peer with all source nodes.

Sink workers can tolerate communication with dead peers in the source cluster, so sink configuration should be added in before expanding source clusters.

The number of replrtq_sinkworkers needs to be tuned for each installation.  Higher throughput nodes may require more sink workers to be added.  If insufficient sink workers are added queues will build up.  The size of replication queue is logged as follows:

`@riak_kv_replrtq_src:handle_info:414 QueueName=replq has queue sizes p1=0 p2=0 p3=0`

More workers and sink peers can be added at run-time via the `remote_console`.

### Configure full-sync Replication

To enable full-sync replication on a cluster, the following configuration is required:

```
ttaaefs_scope = all
ttaaefs_queuename = replq
ttaaefs_queuename_peer = replq
ttaaefs_localnval = 3
ttaaefs_remotenval = 3
```

Then to configure a peer relationship:

```
ttaaefs_peerip = <ip_addr_node1>
ttaaefs_peerport = 8087
ttaaefs_peerprotocol = pb
```

Unlike when configuring a real-time replication sink, each node can only have a single peer relationship with another node in the remote cluster.  Note though, that all full-sync commands run across the whole cluster.  If a single peer relationship dies, some full-sync capacity is lost, but other peer relationships between different nodes will still cover the whole data set.

Once there are peer relationships, a schedule is required, and a capacity must be defined.

```
ttaaefs_allcheck = 0
ttaaefs_hourcheck = 0
ttaaefs_daycheck = 0
ttaaefs_autocheck = 24
ttaaefs_rangecheck = 0

ttaaefs_maxresults = 64
ttaaefs_rangeboost = 8

ttaaefs_allcheck.policy = window
ttaaefs_allcheck.window.start = 22
ttaaefs_allcheck.window.end = 6
```

The schedule is how many times each 24 hour period to run a check of the defined type.  The schedule is re-shuffled at random each day, and is specific to that node's peer relationship.  It is recommended that only `ttaaefs_autocheck` be used in schedules by default, `ttaaefs_autocheck` is an adaptive check designed to be efficient in a variety of scenarios.  The other checks should only be used if there is specific test evidence to demonstrate that they are more efficient.

If the `ttaaefs_scope` is `all`, then the comparison is between all data in the clusters (so the clusters must be intended to have an entirely common set of data).  Otherwise alternative scopes can be used to just sync an individual bucket or bucket type.

There are three stages to the full-sync process.  The discovery of the hashtree delta, then the discovery of key and clock deltas within a subset of the found tree delta, then the repairing of that delta.  If `ttaaefs_scope` is set to `all` then the discovery of tree deltas will always be fast and efficient, as the comparison will use cached trees.  For other scopes the cost of discovery is much higher, and in proportion to the number of keys in the bucket being compared.  However, in this case the different `ttaaefs_check` types are designed to make this faster by focusing only on certain ranges (e.g. the `ttaaefs_hourcheck` will only look at data modified in the past hour).

For all scopes, the discovery of key and clock deltas is proportionate to the size of the keyspace is covered (but is more efficient than a full key scan because the database contains hints to help it skip data that is not in the damaged area of the tree).  The different `ttaaefs_check` types make this process more efficient by focusing the key and clock comparison on specific ranges.  

The `all`, `day` and `hour` check's restrict the modified date range used in the full-sync comparison to all time, the past day or the past hour.  the `ttaaefs_rangecheck` uses information gained from previous queries to dynamically determine in which modified time range a problem may have occurred (and when the previous check was successful it assumes any delta must have occurred since that previous check).  The `ttaaefs_allcheck` is an adaptive check, which based on the previous checks and the `ttaaefs_allcheck.window`, will determine algorithmically whether it is best to run a `ttaaefs_allcheck`, a `ttaaefs_daycheck`, a `ttaaefs_rangecheck` or `ttaaefs_nocheck`.

It is normally preferable to under-configure the schedule.  When over-configuring the schedule, i.e. setting too much repair work than capacity of the cluster allows, there are protections to queue those schedule items there is no capacity to serve, and proactively cancel items once the manager falls behind in the schedule.  However, those cancellations will reset range_checks and so may delay the overall time to recover.

Adding `ttaaefs_nocheck` into the schedule can help randomise when checks take place, and mitigate the risk of overlapping checks.

Each check is constrained by `ttaaefs_maxresults`, so that it only tries to resolve issues in a subset of broken leaves in the tree of that scale (there are o(1M) leaves to the tree overall).  However, the range checks will try and resolve more (as they are constrained by the range) - this will be the multiple of `ttaaefs_maxresults` and `ttaaefs_ranegboost`.

It is possible to enhance the speed of recovery when there is capacity by manually requesting additional checks, or by temporarily overriding `ttaaefs_maxresults` and/or `ttaaefs_rangeboost`.

In a cluster with 1bn keys, under a steady load including 2K PUTs per second, relative timings to complete different sync checks (assuming there exists a delta):

- all_sync 150s - 200s;

- day_sync 20s - 30s;

- hour_sync 2s - 5s;

- range_sync (depends on how recent the low point in the modified range is).

Timings will vary depending on the total number of keys in the cluster, the rate of changes, the size of the delta and the precise hardware used.  Full-sync repairs tend to be relatively demanding of CPU (rather than disk I/O), so available CPU capacity is important.

The `ttaaefs_queuename` is the name of the queue on this node, to which deltas should be written (assuming the remote cluster being compared has sink workers fetching from this queue).  If the `ttaaefs_queuename_peer` is set to disabled, when repairs are discovered, but it is the peer node that has the superior value, then these repairs are ignored.  It is expected these repairs will be picked up instead by discovery initiated from the peer.  Setting the `ttaaefs_queuename_peer` to the name of a queue on the peer which this node has a sink worker enabled to fetch from will actually trigger repairs when the peer cluster is superior.  It is strongly recommended to make full-sync repair bi-directionally in this way.

If there are 24 sync events scheduled a day, and default `ttaaefs_maxresults` and `ttaaefs_rangeboost` settings are used, and an 8-node cluster is in use - repairs via ttaaefs full-sync will happen at a rate of about 100K per day.  It is therefore expected that where a large delta emerges it may be necessary to schedule a `range_repl` fold, or intervene to raise the `ttaaefs_rangeboost` to speed up the closing of the delta.

### Configure work queues

There are two per-node worker pools which have particular relevance to full-sync:

```
af1_worker_pool_size = 2
af3_worker_pool_size = 4
```

The AF1 pool is used by rebuilds of the AA tree cache.

### Monitoring full-sync exchanges via logs

Full-sync exchanges will go through the following states:

`root_compare` - a comparison of the roots of the two cluster AAE trees.  This comparison will be repeated until a consistent number of repeated differences is seen.  If there are no repeated differences, we consider the clusters to be in_sync - otherwise run `branch_compare`.

`branch_compare` - repeats the root comparison, but now looking at the leaves of those branches for which there were deltas in the `root_compare`.  If there are no repeated differences, we consider the clusters to be in_sync - otherwise run `clock_compare`.

`clock_compare` - will run a fetch_clocks_nval or fetch_clocks_range query depending on the scheduled work item.  This will be constrained by the maximum number of broken segments to be fixed per run (`ttaaefs_maxresults`).  This list of keys and clocks from each cluster will be compared, to look for keys and clock where the source of the request has a more advanced clock - and these keys will be sent for read repair.

At clock_compare stage, a log will be generated for each bucket where repairs were required, with the low and high modification dates associated with the repairs:

```
riak_kv_ttaaefs_manager:report_repairs:1071 AAE exchange=122471781 work_item=all_check type=full repaired key_count=18 for bucket=<<"domainDocument_T9P3">> with low date {{2020,11,30},{21,17,40}} high date {{2020,11,30},{21,19,42}}
riak_kv_ttaaefs_manager:report_repairs:1071 AAE exchange=122471781 work_item=all_check type=full repaired key_count=2 for bucket=<<"domainDocument_T9P9">> with low date {{2020,11,30},{22,11,39}} high date {{2020,11,30},{22,15,11}}
```

If there is a need to investigate further what keys are the cause of the mismatch, all repairing keys can be logged by setting via `remote_console`:

```
application:set_env(riak_kv, ttaaefs_logrepairs, true).
```

This will produce logs for each individual key:

```
@riak_kv_ttaaefs_manager:generate_repairfun:973 Repair B=<<"domainDocument_T9P3">> K=<<"000154901001742561">> SrcVC=[{<<170,167,80,233,12,35,181,35,0,49,73,147>>,{1,63773035994}},{<<170,167,80,233,12,35,181,35,0,97,246,69>>,{1,63773990260}}] SnkVC=[{<<170,167,80,233,12,35,181,35,0,49,73,147>>,{1,63773035994}}]

@riak_kv_ttaaefs_manager:generate_repairfun:973 Repair B=<<"domainDocument_T9P3">> K=<<"000154850002055021">> SrcVC=[{<<170,167,80,233,12,35,181,35,0,49,67,85>>,{1,63773035957}},{<<170,167,80,233,12,35,181,35,0,97,246,68>>,{1,63773990260}}] SnkVC=[{<<170,167,80,233,12,35,181,35,0,49,67,85>>,{1,63773035957}}]

@riak_kv_ttaaefs_manager:generate_repairfun:973 Repair B=<<"domainDocument_T9P3">> K=<<"000154817001656137">> SrcVC=[{<<170,167,80,233,12,35,181,35,0,49,71,90>>,{1,63773035982}},{<<170,167,80,233,12,35,181,35,0,97,246,112>>,{1,63773990382}}] SnkVC=[{<<170,167,80,233,12,35,181,35,0,49,71,90>>,{1,63773035982}}]

@riak_kv_ttaaefs_manager:generate_repairfun:973 Repair B=<<"domainDocument_T9P3">> K=<<"000154801000955371">> SrcVC=[{<<170,167,80,233,12,35,181,35,0,49,70,176>>,{1,63773035978}},{<<170,167,80,233,12,35,181,35,0,97,246,70>>,{1,63773990260}}] SnkVC=[{<<170,167,80,233,12,35,181,35,0,49,70,176>>,{1,63773035978}}]
```

At the end of each stage a log EX003 is produced which explains the outcome of the exchange:

```
log_level=info log_ref=EX003 pid=<0.30710.6> Normal exit for full exchange purpose=day_check in_sync=true  pending_state=root_compare for exchange id=8c11ffa2-13a6-4aca-9c94-0a81c38b4b7a scope of mismatched_segments=0 root_compare_loops=2  branch_compare_loops=0  keys_passed_for_repair=0

log_level=info log_ref=EX003 pid=<0.13013.1264> Normal exit for full exchange purpose=range_check in_sync=false  pending_state=clock_compare for exchange id=921764ea-01ba-4bef-bf5d-5712f4d81ae4 scope of mismatched_segments=1 root_compare_loops=3  branch_compare_loops=2  keys_passed_for_repair=15
```

The mismatched_segments is an estimate of the scope of damage to the tree.  Even if clock_compare shows no deltas, clusters are not considered in_sync until deltas are not shown with tree comparisons (e.g. root_compare or branch_compare return 0).

## Useful `remote_console` commands

By running `riak remote_console` in Riak 3.0, it is possible to attach to the running riak node.  From here it is possible to locally run requests on the cluster from that node.  This section covers some requests which may be useful when operating a cluster with NextGenREPL.

### Prompting a check

Individual full-syncs between clusters can be triggered outside the standard schedule:

```
riak_client:ttaaefs_fullsync(all_check).
```

The `all_check` can be replaced with `hour_check`, `day_check` or `range_check` as required.  The request will uses the standard max_results and range_boost for the node.

### Update the request limits

If there is sufficient capacity to resolve a delta between clusters, but the current schedule is taking too long to resolve - the max_results and range_boost settings on a given node can be overridden.

```
application:set_env(riak_kv, ttaaefs_maxresults, 256).
application:set_env(riak_kv, ttaaefs_rangeboost, 16).
```

Individual repair queries will do more work as these numbers are increased, but will repair more keys per cycle.  This can be used along with prompted checks (especially range checks) to rapidly resolve a delta.

### Overriding the range

When a query successfully repairs a significant number of keys, it will set the range property to guide any future range queries on that node.  This range can be temporarily overridden, if, for example there exists more specific knowledge of what the range should be.  It may also be necessary to override the range when an even erroneously wipes the range (e.g. falling behind in the schedule will remove the range to force range_checks to throttle back their activity).

To override the range (for the duration of one request):

```
riak_kv_ttaaefs_manager:set_range({Bucket, KeyRange, ModifiedRange}).
```

Bucket can be a specific bucket (e.g. `{<<"Type">>, <<"Bucket">>}` or `<<"Bucket">>`) or the keyword `all` to check all buckets (if nval full-sync is configured for this node). The KeyRange may also be `all` or a tuple of StartKey and EndKey.

To remove the range:

```
riak_kv_ttaaefs_manager:clear_range().
```

Remember the `range_check` queries will only run if either: the last check on the node found the clusters in sync, or; a range has been defined.  Clearing the range may prevent future range_check queries from running until another check re-assigns a range.

### Re-replicating keys for a given time period

The aae_fold `repl_keys_range` will replicate any key within the defined range to the clusters consuming from a defined queue.  For exampled, to replicate all keys in the bucket `<<"domainRecord">>` that were last modified between 3am and 4am on a 2020/09/01, to the queue `cluster_b`:

```
riak_client:aae_fold({repl_keys_range, <<"domainRecord">>, all, {date, {{2020, 9, 1}, {3, 0, 0}}, {{2020, 9, 1}, {4, 0, 0}}}, cluster_b}).
```

The fold will discover the keys in the defined range, and add them to the replication queue - but with a lower priority than freshly modified items, so the sink-side consumers will only consume these re-replicated items when they have excess capacity over and above that required to keep-up with current replication.

The `all` in the above query may be replaced with a range of keys if that can be specified.  Also if there is a specific modified range, but multiple buckets to be re-replicated the bucket reference can be replaced with `all`.

### Reaping tombstones


With the recommended delete_mode of `keep`, tombstones will be left permanently in the cluster following deletion.  It may be deemed, that a number of days after a set of objects have been deleted, that it is safe to reap tombstones.

Reaping can be achieved through an aae_fold.  However, reaping is local to a cluster.  If full-sync is enabled when reaping from one cluster then a full-sync operation will discover a delta (between the existence of a tombstone, and the non-existence of an object) and then work to repair that delta (by re-replicating the tombstone).

If reaping using aae_fold, it is recommended that the operator:

- should reap in parallel from any clusters kept i sync with this one;
- should have TTAAE full-sync temporarily suspended during the reap.

To issue a reap fold, for all the tombstones in August, a query like this may be made at the `remote_console`:

```
riak_client:aae_fold({reap_tombs, all, all, all, {date, {{2020, 8, 1}, {0, 0, 0}}, {{2020, 9, 1}, {0, 0, 0}}}, local}).
```

Issue the same query with the method `count` in place of `local` if you first wish to count the number of tombstones before reaping them in a separate fold.


### Erasing keys and buckets

It is possible to issue an aae_fold from `remote_console` to erase all the keys in a bucket, or a given key range or last-modified-date range within that bucket.  This is a replicated operation, so as each key is deleted, the delete action will be replicated to any real-time sync'd clusters.

Erasing keys will not happen immediately, the fold will queue the keys at the `riak_kv_eraser` for deletion and they will be deleted one-by-one as a background process.

This is not a reversible operation, once the deletion has been de-queued and acted upon.  The backlog of queue deletes can be cancelled at the `remote_console` on each node in turn using:

```
riak_kv_eraser:clear_queue(riak_kv_eraser).
```

See the `riak_kv_cluseraae_fsm` module for further details on how to form an aae_fold that erases keys.

### Gathering per-bucket object stats

It is useful when considering the tuning of full-sync and replication, to understand the size and scope of data within the cluster.  On a per-bucket basis, object_stats can be discovered via aae_fold from the remote_console.

```
riak_client:aae_fold({object_stats, <<"domainRecord">>, all, all}).
```

The above fold will return:

- a count of objects in the buckets;

- the total size of all the objects combined;

- a histogram of count by number of siblings sibling;

- a histogram of count by the order of magnitude of object size.

The fold is run as a "best efforts" query on a constrained queue, so may take some time to complete.  A last-modified-date range may be used to get results for objects modified since a recent check.


```
riak_client:aae_fold({object_stats, <<"domainRecord">>, all, {date, {{2020, 8, 1}, {0, 0, 0}}, {{2020, 9, 1}, {0, 0, 0}}}}).
```

To find all the buckets with objects in the cluster for a given n_val (e.g. n_val = 3):

```
riak_client:aae_fold({list_buckets, 3}).
```

To find objects with more than a set number of siblings, and objects over a given size a `find_keys` fold can be used (see `riak_kv_clusteraae_fsm` for further details).  It is possible to run `find_keys` with a last_modified_date range to find only objects which have recently been modified which are of interest due to either their sibling count or object size.

### Participate in Coverage

The full-sync comparisons between clusters are based on coverage plans - a plan which returns a set of vnode to give r=1 coverage of the whole cluster.  When a node is known not to be in a good state (perhaps following a crash), it can be rejoined to the cluster, but made ineligible for coverage plans by using the `participate_in_coverage` configuration option.

This can be useful when tree caches have not been rebuilt after a crash. The `participate_in_coverage` option can also be controlled without a re-start via the `riak remote_console`:

```
riak_client:remove_node_from_coverage()
```

```
riak_client:reset_node_for_coverage()
```

The `remove_node_from_coverage` function will push the local node out of any coverage plans being generated within the cluster (the equivalent of setting participate_in_coverage to false).  The `reset_node_for_coverage` will return the node to its configured setting (in the riak.conf file loaded at start up).

### Increasing Sink Workers

If queues are forming on the src cluster, more sink worker capacity might be required.  On each node the number of sink workers can be altered using:

```
riak_kv_replrtq_snk:set_workercount(replq, 32)
```

In this case `replq` is the name of the source-side queue from which this sink consumes, and 32 is the new number of workers to consume from this queue on that node.

### Suspend full-sync

If there are issues with full-sync and its resource consumption, it maybe suspended:

```
riak_kv_ttaaefs_manager:pause()
```

when full-sync is ready to be resumed on a node:

```
riak_kv_ttaaefs_manager:resume()
```

These run-time changes are relevant to the local node only and its peer relationships.  The node may still participate in full-sync operations prompted by a remote cluster even when full-sync is paused locally.
