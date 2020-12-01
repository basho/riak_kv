# Riak (NextGen) Replication - Getting started

The Riak NextGen replication is an alternative to the riak_repl replication solution, with the benefits:

- allows for replication between clusters with different ring-sizes and n-vals;

- provides very efficient reconciliation to confirm clusters are synchronised;

- efficient and fast resolution of small deltas between clusters;

- extensive configuration control over the behaviour of replication;

- a comprehensive set of operator tools to troubleshoot and resolve issues via `remote_console`.

The relative negatives of the Riak NextGen replication solution are:

- greater responsibility on the operator to ensure that the provided configuration provides sufficient capacity and resilience.

- very slow to resolve very large deltas between clusters, operator intervention to use alternative tools may be required.

- relative little production testing when compared to riak_repl.

## Getting started

For this getting started, it is assumed that the setup involves:

- 2 8-node clusters where all data is nval=3

- A requirement to both real-time replicate and full-sync reconcile between clusters;

- Each cluster has o(1bn) keys;

- bi-directional replication required.

### Set the Delete mode

There are three possible delete modes for riak

- Timeout (default 3s);

- Keep;

- Immediate.

When running replication, it is strongly recommended to change from the default setting, and use to the delete mode of `keep`.  This needs to be added via a `riak_kv` section of the advanced.config file (there is no way of setting the delete mode via riak.conf).

Running `keep` will retain semi-permanent tombstones after deletion, that are important to avoid issues of object resurrection when running bi-directional replication between clusters.

When running Tictac AAE, the tombstones can now be reaped using the `reap_tomb` aae_fold query.  This allows for tombstones to be reaped after a long delay (e.g. 1 week).  Note though, that tombstone reaping needs to be conducted concurrently on replicating clusters, ideally with full-sync replication temporarily disabled - there is no automatic replication-friendly way of reaping tombstones.

Running an alternative delete mode, is tested, and will work, but there will be a significantly increased probability of false-negative reconciliation events, that may consume resource on the cluster.

### Enable Tictac AAE

to use the full-sync mechanisms, and the operational tools then TictacAAE must be enabled:

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

The number of replrtq_sinkworkers needs to be tuned for each installation.  Higher throughput nodes may require more sinkworkers to be added.  If insufficient sink workers are added queues will build up.  The size of replication queue is logged as follows:

`@riak_kv_replrtq_src:handle_info:414 QueueName=replq has queue sizes p1=0 p2=0 p3=0`

More workers and sink peers can be added at run-time via the `remote_console`.

### Configure full-sync Replication

To enable full-sync replication on a cluster, the following configuration is required:

```
ttaaefs_scope = all
ttaaefs_queuename = replq
ttaaefs_localnval = 3
ttaaefs_remotenval = 3
```

Then to configure a peer relationship:

```
ttaaefs_peerip = <ip_addr_node1>
ttaaefs_peerport = 8087
ttaaefs_peerprotocol = pb
```

Unlike when configuring a real-time replication sink, each node can only have a single peer relationship with another node in the remote cluster.  note though, that all full-sync commands run across the whole cluster - so if a single peer relationship dies, some full-sync capacity is lost, but other peer relationships between different nodes still each cover the whole data set.

Once there are peer relationships, a schedule is required, and a capacity must be defined.

```
ttaaefs_allcheck = 2

ttaaefs_hourcheck = 0

ttaaefs_daycheck = 10

ttaaefs_rangecheck = 24

```

The schedule is how many times each 24 hour period to run a check of the defined type.  The schedule is re-shuffled at random each day, and is specific to that nodes peer relationship.

As this is a configuration for nval full-sync, all of the data will always be compared - by merging a cluster-wide tictac tree and comparing the trees of both clusters. If a delta is found by that comparison, the scheduled work item determines what to do next:

- `all` indicates that the whole database should be scanned for all time looking for deltas, but only for deltas in a limited numbe of broken leaves of the merkle tree (the `ttaaefs_maxresults`).

- `hour` or `day` restricts he scan to data modified in the past hour or past 24 hours.

- `range`  is a "smart" check.  It will not be run when past queries have indicated nothing can be done to resolve the delta (for example as the other cluster is ahead, and only the source cluster can prompt fixes).  If past queries have shown the clusters to be synchronised, but then a delta occurs, the range_check will only scan for deltas since the last successful synchronisation.  If another check discovers the majority of deltas are in a certain bucket or modified range, the range query will switch to using this as a constraint for the scan.

Each check is constrained by `ttaaefs_maxresults`, so that it only tries to resolve issues in a subset of broken leave sin the tree of that scale (there are o(1M) leaves to the tree overall).  However, the range checks will try and resolve more (as they are constrained by the range) - this will be the multiple of `ttaaefs_maxresults` and `ttaaefs_ranegboost`.

The max results, the range boost can be updated at run-time via the `remote_console`.  Individual checks can also be prompted by the console to be unr in addition to the scheduled checks.

It is normally preferable to under-configure the schedule.  When over-configuring the schedule, i.e. setting too much repair work than capacity of the cluster allows, there are protections to queue those schedule items there is no capacity to serve, and proactively cancel items once the manager falls behind in the schedule.  However, those cancellations will reset range_checks and so may delay the overall time to recover.

It is possible to enhance the speed of recovery when there is capacity by manually requesting additional checks, or by temporarily overriding `ttaaefs_maxresults` and/or `ttaaefs_ranegboost`.

### Configure work queues


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



## Useful `remote_console` commands
