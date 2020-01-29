# Next Generation Replication

## Replication History

Replication in Riak has an interesting history, going through multiple stages of reinvention, whilst being hidden from public view as a closed-source add-on for many years.  The code is now open-source, and the [`riak_repl`](https://github.com/basho/riak_repl) codebase amounts to over 22K lines of code, which can be run in multiple different modes of operation.  There are many ways in which you can configure replication, but all methods have hidden caveats and non-functional challenges.

In the final implementation of replication prior to the end of Basho, the replication services were based on:

* A real-time replication solution which tries to reliably (i.e. handling failure and back-pressure) queue and send updates from one cluster to another;

* A key-listing full-synchronisation feature which allows for reconciliation between clusters - that has significant resource overheads, and in large clusters will take many hours to complete.

* An alternative full-synchronisation service that reuses the intra-cluster active anti-entropy feature.  The AAE-based full-sync is much more efficient than key-listing full-sync, but will frequently fail (in some cases impacting the availability of the cluster on which it was run).  Note that the AAE-based full-sync was never released by basho other than as a *technical preview*.

These replication mechanisms had some key limitations:

* `riak_repl` cannot be reliably used to replicate between clusters of [different ring-sizes](https://docs.riak.com/riak/kv/2.2.3/using/reference/v3-multi-datacenter/architecture/index.html#restrictions) - meaning that data migrations necessary for customers with ring-size expansion needs had to be handled by bespoke customer application planning and logic (following the deprecation of the unreliable ring-resizing feature).

* `riak_repl` can only be used for complete replication between clusters, partial replication rules (e.g. replicating an individual bucket or bucket-type) cannot be configured.  Paradoxically although only complete replication can be configured, some types of data cannot be replicated, so partial replication may be the unexpected outcome of configuring total replication.

* `riak_repl` leads to unnecessary bi-directional replication, with full-sync replication only able to tell differences between sites and not consider causal context, meaning that out-of-date data may potentially be needlessly sent to a more up-to-date cluster.

* The partial reliability of real-time replication generated complexity, delays in replication, and failures in operation - but the reliability is necessary due to the limited reliability and high cost of full-sync replication.

## Concept of an Enhanced Replication Solution

The starting point for enhancing replication was to consider an improved full-sync solution: one that can reconcile between clusters quickly, reliably and at low resource cost.  With an improved full-sync solution, the real-time replication solution can be significantly simplified - as it may discard deltas in some failure scenarios protected by the knowledge that eventual consistency will be maintained in a timely manner through the full-sync process.   

The building block for an efficient full-sync solution is the [`kv_index_tictactree`](https://github.com/martinsumner/kv_index_tictactree) project.  This is intended to provide an alternative to Riak's [`hashtree`](https://github.com/basho/riak_core/blob/develop-2.9/src/hashtree.erl) anti-entropy mechanism, for both intra-cluster and inter-cluster reconciliation and repair.  The features of kv_index_hashtree differs from the current Riak hashtree solution in that:

* It is designed to be always on and available, even during tree rebuilds and exchanges - so need to request, wait-for and retain locks to complete anti-entropy processes.

* The [merkle trees](https://en.wikipedia.org/wiki/Merkle_tree) generated are mergeable by design, and can be built incrementally (without requiring first a full list of keys and hashes to be produced ahead of the tree build).  This allows for cluster-wide trees to be created through coverage queries.  Clusters with different internal structures can then be compared, even where cache trees within the cluster are aligned with those structures.

* The supporting key-store is ordered by key and not arranged according to the hashtree structure of the merkle trees. The implementation does have acceleration for lookups by hashtree structure though, meaning that it can be used for key-range and by-bucket queries but still serve hashtree related queries with sufficient efficiency.

* The supporting key-store is accelerated for query by modified date, so it is possible to build dynamic trees not just for key ranges or buckets, but also for ranges of modified times e.g. resolve entropy issues in objects sent in the past hour.

* The supporting key-store contains causal context information, not just hashes, allowing for genuine comparison between differences and targeted anti-entropy recovery action.

The `kv_index_tictactree` process was introduced as part of Riak 2.9.0, where it is primarily used for intra-cluster entropy protection.  In Riak 2.9.1 this can now be used in an automated and efficient way for inter-cluster reconciliation and repair.

With an efficient and flexible anti-entropy inter-cluster reconciliation mechanism, that supports frequent running - a simpler real-time replication solution is possible.  Characteristics of this simplified real-time solution are:

* No acks required of fetches from the queue, no need to failover queues between nodes during cluster-change events: the system can let it fail, and address resultant entropy through full-sync reconciliation.

* Leader-free replication, although with greater requirement on configuration by the operator to manage redundancy within the setup via `riak.conf`.

* By default replication queues contain pointers to objects only, stripped of their values, to allow for longer queues to be supported by default (i.e higher bounds on bounded queues) - as the queues need not consider the cost of storing the full size of the object.  There is a distributed cache, bounded in size, of recently changed objects.  This cache means that when a reference is fetched from the queue its value can normally be filled-in efficiently.

* Priority-based queueing - so that admin-driven replication activity (e.g. for service transition or data migration), full-sync reconciliation and real-time replication activity can be separately prioritised - with real-time replication given the highest absolute priority.

The overall solution produced using these concepts, now supports the following features:

* Replication between clusters with different ring-sizes and different n-vals, with both real-time and full-sync replication support;

* Full-sync reconciliation between clusters containing the same data in less than 1 minute where all data is to be compared, and equivalent reconciliation times if just a bucket or key range is to be compared to validate inter-cluster consistency of recent modifications only.

* Support for transition operations.  For example it is common when a cluster becomes very large, to improve efficiency by migrating one or more buckets onto a new cluster.  The new replication provides assistance to the process of managing such a data migration safely.

* Many-to-many cluster replication, with both independent configuration and independent operation of different inter-cluster relationships (e.g. Cluster A and Cluster B can be aggressively have all data kept in sync, whereas Cluster C can be concurrently have part of the data set kept in sync with Cluster A and B with a lazier and less resource-intensive synchronisation strategy).

* Failure management of replication handled within existing `riak_kv` processes (e.g. through automatic adjustment of coverage plans), not through repl-specific leadership negotiation.

* A smaller simpler repl codebase (less than 2K lines of code), with fewer moving parts to monitor, and delivered as an integrated part of `riak_kv`.

* Generally lower-latency real-time replication as replication is now triggered following completion of the PUT co-ordinator, not waiting for the post-commit stage (e.g. when 1 node has completed the PUT, not waiting for 'enough' nodes to have completed the PUT).

* Real-time replication and full-sync replication of objects in the write-once path - which had previously been restricted to [full-sync support](https://docs.riak.com/riak/kv/2.2.3/developing/app-guide/write-once/).

## Next Generation Replication - How it Works

### Replication Actors

Each node in `riak_kv` starts three processes that manage the inter-cluster replication.  A tictac AAE full-sync manager, a replication queue source manager, and a replication queue sink manager.  All processes are started by default (whether or not replication is enabled), but will only play an active role should replication be configured.  Further details on the processes involved:

* __Tictac AAE Full-Sync Manager__ - [`riak_kv_ttaaefs_manager`](https://github.com/martinsumner/riak_kv/blob/mas-i1691-ttaaefullsync/src/riak_kv_ttaaefs_manager.erl)

  * There is a single actor on each node that manages the full-sync reconciliation workload configured for that node.  

  * Each node is configured with the details of a peer node at a remote cluster.  Each manager is responsible for controlling cluster-wide hashtree exchanges between the local node and the peer node, and to prompt any repairs required across the cluster (not just on this node).  The information is exchanged between the peers, but that information represents the data across the whole cluster.  Necessary repairs are prompted through the replication queue source-side manager `riak_kv_replrtq_src`.

  * Each node is configured with a schedule to determine how frequently this manager will run its reconcile and repair operations.

  * It is is an administrator responsibility to ensure the cluster AAE workload is distributed across nodes with sufficient diversity to ensure correct operation under failure.  Work is not re-distributed between nodes in response to failure on either the local or remote cluster, so there must be other nodes already configured to share that workload to continue operation under failure conditions.

  * Each node can only full-sync with one other cluster (via the one peer node).  If the cluster needs to full-sync with more than one cluster, then the administrator should ensure different nodes have the different configurations necessary to achieve this.

  * Scheduling of work to minimise concurrency of reconciliation operations is managed by this actor using a simple, coordination-free mechanism.

  * The administrator may at run-time suspend or resume the regular running of full-sync operations on any given node via the `riak_kv_ttaaefs_manager`.

* __Replication Queue Source-Side Manager__ [`riak_kv_replrtq_src`](https://github.com/martinsumner/riak_kv/blob/mas-i1691-ttaaefullsync/src/riak_kv_replrtq_src.erl)

  * There is a single actor on each node that manages the queueing of replication object references to be consumed from other clusters. This actor runs a configurable number of queues, which contain pointers to data which is required to be consumed by different remote clusters.

  * The general pattern is that each delta within a cluster will be published once via the `riak_kv_replrtq_src` on a node local to the discovery of the change.  Each queue which is a source of updates will have multiple consumers spread across multiple sink nodes on the receiving cluster - where each sink-side node's consumers are being managed by a `riak_kv_replrtq_snk` process on that node.  The [publish and consume topology](https://github.com/russelldb/rabl/blob/master/docs/many-2-many-2.png) is based on that successfully tested in the [rabl riak replication add-on](https://github.com/russelldb/rabl/blob/master/docs/introducing.md).

  * Queues may have data filtering rules to restrict what changes are distributed via that queue.  The filters can restrict replication to a specific bucket, or bucket type, a bucket name prefix or allow for any change to be published to that queue.

  * __Real-time replication__ changes (i.e. PUTs that have just been co-ordinated on this node within the cluster), are sent to the `riak_kv_replrtq_src` in one of the following formats:
    * {Bucket, Key, Clock, {tombstone, Object}};
    * {Bucket, Key, Clock, {object, Object}};
    * {Bucket, Key, Clock, to_fetch}.

  * Real-time replicated objects are the highest priority items to be queued, and are placed on __every queue whose data filtering rules are matched__ by the object.  If the priority queue has grown beyond a limited number of items (the number being defined in `riak_kv.replrtq_srcobjectlimt`), then any {object, Object} references is stripped and replaced with `to_fetch`.  This is to help limit the memory consumed by the queue during failure conditions i.e. when a sink has stopped consuming from the source queue.

  * Changes identified by __AAE full-sync replication__ processes run by the `riak_kv_ttaaefs` manager on the local node are sent to the `riak_kv_replrtq_src` as references, and queued as the second highest priority.  These changes are queued only on __a single queue defined within the configuration__ of `riak_kv_ttaaefs_manager`.  The changes queued are only references to the object (Bucket, Key and Clock) not the actual object.

  * Changes identified by __AAE fold operations__ for administrator initiated transition or repair operations (e.g. fold over a bucket or key-range, or for a given range of modified dates), are sent to the `riak_kv_replrtq_src` to be queued as the lowest priority onto __a single queue defined by the administrator when initiating the AAE fold operation__.  The changes queued are only references to the object (Bucket, Key and Clock) not the actual object - and are only the changes discovered through the fold running on vnodes local to this node.

  * Should the local node fail, all undelivered object references will be dropped.

  * Queues are bounded, with limits set separately for each priority.  Items are consumed from the queue in strict priority order.  So a backlog of non-real-time replication events cannot cause a backlog or failure in real-time events.

  * The queues are provided using the existing `riak_core_priority_queue` module in Riak.

  * The administrator may at run-time suspend or resume the publishing of data to specific queues via the `riak_kv_replrtq_src` process.

* __Replication Queue Sink-Side Manager__ [`riak_kv_replrtq_snk`](https://github.com/martinsumner/riak_kv/blob/mas-i1691-ttaaefullsync/src/riak_kv_replrtq_snk.erl)

  * There is a single actor on each node that manages the process of consuming from queues on the `riak_kv_replrtq_src` on remote clusters.

  * The `riak_kv_replrtq_snk` can be configured to consume from multiple queues, across an open-ended number of peers.  For instance if each node on Cluster A maintains a queue named `cluster_c_full`, and each node on Cluster B maintains a queue named `cluster_c_partial` - then `riak_kv_replrtq_snk` can be configured to consume from the `cluster_c_full` from every node in Cluster A and from `cluster_c_partial` from every node in Cluster B.

  * The `riak_kv_replrtq_snk` manages a finite number of workers for consuming from remote peers.  The `riak_kv_replrtq_snk` tracks the results of work in order to back-off slightly from peers regularly not returning results to consume requests (in favour of those peers indicating a backlog by regularly returning results).  The `riak_kv_replrtq_snk` also tracks the results of work in order to back-off severely from those peers returning errors (so as not to lock too many workers consuming from unreachable nodes).

  * The administrator may at run-time suspend or resume the consuming of data from specific queues or peers via the `riak_kv_replrtq_snk`.


### Real-time Replication - Step by Step

Previous replication implementations initiate replication through a post-commit hook.  Post-commit hooks are fired from the `riak_kv_put_fsm` after "enough" responses have been received from other vnodes (based on n, w, dw and pw values for the PUT).  Without enough responses, the replication hook is not fired, although the client should receive an error and retry. This process of retrying may eventually fire the hook - although it is possible for a PUT to fail, the hook not to be fired, but a GET be locally successful (due to read-repair and anti-entropy) and there be no clue that the object has not been replicated.

In implementing the new replication solution, the point of firing off replication has been changed to the point that the co-ordinated PUT is completed.  So the replication of the PUT to the clusters may occur in parallel to the replication of the PUT to other nodes in the source cluster.  This is the first opportunity where sufficient information is known (e.g. the updated vector clock), and reduces the size of the time-window of inconsistency between the clusters, and also reduce the window of opportunity for a PUT to succeed but not have replication triggered.

Replication is fired within the `riak_kv_vnode` `actual_put/8`.  On condition of the vnode being a co-ordinator of the put, and of `riak_kv.replrtq_enablesrc` being set to enabled (true), the following work is done:

- The object reference to be replicated is determined, this is the type of reference to be placed on the replication queue.  

  - If the object is now a tombstone, the whole object is used as the replication reference.  The whole object is used due to the small size of the object, and the need to avoid race conditions with reaping activity if `delete_mode` is not `keep` - the cluster may not be able to fetch the tombstone to replicate in the future.  The whole object must be kept on the queue and not be filtered by the `riak_kv_replrtq_src` to be replaced with a `to_fetch` reference.

  - If the object is below the `riak_kv.replrtq_srcobjectsize` (default 200KB) then whole object will be sent to the `riak_kv_replrtq_src`, and it will be queued as a whole object as long as the current size of the priority real-time queue does not exceed the `riak_kv.replrtq_srcobjectlimit` (default 1000).  If an object is over the size limit a `to_fetch` references will be sent instead of the object, and if the queue is too large the `riak_kv_replrtq_src` will substitute a `to_fetch` reference before queueing.

- The `{Bucket, Key, Clock, ObjectReference}` is cast to the `riak_kv_replrtq_src` and placed by the `riak_kv_replrtq_src` on the priority queue.

- The queue has a configurable absolute limit, that is applied individually for each priority.  The limit is configured via `riak_kv.replrtq_srcqueuelimit` and defaults to 300,000 references (5 minutes of traffic at 1,000 PUTs per second).  When this limit is reached, new replication references are discarded on receipt rather than queued - these discarded references will need to eventually be re-replicated via full-sync.

The reference now needs to be handled by the `riak_kv_replrtq_src`.  The task list for this process is:

- Assign a priority to the replication event depending on what prompted the replication (e.g. highest priority to real-time events received from co-ordinator vnodes).

- Add the reference to the tail of the __every__ matching queue based on priority.  Each queue is configured to either match `any` replication event, no real-time events (using the configuration `block_rtq`), or a subset of events (using either a bucket `type` filter or a `bucket` filter).

In order to replicate the object, it must now be fetched from the queue by a sink.  A sink-side cluster should have multiple consumers, on multiple nodes, consuming from each node in the source-side cluster.  These workers are handed work items by the `riak_kv_replrtq_snk`, with a Riak client configured to communicate to the remote node, and the worker will initiate a `fetch` from that node.

On receipt of the `fetch` request the source node should:

- Initiate a `riak_kv_get_fsm`, passing `{queuename, QueueName}` in place of `{Bucket, Key}`.

- The GET FSM should go directly into the `queue_fetch` state, and try and fetch the next replication reference from the given queue name via the `riak_kv_replrtq_src`.

  - If the fetch from the queue returns `queue_empty` this is relayed back to the sink-side worker, and ultimately the `riak_kv_replrtq_snk` which may then slow down the pace at which fetch requests are sent to this node/queue combination.  To reduce the volume of individual requests when queues are mainly empty, the queue is only considered empty if it has reported empty 8 times from requests 4ms apart.

  - If the fetch returns an actual object, this is relayed back to the sink worker.

  - If the fetch returns a replication reference with the flag `to_fetch`, the `riak_kv_get_fsm` will continue down the standard path os states starting with `prepare`, and fetch the object which the will be returned to the sink worker.

- If a successful fetch is relayed back to the sink worker it will replicate the PUT using a local `riak_client:push/4`.  The push will complete a PUT of the object on the sink cluster - using a `riak_kv_put_fsm` with appropriate options (e.g. `asis`, `disable-hooks`).

  - The code within the `riak_client:push/4` follows the behaviour of the existing `riak_repl` on receipt of a replicated object.

- If the fetch and push request fails, the sink worker will report this back to the `riak_kv_replrtq_snk` which should delay further requests to that node/queue so as to avoid rapidly locking sink workers up communicating to a failing node.


### Full-Sync Reconciliation and Repair - Step by Step

The `riak_kv_ttaaefs_manager` controls the full-sync replication activity of a node.  Each node is configured with a single peer with which it is to run full-sync checks and repairs, assuming that across the cluster sufficient peers to sufficient clusters have been configured to complete the overall work necessary for that cluster.  Ensuring there are sufficient peer relations is an administrator responsibility, there are no re-balancing or re-scaling scenarios during failure scenarios.

The `riak_kv_ttaaefs_manager` is a source side process.   It will not attempt to repair any discovered discrepancies where the remote cluster is ahead of the local cluster - the job of the process is to ensure that a remote cluster is up-to-date with the changes which have occurred in the local cluster.  For mutual full-sync replication, there will be a need for an equivalent configuration on the peer cluster.

The `riak_kv_ttaaefs_manager` has a schedule of work obtained from the configuration.  The schedule has wants, the number of times per day that it is desired that this manager will:

- Reconcile changes across all the whole cluster over all time;

- Skip work for a schedule slot and do nothing;

- Reconcile changes that have occurred in the past hour;

- Reconcile changes that have occurred in the past day.

On startup, the manager looks at these wants and provides a random distribution of work across slots.  The day is divided into slots evenly distributed so there is a slot for each want in the schedule.  It will run work for the slot at an offset from the start of the slot, based on the place this node has in the sorted list of currently active nodes.  So if each node is configured with the same total number of wants, work will be synchronised to have limited overlapping work within the cluster.

When, on a node, a scheduled piece of work comes due, the `riak_kv_ttaaefs_manager` will start an `aae_exchange` to run the work between the two clusters (using the peer configuration to reach the remote cluster).  Once the work is finished, it will schedule the next piece of work - unless the start time for the next piece of work has already passed, in which case the next work is skipped.  When all the work in the schedule is complete, a new schedule is calculated from the wants.

When starting an `aae_exchange` the `riak_kv_ttaaefs_manager` must pass in a repair function.  This function will compare clocks from identified discrepancies, and where the source cluster is ahead of the sink, send the `{Bucket, Key, Clock, to_fetch}` tuple to a configured queue name on `riak_kv_replrtq_src`.  These queued entries will then be replicated through being fetched by the `riak_kv_replrtq_snk` workers, although this will only occur when there is no higher priority work to replicate i.e. real-time replication events prompted by locally co-ordinated PUTs.

### Notes on Configuration and Operation

TODO

* [`riak_kv_ttaaefs_manager`](https://github.com/martinsumner/riak_kv/blob/mas-i1691-ttaaefullsync/src/riak_kv_ttaaefs_manager.erl)

  * [Schema](https://github.com/martinsumner/riak_kv/blob/mas-i1691-ttaaefullsync/priv/riak_kv.schema#L837-L974)

* [`riak_kv_replrtq_src`](https://github.com/martinsumner/riak_kv/blob/mas-i1691-ttaaefullsync/src/riak_kv_replrtq_src.erl)

  * [Schema](https://github.com/martinsumner/riak_kv/blob/mas-i1691-ttaaefullsync/priv/riak_kv.schema#L976-L1086)

* [`riak_kv_replrtq_snk`](https://github.com/martinsumner/riak_kv/blob/mas-i1691-ttaaefullsync/src/riak_kv_replrtq_snk.erl)

  * [Schema](https://github.com/martinsumner/riak_kv/blob/mas-i1691-ttaaefullsync/priv/riak_kv.schema#L1088-L1144)

### Frequently Asked Questions

*Previously there were moves to run real-time replication via RabbitMQ, is this still the intention?*

Not presently, although it would not be a significant code change to modify the `riak_kc_replrtq_src` so that it would point to an external queue not an internal riak_core_priority_queue.


*Can this be used to synchronise data between Riak and another data store (e.g. Elastic Search, Hadoop etc)?*

The intention is to open up this possibility by removing the need to understand the internal riak infrastructure, when trying to interface to Riak for replication.  The API calls used by replications (fetch, aae_fold) are now exposed and documented.  However, it will still require some implementation work in the destination database to replicate sink-side functionality - in particular the storing of keys and clocks so that they can be converted into a cluster-wide Merkle tree for comparison.


*In this future will this mean there is 1 way of doing replication in Riak, or n + 1 ways?*

In the medium term it will certainly be an additional replication feature and not a replacement.  The `riak_repl` remains to assist in transition, but also Riak customers are committed to continued investment in improving `riak_repl`, and adding features such as improved replication filtering logic.


*What is left to do to make this production ready?  When might it be production ready?*

Original release target was Autumn 2019, and the current target for release is e/o January 2020.  The release schedule has been extended as:

- There was an increase in scope for the release to add in `riak_kv_reaper` and `riak_kv_eraser` functionality to improve support for mass deletion and tombstone management.

- Initial volume tests indicated a need for improvements in observability and median replication latency.


*Does this work with all Riak backends?  Does the efficiency of the solution change depending on backend choice?*

The full-scope of the solution is intended to work with all backends *if* Tictac AAE has been enabled as an anti-entropy mechanism.  For non-leveled backends there is a cost in enabling Tictac AAE (due to the need for the AAE keystore), and so the replication features have a higher cost in those backends.

The replication sink workers, and the process of copying objects between processes can lead to increased CPU utilisation with nextgenrepl enabled.


*How reliably does this mechanism handle deletions?  Might deleted data be resurrected?*

Whether or not deletions can be resurrected is a function of the delete_mode chosen, and the additional replication changes make no difference in this regard.  The release 2.9.1 does though enable the new `riak_kv_eraser` and `riak_kv_reaper` features, which allow for more efficient use of the delete_mode of keep.


*Is this replication approach compatible with fixing a Time to Live for objects?*

If a backend TTL is used in Riak, objects will be removed from the backend without being erased from the AAE stores.  This will lead to a build-up of data in the AAE store, and when AAE store rebuilds occur large (and false) deltas being reported during intra-cluster and inter-cluster AAE checks.  If the intention is to used object TTLs, it would be better to use the `riak_kv_eraser` feature via `aae_fold` to expire objects rather than a backend TTL.


*Will this replication approach work with Riak's strong consistency module `riak_ensemble`?*

No, and there are no plans to extend the solution to support strongly consistent PUTs.  It should be noted that the `riak_ensemble` feature remains an experimental solution not assured for production use (although customers have been known to sue it successfully in production).


*What about hash collisions in the merkle tree - as only 4 byte hashes are used rather than full cryptographic hashes*

The aae_fold feature allows for a modification in the hash_method so that instead of the pre-calculated hash, each hash will be recalculated using a combination of the vector clock (the current hash input) and an initialisation vector (passed in through the hash_method).  So it is possible to validate synchronisation against hash collisions, although the `riak_kv_ttaaefs_manager` doe snot support the orchestartion of such validation.


### Outstanding TODO

*Extend the replication support to the write once path by implementing it within the riak_kv_w1c_worker.  Initial thoughts are that the push on the repl side should not use the write once path*

...

*If hooks are not used any more for replication, should disable_hooks still be passed in as an option on replicated PUTs*

...

*Add validation functions for tokenised inputs (peer list on rpel sink, queue definitions on repl source). Startup should fail due to invalid config*

...
