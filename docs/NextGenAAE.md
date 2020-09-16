# Next Generation AAE

## History of AAE

The Active Anti-Entropy solution within Riak has been a reliable feature of the database since [release 1.3](https://github.com/basho/riak/blob/develop-3.0/releasenotes/riak-1.3.md#riak-130-release-notes).  The only significant change in its lifetime was a change to make the hash of an object that of the object's sorted vector-clock, not of the value, which improved the performance of AAE for larger object sizes.  The motivation for updating AAE and adding a next generation AAE solution (referred to as Tictac AAE), was not driven by necessity due to any significant flaws.  However, an update to AAE still has the potential to offer the following benefits:

- Provide foundations for a [NextGenRepl](../docs/NextGenREPL.md) solution for full-sync replication.  Previously a full-sync AAE replication feature was added to riak_repl, but it was never formally supported for production use.  There were reliability issues associated with locks, and a lack of flexibility for supporting partial replication strategies (e.g. per-bucket).  A new AAE feature was required to support a [more flexible full-sync AAE feature](../docs/NextGenREPL.md).

- Have an AAE solution that re-used existing vnode key-stores (as found in the leveled backend), rather than needing to maintain parallel keystores.

- When a parallel AAE keystore is required as the vnode backend does not have its own on-disk key and metadata store to re-use, allow that parallel keystore to be multi-purpose and be re-used for operational queries.

- Reduce the need to support the basho leveldb project going forward, with the aim to make Riak support and development require knowledge only of Erlang.

## Overview of NextGenAAE (Tictac AAE)

There are three primary elements to the new AAE approach:

- Use of the [kv_index_tictactree library](https://github.com/martinsumner/kv_index_tictactree) as a replacement for the `riak_kv_entropy_manager`, `riak_kv_index_hashtree` and `riak_kv_exchange_fsm`.  The `kv_index_tictcatree` tibrary is merkle tree based, as with the original Riak AAE solution, but the merkle trees combine hashes through an XOR operation, not by hashing a concatenation of hashes.  This change undermines the cryptographic strength of the merkle tree, but in this use case unlike in blockchain solutions, that cryptographic strength is not a required feature.  This change in the formation of merkle trees allows for trees to be merged, to be bult incrementally, and to have deltas applied at leaves without re-reading the entire content of the leaf.

- Use of the [leveled database](https://github.com/martinsumner/leveled) as an AAE keystore, either in native mode (when the backend is also the vnode store) or in parallel mode (when an alternative vnode backend is used).  The leveled backend can be key-ordered, but it also retains hints through its structure as to the location of data by leaves of the merkle tree; so scans over the store looking for subsets of the merkle tree can be greatly accelerated.  The leveled backend also retains hints through its strucutre as to the lowest last modified dates that can be found within each part of the store; allowing for further acceleration of queries focusing only of data modified since a certain date.

- Use of a poke cycle on each vnode with backoff strategies, randomised stepping of pokes, non-destructive rebuilds (maintaining use of old trees when constructing new) and the riak_core worker pool strategy.  Combined, these remove the need for additional manager processes to provide orchestration and coordination of AAE activity.

## Transition to NextGenAAE (Tictac AAE)

Both forms of AAE can be run concurrently in a Riak cluster, so transitioning from one form of AAE to another can be done so seamlessly without losing anti-entropy protection.  When running both forms concurrently, the frequency of exchanges in NextGenAAE can be controlled by increasing the [`tictacaae_exchangetick`](https://github.com/basho/riak_kv/blob/riak_kv-3.0.1/priv/riak_kv.schema#L91-L102).  There will be an overhead when running both forms concurrently, so environment-specific load testing is essential.

When NextGenREPL is first enabled, it must build a keystore (if in parallel mode), and then build the cached trees.  The former process is not throttled, and so all stores will be built concurrently.  For this reason it would be best to schedule the initial setup of NextGenAAE away from busy periods if possible.  Whilst the stores and caches are being built, then false negative results will be returned (leading to inert repairs), so increasing the [`tictacaae_exchangetick`](https://github.com/basho/riak_kv/blob/riak_kv-3.0.1/priv/riak_kv.schema#L91-L102) is recommended during this phase.  If necessary AAE can be introduced one node at a time, but this will increase the period of false positive results so increasing the `tictacaae_exchangetick` is essential in this case.

The build of the tree caches will use the riak_core_worker_pool for [Best Endeavours](https://github.com/basho/riak_kv/blob/riak_kv-3.0.1/src/riak_kv_vnode.erl#L454).  Increasing the [size of this pool](https://github.com/basho/riak_kv/blob/riak_kv-3.0.1/priv/riak_kv.schema#L174-L177) can increase the concurrency, and hence elapsed time, of this process.

Only once the build of both the keystores and tree_caches are complete across the cluster, will the new anti-entropy service be correctly operational.  If a fresh node is joined into a cluster, it will not need to undertake a build, as the AAE stores and tree caches will be constructed in-line with the vnode stores (via handoff).

## Configuration Options for NextGenAAE (Tictac AAE)

The folowing configuration options exist for NextGenAAE:

- riak_kv.tictacaae_active [active, passive]

Enable or disable tictacaae.  Note that disabling tictacaae will set the use of tictacaae_active only at startup - setting the environment variable at runtime will have no impact.  If tictacaae is disabled for a period, it will not wipe any store or cache, so if it is subsequently re-enabled it will re-start with out of date anti-entropy data.  If AAE is to be re-enabled, then it would normally be preferable to wipe the folders containing these stores before re-starting.

- riak_kv.aae_tokenbucket [enabled, disabled]

It is possible for the vnode in some circumstances to accept writes faster than a parallel AAE store can receive them.  To protect against unbounded queues developing and subsequent timeouts/crashes of the AAE process, back-pressure signalling is used to block the vnode should a backlog develop on the AAE process.  This can be disabled.  Changing the environment variable will have no impact at run-time, however the size of the token buckets [can be changed at run-time using riak_kv_util](https://github.com/basho/riak_kv/blob/riak_kv-3.0.1/src/riak_kv_util.erl#L201-L237) to effectively enable and disable this feature on a node at run-time.

This feature is common to both standard AAE and NextGenAAE - enabling, disabling and token changes via `riak_kv_util` will impact both AAE services.

- riak_kv.tictacaae_dataroot "$platform_data_dir/tictac_aae"

Set the path for storing tree caches and parallel key stores.  Note that at startup folders may be created for every partition, and not removed when that partition hands off (although the contents should be cleared).  If the contents of these folders are cleared during a shutdown, a rebuild will be triggered on startup (and this can be used should an individual partition store become corrupted or out of date).

- riak_kv.tictacaae_parallelstore [leveled_ko, leveled_so]

On startup, if tictacaae is enabled, then the vnode will detect of the vnode backend has the capability to be a "native" store.  If not, then parallel mode will be entered, and a parallel AAE keystore will be started.  There are two potential parallel store backends - leveled_ko, and leveled_so.  Both backends use the leveled database runnin in head_only mode, which means there is no separation of values between journal and ledger as would happen when leveled is used as a vnode backend.  The head_only mode is more efficient for small values, but provides less protection from data loss during abrupt shutdowns - an acceptable risk given the AAe key store is a secondary store of data.

The two modes indicate whether the objects in the store will be stored in segment order (e.g. directly in line with the structure of the merkle tree), or key order (leveled_ko).  The default is key order, as this opens up new functional opportunities, however in some cases segment order (leveled_so) may be more efficient when acting purely as a an anti-entropy store.

There is no automatic support for switching between leveled_so and leveled_ko, a switch will require specific planning, with manual wiping of previous stores to trigger the building of new stores.  It is theoretically possible to run a cluster where some nodes have leveled_so and other leveled_ko backends, but this is not a tested scenario.

 - riak_kv.tictacaae_rebuildwait "336"

 This is the number of hours between rebuilds of the Tictac AAE system for each vnode.  A rebuild will invoke a rebuild of the key store (which is a null operation when in native mode), and then a rebuild of the tree cache from the rebuilt store.  The rebuilds are necessary to handle the scenario when the AAE system has go out of sync with the vnode store - potentially due to a loss of data from disk on the vnode store.  This value is read at startup only, altering a run-time has no impact.

- riak_kv.tictacaae_rebuilddelay "345600"

Once the AAE system has expired (due to the rebuild wait), the rebuild will not be triggered until the rebuild delay which will be a random number up to the size of this delay (in seconds).  By default his will have the effect of staggering all the vnode rebuilds in the cluster over a 4 day period.  This value is read at startup only, altering at run-time has no impact.

- riak_kv.tictacaae_storeheads [enabled, disabled]

By default when running a parallel keystore, only a small amount of metadata is required for AAE purposes, and with store heads disabled only that small amount of metadata is stored.  It is also possible to store the whole "head" of the object (i.e. the whole of the object without the value), which will then allow for AAE folds which examine object metadata.  There is a performance overhead with enabling the storing of the header.  The configuration item takes effect at startup only, and there is no support for switching between configurations, inconsistent results may be returned until the key store is next rebuilt.

- riak_kv.tictacaae_exchangetick "240000"

Exchanges are prompted every exchange tick, on each vnode.  By default there is a tick every 4 minutes.  Exchanges will skip when previous exchanges have not completed, in order to prevent a backlog of fetch-clock scans developing.  Do not reduce this amount without ensuring that it remains more than twice the size of the vnode_inactivity_timeout - the tick can prevent the vnode ever considering itself inactive, and hence prevent handoffs from being triggered.  The tick size can be modified at run-time by setting the environment variable via riak attach.

- riak_kv.tictacaae_rebuildtick "3600000"

Rebuilds will be triggered depending on the riak_kv.tictacaae_rebuildwait, but they must also be prompted by a tick.  The tick size can be modified at run-time by setting the environment variable via riak attach.

- riak_kv.tictacaae_maxresults "256"

The Merkle tree used has 4096 * 1024 leaves.  When a large discrepancy is discovered, only part of the discrepancy will be resolved each exchange - active anti-entropy is intended to be a background process for repairing long-term loss of data, hinted handoff and read-repair are the short-term and immediate answers to entropy.  How much of the tree is repaired each pass is defined by the `tictacaae_maxresults`, if more divergences are found at the root, only `tictacaae_maxresults` branches will be checked to find differences in leaves.  If more than `tictacaae_maxresults` divergences are discovered in the leaves, only `tictacaae_maxresults` leaves will be queried for the key and clock comparison.

With the default settings, on a cluster with divergences spread across all partitions, more than 1M differences can still be repaired per day.

Increasing the `tictacaae_maxresults` will in some cases increase the speed of repair, however it may lead to work backlogs that could overall still decrease the speed of repair.  If there are significant numbers of warnings about queries not being run due to query backlogs and exchanges being skipped, then reducing the `tictacaae_maxresults` could improve overall performance.

This parameter can be changed per-node at run-time using riak attach.

- riak_kv.tictacaae_primaryonly [on, off]

If a node fails, the fallback vnodes will be elected.  By default tictacaae_primaryonly is set on, and so these fallback vnodes will not participate in AAE.  Fallback vnodes will be populated by read-repair and fresh puts, but not by anti-entropy exchanges.


## Logging and Monitoring of NextGenAAE (Tictac AAE) - Exchanges

Monitoring of NextGenAAE like NextGenREPL, is primarily achieved through analysis of logs rather than by the output of statistics.  Key log entries to examine when monitoring AAE are:

- riak_kv_vnode warning "Skipping a tick due to non_zero skip_count=~w"

This is an indication that exchanges are taking longer than the exchange tick, so no exchange has been scheduled for a given tick to prevent an escalation in concurrent load associated with exchanges and a backlog of keystore snapshots.  However, note that at startup the skip_count is set to a random number on each vnode, to space out the initial exchange activity - so immediately following startup this is not an indication of an issue.  Skipping may also be caused by an exchange crashing and not responding to the riak_kv_vnode (AAE exchanges are not linked/monitored processes).  Normal exchanges will be resumed once the skip_count reaches 0, or a response to an exchange is received (which resets the skip_count to 0).

The scheduled exchange will still run when the skip_count reaches 0.  The exchange at the head of the queue is not skipped over, it is deferred by a tick.

- riak_kv_vnode info "Tictac AAE loop completed for partition=~w with exchanges expected=~w exchanges completed=~w total deltas=~w total exchange_time=~w seconds loop duration=~w seconds (elapsed)

When the exchange queue is empty, the vnode logs the statistics gathered with regards to the previous exchange cycle before consulting the cluster claim to re-queue a new set of exchanges.  Normally exchanges expected should equal exchanges completed, except at startup.  The loop duration should be constant between exchange loops unless long-running exchanges have resulted in exchange ticks being skipped.  The exchange_time is the summation of the time take for each exchange, but note exchanges include built in pauses - so this does not necessarily represent CPU or disk activity.  The key_deltas are the number of differences found after running the key and clock comparison between the partitions across all exchanges in the loop, the number of keys that have been flagged for repair.

- riak_kv_get_fsm info "Repairing key_count=~w between ~w"

When a set of deltas are discovered, the read-repair functions within the riak_kv_get_fsm module are used to repair those keys, and before the repairs are commenced the numbr of keys to be repaired is logged - as well as the information regarding the vnodes which were compared in order to determine the repairs.  If the repairs result in an actial repair, the riak_kv_stat for read_repairs will be incremented, so if the total of these key_count values are exceeding the rate of actual repairs, then this is an indication that the repair was as a result of a false difference between the keystores rather than an actual difference between the vnode stores.  As part of this repair process repairs will result in a being initiated rehash which should resolve that set of false deltas.

- riak_kv_get_fsm info "Repaired key_count=~w in repair_time=~w ms with pause_time=~w ms"

Once the repairs have been triggered, this additional log is produced.  Note that the triggering is deliberately spaced out so as not to prompt a large number of concurrent repairs and rehashes, and this spacing will be reflected in the repair_time and pause_time.  For each key delta there will have been a fetch prompted followed by a rehash, so the time to complete each repair is typically longer than the mean node_get time.  Fetches and rehashes for a given set of repairs are never launched concurrently, for each key there must be at least a 10ms wait between attempts to repair. 

- riak_kv_get_fsm info "Prompted read repair Bucket=~w Key=~w Clocks ~w ~w"

In order to receive this log, the environment variable riak_kv:log_readrepair must be set to `true` at runtime (e.g. `riak attach` or `riak rconsole` and `application:set_env(riak_kv, log_readrepair, true)`).  This log is intended to help an operator, with an understanding of the application and of vector clock format, to try and diagnose the what and why of keys becoming in need of repair.

- aae_exchange EX005 info "Exchange id=~s throttled count=~w at state=~w"

The logs of the aae_exchange will be seen in the erlang.log files, not the console.log files.  This log is used to indicate that the `riak_kv.tictacaae_maxresults` limit has been hit.  From this log the proportion of the tree which is divergent can be calculated - at state root_compare it is the `throttled_count / 4096` and at state branch_compare it is `throttled_count / (tictacaae_maxresults * 256)`.

- aae_exchange EX003 info "Normal exit for full exchange at pending_state=~w for exchange_id=~s root_compare_deltas=~w root_compares=~w branch_compare_deltas=~w branch_compares=~w keys_passed_for_compare=~w"

Log raised on successful completion of exchange showing the number of deltas found at each stage and the state at which the exchange exited.  In a healthy cluster with no entropy this should exit when comparing roots with no deltas.  The root_compares and branch_compares indicates how many comparisons were required to reach a steady number of deltas.  Some tree deltas reflect genuine variances, and some reflect differences in timing, only repeated deltas are candidates for consideration as a genuine variances - so at each comparison change the comparison is repeated until the intersection of all deltas begins to return a roughly consistent set of differences.

When pending_state is `waiting_all_results` this indicates the exchange failed to complete as an AAE process failed to respond to an exchange request in a correct and in a timely manner.

## Logging and Monitoring of NextGenAAE (Tictac AAE) - Rebuilds

- riak_kv_vnode info "Prompting tictac_aae rebuild for controller=~w"
- riak_kv_vnode info "AAE pid=~w partition=~w rebuild store complete in duration=~w seconds"
- riak_kv_vnode info "AAE pid=~w rebuild trees queued"
- riak_kv_vnode info "Starting tree rebuild for partition=~w"
- riak_kv_vnode info "Tree rebuild compete for partition=~w in duration=~w seconds"
- riak_kv_vnode info "Rebuild process for partition=~w complete in duration=~w seconds"

This is the set of logs raised during the rebuild process.

The rebuild of the store is prompted, and as store rebuilds are not deferred it will prompt without delay a rebuild of the store.  Depending on the size of the store, the rebuild may take many hours if in parallel mode - obviously in native mode the rebuild is unnecessary and so will be complete immediately.  Once the rebuild of the store is complete, the rebuild of the AAE tree caches is queued, and only when the rebuild request is consumed by the `riak_kv_worker` from the front of the queue will the snapshots be taken and the "Starting tree rebuild" will be logged.  Once complete the duration of the rebuild is logged (not including the time on the queue), and then the time for the overall process (store rebuild, queue time and tree rebuild).



