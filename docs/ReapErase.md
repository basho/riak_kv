# Reap and Erase

## Background

The Reaper and Eraser are now processes supported in Riak as at release 2.9.1.  They are intended to be a starting point of improving a number of situations:

* Riak supports [multiple delete modes](https://docs.riak.com/riak/kv/2.2.3/using/reference/object-deletion/index.html#configuring-object-deletion), and the recommended mode for safety is `keep`.  However, a delete_mode of `keep` creates an unresolved problem of permanently uncollected garbage - the tombstones are never erased.  Note, even with an `interval` delete mode, tombstones may fail to be cleared and then continue to exist forever.

* Riak supports time-to-live for objects configured within the backend, also known as [Global Object Expiration](https://riak.com/products/riak-kv/global-object-expiration/index.html?p=12768.html).  However, there are two flaws with this form of automated expiration:

 * It considers only when the object is added to the backend, not when the object is added to the database (i.e. the object's last modified time).  An object may extend and survive expiry through handoffs.

 * Object expiry is not coordinated with anti-entropy changes, and so as objects expire they may be resurrected by anti-entropy, which in turn will reset their expiry data to further in the future.  It can also lead to huge cascades in false repair action when AAE trees are rebuilt - as one vnode's AAE trees suddenly rebuild without all the expired objects.  This has led to some customers attempting to coordinate AAE tree rebuilds to make the occur concurrently, which can have a significant performance impact during the rebuild period.

To begin to address these problems, the `riak_kv_reaper` and `riak_kv_eraser` have been introduced in Riak KV 2.9.1.

## Riak KV Reaper

The `riak_kv_reaper` is a process that receives requests to reap tombstones, queues those requests, and continuously reaps tombstones from that queue when processing time is available.  

The reaper queue can be fed via a tictac aae fold from the riak client - `aae_reap_tombs`:

```
-spec aae_reap_tombs(pid(),
                    riakc_obj:bucket(), key_range(),
                    segment_filter(),
                    modified_range(),
                    change_method()) ->
                        {ok, non_neg_integer()} | {error, any()}.
```

Reaping tombstones can be done only against a single specific bucket at a time, and can be further restricted to a key range within the bucket.  A segment filter can be added to only reap tombstones within a given part of the AAE tree; the segment filter may be useful when trying to break up reaping activity to do only a proportion of required reaps at a time.  A modified range should be passed so that the reap can be restricted only to tombstones which have existed beyond a certain point - for example to only reap tombstones more than one month old.

The reap fold will then discover keys to be reaped, and queue them for reaping (or count them if `change_method` is set to count).  To run reaps in parallel across nodes in the cluster use `local` as the `change_method`.  To have a single queue of reaps for a single process dedicated to this fold then `{job, JobID}` can be passed as the `change_method`.

The actual reap will remove both the tombstone from the backend as well as removing the reference from the Active Anti-Entropy system.  Before attempting a reap a check is made to ensure all primary vnodes in the preflist are online - and if not the reap will be deferred by switching it to the back of the queue.  If a reap were to proceed without a primary being available, then it is likely to be eventually resurrected through anti-entropy.

The reaping itself will only act if:

* the object to be reaped is confirmed as a tombstone, and;

* the object to be reaped has the same vector clock as when the reap requirement was discovered (the comparison is based on a hash of the sorted vector clock).

Note that when using `riak_kv_ttaaefs_manager` for full-sync, or any riak_repl full-sync mechanism, that is reap jobs are not co-ordinated between clusters tombstones will be resurrected by full-sync jobs.

## Riak KV Eraser

The `riak_kv_eraser` is a process that receives requests to delete objects, queues those requests, and continuously delete objects from that queue when processing time is available.  The eraser is simply an unscheduled garbage collection process at the moment, but is planned to be extended in 2.9.2 to be part of a more complete TTL management solution.

The eraser queue can be fed via a tictac aae fold from the riak client - `aae_erase_keys`:

```
-spec aae_erase_keys(pid(),
                    riakc_obj:bucket(), key_range(),
                    segment_filter(),
                    modified_range(),
                    change_method()) ->
                        {ok, non_neg_integer()} | {error, any()}.
```

The function inputs are the same as with `aae_reap_tombs`.  For this fold, the results will be queued for the `riak_kv_eraser`.  If all primary vnodes are not up, then as with the reap the delete will not be attempted, but will be re-queued.

The delete will only complete if the object to be deleted has a vector clock equal to that discovered at the time the delete was queued.


## Outstanding TODO

* Allow for scheduled reap and erase jobs to generate reap and erase activity.

 * This makes more sense for erase jobs to have them auto-scheduled, as reap jobs won't naturally co-ordinate between clusters - so tombstones may resurrect through full-sync.


* Change replication so that it will filter the sending of tombstones beyond a certain modified time, so as not to resurrect old tombstones via full-sync.

* Have a TTL bucket property so that GETs can be filtered beyond that modified time.  Need to consider local GETs (e.g. at a vnode before a PUT).
