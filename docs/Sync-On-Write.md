# Sync On Write

## Background

The _Sync on Write_ feature, also known as _Selective Sync_, is the second of two features added to Riak intended to provide greater flexibility and control for write operations.  These improvements have been developed to better support the durability and availability guarantees required by the NHS Spine project.  

The first of these improvements, made in the Riak 2.9 release, was to add the [node diversity](Node-Diversity.md#node-diversity) feature.  Node diversity allows Riak users to be explicit about how many physical devices should have received a write, before a write is acknowledged.  The second improvement is required to complete assurance about durability, to be add further controls over when writes have actually been persisted to disk.

For Riak prior to release 3.0.8, being specific about persistence required synchronisation to be enabled on the backend (i.e. leveled, bitcask or eleveldb).  The Riak put process supports the `dw` value, which nominally denotes how many nodes on which the write has been made durable - however the meaning of durability in this case is "with the backend", it offers no information as to whether physical persistence has actually been forced.  The `dw` parameter will mean with the backend *and* flushed to disk, if and only if synchronisation of each and every write is enabled at the backend.

Forcing each and every write to be flushed to disk at the backend, in order to be sure that at least `dw` writes have been persisted, is expensive.  With two clusters, and an n-val of 3 it requires 6 flushes for every write.  This volume of disk sync actions can result in significant direct I/O charges in cloud environments.  In other hosted environments, the costs can be mitigated by employing specialist hardware such as flash-backed write caches, but these devices can have a notable negative impact on node reliability.

Further, with backend sync enabled, all writes are flushes not just writes being currently received from the application.  So writes during transfers, read repairs and replication events all trigger disk syncs.  These overheads combined, had led to a recommendation that sync should not be enabled on Riak backends; but guarantees of a flush are still useful to applications with exacting durability requirements.

For the NHS Spine project, when we considered the actual durability need, the specific requirements were that:

- For some buckets, assuming node diversity, it was necessary to know that a PUT had been flushed at least once before acknowledgement (to protect against concurrent and immediate power failure of multiple nodes).

- For some buckets it was acceptable to simply rely on node diversity and the regular flushing of the file system.

- It was explicity not desirable to flush to disk background writes (such as those resulting from handoffs).

To meet these requirements backend sync is currently enabled, but considering the actual requirement, more than 90% of the sync events in the database are unnecessary.  Greater efficiency is clearly possible.

## The Solution

In [Riak 3.0.8 the bucket property `sync_on_write` has been added](https://github.com/basho/riak_kv/pull/1794).  Like other PUT-related bucket properties it can be over-written via the API on an individual PUT.  There are three possible values:

- 'All'; flush on all n-val nodes for each PUT.

- 'One'; flush only on the co-ordinating vnode, on other nodes the default backend policy will be followed.

- 'Backend'; follow in all cases the backend policy (the current behaviour, and default property).

For PUTs not initiated by a client via the PUT API (e.g. replication events, read repairs, handoffs etc), then the backend policy will be honoured as before.

This allows for the backend policy to be left to *not* sync on write, but still allow for more selective flushes using bucket properties or the Riak client API.

## Notes on Implementation

The three most common persisted backends - bitcask, eleveldb and leveled - have been updated to support this feature.  

Using a `sync_on_write` bucket property of `one` is complimentary to the [vnode mailbox check feature](https://github.com/basho/riak_kv/pull/1670) added in Riak 2.9.  The feature allows the size of the vnode queues to influence the choice of coordinator for a PUT.  Having the coordinator only apply the flush now leads to reduce disk I/O load on nodes that are running slow (and have longer vnode mailbox queues), diverting this overhead and balancing work in the cluster to nodes with more current capacity.

## Notes on Flushing

In this case, the meaning of "flushing to disk"  means the consequence of calling [file sync](http://erlang.org/doc/man/file.html#sync-1) on the file to which the object has just been written.  The actual impact of this, and the precise reliability guarantee this offers may vary greatly depending on the operating system, the file system and the choice of hardware.  More data may be flushed than the last write, and whether that data is [reliably persisted may not be certain](https://danluu.com/file-consistency/).  
