* [Overview](#overview)
* [Managing ensembles](#managing-ensembles)
* [Consistent operations](#consistent-operations)
* [Ensemble backend implementation](#ensemble-backend-implementation)
* [Anti-entropy on consistent data](#Anti-entropy-on-consistent-data)
* [Stats](#stats)
* [Status command](#status-command)
* [Known issues](#known-issues)

# Overview

Strongly consistent operations in Riak are implemented using the riak_ensemble library (1).
Check out the developer documentation to understand how riak_ensemble works (2).
This page describes only the riak_kv integration points. 
There are also integration points in riak_core that ensure that the riak_ensemble cluster information is kept in sync as riak_core nodes are added and removed (3).

Ensembles are mapped to preference lists, the replication groups assigned to an object by consistent hashing. Ensemble peers use a K/V backend that encapsulates riak_kv vnodes (4).

1. [riak_ensemble Repository](https://github.com/basho/riak_ensemble)
2. [riak_ensemble developer documentation](https://github.com/basho/riak_ensemble/tree/develop/doc)
3. [Integration with riak_core](https://github.com/basho/riak_core/wiki#strong-consistency)
4. [riak_kv_ensemble_backend](../src/riak_kv_ensemble_backend.erl)

# Managing Ensembles

An ensemble is created for each primary preference list. Unlike eventually consistent Riak, fallback vnodes never participate in consistent operations.  For example, if there are buckets with a replication factor (n_val) of 2 and some with 3, two ensembles will be created for each partition of the ring.  For partition zero, for example, we will have {0, 2} and {0, 3}.

The riak_kv_ensembles process (1) in the claimant node (2) polls the ring for changes at regular intervals and creates any ensembles that are missing (3) (4).  Notice that there is no way to delete an ensemble yet. Also, vnodes moving to different nodes are not detected here. That happens on the backend tick, called periodically by each ensemble leader (5). If vnodes in the preference list have moved to other nodes, it asynchronously requests the leader to remove the orphaned peers and add new ones corresponding to the new location of the vnodes (6).

The riak_kv_ensembles process is not made part of the riak_kv supervisor hierarchy if ensembles are not enabled. In fact, the whole riak_ensemble process hierarchy is omitted by the riak_core supervisor if not enabled at startup.

1. [riak_kv_ensembles](../src/riak_kv_ensembles.erl)
2. [claimant node](https://github.com/basho/riak_core/wiki#claimant)
3. [riak_kv_ensembles:tick/1][]
4. [riak_kv_ensembles:bootstrap_preflists/2][]
5. [riak_kv_ensemble_backend:tick/5][]
6. [riak_kv_ensemble_backend:maybe_async_update/2][]
7. [riak_kv_sup:init/1][]

# Consistent operations

Consistent operations (1) are handled by the riak_client module, just like regular operations (2). If the bucket has the `{consistent, true}` property, the request is sent to the ensemble system instead (3) (4) (5).
Legacy buckets (that is, in the default bucket type) are not allowed to be consistent (6) (7).

Consistent objects are implemented just like regular riak objects, but their vector clocks only ever have one single entry for the fake `eseq` actor id, where the count is the epoch and sequence number used by riak_ensemble to version operations encoded as a 64 bit integer (8) (9).

Consistent puts come in a variety of flavors with different semantics (1), so put requests in Riak need to be mapped to one of them. If the object in the put request has a vector clock, a conditional atomic update is issued. That is, if the object has been modified since the client fetched it, the request will fail. Otherwise a put_once operation is issued, which will fail if the object already exists. Earlier versions would use unsafe overwrite semantics if the input object did not have a vector clock.  Joe decided that it was too common for clients to send an object without a vector clock, potentially causing unsafe overwrites when not intended. So even if mentioned in the code, the check for the `if_none_matched` option is superfluous now (10) (11).

Delete operations are similar. Riak client deletes (without vector clocks) are converted to unsafe ensemble deletes. Whereas delete_vclock operations map to safe deletes. Specifically, the safe variant is used in the sequence: `Obj = get(), delete(Obj)` where Riak fails the delete if the object has changed since the get (12) (13).

1. [ensemble K/V operations](https://github.com/basho/riak_ensemble/blob/develop/doc/Readme.md#kv-operations)
2. [riak_client](../src/riak_client.erl)
3. [riak_client:consistent_object/2][]
4. [riak_client:get/4][]
5. [riak_client:put/3][]
6. [riak_kv_bucket:validate_update_consistent_props/2][]
7. [riak_kv_bucket:validate_update_bucket_type/2][]
8. [riak_kv_ensemble_backend:set_epoch_seq/3][]
9. [riak_kv_ensemble_backend:get_epoch_seq/1][]
10. [riak_client:consistent_put_type/2][]
11. [riak_client:consistent_put/3][]
12. [riak_client:consistent_delete/5][]
13. [riak_client:consistent_delete_vclock/6][]

# Ensemble backend implementation

The riak_ensemble library allows for pluggable key/value storage backends. The riak_kv_ensemble_backend module (1) translates key/value operations performed by a leader or follower peer into riak_kv vnode commands (2) (3). It also encapsulates handling of object data,  which is stored in regular riak objects as explained in the [consistent operations](#consistent-operations). The backend code makes the peer process monitor both its corresponding vnode and vnode proxy processes (4). There is a note in the code that detects when those processes die
about possible races, so watch out (5). The backend periodic tick callback is responsible for changing the ensemble if vnodes move as described in the [managing ensembles](#managing-ensembles) section. The ready_to_start callback prevents peers from starting before the riak_kv service is up (6).  The ping callback would alert an ensemble leader if its vnode is not available or unbearably slow, which may make the leader step down and allow another one to lead the ensemble (7).

Notice how backend messages to vnodes are all asynchronous casts, so they are handled in the handle_info callback (8), or in handle_overload_info (9) when overload is detected by the vnode proxy. Replies are sent directly from the vnodes back to the peers using the backend reply function (9). 

Notice that there is no special handling of folds, so those do not have any strong consistency guarantees.  Writes that failed to reach a quorum may appear on a fold.

1. [riak_kv_ensemble_backend](../src/riak_kv_ensemble_backend.erl)
2. [riak_kv_ensemble_backend:get/3][]
3. [riak_kv_ensemble_backend:put/4][]
4. [riak_kv_ensemble_backend:init/3][]
5. [riak_kv_ensemble_backend:handle_down/4][]
6. [riak_kv_ensemble_backend:ready_to_start/0][]
7. [riak_kv_ensemble_backend:ping/2][]
8. [riak_kv_vnode:handle_info/2][]
8. [riak_kv_vnode:handle_overload_info/2][]
9. [riak_kv_ensemble_backend:reply/2][]


# Anti-entropy on consistent data

Anti-entropy on consistent objects is mostly the same as with regular objects. The only difference is that when data discrepancy is detected during an AAE exchange, a consistent read with the read_repair flag is issued instead of a regular read (1) (2). Notice that ensembles have their own form of anti-entropy, with peers having their own Merkle trees (3).  This all happens in the riak_ensemble library without any riak_kv involvement except for backend reads and writes.

1. [riak_kv_exchange_fsm:read_repair_keydiff/4][]
2. [riak_kv_exchange_fsm:repair_consistent/1][]
3. [Peer trees in riak_ensemble](https://github.com/basho/riak_ensemble/blob/develop/doc/Readme.md#peer-trees)

# Stats

Consistent operations increment the same vnode level get and put stats as normal Riak operations since the backend uses the same vnode code. However, they do not increment any of the node_fsm_ stats. Instead, there is a separate set of stats that correspond to the equivalent stats for eventually consistent operations (1) updated in the riak_client code for consistent operations (2). 

1. [riak_kv_stat_bc:consistent_stats/0][]
2. [riak_client:maybe_update_consistent_stat/5][]

# Status command

A console command displays information about the ensemble system (1). It simply queries the local ensemble manager process (2) for status and the current list of registered ensembles, then requests information about the current leader, leader status and member list from each ensemble using the ping_quorum command (3) (4).

1. [riak_kv_console:ensemble_status/1][]
2. [riak_ensemble managers](https://github.com/basho/riak_ensemble/blob/develop/doc/Readme.md#managers)
3. [riak_kv_ensemble_console:get_quorums/1][]
4. [riak_kv_ensemble_console:ping_quorum/1][]


# Known issues

* Consistent data does not currently support secondary indexes.
* Consistent operations currently bypass the overload protection system and need their own overload protection to be added.
* Deleting consistent data leaves around tombstones that are not currently harvested.
* Reading a non-existing object leaves a tombstone.




[riak_client:consistent_delete/5]: ../src/riak_client.erl#L388
[riak_client:consistent_delete_vclock/6]: ../src/riak_client.erl#L456
[riak_client:consistent_get/4]: ../src/riak_client.erl#L97
[riak_client:consistent_object/2]: ../src/riak_client.erl#L863
[riak_client:consistent_put/3]: ../src/riak_client.erl#L222
[riak_client:consistent_put_type/2]: ../src/riak_client.erl#L249
[riak_client:get/4]: ../src/riak_client.erl#L143
[riak_client:maybe_update_consistent_stat/5]: ../src/riak_client.erl#L116
[riak_client:put/3]: ../src/riak_client.erl#L276
[riak_kv_bucket:validate_update_bucket_type/2]: ../src/riak_kv_bucket.erl#L128
[riak_kv_bucket:validate_update_consistent_props/2]: ../src/riak_kv_bucket.erl#L330
[riak_kv_console:ensemble_status/1]: ../src/riak_kv_console.erl#L705
[riak_kv_ensemble_backend:get/3]: ../src/riak_kv_ensemble_backend.erl#L142
[riak_kv_ensemble_backend:get_epoch_seq/1]: ../src/riak_kv_ensemble_backend.erl#L86
[riak_kv_ensemble_backend:handle_down/4]: ../src/riak_kv_ensemble_backend.erl#L166
[riak_kv_ensemble_backend:init/3]: ../src/riak_kv_ensemble_backend.erl#L55
[riak_kv_ensemble_backend:maybe_async_update/2]: ../src/riak_kv_ensemble_backend.erl#L214
[riak_kv_ensemble_backend:ping/2]: ../src/riak_kv_ensemble_backend.erl#L229
[riak_kv_ensemble_backend:put/4]: ../src/riak_kv_ensemble_backend.erl#L147
[riak_kv_ensemble_backend:ready_to_start/0]: ../src/riak_kv_ensemble_backend.erl#L234
[riak_kv_ensemble_backend:reply/2]: ../src/riak_kv_ensemble_backend.erl#L158
[riak_kv_ensemble_backend:set_epoch_seq/3]: ../src/riak_kv_ensemble_backend.erl#L79
[riak_kv_ensemble_backend:tick/5]: ../src/riak_kv_ensemble_backend.erl#L194
[riak_kv_ensemble_console:get_quorums/1]: ../src/riak_kv_ensemble_console.erl#L286
[riak_kv_ensemble_console:ping_quorum/1]: ../src/riak_kv_ensemble_console.erl#L297
[riak_kv_ensembles:bootstrap_preflists/2]: ../src/riak_kv_ensembles.erl#L171
[riak_kv_ensembles:tick/1]: ../src/riak_kv_ensembles.erl#L138
[riak_kv_exchange_fsm:read_repair_keydiff/4]: ../src/riak_kv_exchange_fsm.erl#L281
[riak_kv_exchange_fsm:repair_consistent/1]: ../src/riak_kv_exchange_fsm.erl#L299
[riak_kv_stat_bc:consistent_stats/0]: ../src/riak_kv_stat_bc.erl#L443
[riak_kv_sup:init/1]: ../src/riak_kv_sup.erl#L43
[riak_kv_vnode:handle_info/2]: ../src/riak_kv_vnode.erl#L1031
[riak_kv_vnode:handle_overload_info/2]: ../src/riak_kv_vnode.erl#L463
