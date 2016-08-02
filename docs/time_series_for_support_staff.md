Time Series For Devs/Sys-Admins
===============================

So Riak Time Series has gone live in your organisation? What now? What is it? How do you support it?

You can view this document as a prezzo at:
http://remarkjs.com/remarkise

---

Overview
--------

Time Series is a storage and query layer on top of Riak KV that provides new ways of managing data on the normal Riak platform.

It also provides an SQL shell, `riak-shell`, which enables you to configure and query your Time Series system in an intuitive manner.

At its core is a new type of bucket that provides a customisable degree of data colocation.

---

Riak KV Data Location
---------------------

In Riak KV you write data to a key and it is written at (usually) 3 vnodes on (usually) 3 physical nodes in your cluster. You write data to another key and that is written to a different set of 3 vnodes on a different set of 3 physical nodes.

To query over a Key Value store like Riak KV then you need to scan the entire cluster.

---

Riak TS Data Colocation
-----------------------

With Riak TimeSeries you construct a composite key with a timestamp - and for a defined, definable duration (3 seconds, 15 minutes, 2 hours, 9 days) all data with the same composite root will be written to the same set of 3 vnodes on the same set of 3 physical nodes.

Based on this data colocation you can then write queries that go to where ranges of data are stored and get back results.

The querying of Time Series data is done via a quite limited, subset of SQL - so it works with your tools.

---

Differences With Riak KV
------------------------

Time Series is write-once data. So Riak TS uses the Write Once put path - this doesn't require a read-before-write and gives faster performance.

There are consequences to this - the responsibility for writing the data once is passed back to the data producers. If they write data more than once to the same key, and the data is different, and the Riak cluster is partitioned, or in hand-off, or some other non-perfect state, then the behaviour is undetermined. Conflicts will be resolved by a last-write-wins process and the data returned might not be what you expected - in particular data that you thought you were overwriting might be resurrected.

To further improve ingestion speed you can supply pre-batched data to Riak TS. Typically for sensor data a sensor will accumulate a bundle of data over a period of time and push it down the pipe to the database. Riak TS can handle that way of working very efficiently.

Because the data is co-located Riak TS lets you write queries to get the data out. Those queries are standard SQL - but with considerable restrictions. You should read the documentation to understand the limitations.

---

`riak-shell` The Developer/Sys-Admin's New Best Pal
---------------------------------------------------

The `riak-shell` is a new way of interacting with a Riak cluster - it lets you:

* create time series tables 
* insert data into those tables
* query data from those tables

---

`riak-shell` Logging And Replay
-------------------------------

In addition `riak-shell` can be set up to log your commands and replay those logs. This lets you prepare tables for you application in development environments, capture the configuration in text files which can be placed under version control and use those scripts to set up your Integration, UAT and Production environments.

The log facility can also be used to report bugs to Basho. Find a problem, log it happening from the shell and send us the log as a bug report with notes of how it should work. This enables Basho to reproduce your problem.

The replay facility of `riak-shell` also lets us capture your bug straight into our regression test suite - we can kill it for you and ensure that it stays killed in all future releases.

Please read the `riak-shell` documentation for more details:

* [`riak-shell` Documentation](https://github.com/basho/riak_shell/blob/develop/README.md)

---

Data On Disk
------------

When a Time Series table is created in a Riak cluster a dynamic module is generated and loaded in to the Erlang VM. This module is stored in the `data/ddl_ebin` directory alongside the `data/kv_vnode` and other data directories. The state of the compilation and the definitive list of which DDLs have been created and associated information is stored in an Erlang dets table `data/riak_kv_compile_tab.dets`

These helper modules enable the Riak cluster to introspect Time Series data on the fly. By default no source code for them is generated - only `.beam` files. If you wish to see what they look like *under the covers* the riak_ql [`README`](https://github.com/basho/riak_ql/blob/develop/README.md) has an example of one.

---

FAQ 1
-----

**Q:** What tables have been created?

**A:** list the files in `data/ddl_ebin` - they will have names like `riak_ql_table_`**TABLE NAME**`$1.beam`

---

FAQ 2
-----

**Q:** How do I find out the definition of a table that has been created?

**A:** there are 2 ways to do this:

First method:

connect to the shell by running `riak-shell` in `ebin/` and run:
```
riak-shell(7)>describe Aggregation_written_on_old_cluster;
+-------------+---------+-------+-----------+---------+
|   Column    |  Type   |Is Null|Primary Key|Local Key|
+-------------+---------+-------+-----------+---------+
|  myfamily   | varchar | false |     1     |    1    |
|  myseries   | varchar | false |     2     |    2    |
|    time     |timestamp| false |     3     |    3    |
| temperature | double  | true  |           |         |
|  pressure   | double  | true  |           |         |
|precipitation| double  | true  |           |         |
+-------------+---------+-------+-----------+---------+ 

riak-shell(8)>
```

---

FAQ 2 (cont)
------------

**Q:** How do I find out the definition of a table that has been created?

**A:** there are 2 ways to do this:

Second method:

connect to the Riak node with `riak attach` in `ebin/` and run:

`1>DDL = 'riak_ql_table_my_table_name$1':get_dll().`

**Note**: the module name must be a quoted atom because it contains a `$` symbol

You can convert that table representation to a string with a helper function:

`2>riak_ql_to_string:ddl_rec_to_SQL(DDL).`

---

FAQ 3
-----

**Q:** what can I do to change the behaviour of my query system?

**A:** we protect your Riak cluster with some fairly basic rate limiting which controls:
* the number of concurrent queries that each node will run
* the queue length before the clients experience back pressure
* the number of quanta a particular query can span

In addition you can control the timeouts.

For more details please see [the Query System](./query_sub_system.md) documentation

