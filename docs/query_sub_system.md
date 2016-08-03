# The Query Sub-System

You can view this document as a prezzo at:
http://remarkjs.com/remarkise

---

## Caveat Lector

This documentation assumes that you understand Time Series from a user perspective and have read the documentation on it on the main Basho Documentation Site:
http://docs.basho.com

---

## Overview

Time Series introduces new elements into the supervision tree of `riak_kv`

![Time Series Supervision Tree](https://raw.githubusercontent.com/basho/riak_kv/feature_gg_documents_and_architecture/docs/timeseries_supervision_tree.png)

---

## Components

The `riak_kv_qry_sup` and its subsystem handles `SELECT` queries.

The `riak_kv_ts_sup` and its sub-system is used in when a new Time Series table is created on the cluster - the new table definition is gossiped around using `riak_core` and this subsystem compiles new table definitions into the helper modules that are required to make riak understand them.

The `riak_kv_w1c_sup` and its subsystem is the *normal* write-once path for `riak_kv` - which is used to write immutable data. Time Series data is immutable - and it uses this path.

---

## The Query Subsystem And `SELECT` Queries

The query system prevents individual queries from swamping the cluster and also implements a primitive load shedding capability with back-pressure to the clients to cope with the impact of multiple queries.

This overloading (and other behaviours) is configurable in the normal scheme of things via `riak.config` and `clique`.


**NOTE**: the query subsystem also handles the SQL `DESCRIBE` and `INSERT` statements. Normal client puts go directly to the write-once system - text-based `INSERT` statements only come from manual input into `riak-shell`.

---

## How The Query Sub System Works

A query is processed through the the riak_ql lexer/parser and validation pipeline, it arrives at the query sub-system **pre-approved** so to speak

![Query Sub System](https://raw.githubusercontent.com/basho/riak_kv/feature_gg_documents_and_architecture/docs/query_sub_system.png)

The actual client connection process depends on whether or not protobuffs or http is being used to connect.

---

# Configuring The Query Sub-System

* concurrent queries per node
* queue length
* number of quantas a single query can span
* query timeout

---

## 1 Configurable Concurrent Queries

Every node in a Riak TS cluster will accept `SELECT` queries.

The node that accepts the query will act as the co-ordinating node for that query inside one of the `riak_kv_qry_n` gen servers.

The number of concurrent queries a particular co-ordinating node will execute is the number of `riak_kv_qry_n` gen servers started - this is controlled by the `riak.config` variable `riak_kv.query.concurrent_queries`.

This defaults to 3.

**Note**: the value of 3 is a _finger in the air_ at this stage - pending detailed information from customers with real-world loads. The TS team would love to hear the results of experiments with varying the number of query workers under real workloads. 

---

## 2 Configurable Queue Length

To smooth the back-pressure to the clients off a bit, there is a simple queue implemented before the query workers in `riak_kv_qry_queue`.

The size of this queue is set by the `riak.config` setting `riak_kv.query.maximum_query_queue_length` which defaults to 15.

This number should be some small multiple of `riak_kv.query.concurrent_queries` - certainly less than 10.

---

## 3 Configurable Quanta A Query Can Span

The third setting controls how many quanta a `SELECT` query can span.

The value `riak_kv.query.timeseries.max_quanta_span` in `riak.config` controls how many quanta a single query can span.

The default it 5 (in practice this means 4, unless your written queries land exactly on quanta boundaries.)

**Note**: this value is again derived from a 'rule of thumb' - the TS team would love to hear the results of experiments under real workloads.

**Note**: it is now possible to specify TS tables that don't use a quantum function, merely composite keys. In this world a partition key is set like `PRIMARY KEY((family, series), family, series, additionalkey1, additionalkey2)`. Queries against tables like this are like queries that span a single quantum in quantised data.

---

## 4 Query Timeouts

Queries that take too long will timeout. The length of this timeout is controlled by `riak_kv.query.timeseries.timeout` in `riak.config`.

The default is currently 10,000 miliseconds (10 seconds).

This query simply returns an error to the client **it doesn't take any steps to kill work elements executing on other vnodes**. Decreasing the timeout may result in overload of the system.

---

# Fin