# Tabular Bucket Types
## Abstract
Riak TS is built upon Riak KV which is built upon Riak Core in so much as
Riak Core was factored out of a monolithic Riak KV and in so much as Riak TS
utilizes several function call paths of Riak KV. However, Riak TS fundamentally
differs as neither the table nor a record within a table is a Riak Object in the
sense that consumers as well as developers of Riak KV have committed to memory,
the rules to reason about to guarantee eventual consistency in an AP system.
To create a coherent understanding of Riak TS Table and Record entities, we will
explore the operations that act upon these entities with specific callouts to
differences between the TS entities and Riak KV ~entities Bucket Type, Bucket,
Key, and Object.

Subsets of Riak TS operations are exposed via native Erlang, riak-admin, HTTP,
and protobuf (PB). However, the full set of operations described herein are
exposed via the SQL subset supported by Riak TS. The SQL language implementation
is exposed via the HTTP and PB interfaces, so is available to all clients in a
distributed network setting. For this reason, all client operations herein will
be described via SQL while all Riak TS internal operations will be described via
Erlang snippets or message sequence chart.

## Status
This document only fleshes out table creation, which highlights the role that
Riak Core plays in storing and disseminating (via plumtree broadcast) the
table metadata and the role that Riak TS plays in responding to cluster-wide
broadcast and RPC to compile and surface the compiled state throughout the
cluster.

## Table Operations
### Create
When a client issues a SQL `CREATE TABLE <table_name> ...` statement, Riak TS
parses and transforms the SQL into a data-definition language (DDL) format of
the table and requests Riak Core to store the DDL as a new bucket type property.

To ensure the bucket type operation is consistent, the bucket type activation
request is serialized through the Riak Core Claimant. The Claimant orchestrates
the activity of creating and activating the bucket type which results in the
plumtree broadcast of the bucket type metadata around the cluster.

Since LevelDB and Riak KV are unaware of the Riak TS record format, translation
to and from a raw Riak Object format are performed by Riak TS. While a general
translation could be performed, this would require traversing lists to access
fields. Therefore table-specific mapping is preferred and provided by compiling
such modules, generally termed DDL module (compiled and beamed around the
cluster for each table) within Riak TS.

The compilation of the table module on each node is handled by a Riak TS specific
metadata store listener which forwards metadata store events to the Riak TS
new type module which translates the DDL into a compiled module. Compilation
results in storing the beam in the ddl_ebin directory. The presence of these
modules is used in determining the status of the table-specific compiled module
on each node.

While the SQL `CREATE TABLE <table_name> ...` operation could be implemented as
an eventually consistent operation, like most Riak KV storage operations, this
would put the burden upon the caller to retry on failure due to a "not yet
active" state for subsequent queries. Such a lack of certainly consistent
postcondition, especially for the high frequency of SQL `INSERT INTO <table_name> ...`
or PB PUT of records for Riak TS workloads, would lead not only a less desireable
user experience, but also a degradation in performance. To that end, the table
create operation is consistent, not returning until the table is created on all
active nodes in the cluster.

A high-level trace of the activity of table creation is provided to highlight
the interaction points between key modules as well as indicate sync and async
points within the flow.
[Message Sequence Chart](./tabular_buckets_create_table.svg)

### List
TODO: document SQL `SHOW TABLES`
### Update
TODO: document SQL `ALTER TABLE <table_name> ...`, when it is available
### Delete
TODO: document SQL `DROP TABLE <table_name>`, when it is available

## Record Operations
### List
TODO: document read operation, SQL `SELECT ... FROM <table_name> WHERE ...`
### Update
TODO: document update operation, SQL `INSERT INTO <table_name> ...`
TODO: document update operation, SQL `UPDATE <table_name> SET ... WHERE ...`, when it is available
TODO: document HTTP Put of a TS record
### Delete
TODO: document delete operation, SQL `DELETE FROM <table_name> WHERE ...`
TODO: document SQL `TRUNCATE TABLE <table_name>`, when it is available
TODO: document HTTP Delete of a TS record
