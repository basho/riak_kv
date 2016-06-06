## Context

Historically Riak's coverage plans were strictly internal constructs
to support 2i, mapreduce, and similar mechanisms that needed to know
the minimal set of servers required to extract a single copy of every
key in the system.

With Spark integration for Basho's data platform and timeseries, this
became an externally-accessible API so that data could be queried in
parallel by clients, bypassing Riak's usual central coordination of
bulk queries.

The high level flow:

1. Client asks for a segmented coverage plan.
    * For timeseries, a SQL query must be included with the request;
      coverage plans for timeseries are **not** independent of the
      data, unlike Riak KV.
2. Each chunk that is returned contains an opaque binary to be sent
   back to Riak and a description of the node for which that chunk is
   destined.
3. Client contacts each Riak node referenced in a chunk. The query is
   the same across all nodes.
4. If the client cannot reach a Riak node, it can ask a reachable node
   for a replacement for that specific chunk of coverage plan.
5. The client assembles all results itself as relevant.

For comparison, the usual bulk query flow for Riak, from the client's
perspective:

1. Client hands its query to any Riak node.
2. Riak hands back a sorted result set.

## Key differences between KV and TS

* For KV coverage queries, the result is completely independent of the
  intended Riak query. This is due to the unstructured nature of KV
  data.
* For TS, the coverage query results are entirely dependent on the TS
  query to be issued.
* For KV, each chunk represents one of three possibilities:
    * A vnode with optional partition filter list (the absence of such
      a list indicating that the full contents of the vnode are
      required). This is a representation of the original
      ("traditional") internal coverage plan.
    * The full contents of one partition
    * A subset of one partition
* For TS, each chunk maps to exactly one quantum of data (aka one
  partition). Unlike KV, only a subset of full partition space will be
  needed, unless the TS query is extremely large.

## Code paths

### Issuing a coverage request

`riak_kv_ts_svc:sub_tscoveragereq/4` takes 4 parameters: the
table-specific helper module, the table's DDL (which is ignored), the
coverage request with a query (expressed as a `tsinterpolation`
record, table name (binary), coverage chunk to replace, and a list of
other "unavailable" coverage chunks.

A coverage chunk (including the one we're attempting to replace) is
*unavailable* when the client cannot connect to the server where one
copy of the data lives (or perhaps receives some error when issuing a
request against it).

`riak_kv_ts_svc:decode_query/2` and
`riak_kv_ts_api:compile_to_per_quantum_queries/2` will convert the
original query to a list of `riak_select_v1` records, each tied to a
specific quantum somewhere in the original query range.

### Translating SQL to coverage

The coverage plan generation begins with `riak_kv_ts_util:sql_to_cover/6`. Its arguments:

* An instance of `riak_client`
* The list of `riak_select_v1` records
* A bucket in full `{Table, Table}` format
* The coverage plan chunk to be replaced (or `undefined` if this is an entirely new request)
* A list of coverage plan chunks which could not be serviced (empty for a new request)
* An empty list for accumulation purposes (bad design, John)

See **Experimenting with coverage queries** below for an example of
what coverage chunks look like.

`riak_kv_ts_util:sql_to_cover/6` hands off responsibility to the
internal Riak client via `get_cover/5` or `replace_cover/7` (prior to
supporting TS, this was a 6-arity function).

It should be noted that the relevant function from `riak_client` is
invoked once per quantum by `riak_kv_ts_util:sql_to_cover`. It returns
a simple coverage plan (1 vnode identifier, 1 node) without the
additional where boundary metadata.

### Riak client (new coverage plan)

Arguments to `riak_client:get_cover/5`:

* The module that will generate a coverage plan
    * `riak_kv_qry_coverage_plan` for TS
    * `riak_core_coverage_plan` for KV
* The TS bucket
* The parallelization factor (`undefined` for timeseries, since we
  don't support less or more than 1 chunk per relevant partition)
    * Arguable this should be 0 instead of `undefined` to fit better with KV
* The target of this coverage plan to be passed through to the module
    * For KV this is `all` or `allup`; for TS `{#riak_select_v1{}, Bucket}`
* The client reference itself, a hidden argument in most cases

Next significant call: `create_plan/6` in the coverage plan module. Arguments:

* Target (see above)
* `n_val` for this bucket
* A `PVC` value, which indicates how many copies of each relevant
  partition should appear in the coverage plan. We use 1 consistently
  across Riak, but KV chunked coverage plans can support more than 1
  on request.
    * The use of 1 as a standard value means 2i, mapreduce are
      effectively `r=1` queries.
    * Timeseries does not support values >1 for PVC.
* A request ID generated by `riak_client:mk_reqid/0`.
* A service to check to see what nodes are up. Consistently `riak_kv`.
* A request of unknown purpose. Passed as `undefined` from
  `riak_client` and ignored. Possibly entirely my fault and ready for
  deletion. Could be intended for coverage plan replacement, which is
  now handled via separate functions.

### Riak client (replacement coverage plan)

The first 3 arguments to `riak_client:replace_cover/7` are the same as
`get_cover/5` above.

The remaining arguments:

* The coverage chunk to be replaced, as returned by
  `riak_kv_pb_coverage:checksum_binary_to_term`
* A list of other coverage chunks (also converted back to terms) that
  the client deems problematic, so we can identify other nodes to
  exclude from the replacement plan
* The client reference itself, a hidden argument in most cases

If the coverage chunk to be replaced includes a `subpartition`
property, `replace_subpartition_cover/4` is invoked, else
`replace_traditional_cover/4`, with identical arguments:

* The coverage module to be invoked
* The `n_val` for the bucket
* The chunk to be replaced
* The other down nodes as extracted from the other problematic chunks

Either `replace_traditional_chunk/7` or `replace_subpartition_chunk/7`
in the coverage module will be called and the results converted into a
list.

## Using a coverage chunk in a query (server side)

* `riak_kv_ts_svc:process/2` with `?SQL_SELECT` as its first argument
  (including the binary coverage chunk in the record) takes us
  indirectly to `sub_tsqueryreq/4`.
* `riak_kv_ts_api:query/2` is invoked with the select record,
  including coverage chunk, and DDL.
* `riak_kv_qry:submit/2` and then `do_select` with same arguments.
* `riak_kv_qry_compiler:compile/2` invokes `compile_select_clause/2`
  and `compile_where_clause/3`, the latter of which we care about.
* `riak_kv_qry_compiler:unwrap_cover/1` will split the chunk into two
  pieces: the "real" coverage proplist that matches other types of
  coverage chunks, and the "where modifications" that constrain the
  time range. The former will be stashed back into the select record
  as the `cover_context`, and the latter will be fed through the query
  chain.

## Experimenting with coverage queries

```
26> {ok, C} = riakc_ts:get_coverage(Pid1, <<"jd">>, <<"select * from jd where a = 5 and b > 0 and b < 10000000">>).
{ok,[{tscoverageentry,<<"127.0.0.1">>,10027,
                      <<131,104,2,110,4,0,131,23,20,157,104,2,108,0,0,0,2,104,2,
                        100,0,10,...>>,
                      {tsrange,<<"b">>,1,true,900000,false,
                               <<"jd / b >= 1 and b < 900000">>}},
     {tscoverageentry,<<"127.0.0.1">>,10027,
                      <<131,104,2,110,4,0,69,24,189,230,104,2,108,0,0,0,2,104,2,
                        100,0,...>>,
                      {tsrange,<<"b">>,900000,true,1800000,false,
                               <<"jd / b >= 900000 and b < 1800000">>}},
     ...
]}
31> C1 = hd(C).
#tscoverageentry{ip = <<"127.0.0.1">>,port = 10027,
                 cover_context = <<131,104,2,110,4,0,131,23,20,157,104,2,
                                   108,0,0,0,2,104,2,100,0,10,118,110,111,
                                   ...>>,
                 range = #tsrange{field_name = <<"b">>,lower_bound = 1,
                                  lower_bound_inclusive = true,upper_bound = 900000,
                                  upper_bound_inclusive = false,
                                  desc = <<"jd / b >= 1 and b < 900000">>}}
32> C1#tscoverageentry.cover_context.
<<131,104,2,110,4,0,131,23,20,157,104,2,108,0,0,0,2,104,2,
  100,0,10,118,110,111,100,101,95,104,...>>
33> binary_to_term(v(-1)).
{2635339651,
 {[{vnode_hash,1027618338748291114361965898003636498195577569280},
   {node,'dev2@127.0.0.1'}],
  {<<"b">>,{{1,true},{900000,false}}}}}
```

### Opaque coverage chunks

#### Traditional coverage plan

```
[
 {vnode_hash,1415829711164312202009819681693899175291684651008},
 {node,'dev3@127.0.0.1'}
]
```

#### Traditional coverage plan, filters on this chunk

```
[
 {vnode_hash,1370157784997721485815954530671515330927436759040},
 {node,'dev1@127.0.0.1'},
 {filters,[1370157784997721485815954530671515330927436759040]}
]
```

#### Subpartition coverage plan

```
[
 {vnode_hash,68507889249886074290797726533575766546371837952},
 {node,'dev3@127.0.0.1'},
 {subpartition,{0,150}}
]
```

#### Timeseries coverage plan

Note that this one is much unlike the others. Careless on my
part. Comments in the code indicate where we can resolve this at 1.6
(or merged KV equivalent).

`riak_client` generates the proplist; `riak_kv_ts_util` adds the where
boundary metadata.


```
{
 [{vnode_hash,753586781748746817198774991869333432010090217472},
  {node,'dev2@127.0.0.1'}],
 {<<"time">>,{{0,true},{900000,false}}}
}
```
