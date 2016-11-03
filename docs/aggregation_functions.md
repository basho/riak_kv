# Aggregation Functions


You can view this document as a prezzo at:
http://remarkjs.com/remarkise

---

## Introduction

This document explains how windows aggregation functions are executed:
* query specification
* execution

The term 'windows aggregation functions' comes from the SQL standard.

---

## Query Specification

Aggregate functions are functions that operate on a **column** of data.

```sql
SELECT AGG_FN(mycolumn) FROM mytable WHERE ...
```

The base query returns a column which is then fed into the function:

```
+==========+
| mycolumn |
+==========+
| 1        |
+----------+
| 2        |
+----------+
| hell     |
+----------+
| no       |
+==========+
```

---

### Aggregation Functions

The aggregation functions are:

* `AVG`
* `COUNT`
* `MAX`
* `MEAN` (alias for `AVG`)
* `MIN`
* `STDDEV_POP`
* `STDDEV`
* `STDEV_SAMP`
* `SUM`

More details are [here](http://docs.basho.com/riak/ts/1.3.0/using/aggregate-functions/)

---

## Execution

Consider the following query:
```sql
SELECT AVG(temperature) FROM GeoCheckin
WHERE mytime > 1452252523182 AND mytime < 1452252543182
   AND region = 'South Atlantic' AND state = 'South Carolina';
```

The logical execution of this is as follows:
```sql
For all quanta:
[
$vnode_selection = SELECT * FROM GeoCheckin 
  WHERE mytime > $lowerbound AND mytime < $upperbound 
  AND region = 'South Atlantic' AND state = 'South Carolina';
]
$finalise = SELECT AGV(temperature) FROM [$vnode_selections...]
```

---

### Chunking

Chunks of data are read from the vnodes and sent to the coordinator (`riak_kv_qry_worker`) to be consumed by the calculation.

The calculation currently consumes whole quanta in chunks - but will work equally well with streamed chunks of quanta in the future.

The actual calculations are done in `riak_ql_window_agg_fns.erl`

At the beginning of the calculation a `start_state` function is called for each calculation. As each chunk comes in the function itself is called with the continuation state - returning a new continuation state. Once the query worker is happy all the chunks have been applied a `finalise` function is called on the continuation state - yielding the query.

---

### Invocation Notes I

The vector of values being consumed can be defined by arithmetic expressions like `COUNT((1 + temperature)/3.4)`. This makes the invocation of the function call slightly more complex.

The functions themselves are typed - you can't `AVG` a column of type `varchar`.

---

### Invocation Notes II

The process is:
* typechecking the desired expression - seeing if it is valid, working out the return type and checking that against the acceptable type signatures of the windows aggregation function

Then for each row in each chunk:
* pulling the column values out of the returned results (a `SELECT` operation)
* creating an expression with the appropriate variables substituted with the actual variable values
* evaluating that expression
* invoking the chunk consumption functions against that expression value with the continuation state

Finally:
* calling the finalise function

---

# Invocation Notes III

![Sequence Diagram](https://raw.githubusercontent.com/basho/riak_kv/feature_gg_documents_and_architecture/docs/aggregation_functions.png)

---

# Future Work

The biggest performance improvement will come from query rewriting and executing `SELECT` and windows aggregation functions at the vnode as appropriate:

```sql
For all quanta:
[
$vnode_selection = SELECT COUNT(temperature) AS MyCount,
 SUM(temperature) AS MySum 
 FROM GeoCheckin 
  WHERE mytime > $lowerbound AND mytime < $upperbound 
  AND region = 'South Atlantic' AND state = 'South Carolina';
]
$finalise = SELECT SUM(MySum)/SUM(MyCount) FROM [$vnode_selections...]
```

---
# FIN