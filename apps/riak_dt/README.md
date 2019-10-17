# riak dt

## WHAT?

A set of state based CRDTs implemented in Erlang and  on the paper -
[A Comprehensive study of Convergent and Commutative Replicated Data Types]
(http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf) - which you
may find an interesting read.

### What happend to riak_dt, the database?

Riak is getting CRDT support built in, so we've archived the old
riak_dt in the branch `prototype`. No further work will be done on
it. This repo is now a resuable library of Quickcheck tested
implementations of CRDTs.

