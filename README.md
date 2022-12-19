# Riak KV

[![Build Status](https://github.com/basho/riak_kv/actions/workflows/erlang.yml/badge.svg?branch=develop)](https://github.com/basho/riak_kv/actions)


## Overview

Riak KV is an open source Erlang application that is distributed using the [riak_core](https://github.com/basho/riak_kv) Erlang
library. Riak KV provides a key/value datastore and features MapReduce, lightweight data relations, and several different client APIs.

## Quick Start

You must have [Erlang/OTP 22](http://erlang.org/download.html) or later and a GNU-style build system to compile and run Riak KV. The easiest way to utilise Riak KV is by installing the full Riak application available on [Github](https://github.com/basho/riak).

## Contributing

We encourage contributions to riak_kv from the community.

1) Fork the riak_kv repository on [Github](https://github.com/basho/riak_kv).

2) Clone your fork or add the remote if you already have a clone of the repository.

```
git clone git@github.com:yourusername/riak_kv.git
```
or

```
git remote add mine git@github.com:yourusername/riak_kv.git
```

3) Create a topic branch for your change.

```
git checkout -b some-topic-branch
```

4) Make your change and commit. Use a clear and descriptive commit message, spanning multiple lines if detailed explanation is needed.


5) Push to your fork of the repository and then send a pull-request through Github.

```
git push mine some-topic-branch
```

6) A community maintainer will review your patch and merge it into the main repository or send you feedback.

## Testing

```
./rebar3 do xref, dialyzer, eunit
./rebar3 as test eqc --testing_budget 600
```

For a more complete set of tests, update riak_kv in the full Riak application and run any appropriate [Riak riak_test groups](https://github.com/basho/riak_test/tree/develop-3.0/groups)
