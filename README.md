# Riak Protocol Buffers Messages

[![Build Status](https://secure.travis-ci.org/basho/riak_pb.png?branch=master)](http://travis-ci.org/basho/riak_pb)

This repository contains the message definitions for the Protocol
Buffers-based interface to [Riak](https://github.com/basho/riak) and
various Erlang-specific utility modules for the message types.

This is distributed separately from the Riak server and clients,
allowing it to serve as an independent representation of the supported
messages. Additionally, the `.proto` descriptions are broken out by
functional area:

* `riak.proto` contains "global" messages like the error message and
  the "server info" calls.
* `riak_kv.proto` contains messages related to Riak KV.

Other specifications may arise as more features are exposed via the PB
interface.

## Protocol

The Riak PBC protocol encodes requests and responses as Protocol
Buffers messages.  Each request message results in one or more
response messages.  As message type and length are not encoded by PB,
they are sent on the wire as:

    <length:32> <msg_code:8> <pbmsg>

* `length` is the length of `msg_code` (1 byte) plus the message length
  in bytes encoded in network order (big endian).

* `msg_code` indicates what is encoded as `pbmsg`

* `pbmsg` is the encoded protocol buffer message

On connect, the client can make requests and will receive responses.
For each request message there is a corresponding response message, or
the server will respond with an error message if something has gone
wrong.

The client should be prepared to handle messages without any `pbmsg`
(i.e. `length == 1`) for requests where the response is simply an
acknowledgment.

In some cases, a client may receive multiple response messages for a
single request. The response message will typically include a boolean
`done` field that signifies the last message in a sequence.

### Registered Message Codes

[Message codes](http://docs.basho.com/riak/latest/dev/references/protocol-buffers/#Message-Codes) and documentation can be found in the protocol-buffers
[section](http://docs.basho.com/riak/latest/dev/references/protocol-buffers/) of the online docs.

## Contributing

Generally, you should not need to modify this repository unless you
are adding new client-facing features to Riak or fixing a
bug. Nevertheless, we encourage contributions to `riak_pb` from the
community.

1. Fork the [`riak_pb`](https://github.com/basho/riak_pb) repository
   on Github.
2. Clone your fork or add the remote if you already have a clone of
   the repository.

    ```
    git clone git@github.com:yourusername/riak_pb.git
    # or
    git remote add mine git@github.com:yourusername/riak_pb.git
    ```

3. Create a topic branch for your change.

    ```
    git checkout -b some-topic-branch
    ```

4. Make your change and commit. Use a clear and descriptive commit
   message, spanning multiple lines if detailed explanation is needed.
5. Push to your fork of the repository and then send a pull-request
   through Github.

    ```
    git push mine some-topic-branch
    ```

6. A Basho engineer or community maintainer will review your patch and
   merge it into the main repository or send you feedback.

## Build Prerequisites

* protoc v 2.5.0

On OSX the default version installed by brew is 2.6.x (Nov 2015)
this version is too new for the Maven protocol-buffers plugin.

You can install the correct version like so.
    
  ```
    brew tap homebrew/versions
    brew install homebrew/versions/protobuf250
  ```

