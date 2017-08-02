# Node Diversity
## Background
At NHS Digital, we use Riak as part of the Spine system that stores patient information and data, and which is vital to the efficient running of the UK NHS technology systems that support the healthcare process. The data stored includes patient demographics data, prescriptions, clinical summary records, and much more.

It is important that this system is available, but also that the storage of data is durable.  Once the system receives and acknowledges a reliable message from a partner system, we guarantee that we will not lose this message, and that it will be processed.  As a distributed database Riak helps us keep this guarantee, and helps us to ensure once we have processed such a message, we don't lose the patient data or the changes made to it.

An important feature of a distributed database is that this data is written to more than one node, to help us ensure that data isn't lost due to individual hardware failures. To achieve this, the Spine system has historically used the primary write option in Riak, setting pw=2. The claim algorithm which distributes partition responsibility around the nodes will guarantee that primary vnodes are on different nodes.  The pw setting is used therefore as a proxy for physical diversity guarantees.  With this condition applied, the durability of writes can be guaranteed even when individual nodes in the cluster fail - and acknowledgements can safely be issued once database writes have completed.

## The Problem
The problem starts when a second node fails. This means that pw=2 cannot be satisfied for all writes, as the preflist (the list of vnodes currently supporting writes for that partition of the keyspace) for some writes may now contain a single primary node and two fallback nodes. In this case, since pw=2 has not been satisfied, the cluster begins to reject writes.

This is not a problem for availability of the Spine system, as the system runs on two separate sites with reconciliation and automated fail-over between them, so service is maintained seamlessly by the switch of service to the alternative site.  It would though, be easier and smoother for us if we could handle this scenario without having to do a site-switch.

Since each of our database clusters have 7-9 nodes, when the claim algorithm works efficiently, fallback nodes are almost certainly also physically diverse nodes to the remaining primary nodes.  That is to say, the writes that are being failed when two nodes are down, almost certainly have been written to physically diverse nodes.  The writes are not being failed as the desired condition (of physical diversity) has not been met, it is just that the ring-based promise within the pw option cannot confirm that it has been met.

# The Solution
The solution to the problem is to more directly tie configuration options over diversity to the physical layout of the system at run-time, not the abstract concept of the ring.  It would be helpful if Riak requests could be configured to directly confirm if they have achieved a desired level of node diversity.

## Implementation
When riak receives a 'put', it starts up a [riak_kv_put_fsm](../src/riak_kv_put_fsm.erl) (finite state machine). This [prepares](../src/riak_kv_put_fsm.erl#L277) and then [validates](../src/riak_kv_put_fsm.erl#L386) the options, then calls any [precommit hooks](../src/riak_kv_put_fsm.erl#L501), before [executing a put to the local vnode](../src/riak_kv_put_fsm.erl#L542) in the preflist, which becomes the co-ordinating node. This then [waits for the local vnode response](../src/riak_kv_put_fsm.erl#L564) before [executing the put request remotely](../src/riak_kv_put_fsm.erl#L598) on the two remaining nodes in the preflist.

The fsm then [waits for the remote vnode responses](../src/riak_kv_put_fsm.erl#L628), and as it receives responses, it [adds these results](../src/riak_kv_put_core.erl#L88) and checks whether [enough](../src/riak_kv_put_core.erl#L111) results have been collected to satisfy the bucket properties such as _'dw'_ and _'pw'_. 

This stage has now been enhanced through this branch.  When analysing the responses, Riak will now [count the number of different nodes](../src/riak_kv_put_core.erl#L246) from which results have been returned.  The finite state machine can now be required to wait for a minimum number of confirmations from different nodes, whilst also ensuring all other configured options are satisfied.

Once all options are satisfied, the [response is returned](../src/riak_kv_put_fsm.erl#L766), [post commit hooks](../src/riak_kv_put_fsm.erl#L666) are called and the [fsm finishes](../src/riak_kv_put_fsm.erl#L683).

### What's in an option name?
The hardest question is what to call the option? _'pd'_ (physical diversity) was considered, but the option only provides node diversity and cannot guarantee physical diversity (see Limitations). _'nd'_ was considered but both 'n' and 'd' are already specific terms within Riak and may lead to confusion.

Terms such as _'node_diversity'_ sound like a boolean option, rather than expecting an integer number.

Hence we have decided to call this option _'node_confirms'_,as this is the number of different nodes we require to confirm their write has been successful. We chose to break from the 'traditional' two letter options such as pw and dw as they are vnode based options, whereas this is a node based option, and it is also more descriptive.

## Testing
Testing is done by adding unit tests for the count mechanisms, and by creating a [riak_test](https://github.com/ramensen/riak_test/blob/rs-physical-promises/tests/node_confirms_vs_pw.erl) that does the following:
* Start a 5 node cluster
* Get a preflist for a key
* Stop 2 primary nodes for that key
* Wait for a new preflist confirming there are 2 fallbacks
* Check a pw=2 write fails
* Check a node_confirms=2 write succeeds
* Check a node_confirms=4 write is rejected as a bad value (n_val=3!)
* Stop another node in the preflist (now only 2 nodes remain up)
* Wait for a new preflist
* Check a node_confirms=3 write fails

## Limitations
While this goes a way to providing physical diversity, there are limitations to this solution, and it is only a start. For instance, there is no concept of rack awareness and rack diversity. If the nodes are based in the cloud, there is no easy way to determine whether the backend database for a node is stored on the same physical storage device as that for another node, nor is there any awareness of availability zones.

Solving these issues would require more engineering effort, and for the purposes of NHS Riak, is not currently required.
