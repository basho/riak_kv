# Extending Secondary Index Queries

## Background

The current secondary index features are explained within the Riak documents - https://docs.riak.com/riak/kv/latest/developing/usage/secondary-indexes/index.html.  However, those documents don't cover some key features and issues:

- The preferred method of searching is still to use secondary indexes, not Riak search;
- The ability to support an equivalent feature to Projected Attributes;
- Advancements in the use of Map/Reduce features in relation to secondary indexes and Projected Attributes;
- The limitations in arbitrary binary support for binary indexes.
- Integrating with other search products.

## Preferred Method of Search

In the early days of Riak, there were no secondary indexes, but there was an indexing solution which partially replicated the Solr API and was built in Erlang using the [`merge_index` backend](https://github.com/basho/merge_index).  Then secondary indexes were introduced, and recommended as a simpler option with less side effects when compare to Riak Search - in particular as Riak search was considered to have the potential to be eventually inconsistent due to a lack of anti-entropy mechanisms.

At some stage, an investment was made in integrating Riak with the off-the-shelf Solr product: with the hope that this would offer-up the improved query power and performance of Solr, whilst freeing up Riak development time to focus on ensuring eventual consistency between the stores.  This effort produced the [yokozuna extension to Riak](https://github.com/basho/yokozuna), and this lead to a recommendation that this should be the preferred method of querying Riak data not secondary indexes.

There are overheads in maintaining the integration between Riak and Solr as both these products evolve over time, though.  In particular there are significant overheads in maintaining tests that demonstrate the safe migration from one version to another.  There has not been sufficient development bandwidth to maintain those tests, so the use of yokozuna is deprecated with version 3.0 of Riak, until such time as that test maintenance backlog can be addressed.

This means that again, secondary indexes are the preferred method for querying data in Riak.  However, the intention is to continue to invest in:

- Improving the features available via secondary indexes;
- Offering low-level features that simplify managing eventual consistency between Riak and any other external data store.

## Existing Secondary Index Features

### Basic Functionality

The basic query functionality is explained in the [riak documents](https://docs.riak.com/riak/kv/latest/developing/usage/secondary-indexes/index.html).

Unlike other NoSQL databases, secondary indexes are not built from the database applying rules to the stored value, they are defined by the application and explicitly added to each object - there is application-driven not schema-definition of secondary indexes.  It is possible to have an arbitrarily large number of index entries for each object, both per-index and across all indexes (although at some scale problems may be reached with HTTP client libraries managing the size of HTTP headers).  There is no way of rolling out a new index definition directly via the database, the application needs to manage the implementation of a new index, and the removal of an old index - so developers using Riak secondary indexes need to factor this requirement into their delivery and operation plans.

Secondary index changes are, outside of failure scenarios, generally immediately consistent to object changes.  At a vnode level secondary index changes happen as part of the same transaction with object changes, there is no post-commit update required that ensures a delay between object change and index update.  In failure scenarios, as a node recovers from failure, it will become a candidate for secondary index queries prior to handoff being completed - and so can return out-of-date results.  Secondary index queries are `r = 1`, so there will be no run-time validation that results represent the most up-to-date sate in the cluster.  When returning nodes following a long outage, the [`participate in coverage`](https://github.com/basho/riak_core/pull/917) option can help mitigate these risks.

There are no default limits on page sizes for queries, either in terms of results pulled from disk or results sent to the client.  This does though require for the developer to manage submitted queries to control those that could emit large result sets, and handle timeouts accordingly if an extremely large query is submitted.

Indexes have two types, integers and binary indexes.  Binary indexes can be any binary value, like bucket and key names; however, as with bucket and key names only binary values that map to strings will allow the object to be accessed via the HTTP API and via all clients.  If a non-string binary secondary index term is added to an object via the erlang client, the object will not be retrievable via the HTTP API, and any HTTP API based query request which includes that object in the result set will fail if return_terms is selected.

### Projected Attributes - Current 

Riak has no direct implementation of Projected Attributes on secondary indexes.  However, there is equivalent functionality available (albeit at a lower level of developer convenience) through the ability to overload secondary index terms.  

In Riak all queries are range queries, there is no underlying difference between the implementation of querying for a range of terms, and querying for a single term.  As there are no constraints on constructing secondary index terms, those terms can be overloaded by appending additional information, and in Riak there exists a `return_terms` feature that means that terms will be returned as well as Keys in the query results.  This allows for the application to decompose the overloaded information on the term and make additional filtering decisions based on that information.

For example, if there are records for people and a requirement to find people by their details, a secondary index could be constructed to find people based on knowledge of Year Of Birth and Family Name:

`YoBFamilyName_bin : 1982SMITH`

However, if there is potentially additional information; such as postcode history, exact date of birth and given names - these could be appended to the term with delimiters to allow them to be easily processed by an application:

`YoBFamilyName_bin : 1982SMITH|LS1_4BT.LS6_1BN|19820328|MARY.JAI.JADE`

This allows for simple concatenation queries by combining index ranges and filters, just as with projected attributes, and without the overheads of fetching the original objects from disk.  

The flexibility of allowing the developer to compose any index term with a schema they control, does though also require that the developer plan for and manage index changes and migrations.  The indexes are not defined by the database, and so cannot be smoothly transitioned between versions by the database, the transition must be managed from within the application.

The same limitations as with Projected Attributes exist:

- The developer is the query planner, and needs to understand what combinations of index ranges and filters are required to answer all the query requirements of the application; and plan within the application which combination to use in each circumstance.

- Queries over large ranges will still incur costs retrieving and processing large numbers of results; there is a penalty for ineffective query planning.

To reduce the costs associated with queries over large ranges, where many results may be filtered, Riak has an additional feature to allow results to be filtered within the database (at a vnode store level).  This prevents overheads associated with network transmission, sorting and serialisation of results prior to filtering.  The feature allows a filter in the form of a regular expression to be added to the query - `term_regex`.  The regular expression will be applied once the term has been lifted from disk, but before the result is added to any accumulator, and the processing of the regular expression application will be distributed across CPU core in the cluster to improve speed of response.

Regular expressions can provide for powerful filtering of terms, but poorly-defined or overly-complex regular expressions resulting in significant [backtracking](https://regular-expressions.mobi/catastrophic.html?wlr=1), can create a significant load.  As the developer is the query planner, not the database, the developer must ensure the the benefits of reduced serialisation outweigh the overheads of additional computation before using the `term_regex` filter.

## Projected Attributes - Extended

More complex manipulation of query results can, in theory, be managed in riak using the [Map/Reduce](https://docs.riak.com/riak/kv/2.2.3/developing/app-guide/advanced-mapreduce.1.html) capability.  However, in Riak 2.x and Riak 3.0 - only Buckets and Keys can be outputted from an index query in a Map/Reduce flow.  Given this constraint, Map/Reduce can only be efficiently used to count (rather than return) query results, whilst potentially applying filters based on the object key.  

Any more complex operations would require a Map function, and Map functions will always promp an object fetch - and the distributed object fetches in Map/Reduce queries can lead to overheads in production that are difficult to control.  Historically, although Basho advertised Map/Reduce as a feature, the operational team within Basho actively discouraged its use because of the problems controlling the overheads of distributed object fetches, especially as those fetches are not necessarily aligned-with and optimised-for the on-disk layout of objects.

A proposed improvement has been developed to allow for the Map/Reduce framework to be used with secondary index queries, and `return_terms` to allow for a more simple and flexible way to develop solutions that utilise distributed computations on Projected Attributes (passed as an overloaded term).  

For this feature, the current Map/Reduce index query API has been extended from `{index, Bucket, Index, StartKey, EndKey}` to `{index, Bucket, Index, StartKey, EndKey, ReturnTerms, TermRegex, PreReduceFuns}` where:

- ReturnTerms is a boolean to indicate if the output of the index query should be `{{Bucket, Key}, IndexData}` rather than simply {Bucket, Key} where IndexData is a list of attributes and values, with the default attribute being `term` where the value is the matching IndexTerm.

- TermRegex is a compiled regular expression that can be applied to the term to filter out so the processed result set any result that does not `match` the regular expression.

- PreReduceFuns is a list of functions that can be applied to `{{Bucket, Key}, IndexData}` tuples, and return either a (potentially altered) `{{Bucket, Key}, IndexData}` tuple or `none` (if the tuple is to be removed from the result set).  IndexData will always be a list of `{attribute, value}` tuples, initially starting as `[{term, IndexTerm}]`.  PreReduceFuns will always be either filter functions (which strip results based on some match against an attribute) or extract functions (which pull out new terms from existing terms).

Once a subset of results has been returned from a secondary index query in the new Index Map/Reduce system, it will be passed through the flters and extracts of the PreReduceFuns, and then will be either:

- Returned as a list of results back to the client;

- Or fittted to map or reduce pipes for further processing.  

### Extended Projected Attributes - Examples

The following pre-reduce extract functions have been implemented:

- Extract an integer from a binary term by position

```
%% @doc
%% Extract an integer from a binary term:
%% InputTerm - the name of the attribute from which to eprform the extract
%% OutputTerm - the name of the attribute for the extracted output
%% Keep - set to 'all' to keep all terms in the output, or 'this' to make the
%% output attribute/value the only element in the IndexData after extract
%% PreBytes - the number of bytes in the term for the start of the integer
%% IntSize - the length of the integer in bits.
%% If the InputTerm is not present, or does not pattern match to find an
%% integer then the result will be filtered out
-spec prereduce_index_extractinteger_fun({attribute_name(),
                                            attribute_name(),
                                            keep(),
                                            non_neg_integer(),
                                            pos_integer()}) ->
                                                riak_kv_pipe_index:prereduce_fun().
```

- Extract a binary from a binary term by position

```
%% @doc
%% As with prereduce_index_extractinteger_fun/4 except the Size is expressed in
%% bytes, and the atom 'all' can be used for size to extract a binary of
%% open-ended size
-spec prereduce_index_extractbinary_fun({attribute_name(),
                                            attribute_name(),
                                            keep(),
                                            non_neg_integer(),
                                            pos_integer()|all}) ->
                                                riak_kv_pipe_index:prereduce_fun().
```

- Extract a list of binary attributes via a regular expression

```
%% @doc
%% Extract a list of attribute/value pairs via regular expression, using the
%% capture feature in regular expressions:
%% InputTerm - the binary term to which the regular expression should be
%% applied
%% OutputTerms - a list of attribute names which must match up with names of
%% capturing groups within the regular expression
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% output attributes/values the only elements in the IndexData after extract
%% Regex - a regular expression (string)
-spec prereduce_index_extractregex_fun({attribute_name(),
                                        list(attribute_name()),
                                        keep(),
                                        string()}) ->
                                            riak_kv_pipe_index:prereduce_fun().
```

- Extract a subset of a bitmap using a bitmap mask

```
%% @doc
%% Apply a mask to a bitmap to make sure only bits in the bitmap aligning
%% with a bit in the mask retain their value, all other bits are zeroed. 
%% InputTerm - the name of the attribute from which to eprform the extract
%% OutputTerm - the name of the attribute for the extracted output
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% output attribute/value the only element in the IndexData after extract
%% Mask - mask expressed as an integer
-spec prereduce_index_extractmask_fun({attribute_name(),
                                        attribute_name(),
                                        keep(),
                                        non_neg_integer()}) ->
                                        riak_kv_pipe_index:prereduce_fun().
```

- Calculate then extract ahamming distance by comparison to a simhash

```
%% @doc
%% Where an attribute value is a simlarity hash, calculate and extract a
%% hamming distance between that similarity hash and one passed in for
%% comparison:
%% InputTerm - the attribute name whose value is to be tested
%% OutputTerm - the name of the attribute for the extracted hamming distance
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% calculated hamming distance the only output
%% Comparator - binary sim hash for comparison
-spec prereduce_index_extracthamming_fun({attribute_name(),
                                            attribute_name(),
                                            keep(),
                                            binary()})
                                        -> riak_kv_pipe_index:prereduce_fun().
```


The following pre-reduce filter functions have been implemented:

- Filter by range testing an attribute

```
%% @doc
%% Filter results based on whether the value for a given attribute is in a
%% range
%% InputTerm - the attribute name whose value is to be tested
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% tested attribute/value the only element in the IndexData after extract
%% LowRange - inclusive, using erlang comparator
%% HighRange - inclusive, using erlang comparator
-spec prereduce_index_applyrange_fun({attribute_name(),
                                        keep(),
                                        term(),
                                        term()}) ->
                                        riak_kv_pipe_index:prereduce_fun().
```

- Filter by matching a regular expression

```
%% @doc
%% Filter an attribute with a binary value by ensuring a match against a
%% compiled regular expression
%% InputTerm - the attribute name whose value is to be tested
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% tested attribute/value the only element in the IndexData after extract
%% Regex - a regular expression (string)
-spec prereduce_index_applyregex_fun({attribute_name(),
                                        keep(),
                                        string()}) ->
                                riak_kv_pipe_index:prereduce_fun().
```

- Filter by checking bits in a bitmap against a bitmap mask

```
%% @doc
%% Filter an attribute with a bitmap (integer) value by confirming that all
%% bits in the passed mask
%% InputTerm - the attribute name whose value is to be tested
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% tested attribute/value the only element in the IndexData after extract
%% Mask - bitmap maks as an integer
-spec prereduce_index_applymask_fun({attribute_name(),
                                        keep(),
                                        non_neg_integer()}) ->
                                riak_kv_pipe_index:prereduce_fun().
```

```
%% @doc
%% Filter an attribute by checking if it exists in a passed in bloom filter
%% InputTerm - the attribute name whose binary value is to be checked - use
%% the atom key if the key is to be checked
%% Keep - set to `all` to keep all terms in the output, or `this` to make the
%% tested attribute/value the only element in the IndexData after extract.  If
%% key is the tested attribute all IndexKeyData will be dropped if Keep is 
%% `this`
%% {Module, Bloom} - the module for the bloom code, which must have a check_key
%% function, and a bloom (produced by that module) for checking.
-spec prereduce_index_applybloom_fun({attribute_name(),
                                        keep(),
                                        {module(), any()}}) ->
                                riak_kv_pipe_index:prereduce_fun().
```

Once extracts and filters have been applied the Map/Reduce pipe will pass on a list of {{Bucket, Key}, IndexData} tuples to the next stage, where IndexData is a list of {attribute, value} tuples.  

If no map or reduce functions are added to the map/reduce pipe (i.e. an empty list is the pipeline), an unsorted set of results will be returned back to the client in the {{Bucket, Key}, IndexData} format.  Other reduce functions can be applied to refine ad reformat the results, although it should be noted that due to re-reduce overheads there may be a variable impact on response time from adding reduce functions.

- `reduce_count_inputs`.  Count the results, without stripping duplicates.

- `reduce_index_sort(attribute_name()|key)`.  Sort the results before returning, using the atom `key` as the single function argument to sort by key, or an attribute name to sort on the values of that attribute.

- `reduce_index_max({attribute_name(), MaxCount})`.  Return the MaxCount results with the highest value for the identified attribute.  Ties are resolved by returning the higher keys.  

- `reduce_index_min({attribute_name(), MinCount})`.  Return the MinCount results with the lowest value for the identified attribute.  Ties are resolved by returning the lower keys.

- `reduce_index_countby({attribute_name(), binary|integer})`.  Return a property list mapping terms to counts of terms for the identified attribute.  Attribute values can be a binary or an integer, but they must be specified in the function arguments as such.

- `reduce_index_union({attribute_name(), binary|integer})`.  Return a list of all values for the identified attribute.  Attribute values can be a binary or an integer, but they must be specified in the function arguments as such.


### Extended Projected Attributes - Using Projected Attributes

Example 1 - People Search

Let us say we want to produce a compact index that supports queries across a large number of customers, based on:

Family Name - with wildcard support, and at least first two characters supported;
Date of Birth range - which can be open-ended;
Given Name - optionally provided - normailised and phonetically matched;
Current Address - approximate matches supported.

To support this we add a pipe-delimited index entry for each customer, like this:

<<"pfinder_bin">> : <FamilyName>|<DateOfBirth>|<GivenNameSoundexCodes>|<AddressHash>

The GiveNameSoundexCodes take each GiveName of the customer, and provide a sequence of soundex codes for those given names (and nay normalised versions of those Given Names). The AddressHash takes a [similarity of hash](https://en.wikipedia.org/wiki/MinHash) of the customer address, and base64 encodes it to make sure it can fetched from the HTTP API without error.

So:

Susan Jane Sminokowski, DoB 1939/12/1, "1 Acacia Avenue, Gorton, Manchester" 

would generate an index of:

SMINOKOWSKI|19391201|S250S000J500|hC8Nky4S/u/sSTnXjzpoOg==

If we now have a query for:

FamilyName: SM.*KOWSKI
DoB: before 1941/1/1
GivenName: sounds like "Sue"
Address: similar to "Acecia Avenue, Gorton, Manchester"

This can be done as a Map/Reduce query as follows (this uses the syntax for a direct RPC-based query to a node):

```
rpcmr(
    hd(Nodes),
    {index, 
        ?BUCKET,
            <<"psearch_bin">>,
            <<"SM">>, <<"SM~">>,
            true,
            "^SM[^\|]*KOWSKI\\|",
            % query the range of all family names beginning with SM
            % but apply an additional regular expression to filter for
            % only those names ending in *KOWSKI 
            [{riak_kv_mapreduce,
                    prereduce_index_extractregex_fun,
                    {term,
                        [dob, givennames, address],
                        this,
                        "[^\|]*\\|(?<dob>[0-9]{8})\\|(?<givennames>[A-Z0-9]+)\\|(?<address>.*)"}},
                % Use a regular expresssion to split the term into three different terms
                % dob, givennames and address.  As Keep=this, only those three KV pairs will
                % be kept in the indexdata to the next stage
                {riak_kv_mapreduce,
                    prereduce_index_applyrange_fun,
                    {dob,
                        all,
                        <<"0">>,
                        <<"19401231">>}},
                % Filter out all dates of births up to an including the last day of 1940.
                % Need to keep all terms as givenname and address filters still to be
                % applied
                {riak_kv_mapreduce,
                    prereduce_index_applyregex_fun,
                    {givennames,
                        all,
                        "S000"}},
                % Use a regular expression to only include those results with a given name
                % which sounds like Sue
                {riak_kv_mapreduce,
                    prereduce_index_extractencoded_fun,
                    {address,
                        address_sim,
                        this}},
                % This converts the base64 encoded hash back into a binary, and only `this`
                % is required now - so only the [{address_sim, Hash}] will be in the
                % IndexData downstream
                {riak_kv_mapreduce,
                    prereduce_index_extracthamming_fun,
                    {address_sim,
                        address_distance,
                        this,
                        riak_kv_mapreduce:simhash(<<"Acecia Avenue, Manchester">>)}},
                % This generates a new projected attribute `address_distance` which
                % id the hamming distance between the query and the indexed address
                {riak_kv_mapreduce,
                    prereduce_index_logidentity_fun,
                    address_distance},
                % This adds a log for troubleshooting - the term passed to logidentity
                % is the projected attribute to log (`key` can be used just to log
                % the key
                {riak_kv_mapreduce,
                    prereduce_index_applyrange_fun,
                    {address_distance,
                        this,
                        0,
                        50}}
                % Filter out any result where the hamming distance to the query
                % address is more than 50
                    ]},
    [{reduce, {modfun, riak_kv_mapreduce, reduce_index_min}, {address_distance, 10}, false},
        % Restricts the number of results to be fetched to the ten matches with the
        % smallest hamming distance to the queried address
        {map, {modfun, riak_kv_mapreduce, map_identity}, none, true}
        % Fetch all the matching objects
    ]),
```

Without the term_regex feature, using a standard secondary index range query would have extracted a large number of results and sorted and serialised those results for streaming to the client (as the range would include all the SMITHs).  This sorting/serilisation delay could be significant.

The regular expression reduces this load significantly.  It could be further optimised to filter on given name rather than doing this at the prereduce stage (e.g. `"^SM.*KOWSKI\|[0-9]+\|[^\|]*S000"`), but from a development perspective forcing more work onto regular expressions can quickly become relative complex and risky.

Decoding the similarity hash, and finding the hamming distance, coulf also be done in the application (by just returning the results).  Doing this at the prereduce stage will parallelise this decoding and hamminng-distance calculation work across all the cores in the cluster though - so in some cases this may significantly reduce response times, as well as reducing the count of results to be serilaised and the number of round trips required.

As the final stage restricts the total number of results to the ten results with the closest addresses, the actual objects are fetched at this point - as there is a controllerd overhead of fecthing a fixed number of objects within the query.


Example 2 - Reporting on Conditions


Example 3 - Approximate Concatenations

Searching first for Postcode, then applying the bloom