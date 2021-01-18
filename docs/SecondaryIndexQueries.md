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

At some stage, an investment was made in integrating Riak with the off-the-shelf Solr product: with the hope that this would offer-up the improved query power and performance of Solr, whilst freeing up Riak development time to focus on ensuring eventual consistency between the stores.  This effort produced the [yokozuna extension to Riak](https://github.com/basho/yokozuna), and this lead to a recommendation that this should be the preferred method of querying Riak data instead of secondary indexes.

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

### Projected Attributes - Equivalent Feature

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

## Extending Projected Attributes

More complex manipulation of query results can, in theory, be managed in riak using the [Map/Reduce](https://docs.riak.com/riak/kv/2.2.3/developing/app-guide/advanced-mapreduce.1.html) capability.  However, in Riak 2.x and Riak 3.0 - only Buckets and Keys can be outputted from an index query in a Map/Reduce flow.  Given this constraint, Map/Reduce can only be efficiently used to count (rather than return) query results, whilst potentially applying filters based on the object key.

Any more complex operations would require a Map function, and Map functions will always promp an object fetch - and the distributed object fetches in Map/Reduce queries can lead to overheads in production that are difficult to control.  Historically, although Basho advertised Map/Reduce as a feature, the operational team within Basho actively discouraged its use because of the problems controlling the overheads of distributed object fetches, especially as those fetches are not necessarily aligned-with and optimised-for the on-disk layout of objects.

A proposed improvement has been developed to allow for the Map/Reduce framework to be used with secondary index queries, and `return_terms` to allow for a more simple and flexible way to develop solutions that utilise distributed computations on Projected Attributes (passed as an overloaded term).

For this feature, the current Map/Reduce index query API has been extended from `{index, Bucket, Index, StartKey, EndKey}` to `{index, Bucket, Index, StartKey, EndKey, ReturnTerms, TermRegex, PreReduceFuns}` where:

- ReturnTerms is a boolean to indicate if the output of the index query should be `{{Bucket, Key}, IndexData}` rather than simply {Bucket, Key} where IndexData is a list of attributes and values, with the default attribute being `term` where the value is the matching IndexTerm.

- TermRegex is a compiled regular expression that can be applied to the term to filter out so the processed result set any result that does not `match` the regular expression.

- PreReduceFuns is a list of functions that can be applied to `{{Bucket, Key}, IndexData}` tuples, and return either a (potentially altered) `{{Bucket, Key}, IndexData}` tuple or `none` (if the tuple is to be removed from the pipeline and hence the result set).  IndexData will always be a list of `{attribute, value}` tuples, initially starting as `[{term, IndexTerm}]`.  PreReduceFuns will always be either filter functions (which strip results based on some match against an attribute) or extract functions (which pull out new terms from existing terms).

Once a subset of results has been returned from a secondary index query in the new Index Map/Reduce system, it will be passed through the filters and extracts of the PreReduceFuns, and then will be either:

- Returned as a list of results back to the client;

- Or fitted to map or reduce pipes for further processing.

## Extending Projected Attributes - Examples


### Pre-defined Functions

In order to make secondary-indexing more powerful, we introduce a number of primitive functions that can be used to create a query. There are a number of pre-reduce functions defined that just manipulate the indices, without obtaining the real object from disk.

The input to a pre-reduce funtion is the complete index term of an object. This index term can be split in named components, for example a date of birth component and a name component. These components are hard wired in the index terms by the application level, but by naming them we can manipulate them. For example, we can manipulate a date of birth component into an age component, or filter a name component with a "sounds like" filter.
Thus, the initial input of a pre-reduce function is the index-term in the database, this is then split in a set of named index terms. Each consecutive pre-reduce function either manipulates one of the index terms by changing the content and adding one more term to the set of index terms (`extract` functions), or filters (`filter` functions)  using a specific index term, meaning that only objects for which the filter holds are considered for further processing. For performance optimizations, but in particular for manipulating the final result of a pre-reduce, there are two functions `with` and `without` to reduce the set of index terms one works with.

There are a number of predefined functions in the `riak_kv_index_prereduce` module (for prereduce functions) and the `riak_kv_mapreduce` (for reduce functions) which can be used to implement advanced queries.  The aim is to be able to form pipelines of these simple functions at the reduce stage, to support potentially complex filtering and enrichment tasks, and greatly expanding the scope of what could otherwise be achieved with projected attributes.



The following prereduce *extract* functions are available. Each of the extract functions takes as input the name of one specific index entry, the name of the resulting index term and one additional argument.

- `extract_split_index`: Splits the input field in list of new additional index fields and names each field by provided list of names. The number of fields present in the splitted index term must be equal to the number of names provided.

- `extract_integer`: Extract an integer from a binary term by position

- `extract_binary`: Extract a binary from a binary term by position

- `extract_regex`: Extract a list of binary attributes via a regular expression

- `extract_mask`: Extract a subset of a bitmap using a bitmap mask

- `extract_hamming_simhash`: Calculate then extract hamming distance by comparison to a simhash

- `extract_hamming`: Extract hamming distance by comparison to a provided hash

- `extract_hash`: Create a new index term with  the hash of term

- `extract_decode`: Decode a base64 encoded term into a binary

- `extract_buckets`: Provide a mapping to categorise the sizes of value (e.g. for histogram production)


The following prereduce *filter* functions are available:

- `apply_range`: Filter by range testing an attribute

- `apply_regex`: Filter by matching a regular expression

- `apply_mask`: Filter by checking bits in a bitmap against a bitmap mask

- `apply_remotebloom`: Filter either the key or an attribute value by checking for existence in a passed-in bloom filter

The following pre-reduce *indices*  functions exist, these functions work on all indices at once, returning a new set of indices.

- `indices_with`:  Reduces the set of named index terms to the once provided in the argument

- `indices_without`: Reduces the set of named index terms to all but provided in the argument

- `indices_coalesce`: Replaces the set of provided named index terms into one new index term with provided name.

Once extracts and filters have been applied the Map/Reduce pipe will pass on a list of {{Bucket, Key}, IndexData} tuples to the next stage, where IndexData is the list of all index terms resulting from the pre-reduce functions, The index terms are encoded as a list of {attribute, value} tuples, where the attirbute is the earlier mentioned index term name and value is the actual index term.

If no map or reduce functions are added to the map/reduce pipe (i.e. an empty list is the pipeline), an unsorted set of results will be returned back to the client in the {{Bucket, Key}, IndexData} format.  Other reduce functions can be applied to refine ad reformat the results, although it should be noted that due to re-reduce overheads there may be a variable impact on response time from adding reduce functions.

- `reduce_count_inputs`.  Count the results, without stripping duplicates.

- `reduce_index_sort(attribute_name()|key)`.  Sort the results before returning, using the atom `key` as the single function argument to sort by key, or an attribute name to sort on the values of that attribute.

- `reduce_index_max({attribute_name(), MaxCount})`.  Return the MaxCount results with the highest value for the identified attribute.  Ties are resolved by returning the higher keys.

- `reduce_index_min({attribute_name(), MinCount})`.  Return the MinCount results with the lowest value for the identified attribute.  Ties are resolved by returning the lower keys.

- `reduce_index_countby({attribute_name(), binary|integer})`.  Return a property list mapping terms to counts of terms for the identified attribute.  Attribute values can be a binary or an integer, but they must be specified in the function arguments as such.

- `reduce_index_union({attribute_name(), binary|integer})`.  Return a list of all values for the identified attribute.  Attribute values can be a binary or an integer, but they must be specified in the function arguments as such.

- `reduce_index_collateresults({attribute_name(), attribute_name(), keep(), min|max, pos_integer()})`.  Returns a list of results, sorted by specific attribute's value, up to a maximum count of results.  Alongside this returns a facet count for a given attribute of all results (not just those in the maximum set).

Some basic examples using these prereduce and reduce functions can be seen in action in the [`mapred_index_general`](https://github.com/basho/riak_test/blob/mas-i1737-indexkeydata1/tests/mapred_index_general.erl) riak_test.


### Example 1 - People Search

This query can be seen in action in the [`mapred_index_peoplesearch`](https://github.com/basho/riak_test/blob/mas-i1737-indexkeydata1/tests/mapred_index_peoplesearch.erl) riak_test.

Let us say we want to produce a compact index that supports queries across a large number of customers, based on:

- Family Name (with wildcard support);

- Date of Birth range (which can be open-ended);

- Given Name (optionally provided, to be phonetically matched);

- Current Address (approximate matches supported).


To support this the application level has added a pipe-delimited index entry for each customer, like this:

`<<"pfinder_bin">> : FamilyName|DateOfBirth|GivenNameSoundexCodes|AddressHash`

The GiveNameSoundexCodes take each GiveName of the customer, and provide a sequence of soundex codes for those given names (and any normalised versions of those Given Names). The AddressHash takes a [similarity hash](https://en.wikipedia.org/wiki/MinHash) of the customer address, and then base64 encodes it to make sure it can fetched from the HTTP API without error.

For example, the following details would map to the following index entry:

Susan Jane Sminokowski, DoB 1939/12/1, "1 Acacia Avenue, Gorton, Manchester"
    -> `SMINOKOWSKI|19391201|S250S000J500|hC8Nky4S/u/sSTnXjzpoOg==`

If we now have a query for:

- FamilyName: `SM?KOWSKI`

- DoB: `before 1941/1/1`

- GivenName: `sounds like "Sue"`

- Address: `similar to "Acecia Avenue, Gorton, Manchester"`


This query should match the example record, and this can be found by creating a Map/Reduce query.  The required query builds a set of results with projected attributes from an index query - and then manipulates those projected attributes through prereduce functions to filter and refine the results down.

*Stage 1 - Query:*

We start with one query over the full index term (before splitting it and looking into details):

```
{index,
    ?BUCKET,
    <<"pfinder_bin">>,
    <<"SM">>, <<"SM~">>,
    true,
    undefined}
```

This will perform a range query for all the family names beginning with `SM`.

*Stage 2 - Index Prereduce Functions*

To filter the results down, we check that the family name ends on "KOWSKI" using a regular expression matcher. Alternatively we coud have used a filter to check that the name does contain "KOWASKI".
To continue, the date of birth needs to be range checked, the matched name needs to be checked against the set of given name codes and finally there is a need to see how "close" lexicographically the address is to the query address.  The sequence of prereduce functions required to complete these tasks are:

```
extract_split_index([family_name, dob, givennames, address])  -> [term, family_name, dob, givennames, address]
   apply_regex(family_name, "^SM.*KOWSKI") -> only family_names matching "^SM.*KOWSKI" in  [term, family_name, dob, givennames, address]
   indices_without([term, family_name]) -> keep only the index terms [dob, givennames, address]

    apply_range(dob, [0, 19401231]) -> only people born before 1941 in [dob, givennames, address]
    apply_regex(givennames, "S000") -> only names sounding like Sue in [dob, givennames, address]
    indices_with([address]) -> only keep [address] for further index manipulations

    extract_decode(address, decoded_address, base64) -> [address, decoded_address] where address_sim is base64 decoded address
    extract_hamming_simhash(decoded_address, address_distance, "Acecia Avenue, Manchester") -> [address, decoded_address, address_distance] distance between decoded address and "Acecia Avenue, Manchester"
    apply_range(address_distance, [0,50]) -> only addresses that are at most 50 away from "Acecia Avenue, Manchester" in [address, decoded_address, address_distance]
    indices_with([decoded_address]) -> only pass on [decoded_address] to next stage
```

Detail of each function within Stage 2:

```
{riak_kv_index_prereduce, extract,
    extract_split_index,
    {term,
        [family_name,  dob,  givennames, address]}}
```

The first stage is to take the term which is delimited using "|", and split it into four parts.  The original index term `term` is kept.
After this extract, the next stage will see five projected attributes - `term`, `family_name`, `dob`, `givennames` and `address`. The used function `extract_split_index` with one argument maps to this more general version.

```
{riak_kv_index_prereduce, extract,
    apply_regex,
    {family_name,
       "^SM.*KOWSKI"}}
```
Filter all family names starting with SM and ending with KOWSKI. Then free some memory by onbly carrying around 3 of the four index fields, since we don't need the index term in family name any more.

```
{riak_kv_index_prereduce, filter,
    apply_range,
    {dob,
        [0, 19401231]}}
```

Now we have extracted the dob, we can filter out all the dates of birth outside of the range (i.e. we only want those that are born before 1941).

```
{riak_kv_index_prereduce, filter,
    apply_regex,
    {givennames,
       "S000"}}
```

Use a regular expression to only include those results with a given name which sounds like Sue (which would map to S000 in Soundex).

```
{riak_kv_index_prereduce, extract,
    extract_decoded,
    {address, address_sim,
        base64}}
```

The address sim has been base64 encoded, so this will decode.  As all other projected attributes have been used, only the outcome of this extract (address_sim) now needs be taken forward; hence the 'Keep' input is set to `this`.

```
{riak_kv_index_prereduce, extract,
    extract_hamming
    {address_sim, address_distance,
        riak_kv_index_prereduce:simhash(<<"Acecia Avenue, Manchester">>)}}
```

This generates a new projected attribute address_distance which is the hamming distance between the query and the indexed address. Note that `extract_hamming_simhash` is shorthand for using the simhash function on the provided argument.

```
{riak_kv_index_prereduce, filter,
    apply_range,
    {address_distance,
        [0, 50]}}
```

Filter out any result where the hamming distance to the query address is more than 50.

*Stage 2 - Map and reduce functions*

```
reduce_index_min ->
    map_identity
```

Detail of each function within Stage 3:

```
{reduce,
    {modfun, riak_kv_mapreduce, reduce_index_min},
    {address_distance, 10},
    false}
```

This reduce function filters the results to just the ten results with the smallest distance from the queried address.  Where there are ties, ties are resolved by sorting on Key.

```
{map,
    {modfun, riak_kv_mapreduce, map_identity},
    none,
    true}
```

Fetch the final results, which can be done safely as the previous reduce function has limited the final results to just 10.

*Notes*

Without the term_regex feature, using a standard secondary index range query would have extracted a large number of results and sorted and serialised those results for streaming to the client (as the range would include all the SMITHs).  This sorting/serialisation delay could be significant.

The regular expression reduces this load significantly.  It could be further optimised to filter on given name rather than doing this at the prereduce stage (e.g. `"^SM.*KOWSKI\|[0-9]+\|[^\|]*S000"`), but from a development perspective forcing more work onto regular expressions can quickly become relatively complex and add the risk of generating computationally complex back-tracking expressions.

Decoding the similarity hash, and finding the hamming distance, could also be done in the application (by just returning the results).  Doing this at the prereduce stage will parallelise this decoding and hamming-distance calculation work across all the cores in the cluster though - so in some cases this may significantly reduce response times, as well as reducing the count of results to be serialised and the number of round trips required.

As the final stage restricts the total number of results to the ten results with the closest addresses, the actual objects are fetched at this point.  Using map statements needs to be carefully controlled in Riak as it can create a large parallel storm of vnode activity, however in this case there is a controlled overhead of fetching a fixed number of objects, due to the result restriction made in the prior reduce statement.

If the result set being passed to the `reduce_index_min` reduce stage is large, the processing time for this stage will become a dominant factor in the overall latency of the query, as reduce stages are not parallelised.  This stage, as it involves a comparison between results, can not be converted into a prereduce function - but it can still be converted from a reduce stage to a prereduce stage (as it is the first reduce statement), to parallelise the workload.  This conversion cna be made by changing the first item of the tuple from `reduce` to `prereduce`.  For smaller results sets this change will not lead to a significant performance boost.


### Example 2 - Reporting on Conditions

This query can be seen in action in the [`mapred_index_reporting`](https://github.com/basho/riak_test/blob/mas-i1737-indexkeydata1/tests/mapred_index_reporting.erl) riak_test.

Projected attributes may also be useful when reporting on data in the database.  Where records have different categories and group memberships, it can be useful to report on counts of records by those categories and group memberships.

In this case we will consider each record to be a clinical history for the patient, and the patient has the following interesting characteristics from a reporting basis:

- Date of Birth (as reports are often required split by age groupings)

- GP Provider (the primary care organisation to which the patient is registered)

- Common significant conditions (it maybe that there are many common conditions that can be represented as a bitmap, with each bit set to 1 if the condition is true for that patient, and 0 if false; this is then base64 encoded before being added to the index to avoid issues with the HTTP API)


To support this we add a fixed-width index entry for each customer, like this [size in bytes]:

`<<"conditions_bin">> : <DateOfBirth[8]><GPProvider>[6]<EncodedConditionsBitmap>[*]`

For example, it may now be required to understand the spread of diabetes amongst the elderly across the estate by age group.  The requirement is to have a count of those with and without diabetes by age and GP Provider.  This can be supported using the following Map/Reduce query:

*Stage 1 - Query*

```
{index,
    ?BUCKET,
    <<"conditions_bin">>,
    <<"0">>, <<"19551027">>,
    true,
    undefined}
```

In this example the requirement is only to count patients over the age of 65, and we assume that 27th October 2020 is Groundhog Day for the purpose of this illustration

*Stage 2 - Index Prereduce Functions*

The sequence of functions required is:

```
extract_binary(term) -> dob

    extract_binary(term) -> gpprovider
    extract_binary(term) -> conditions_b64

    extract_buckets(dob) -> age
    extract_encoded(conditions_b64) -> conditions_bin
    extract_integer(conditions_bin) -> conditions
    extract_mask(conditions) -> is_diabetic_int
    extract_buckets(is_diabetic_int) -> diabetic_flag

    extract_coalesce([gpprovider, age, diabetic_flag]) -> counting_term

```

Detail of each function within Stage 2:

```
{riak_kv_index_prereduce,
                extract_binary,
                {term,
                    dob,
                    all,
                    0, 8}},
{riak_kv_index_prereduce,
    extract_binary,
    {term,
        gpprovider,
        all,
        8, 6}},
{riak_kv_index_prereduce,
    extract_binary,
    {term,
        conditions_b64,
        all,
        14, all}}
```

The index term is broken up into binaries using the fact that the first two elements are fixed width.

```
{riak_kv_index_prereduce,
    extract_buckets,
    {dob,
        age,
        all,
        AgeMap,
        <<"Unexpected">>}}
```

Take each date of birth, and map it to an Age category.  There should not be results over the highest Date Of Birth in the Age Map (i.e. people younger than 65).  The query should not produce results for anyone under 65 - so any dob over the highest date of birth in the AgeMap would be "Unexpected", and so this tag is used.

In this case the AgeMap is a list of tuples mapping a date of birth high-point to an Age"

```
AgeMap =
    [{<<"19301027">>, <<"Over90">>}|
        lists:map(fun(Y) -> {iolist_to_binary(io_lib:format("~4..0B", [Y]) ++ "1027"),
                                iolist_to_binary(io_lib:format("Age~2..0B", [2020 - Y]))}
                            end,
                        lists:seq(1931, 1955))],
```

The next phase requires the conditions bitmap to be decoded, and converted to be usable:

```
{riak_kv_index_prereduce,
    extract_encoded,
    {conditions_b64,
        conditions_bin,
        all}},

{riak_kv_index_prereduce,
    extract_integer,
    {conditions_bin,
        conditions,
        all,
        0, 8}},

{riak_kv_index_prereduce,
    extract_mask,
    {conditions,
        is_diabetic_int,
        all,
        1}},

{riak_kv_index_prereduce,
    extract_buckets,
    {is_diabetic_int,
        is_diabetic,
        all,
        [{0, <<"NotD">>}, {1, <<"IsD">>}],
        <<"Unexpected">>}}
```

The bitmap is base64 encoded as a binary, but then needs to be converted to an integer.  Once it is an integer, only the least significant bit is interesting (the one that represents whether or not a patient is diabetic), and so this is extracted with a mask.  Finally the 1 or 0 is mapped back into a binary tag.

```
{riak_kv_index_prereduce,
    extract_coalesce,
    {[gpprovider, age, is_diabetic],
        counting_term,
        this,
        <<"|">>}}
```

A combined term is then made by merging together these flags, and it is these terms that need to be counted.  As none of the previous terms are interesting for the reduce function, the keep attribute is set to `this` so those terms are discarded and only the `counting_term` will be passed forward.

*Stage 3 - Map and reduce functions*

A single reduce function is required:

```
{reduce,
    {modfun, riak_kv_mapreduce, reduce_index_countby},
    [{reduce_phase_batch_size, 1000},
        {args, {counting_term, binary}}
    ],
    true}
```

The `reduce_index_countby` will produce a facet count for unique `counting_term` i.e. a count of the diabetics/non-diabetics in each age band for each primary-care provider.  The reduce phase is single-threaded (it is not distributed across the nodes, like the application of the prereduce functions), and to make this more efficient the `reduce_phase_batch_size` is increased.  As the reduce phase properties are controlled via passed-in arguments, the actual function arguments now need to be passed as a tuple labelled with the atom `args`.

*Notes*

Running this query on a single machine (so with constrained parallelisation), for a non-trivial number of patients (o(100K)) reveals that 30.3% of the time is spent on the 2i query, 8.6% on the prereduce functions and 61.1% of the time in the reduce function.

The response time of this query can be greatly improved by forcing the reduce function to be pre-reduced - in other words forcing the Map/Reduce system to pre-calculate a partial result locally at each vnode worker before sending to the single reduce function to combine.  This is possible, as all reduce functions must be commutative, associative and idempotent.

To force a reduce statement to prereduce, *in the case of the first reduce statement in an index Map/Reduce operation*, then the `reduce` keyword should be changed to `prereduce`:

```
{prereduce,
    {modfun, riak_kv_mapreduce, reduce_index_countby},
    [{args, {counting_term, binary}}
    ],
    true}
```

This change will make an order-of-magnitude change in the speed of the reduce part of the query.  The lack of parallelisation in standard reduce functions will create a bottleneck, although the flip-side of parallelisation is increased CPU usage across the cluster.  Prereduce simply [increases parallelisation of the workload](https://speakerdeck.com/basho/riak-pipe-distributed-processing-system-ricon-2012?slide=19), and so reduces response times.


### Example 3 - Inverted Indices and Anti-Entropy

Inverted indexes can be used in Riak to improve consistency of query results, and the performance of queries, when terms change frequently (normally at the expense of false positive results, and overheads in the PUT path).  An inverted index is an object which contains a set of index results for a given term, meaning that queries can be made with r > 1 (i.e. checking the results are correct in more than one read replica, whereas 2i and M/R queries are always r=1.  Changes can be coordinated between inverted indexes and objects using "unit of work" patterns or pre-commit hooks, but there are challenges confirming that this has been done reliably, that is to say proving that inverted indexes never become inconsistent.

This example looks at using secondary indexes and map/reduce to make anti-entropy checks between inverted indexes and objects at scale.

.....
