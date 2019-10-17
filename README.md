riak_repl
=========

Riak MDC Replication

--- 
# Pull Request template

### Testing
- [ ] manual verification of code
- [ ] eunit     (w/ gist of output) 
- [ ] EQC       (w/ gist of output) 
- [ ] riak_test (w/ gist of output) 
- [ ] Dialyzer
- [ ] XRef
- [ ] Coverage reports

### Documentation
- [ ] internal docs (design docs)
- [ ] external docs (docs.basho.com)
- [ ] man pages


---

# New Feature Deliverables

- design documentation + diagrams
  - nothing formal
  - to help out during support, "this is how xyz works"
- eunit tests
- riak_tests
- EQC + Pulse tests
- tests at scale and under heavy load
  - Boston Cluster or AWS
- notes for public documentation
  - for the docs team   
  - 
  
---

# BEAM release process

1. git tag the specific commit(s) that will be released
2. run all eunit tests, EQC tests, store the output in a gist.
3. if possible, run all riak_tests for replication
4. record specific commit(s) that the beam targets in a README.txt file
5. create a tar file. 
  - Note that OSX will include a hidden directory in the tar file. Find the knob to prevent those files from being added to the .tar file, or build/test the beams on Linux. (you can use 'find' to pipe the exact files you want into the tar, see: https://github.com/basho/node_package/blob/develop/priv/templates/fbsd/Makefile#L27 for an example of using -rf with a pipe)
  - include the README.txt file from the step above
6. once .tar is built, calc an MD5 to pass along with the file
7. create an entry on https://github.com/basho/internal_wiki/wiki/Releases page
	- include:
		- link to the gist output
		- version of Erlang the beams were built with
		- MD5 of the file
		- link to compiled beams		
8. notify client services + TAMs
9. port the PR to the `develop` branch if applicable


