

.PHONY: deps test

all: deps compile

compile:
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean
	rm -rf test.*-temp-data

distclean: clean 
	./rebar delete-deps

test: all
	./rebar skip_deps=true eunit

docs:
	./rebar skip_deps=true doc

old_dialyzer: compile
	@dialyzer -Wno_return -c apps/riak_kv/ebin

dialyzer:
	@echo "Not implemented for 1.4 branch.  Sorry buildbot.  (skipping)"

xref:
	@echo "Not implemented for 1.4 branch.  Sorry buildbot.  (skipping)"
