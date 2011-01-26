

.PHONY: deps

all: deps compile

compile:
	@echo Warning: deps/riak_core is a symlink
	./rebar compile

deps:
	@echo Warning: deps/riak_core is a symlink
	./rebar get-deps

clean:
	@echo Warning: deps/riak_core is a symlink
	./rebar clean

distclean: clean 
	@echo Warning: deps/riak_core is a symlink
	./rebar delete-deps

eunit:
	@echo Warning: deps/riak_core is a symlink
	./rebar skip_deps=true eunit

docs:
	@echo Warning: deps/riak_core is a symlink
	./rebar skip_deps=true doc

dialyzer: compile
	@echo Warning: deps/riak_core is a symlink
	@dialyzer -Wno_return -c apps/riak_kv/ebin


