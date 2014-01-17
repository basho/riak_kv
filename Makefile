.PHONY: deps test

has_eqc := $(shell erl -eval 'try eqc:version(), io:format("true") catch _:_ -> io:format(false) end' -noshell -s init stop)

all: deps compile

compile: deps
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean
	rm -rf test.*-temp-data

distclean: clean
	./rebar delete-deps

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool snmp public_key mnesia eunit syntax_tools compiler

include tools.mk

test-compile:
ifeq (${has_eqc}, true)
	cd deps/riak_dt; ./rebar -DEQC -DTEST clean compile
else
	@echo "EQC not present, skipping recompile of riak_dt"
endif

test: test-compile
