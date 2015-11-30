#!/usr/bin/make -f
# -*- Makefile -*-

.PHONY: deps test

DIALYZER_FLAGS =

all: deps compile

compile: deps
	./rebar compile

deps:
	git submodule update --init --recursive
	./rebar get-deps

clean:
	./rebar clean
	-rm -rf test.*-temp-data $(TS_CLEAN)

distclean: clean
	./rebar delete-deps

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool snmp public_key mnesia eunit syntax_tools compiler

include tools.mk

nif_build := $(wildcard riak_kv_ts/Makefile)

$(info nif_build wildcard is $(wildcard riak_kv_ts/Makefile) and curdir is $(CURDIR))

ifneq ($(nif_build),)
B:=$(CURDIR)
pb_nif : $B/riak_kv_ts/compile

$(info Attempt to load nif_build: $(nif_build))
include $(nif_build)
else
pb_nif:

endif
