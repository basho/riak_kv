.PHONY: deps test

# Limit exometer dependencies
EXOMETER_PACKAGES="(basic)"
export EXOMETER_PACKAGES

DIALYZER_FLAGS =

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
