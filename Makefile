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

test-compile:
ifeq (${has_eqc}, true)
	cd deps/riak_dt; ./rebar -DEQC -DTEST clean compile
else
	@echo "EQC not present, skipping recompile of riak_dt"
endif

test: all test-compile
	./rebar skip_deps=true eunit

docs:
	./rebar skip_deps=true doc

APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool snmp public_key mnesia eunit syntax_tools compiler \
	deps/**/ebin
COMBO_PLT = $(HOME)/.riak_kv_dialyzer_plt

check_plt: compile
	dialyzer --check_plt --plt $(COMBO_PLT) --apps $(APPS) ebin

build_plt: compile
	dialyzer --build_plt --output_plt $(COMBO_PLT) --apps $(APPS) ebin

dialyzer: compile
	@echo
	@echo Use "'make check_plt'" to check PLT prior to using this target.
	@echo Use "'make build_plt'" to build PLT prior to using this target.
	@echo
	@sleep 1
	dialyzer -Wunmatched_returns -Werror_handling -Wrace_conditions \
		-Wunderspecs --plt $(COMBO_PLT) ebin

cleanplt:
	@echo
	@echo "Are you sure?  It takes about 1/2 hour to re-build."
	@echo Deleting $(COMBO_PLT) in 5 seconds.
	@ech
	sleep 5
	rm $(COMBO_PLT)

