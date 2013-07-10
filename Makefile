
VSN := $(shell erl -eval 'io:format("~s~n", [erlang:system_info(otp_release)]), init:stop().' | grep 'R' | sed -e 's,R\(..\)B.*,\1,')
NEW_HASH := $(shell expr $(VSN) \>= 16)


.PHONY: deps test

all: deps compile

compile:
ifeq ($(NEW_HASH),1)
	./rebar compile -Dnew_hash
else
	./rebar compile
endif

deps:
	./rebar get-deps

clean:
	./rebar clean
	rm -rf test.*-temp-data

distclean: clean 
	./rebar delete-deps

test: all	
ifeq ($(NEW_HASH),1)
	./rebar skip_deps=true -Dnew_hash eunit
else
	./rebar skip_deps=true eunit
endif

deps:
	./rebar get-deps


docs:
	./rebar skip_deps=true doc

dialyzer: compile
	@dialyzer -Wno_return -c apps/riak_kv/ebin


