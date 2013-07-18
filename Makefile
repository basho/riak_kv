# The code below figures out the OTP release and introduces a macro at 
# build and test time to tell later released to use the new hash
# functions introduced in R15B02.  Older versions still use the old
# hash functions.
VSN := $(shell erl -eval 'io:format("~s~n", [erlang:system_info(otp_release)]), init:stop().' | grep 'R' | sed -e 's,R\(..\)B.*,\1,')
OLD_HASH := $(shell expr $(VSN) \<= 15)
ifeq ($(OLD_HASH),1)
hash := "-Dold_hash"
endif

.PHONY: deps test

all: deps compile

compile:
	./rebar compile $(hash)

deps:
	./rebar get-deps

clean:
	./rebar clean
	rm -rf test.*-temp-data

distclean: clean 
	./rebar delete-deps

test: all	
	./rebar skip_deps=true $(hash) eunit

deps:
	./rebar get-deps


docs:
	./rebar skip_deps=true doc

dialyzer: compile
	@dialyzer -Wno_return -c apps/riak_kv/ebin


