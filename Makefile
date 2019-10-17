.PHONY: compile rel cover test dialyzer
REBAR=./rebar3

compile:
	$(REBAR) compile

clean:
	rm -f include/*_pb.hrl
	rm -f src/*_pb.erl
	$(REBAR) clean

distclean:
	$(REBAR) clean --all

cover: test
	$(REBAR) cover

test: compile
	$(REBAR) eunit

dialyzer:
	$(REBAR) dialyzer

xref:
	$(REBAR) xref

check: test dialyzer xref
