.PHONY: compile rel cover test dialyzer eqc
REBAR=./rebar3

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

cover: test
	$(REBAR) cover

test: compile
	$(REBAR) as test do eunit

eqc:
	$(REBAR) eqc


dialyzer:
	$(REBAR) dialyzer

xref:
	$(REBAR) xref

check: test dialyzer xref
