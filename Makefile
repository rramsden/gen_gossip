REBAR=./rebar

all: compile

compile: get-deps
	@$(REBAR) compile

get-deps:
	@$(REBAR) get-deps

clean:
	@$(REBAR) clean

test: clean compile
	mv rebar.config rebar.prod.config
	mv rebar.test.config rebar.config
	@$(REBAR) eunit skip_deps=true
	mv rebar.config rebar.test.config
	mv rebar.prod.config rebar.config

dist-clean: clean
