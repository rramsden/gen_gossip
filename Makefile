REBAR=./rebar

all: compile

compile: get-deps
	@$(REBAR) compile

get-deps:
	@$(REBAR) get-deps

clean:
	@$(REBAR) clean

test: clean compile
	@$(REBAR) eunit skip_deps=true

dist-clean: clean
