REBAR=./rebar

compile: get_deps
	@$(REBAR) compile

get_deps:
	@$(REBAR) get-deps

clean:
	@$(REBAR) clean

eunit: clean compile
	mv rebar.config rebar.prod.config
	mv rebar.test.config rebar.config
	@$(REBAR) get-deps compile
	@$(REBAR) eunit skip_deps=true
	mv rebar.config rebar.test.config
	mv rebar.prod.config rebar.config

ct: clean get_deps compile
	rm -f test/*.beam
	@$(REBAR) ct skip_deps=true
	rm -f test/*.beam
