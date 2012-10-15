REBAR = ./rebar
REBAR_CONFIG :=

all : compile

compile : get-deps
	@$(REBAR) $(REBAR_CONFIG) compile

get-deps :
	@$(REBAR) $(REBAR_CONFIG) get-deps

clean :
	@$(REBAR) $(REBAR_CONFIG) clean

test : REBAR_CONFIG = -C rebar.test.config
test : clean compile
	@$(REBAR) $(REBAR_CONFIG) eunit skip_deps=true

dist-clean : clean
