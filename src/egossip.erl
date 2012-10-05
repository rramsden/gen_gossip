-module(egossip).
-export([new/1, new/2]).

-define(HANDLER(Module), list_to_atom("egossip_" ++ atom_to_list(Module))).
-type opts() :: [sync].

%% @doc
%% Spawn a new gossip handler
%% @end

-spec new(module()) -> {ok, pid()}.

new(Module) ->
    new(Module, []).

-spec new(module(), opts()) -> {ok, pid()}.

new(Module, Opts) ->
    egossip_sup:start_child(Module, Opts).
