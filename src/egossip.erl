-module(egossip).
-export([register_handler/1]).

%% @doc
%% Spawn a new gossip handler
%% @end
-spec register_handler(module()) -> {ok, pid()}.

register_handler(Module) ->
    egossip_sup:start_child(Module).
