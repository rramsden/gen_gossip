-module(egossip).
-export([new/1]).

%% @doc
%% Spawn a new egossip_server which talks with Module
%% @end
-spec new(module()) -> {ok, pid()}.

new(Module) ->
    eg_gossip_sup:start_child(Module).
