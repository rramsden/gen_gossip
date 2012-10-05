-module(egossip_sup).
-behaviour(supervisor).

%% API
-export([start_link/0,
         start_child/1,
         start_child/2]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Module) ->
    start_child(Module, []).

start_child(Module, Opts) ->
    supervisor:start_child(?MODULE, [Module, Opts]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    RestartStrategy = simple_one_for_one,
    MaxRestarts = 3,
    MaxSecondsBetweenRestarts = 10,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    Restart = transient,

    Shutdown = 2000,
    Type = worker,
    Child = {'egossip_server', {'egossip_server', start_link, []},
                      Restart, Shutdown, Type, ['egossip_server']},

    {ok, {SupFlags, [Child]}}.
