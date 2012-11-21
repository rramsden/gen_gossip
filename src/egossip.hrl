-record(state, {
    cgossip = {0, {0,0,0}}, % current gossip count
    mgossip = {1, {1,1,1}}, % max gossip count
    nodes = [node()], % list of nodes and their respective epochs
    epoch = 0,
    wait_for :: integer(), % set to epoch we're waiting for
    max_wait = 0,
    cycle = 0,
    module,
    mstate,
    mode = epidemic
}).

-type from() :: node().
-type token() :: atom().
-type epoch() :: integer().
-type message() :: {token(), term(), from()}.
-type module_state() :: any().
-type nodelist() :: [node()].

-type handle_event_ret() :: {next_state, NextStateName :: atom(), NewStateData :: term()} |
    {next_state, NextStateName :: atom(), NewStateData :: term(),
     timeout() | hibernate} |
    {stop, Reason :: term(), NewStateData :: term()}.
