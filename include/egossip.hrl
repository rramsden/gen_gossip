-record(state, {
    cgossip = {0, {0,0,0}}, % current gossip count
    mgossip = {1, {1,1,1}}, % max gossip count
    nodes = [node()], % list of nodes and their respective epochs

    % required for aggregation protocol
    epoch = 0,
    wait_for :: pos_integer(),
    cycle = 0,
    callers = [],

    module
}).

-type from() :: node().
-type token() :: atom().
-type epoch() :: integer().
-type message() :: {token(), term(), from()}.

-type handle_event_ret() :: {next_state, NextStateName :: atom(), NewStateData :: term()} |
    {next_state, NextStateName :: atom(), NewStateData :: term(),
     timeout() | hibernate} |
    {stop, Reason :: term(), NewStateData :: term()}.
