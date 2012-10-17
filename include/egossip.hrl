-record(state, {
    cgossip = {0, {0,0,0}}, % current gossip count
    mgossip = {1, {1,1,1}}, % max gossip count
    nodecache = [node()],

    % required for aggregation protocol
    epoch = 0,
    cycle = 0,
    callers = [],

    module
}).
