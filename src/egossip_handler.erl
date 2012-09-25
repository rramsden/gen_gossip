-module(egossip_handler).
-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
        {gossip_freq, 0},
        {digest, 0},
        {push, 2},
        {symmetric_push, 2},
        {commit, 2},
        {join, 1},
        {expire, 2}
    ];
behaviour_info(_) ->
    undefined.
