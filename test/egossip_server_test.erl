-module(egossip_server_test).  -include_lib("eunit/include/eunit.hrl").

-record(state, {
    cgossip = {0, {0,0,0}}, % current gossip count
    mgossip = {1, {1,1,1}}, % max gossip count
    nodes = [node()],
    expired = [],
    module
}).

-define(RECHECK, 5).

reconcile_nodes_test() ->
    % Handles network partitions and new nodes joining the cluster
    Module = gossip_test,
    meck:new(Module),
    meck:expect(Module, join, 1, ok),

    % equally sized clusters must do a tiebreaker by comparing their
    % node lists to see which island has to join another
    ?assertEqual([a,c,d], eg_gossip_server:reconcile_nodes([a,b], [c,d], d, Module)),
    ?assert( meck:called(Module, join, [ [c,d] ]) ),

    % smaller sized islands must join a larger island
    ?assertEqual([a,b,c,d], eg_gossip_server:reconcile_nodes([a], [b,c,d], d, Module)),
    ?assert( meck:called(Module, join, [ [b,c,d] ]) ),
    ?assertEqual([a,b,c,d], eg_gossip_server:reconcile_nodes([a,c,d], [b], b, Module)),

    % node lists are the same, don't do anything
    ?assertEqual([a,b], eg_gossip_server:reconcile_nodes([a,b], [a,b], b, Module)),

    % Two islands [a,b] and [c,d], a joins c #=> [a,c,d] and b joins d #=> [b,c,d]
    % an intersection now exists if these two islands talk with eachother.
    % reconcile_nodes should just perform an union and not trigger a join event.
    ?assertEqual([a,b,c,d], eg_gossip_server:reconcile_nodes([a,c,d], [b,c,d], c, Module)),

    meck:unload(Module).

never_can_gossip_test() ->
    Module = gossip_test,

    State0 = eg_gossip_server:reset_gossip(#state{module=Module}),
    {_, State1} = Result1 = eg_gossip_server:can_gossip(State0),
    {_, State2} = Result2 = eg_gossip_server:can_gossip(State1),
    {_, State3} = Result3 = eg_gossip_server:can_gossip(State2),

    % simulate ?RECHECK seconds passing
    #state{ cgossip={CM, {Meg, Sec, Mic}} } = State3,
    {_, State4} = Result4 = eg_gossip_server:can_gossip(State3#state{ cgossip={CM, {Meg, Sec+?RECHECK, Mic}} }),
    {_, _} = Result5 = eg_gossip_server:can_gossip(State4),

    ?assertMatch({false, _}, Result1),
    ?assertMatch({false, _}, Result2),
    ?assertMatch({false, _}, Result3),
    ?assertMatch({false, _}, Result4),
    ?assertMatch({false, _}, Result5).

never_can_gossip_flips_test() ->
    Module = gossip_test,

    State0 = eg_gossip_server:reset_gossip(#state{module=Module}),
    {_, State1} = Result1 = eg_gossip_server:can_gossip(State0),
    {_, State2} = Result2 = eg_gossip_server:can_gossip(State1),
    {_, State3} = Result3 = eg_gossip_server:can_gossip(State2),

    % simulate ?RECHECK seconds passing
    #state{ cgossip={CM, {Meg, Sec, Mic}} } = State3,
    {_, State4} = Result4 = eg_gossip_server:can_gossip(State3#state{ cgossip={CM, {Meg, Sec+?RECHECK, Mic}} }),
    {_, State5} = Result5 = eg_gossip_server:can_gossip(State4),
    {_, _} = Result6 = eg_gossip_server:can_gossip(State5),

    ?assertMatch({false, _}, Result1),
    ?assertMatch({false, _}, Result2),
    ?assertMatch({false, _}, Result3),
    ?assertMatch({true, _}, Result4),
    ?assertMatch({true, _}, Result5),
    ?assertMatch({false, _}, Result6).

normal_can_gossip_test() ->
    Module = gossip_test,

    State0 = eg_gossip_server:reset_gossip(#state{module=Module}),
    {_, State1} = Result1 = eg_gossip_server:can_gossip(State0),
    {_, State2} = Result2 = eg_gossip_server:can_gossip(State1),
    {_, State3} = Result3 = eg_gossip_server:can_gossip(State2),

    % simulate 2 seconds passing
    #state{ cgossip={CM, {Meg, Sec, Mic}} } = State3,
    {_, State4} = Result4 = eg_gossip_server:can_gossip(State3#state{ cgossip={CM, {Meg, Sec+2, Mic}} }),
    {_, _} = Result5 = eg_gossip_server:can_gossip(State4),

    ?assertMatch({true, _}, Result1),
    ?assertMatch({false, _}, Result2),
    ?assertMatch({false, _}, Result3),
    ?assertMatch({true, _}, Result4),
    ?assertMatch({false, _}, Result5).
