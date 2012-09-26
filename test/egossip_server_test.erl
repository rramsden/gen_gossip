-module(egossip_server_test).
-include_lib("eunit/include/eunit.hrl").

-record(state, {
    cgossip = {0, {0,0,0}}, % current gossip count
    mgossip = {1, {1,1,1}}, % max gossip count
    nodes = [node()],
    expired = [],
    module
}).

app_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
            fun reconcile_nodes/1,
            fun never_can_gossip/1,
            fun never_can_gossip_flips/1,
            fun normal_can_gossip/1
            ]}.

setup() ->
    Module = gossip_test,
    meck:new(Module),
    meck:expect(Module, gossip_freq, 0, {1,2}),
    meck:expect(Module, digest, 0, ok),
    meck:expect(Module, push, 2, ok),
    meck:expect(Module, symmetric_push, 2, ok),
    meck:expect(Module, commit, 2, ok),
    meck:expect(Module, join, 1, ok),
    meck:expect(Module, expire, 2, ok),
    Module.

cleanup(Module) ->
    meck:unload(Module).

reconcile_nodes(Module) ->
    fun() ->
        % equally sized clusters must do a tiebreaker by comparing their
        % node lists to see which island has to join another
        ?assertEqual([a,c,d], egossip_server:reconcile_nodes([a,b], [c,d], d, Module)),
        ?assert( meck:called(Module, join, [ [c,d] ]) ),

        % smaller sized islands must join a larger island
        ?assertEqual([a,b,c,d], egossip_server:reconcile_nodes([a], [b,c,d], d, Module)),
        ?assert( meck:called(Module, join, [ [b,c,d] ]) ),

        % since the remote node is joining our cluster we don't get a join notice.
        % once we talk to him he will get a join notice on his side
        ?assertEqual([a,b,c,d], egossip_server:reconcile_nodes([a,c,d], [b], b, Module)),
        ?assert( not meck:called(Module, join, [ [b] ]) ),

        % node lists are the same, don't do anything
        ?assertEqual([a,b], egossip_server:reconcile_nodes([a,b], [a,b], b, Module)),

        % Two islands [a,b] and [c,d], a joins c #=> [a,c,d] and b joins d #=> [b,c,d]
        % an intersection now exists if these two islands talk with eachother.
        % reconcile_nodes should just perform an union and not trigger a join event.
        ?assertEqual([a,b,c,d], egossip_server:reconcile_nodes([a,c,d], [b,c,d], c, Module))
    end.

never_can_gossip(Module) ->
    fun() ->
        meck:expect(Module, gossip_freq, 0, never),

        State0 = egossip_server:reset_gossip(#state{module=Module}),
        {_, State1} = Result1 = egossip_server:can_gossip(State0),
        {_, State2} = Result2 = egossip_server:can_gossip(State1),
        {_, State3} = Result3 = egossip_server:can_gossip(State2),

        Recheck = 5,
        #state{ cgossip={CM, {Meg, Sec, Mic}} } = State3,
        {_, State4} = Result4 = egossip_server:can_gossip(State3#state{ cgossip={CM, {Meg, Sec+Recheck, Mic}} }),
        {_, _} = Result5 = egossip_server:can_gossip(State4),

        ?assertMatch({false, _}, Result1),
        ?assertMatch({false, _}, Result2),
        ?assertMatch({false, _}, Result3),
        ?assertMatch({false, _}, Result4),
        ?assertMatch({false, _}, Result5)
    end.

never_can_gossip_flips(Module) ->
    fun() ->
        meck:expect(Module, gossip_freq, 0, never),

        State0 = egossip_server:reset_gossip(#state{module=Module}),
        {_, State1} = Result1 = egossip_server:can_gossip(State0),
        {_, State2} = Result2 = egossip_server:can_gossip(State1),
        {_, State3} = Result3 = egossip_server:can_gossip(State2),

        meck:expect(Module, gossip_freq, 0, {2,1}),

        Recheck = 5, % simulate Recheck seconds passing

        #state{ cgossip={CM, {Meg, Sec, Mic}} } = State3,
        {_, State4} = Result4 = egossip_server:can_gossip(State3#state{ cgossip={CM, {Meg, Sec+Recheck, Mic}} }),
        {_, State5} = Result5 = egossip_server:can_gossip(State4),
        {_, _} = Result6 = egossip_server:can_gossip(State5),

        ?assertMatch({false, _}, Result1),
        ?assertMatch({false, _}, Result2),
        ?assertMatch({false, _}, Result3),
        ?assertMatch({true, _}, Result4),
        ?assertMatch({true, _}, Result5),
        ?assertMatch({false, _}, Result6)
    end.

normal_can_gossip(Module) ->
    fun() ->
        State0 = egossip_server:reset_gossip(#state{module=Module}),
        {_, State1} = Result1 = egossip_server:can_gossip(State0),
        {_, State2} = Result2 = egossip_server:can_gossip(State1),
        {_, State3} = Result3 = egossip_server:can_gossip(State2),

        % simulate 2 seconds passing
        #state{ cgossip={CM, {Meg, Sec, Mic}} } = State3,
        {_, State4} = Result4 = egossip_server:can_gossip(State3#state{ cgossip={CM, {Meg, Sec+2, Mic}} }),
        {_, _} = Result5 = egossip_server:can_gossip(State4),

        ?assertMatch({true, _}, Result1),
        ?assertMatch({false, _}, Result2),
        ?assertMatch({false, _}, Result3),
        ?assertMatch({true, _}, Result4),
        ?assertMatch({false, _}, Result5)
    end.
