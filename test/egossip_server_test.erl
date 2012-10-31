-module(egossip_server_test).
-include_lib("eunit/include/eunit.hrl").

-include("egossip.hrl").

app_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
            fun reconcile_nodes_/1,
            fun never_can_gossip_/1,
            fun never_can_gossip_flips_/1,
            fun normal_can_gossip_/1,
            fun prevent_forever_wait_/1,
            fun transition_wait_to_gossip_state_/1,
            fun transition_gossip_to_wait_state_/1,
            fun gossips_if_nodelist_and_epoch_match_/1,
            fun use_latest_epoch_if_nodelist_match_/1,
            fun reconciles_nodelists_/1,
            fun remove_downed_node_/1,
            fun dont_increment_cycle_in_wait_state_/1
            ]}.

setup() ->
    meck:new(egossip_server, [passthrough]),
    meck:expect(egossip_server, send_gossip, fun(_, _, _, State) -> {ok, State} end),

    Module = gossip_test,
    meck:new(Module),
    meck:expect(Module, gossip_freq, 0, {1,2}),
    meck:expect(Module, cycles, 1, 10),
    meck:expect(Module, digest, 0, ok),
    meck:expect(Module, push, 2, {ok, reply}),
    meck:expect(Module, symmetric_push, 2, {ok, reply}),
    meck:expect(Module, commit, 2, {ok, reply}),
    meck:expect(Module, join, 1, ok),
    meck:expect(Module, expire, 2, ok),
    Module.

cleanup(Module) ->
    meck:unload(egossip_server),
    meck:unload(Module).


reconcile_nodes_(Module) ->
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

never_can_gossip_(Module) ->
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

never_can_gossip_flips_(Module) ->
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

normal_can_gossip_(Module) ->
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

prevent_forever_wait_(Module) ->
    % by some freak chance if we were waiting for an epoch to roll around
    % that never occurred because a higher epoch appeared then we should
    % wait for the next highest to occur to prevent waiting forever
    fun() ->
        R_Epoch = 2,
        R_Nodelist = [b],
        WaitFor = 1,
        Nodelist = [a],

        State0 = #state{module=Module, wait_for=WaitFor, nodes=Nodelist},
        Send = {R_Epoch, {push, msg, from}, R_Nodelist},

        {next_state, waiting, State1} = egossip_server:waiting(Send, State0),

        ?assertEqual(State1#state.wait_for, R_Epoch + 1)
    end.

transition_wait_to_gossip_state_(Module) ->
    % to transition from waiting -> gossiping the epoch
    % specified in #state.wait_for must equal the callers epoch
    fun() ->
        R_Epoch = 1,
        R_Nodelist = [b],
        Epoch = 1,
        Nodelist = [a],

        State0 = #state{module=Module, wait_for=Epoch, nodes=Nodelist},
        Msg = {R_Epoch, {push, msg, from}, R_Nodelist},

        {next_state, gossiping, _} = egossip_server:waiting(Msg, State0)
    end.

transition_gossip_to_wait_state_(Module) ->
    fun() ->
        R_Epoch = 2,
        R_Nodelist = [b],
        Epoch = 1,
        Nodelist = [a],

        State0 = #state{module=Module, nodes=Nodelist, epoch=Epoch},
        Msg = {R_Epoch, {push, msg, from}, R_Nodelist},

        {next_state, waiting, _} = egossip_server:gossiping(Msg, State0)
    end.

gossips_if_nodelist_and_epoch_match_(Module) ->
    fun() ->
        R_Epoch = 1,
        R_Nodelist = [a,b],
        Epoch = 1,
        Nodelist = [a,b],

        State0 = #state{module=Module, nodes=Nodelist, epoch=Epoch},
        Msg = {R_Epoch, {push, msg, from}, R_Nodelist},

        {next_state, gossiping, _} = egossip_server:gossiping(Msg, State0),

        % some data was pushed to the module, so it should reply with a symmetric_push
        ?assert( meck:called(Module, push, [ msg, from ]) ),
        ?assert( meck:called(egossip_server, send_gossip, [from, symmetric_push, reply, State0]) )
    end.

use_latest_epoch_if_nodelist_match_(Module) ->
    % since there is clock-drift, ticks will never truly be in sync.
    % this causes other nodes to switch to the next epoch before another.
    % to do our best at synchrnoization we always use the latest epoch.
    fun() ->
        R_Epoch = 10,
        R_Nodelist = [a,b],
        Epoch = 1,
        Nodelist = [a,b],

        State0 = #state{module=Module, nodes=Nodelist, epoch=Epoch},
        Send = {R_Epoch, {push, msg, from}, R_Nodelist},

        {next_state, gossiping, State1} = egossip_server:gossiping(Send, State0),

        % should also send a gossip message back
        ?assert( meck:called(egossip_server, send_gossip, [from, symmetric_push, reply, State1]) ),
        ?assertEqual(State1#state.epoch, R_Epoch)
    end.

reconciles_nodelists_(Module) ->
    % two cases we will merge nodelists. First case is in
    % #3 second case is in #4 if you look at the source for egossip_server
    fun() ->
        % 3rd case
        R_EpochA = 2,
        R_NodelistA = [b,c],
        EpochA = 1,
        NodelistA = [a,c],

        StateA0 = #state{module=Module, nodes=NodelistA, epoch=EpochA},
        SendA = {R_EpochA, {push, msg, from}, R_NodelistA},

        {next_state, gossiping, StateA1} = egossip_server:gossiping(SendA, StateA0),

        ?assertEqual([a,b,c], StateA1#state.nodes),

        % 4th case
        R_EpochB = EpochB = 1,
        R_NodelistB = [b,c],
        NodelistB = [a],

        StateB0 = #state{module=Module, nodes=NodelistB, epoch=EpochB},
        SendB = {R_EpochB, {push, msg, from}, R_NodelistB},

        {next_state, gossiping, StateB1} = egossip_server:gossiping(SendB, StateB0),

        ?assertEqual([a,b,c], StateB1#state.nodes)
    end.

remove_downed_node_(Module) ->
    fun() ->
        Nodelist = [a,b,c],
        Epoch = 1,
        State0 = #state{module=Module, nodes=Nodelist, epoch=Epoch},

        {next_state, statename, State1} = egossip_server:handle_info({nodedown, b}, statename, State0),

        ?assertEqual([a,c], State1#state.nodes)
    end.

dont_increment_cycle_in_wait_state_(Module) ->
    fun() ->
        Nodelist = [a,b,c],
        Epoch = 1,

        State0 = #state{cycle=0, module=Module, nodes=Nodelist, epoch=Epoch},

        {next_state, waiting, State1} = egossip_server:handle_info(tick, waiting, State0),

        % just making sure cycle is being incremented in gossip state
        {next_state, gossiping, State2} = egossip_server:handle_info(tick, gossiping, State0),

        ?assertEqual(0, State1#state.cycle),
        ?assertEqual(1, State2#state.cycle)
    end.
