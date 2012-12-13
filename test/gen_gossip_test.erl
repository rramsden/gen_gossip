-module(gen_gossip_test).
-include_lib("eunit/include/eunit.hrl").

-include("src/gen_gossip.hrl").


app_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
            fun reconcile_equal_win_tiebreaker_/1,
            fun reconcile_equal_lost_tiebreaker_/1,
            fun reconcile_smaller_with_bigger_/1,
            fun reconcile_larger_with_smaller_/1,
            fun reconcile_intersection_with_1_/1,
            fun reconcile_intersection_with_2_/1,
            fun prevent_forever_wait_/1,
            fun transition_wait_to_gossip_state_/1,
            fun transition_gossip_to_wait_state_/1,
            fun gossips_if_nodelist_and_epoch_match_/1,
            fun use_latest_epoch_if_nodelist_match_/1,
            fun remove_downed_node_/1,
            fun dont_increment_cycle_in_wait_state_/1,
            fun dont_increment_cycle_for_other_modes_/1,
            fun dont_gossip_in_wait_state_/1,
            fun dont_wait_forever_/1,
            fun proxies_out_of_band_messages_to_callback_module_/1
            ]}.

setup() ->
    meck:new(gen_gossip, [passthrough]),
    meck:expect(gen_gossip, send_gossip, fun(_, _, _, State) -> {ok, State} end),
    meck:expect(gen_gossip, node_name, 0, a),

    Module = gossip_test,
    meck:new(Module),
    meck:expect(Module, init, 1, {ok, state}),
    meck:expect(Module, gossip_freq, 1, {reply, 1000, state}),
    meck:expect(Module, round_finish, 2, {noreply, state}),
    meck:expect(Module, round_length, 2, {reply, 10, state}),
    meck:expect(Module, digest, 1, {reply, digest, state}),
    meck:expect(Module, handle_push, 3, {reply, digest, state}),
    meck:expect(Module, handle_pull, 3, {reply, digest, state}),
    meck:expect(Module, handle_commit, 3, {reply, digest, state}),
    meck:expect(Module, handle_info, 2, {noreply, state}),
    meck:expect(Module, handle_call, 3, {reply, ok, state}),
    meck:expect(Module, handle_cast, 2, {noreply, state}),
    meck:expect(Module, code_change, 3, {ok, state}),
    meck:expect(Module, terminate, 2, ok),
    meck:expect(Module, join, 2, {noreply, state}),
    meck:expect(Module, expire, 2, {noreply, state}),
    Module.

cleanup(Module) ->
    meck:unload(gen_gossip),
    meck:unload(Module).

called(Mod, Fun) ->
    History = meck:history(Mod),
    List = [match || {_, {M, F, _, _}} <- History, M == Mod, F == Fun],
    List =/= [].

dont_gossip_in_wait_state_(Module) ->
    fun() ->
        State0 = #state{module=Module},

        {next_state, gossiping, State1} = gen_gossip:handle_info('$gen_gossip_tick', waiting, State0),
        ?assert( not meck:called(gen_gossip, send_gossip, [from, handle_pull, digest, State1]) )
    end.

reconcile_equal_win_tiebreaker_(Module) ->
    fun() ->
        State = #state{module=Module, mstate=state},
        MyNode = a,
        MyList = [a,b],
        RemoteNode = c,
        RemoteList = [c,d],

        Expected = [a, b, c],

        meck:expect(gen_gossip, node_name, 0, MyNode),

        {_, Nodelist} = gen_gossip:reconcile_nodes(MyList, RemoteList, RemoteNode, State),
        ?assertEqual(Expected, Nodelist),
        ?assert(not called( Module, join )),

        meck:reset(gen_gossip)
    end.

reconcile_equal_lost_tiebreaker_(Module) ->
    fun() ->
        State = #state{module=Module, mstate=state},
        MyNode = c,
        MyList = [c,d],
        RemoteNode = c,
        RemoteList = [a,b],

        Expected = [a, b, c],

        meck:expect(gen_gossip, node_name, 0, MyNode),

        {_, Nodelist} = gen_gossip:reconcile_nodes(MyList, RemoteList, RemoteNode, State),
        ?assertEqual(Expected, Nodelist),
        ?assert( meck:called(Module, join, [ [a,b], state ]) ),

        meck:reset(gen_gossip)
    end.

reconcile_smaller_with_bigger_(Module) ->
    fun() ->
        State = #state{module=Module, mstate=state},
        MyNode = a,
        MyList = [a,b,c],
        RemoteNode = c,
        RemoteList = [d,e,f,g],

        Expected = [a,d,e,f,g],

        meck:expect(gen_gossip, node_name, 0, MyNode),

        {_, Nodelist} = gen_gossip:reconcile_nodes(MyList, RemoteList, RemoteNode, State),
        ?assertEqual(Expected, Nodelist),
        ?assert( meck:called(Module, join, [ [d,e,f,g], state ]) ),

        meck:reset(gen_gossip)
    end.

reconcile_larger_with_smaller_(Module) ->
    fun() ->
        State = #state{module=Module, mstate=state},
        MyNode = a,
        MyList = [a,b,c],
        RemoteNode = d,
        RemoteList = [d,e],

        Expected = [a,b,c,d],

        meck:expect(gen_gossip, node_name, 0, MyNode),

        {_, Nodelist} = gen_gossip:reconcile_nodes(MyList, RemoteList, RemoteNode, State),
        ?assertEqual(Expected, Nodelist),
        ?assert(not called( Module, join )),

        meck:reset(gen_gossip)
    end.

reconcile_intersection_with_1_(Module) ->
    fun() ->
        % will still trigger a join because intersection has one item in it
        State = #state{module=Module, mstate=state},
        MyNode = b,
        MyList = [a,b,c],
        RemoteNode = d,
        RemoteList = [a,d,e,f],

        Expected = [a,b,d,e,f],

        meck:expect(gen_gossip, node_name, 0, MyNode),

        {_, Nodelist} = gen_gossip:reconcile_nodes(MyList, RemoteList, RemoteNode, State),
        ?assertEqual(Expected, Nodelist),
        ?assert( meck:called(Module, join, [ [a,d,e,f], state ]) ),

        meck:reset(gen_gossip)
    end.

reconcile_intersection_with_2_(Module) ->
    fun() ->
        % will still trigger a join because intersection has one item in it
        State = #state{module=Module, mstate=state},
        MyNode = b,
        MyList = [a,b],
        RemoteNode = d,
        RemoteList = [a,b,c],

        Expected = [a,b,c],

        meck:expect(gen_gossip, node_name, 0, MyNode),

        {_, Nodelist} = gen_gossip:reconcile_nodes(MyList, RemoteList, RemoteNode, State),
        ?assertEqual(Expected, Nodelist),
        ?assert( not called(Module, join) ),

        meck:reset(gen_gossip)
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
        Send = {R_Epoch, {handle_push, msg, from}, R_Nodelist},

        {next_state, waiting, State1} = gen_gossip:waiting(Send, State0),

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
        Msg = {R_Epoch, {handle_push, msg, from}, R_Nodelist},

        {next_state, gossiping, _} = gen_gossip:waiting(Msg, State0)
    end.

transition_gossip_to_wait_state_(Module) ->
    fun() ->
        R_Epoch = 2,
        R_Nodelist = [b],
        Epoch = 1,
        Nodelist = [a],

        State0 = #state{module=Module, nodes=Nodelist, epoch=Epoch},
        Msg = {R_Epoch, {handle_push, msg, from}, R_Nodelist},

        {next_state, waiting, _} = gen_gossip:gossiping(Msg, State0)
    end.

gossips_if_nodelist_and_epoch_match_(Module) ->
    fun() ->
        R_Epoch = 1,
        R_Nodelist = [a,b],
        Epoch = 1,
        Nodelist = [a,b],

        State0 = #state{mstate=state, module=Module, nodes=Nodelist, epoch=Epoch},
        Msg = {R_Epoch, {handle_push, msg, from}, R_Nodelist},

        {next_state, gossiping, _} = gen_gossip:gossiping(Msg, State0),

        % some data was pushed to the module, so it should reply with a handle_pull
        ?assert( meck:called(Module, handle_push, [ msg, from, state ]) ),
        ?assert( meck:called(gen_gossip, send_gossip, [from, handle_pull, digest, State0]) )
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
        Send = {R_Epoch, {handle_push, msg, from}, R_Nodelist},

        {next_state, gossiping, State1} = gen_gossip:gossiping(Send, State0),

        % should also send a gossip message back
        ?assert( meck:called(gen_gossip, send_gossip, [from, handle_pull, digest, State1]) ),
        ?assertEqual(State1#state.epoch, R_Epoch)
    end.

remove_downed_node_(Module) ->
    fun() ->
        Nodelist = [a,b,c],
        Epoch = 1,
        State0 = #state{module=Module, nodes=Nodelist, epoch=Epoch},

        {next_state, statename, State1} = gen_gossip:handle_info({nodedown, b}, statename, State0),

        ?assertEqual([a,c], State1#state.nodes)
    end.

dont_increment_cycle_in_wait_state_(Module) ->
    fun() ->
        Nodelist = [a,b,c],
        Epoch = 1,

        State0 = #state{mode=aggregate, cycle=0, max_wait=1, module=Module, nodes=Nodelist, epoch=Epoch},

        {next_state, waiting, State1} = gen_gossip:handle_info('$gen_gossip_tick', waiting, State0),

        % just making sure cycle is being incremented in gossip state
        {next_state, gossiping, State2} = gen_gossip:handle_info('$gen_gossip_tick', gossiping, State0#state{max_wait=0}),

        ?assertEqual(0, State1#state.cycle),
        ?assertEqual(1, State2#state.cycle)
    end.

dont_increment_cycle_for_other_modes_(Module) ->
    % should only increment the cycle and change rounds when
    % were in aggregate mode
    fun() ->
        Nodelist = [a,b,c],
        Epoch = 1,

        State0 = #state{mode=epidemic, cycle=0, max_wait=0, module=Module, nodes=Nodelist, epoch=Epoch},

        {next_state, gossiping, State1} = gen_gossip:handle_info('$gen_gossip_tick', waiting, State0),

        % just making sure cycle is being incremented in gossip state
        {next_state, gossiping, State2} = gen_gossip:handle_info('$gen_gossip_tick', gossiping, State0),

        ?assertEqual(0, State1#state.cycle),
        ?assertEqual(0, State2#state.cycle)
    end.

dont_wait_forever_(Module) ->
    fun() ->
        MaxWait = 2,
        Nodelist = [a,b,c],
        Epoch = 1,

        State0 = #state{cycle=0, max_wait=MaxWait, module=Module, nodes=Nodelist, epoch=Epoch},

        {next_state, waiting, State1} = gen_gossip:handle_info('$gen_gossip_tick', waiting, State0),
        {next_state, waiting, State2} = gen_gossip:handle_info('$gen_gossip_tick', waiting, State1),
        {next_state, gossiping, _} = gen_gossip:handle_info('$gen_gossip_tick', waiting, State2)
    end.
proxies_out_of_band_messages_to_callback_module_(Module) ->
    fun() ->
        State0 = #state{module=Module, mstate=state},

        {next_state, gossiping, _} = gen_gossip:handle_info(out_of_band, gossiping, State0),
        ?assert( meck:called( Module, handle_info, [out_of_band, state] ) ),

        {next_state, gossiping, _} = gen_gossip:handle_event(out_of_band, gossiping, State0),
        ?assert( meck:called( Module, handle_cast, [out_of_band, state] ) ),

        {reply, ok, gossiping, _} = gen_gossip:handle_sync_event(out_of_band, from, gossiping, State0),
        ?assert( meck:called( Module, handle_call, [out_of_band, from, state] ) ),

        ok = gen_gossip:terminate(shutdown, gossiping, State0),
        ?assert( meck:called( Module, terminate, [shutdown, state] ) ),

        {ok, State0} = gen_gossip:code_change(1, gossiping, State0, []),
        ?assert( meck:called( Module, code_change, [1, state, []]) )
    end.
