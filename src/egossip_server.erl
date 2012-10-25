-module(egossip_server).
-behaviour(gen_fsm).

%% API
-export([ start_link/1, start_link/2 ]).

%% gen_server callbacks
-export([init/1,
         gossiping/2,
         waiting/2,
         handle_info/3,
         handle_event/3,
         handle_sync_event/4,
         terminate/3,
         code_change/4]).

-ifdef(TEST).
-export([reconcile_nodes/4, can_gossip/1, reset_gossip/1]).
-endif.

-include("egossip.hrl").

% debug
-define(RECHECK, 5).
-define(SERVER(Module), list_to_atom("egossip_" ++ atom_to_list(Module))).

-define(TRY(Code), (catch begin Code end)).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Module) ->
    start_link(Module, []).

start_link(Module, Opts) ->
    gen_fsm:start_link({local, ?SERVER(Module)}, ?MODULE, [Module, Opts], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Module, _Opts]) ->
    send_after(Module:gossip_freq(), tick),
    net_kernel:monitor_nodes(true),
    {ok, gossiping, reset_gossip(#state{module=Module})}.

waiting({R_Epoch, _, _} = Msg, #state{wait_for=R_Epoch} = State) ->
    gossiping(Msg, State#state{wait_for=undefined, epoch=R_Epoch});
waiting({R_Epoch, _, _}, #state{wait_for=Epoch} = State0)
        when R_Epoch > Epoch ->
    % prevent a node from waiting forever
    WaitFor = R_Epoch + 1,
    {next_state, waiting, State0#state{wait_for=WaitFor}};
waiting(_, State) ->
    {next_state, waiting, State}.

%
% Gossip Cases
%
% 1. epochs and nodelists match
%       - gossip
% 2. (epoch_remote > epoch_local) and nodelists match
%       - set epoch to epoch_remote, gossip
% 3. (epoch_remote > epoch_local) and nodelists mismatch
%       - wait for (epoch_remote + 1)
% 4. epochs match and nodelists mismatch
%       - merge nodelists

% 1.
gossiping({Epoch, {Token, Msg, From}, Nodelist},
        #state{module=Module, epoch=Epoch, nodes=Nodelist} = State0) ->
    {ok, State1} = do_gossip(Module, Token, Msg, From, State0),
    {next_state, gossiping, State1};
% 2.
gossiping({R_Epoch, {Token, Msg, From}, Nodelist},
        #state{module=Module, epoch=Epoch, nodes=Nodelist} = State0)
        when R_Epoch > Epoch ->
    {ok, State1} = next_round(R_Epoch, State0),
    {ok, State2} = do_gossip(Module, Token, Msg, From, State1),
    {next_state, gossiping, State2};
% 3.
gossiping({R_Epoch, _, R_Nodelist},
        #state{epoch=Epoch, nodes=Nodelist} = State0)
        when R_Epoch > Epoch, R_Nodelist =/= Nodelist ->
    WaitFor = R_Epoch + 1,
    {next_state, waiting, State0#state{wait_for=WaitFor}};
% 4.
gossiping({Epoch, {Token, Msg, From},  R_Nodelist},
        #state{module=Module, nodes=Nodelist, epoch=Epoch} = State0)
        when R_Nodelist =/= Nodelist ->
    {_, Nodelist1} = reconcile_nodes(Nodelist, R_Nodelist, From, State0#state.module),
    {ok, State1} = do_gossip(Module, Token, Msg, From, State0),
    {next_state, gossiping, State1#state{nodes=Nodelist1}};
gossiping(_, State) ->
    {next_state, gossiping, State}.

handle_info({nodedown, Node}, StateName, #state{module=Module} = State) ->
    NodesLeft = lists:filter(fun(N) -> N =/= Node end, State#state.nodes),
    ?TRY(Module:expire(Node)),
    {next_state, StateName, State#state{nodes=NodesLeft}};

handle_info({nodeup, _}, StateName, State) ->
    % this is handled when nodes gossip
    {next_state, StateName, State};

handle_info(tick, StateName, #state{module=Module} = State0) ->
    send_after(Module:gossip_freq(), tick),
    EpochEnabled = erlang:function_exported(Module, cycles, 1),

    {ok, State1} = case EpochEnabled of
        true ->
            next_cycle(State0);
        false ->
            {ok, State0}
    end,

    {ok, State2} = case get_peer(visible) of
        none_available ->
            {ok, State1};
        {ok, Node} ->
            send_gossip(Node, push, Module:digest(), State1)
    end,
    {next_state, StateName, State2}.

handle_event(_Msg, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_gossip(Module, Token, Msg, From, State0) ->
    Exported = erlang:function_exported(Module, next(Token), 2),

    case Module:Token(Msg, From) of
        {ok, Reply} when Exported == true ->
            send_gossip(From, next(Token), Reply, State0);
        _ ->
            {ok, State0}
    end.

next(push) -> symmetric_push;
next(symmetric_push) -> commit;
next(_) -> undefined.

next_cycle(#state{module=Module, cycle=Cycle, epoch=Epoch, nodes=Nodes} = State0) ->
    NextCycle = Cycle + 1,
    NodeCount = length(Nodes),

    case NextCycle > Module:cycles(NodeCount) of
        true ->
            NextEpoch = Epoch + 1,
            next_round(NextEpoch, State0);
        false ->
            {ok, State0#state{cycle=NextCycle}}
    end.

next_round(N, State) ->
    Module = State#state.module,
    NodeCount = length(State#state.nodes),
    ?TRY(Module:round_finish(NodeCount)),
    {ok, State#state{epoch=N, cycle=0}}.

reconcile_nodes(A, B, From, Module) ->
    Intersection = intersection(A, B),

    if
        A == B ->
            {false, union(A, B)};
        length(A) > length(B) andalso Intersection == [] ->
            {false, union(A, [From])};
        length(A) < length(B) andalso Intersection == [] ->
            ?TRY(Module:join(B)),
            {true, union(B, [node_name()])};
        length(Intersection) == 1 andalso length(B) > length(A) ->
            ?TRY(Module:join(B -- A)),
            {true, union(B, [node_name()])};
        length(Intersection) > 0 ->
            {false, union(A, B)};
        A < B ->
            ?TRY(Module:join(B)),
            {true, union(B, [node_name()])};
        true ->
            {false, union(A, [From])}
    end.

-ifdef(TEST).
node_name() -> a.
-else.
node_name() -> node().
-endif.

get_peer(Opts) ->
    case nodes(Opts) of
        [] -> none_available;
        Nodes ->
            N = random:uniform(length(Nodes)),
            {ok, lists:nth(N, Nodes)}
    end.

send_after(never, _Message) ->
    ok;
send_after({Num,Sec}, Message) ->
    send_after(trunc(1000 / (Num/Sec)), Message);
send_after(After, Message) ->
    erlang:send_after(After, self(), Message).

can_gossip(#state{ cgossip={CM, CT}, mgossip={MM, MT} } = State) when CT < MT ->
    {CM < MM, State#state{ cgossip={CM+1, os:timestamp()} }};

can_gossip(State0) ->
    can_gossip(reset_gossip(State0)).

reset_gossip(#state{module=Module} = State0) ->
    case Module:gossip_freq() of
        never ->
            #state{ cgossip={CM, {Mega, Sec, Micro}} } = State0,
            State0#state{ mgossip={CM, {Mega, Sec + ?RECHECK, Micro}} };
        {Num, InSec} ->
            Now = {Mega, Sec, Micro} = os:timestamp(),
            Expire = {Mega, Sec + InSec, Micro},
            State0#state{ cgossip={0, Now}, mgossip={Num, Expire} }
    end.

send_gossip(ToNode, Token, Data, #state{module=Module, nodes=Nodes} = State0) ->
    {CanSend, State1} = can_gossip(State0),
    case CanSend of
        false ->
            {ok, State1};
        true ->
            Epoch = State0#state.epoch,
            gen_fsm:send_event({?SERVER(Module), ToNode}, {Epoch, {Token, Data, node()}, Nodes}),
            {ok, State1}
    end.

union(L1, L2) ->
    sets:to_list(sets:union(sets:from_list(L1), sets:from_list(L2))).

intersection(A, B) ->
    sets:to_list(sets:intersection(sets:from_list(A), sets:from_list(B))).
