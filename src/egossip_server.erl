-module(egossip_server).
-behaviour(gen_fsm).

%% API
-export([start_link/1]).

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

%% @doc
%% Starts egossip server with registered handler module
%% @end
-spec start_link(module()) -> {ok,Pid} | ignore | {error,Error} when
    Pid :: pid(),
    Error :: {already_started, Pid} | term().

start_link(Module) ->
    gen_fsm:start_link({local, ?SERVER(Module)}, ?MODULE, [Module], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Module]) ->
    send_after(Module:gossip_freq(), tick),
    net_kernel:monitor_nodes(true),
    {ok, gossiping, reset_gossip(#state{module=Module})}.

%% @doc
%% A node will transition into waiting state if there exists
%% a higher epoch converstation happening. The node will then
%% wait for (epoch + 1) to roll around to join in on the conversation
%% @end

-spec waiting({epoch(), message(), [node()]}, StateData :: term()) -> handle_event_ret().

waiting({R_Epoch, _, _} = Msg, #state{wait_for=R_Epoch} = State) ->
    gossiping(Msg, State#state{wait_for=undefined, epoch=R_Epoch});
waiting({R_Epoch, _, _}, #state{wait_for=Epoch} = State0)
        when R_Epoch > Epoch ->
    % prevent a node from waiting forever
    WaitFor = R_Epoch + 1,
    {next_state, waiting, State0#state{wait_for=WaitFor}};
waiting(_, State) ->
    {next_state, waiting, State}.

%% @doc
%% Nodes which have the same epoch and haven't been split into islands
%% will be able to gossip in a conversation. Here's what happens
%% in the other cases:
%%
%% 1. epochs and nodelists match
%%       - gossip
%% 2. (epoch_remote > epoch_local)
%%       - use higher epoch, gossip
%% 3. (epoch_remote > epoch_local)
%%       if intersection non-empty
%%         - merge lists, goto #2
%%       else
%%         - wait for (epoch_remote + 1)
%%       end
%% 4. epochs match and nodelists mismatch
%%       - merge nodelists
%% @end
-spec gossiping({epoch(), message(), [node()]}, StateData :: term()) -> handle_event_ret().

% 1.
gossiping({Epoch, {Token, Msg, From}, Nodelist},
        #state{module=Module, epoch=Epoch, nodes=Nodelist} = State0) ->
    {ok, State1} = do_gossip(Module, Token, Msg, From, State0),
    {next_state, gossiping, State1};

% 2.
gossiping({R_Epoch, {Token, Msg, From}, Nodelist},
        #state{epoch=Epoch, module=Module, nodes=Nodelist} = State0)
        when R_Epoch > Epoch ->
    % This case handles when a node flips over the next epoch and contacts
    % another node that hasn't updated its epoch yet. This happens due to
    % clock-drift between nodes. You'll never have everything perfectly in-sync
    % unless your Google and have atomic GPS clocks... To keep up with the highest
    % epoch we simply set our epoch to the new value keeping things in-sync.
    {ok, State1} = next_round(R_Epoch, State0),
    {ok, State2} = do_gossip(Module, Token, Msg, From, State1),
    {next_state, gossiping, State2};

% 3.
gossiping({R_Epoch, {Token, Msg, From}, R_Nodelist},
        #state{epoch=Epoch, module=Module, nodes=Nodelist} = State0)
        when R_Epoch > Epoch ->
    % The intersection is taken to prevent nodes from waiting twice
    % to enter into the next epoch. This happens when islands
    % are trying to join. For example:
    %
    %   Suppose we have two islands [a,b] and [c,d]
    %   'a' and 'b' are waiting, 'c' and 'd' both have a higher epoch
    %   'a' reconciles with [c,d] when epoch rolls around forming island [a,c,d]
    %   'a' then talks to 'b', since [a,b] =/= [a,c,d] 'b' its forced to wait again
    %
    case intersection(R_Nodelist, Nodelist) of
        [] ->
            {next_state, waiting, State0#state{wait_for = (R_Epoch + 1)}};
        _NonEmpty ->
            Nodelist1 = reconcile_nodes(Nodelist, R_Nodelist, From, Module),
            {ok, State1} = next_round(R_Epoch, State0),
            {ok, State2} = do_gossip(Module, Token, Msg, From, State1),
            {next_state, gossiping, State2#state{nodes=Nodelist1}}
    end;
% 4.
gossiping({Epoch, {Token, Msg, From},  R_Nodelist},
        #state{module=Module, nodes=Nodelist, epoch=Epoch} = State0)
        when R_Nodelist =/= Nodelist ->
    Nodelist1 = reconcile_nodes(Nodelist, R_Nodelist, From, State0#state.module),
    {ok, State1} = do_gossip(Module, Token, Msg, From, State0),
    {next_state, gossiping, State1#state{nodes=Nodelist1}};
gossiping(_, State) ->
    {next_state, gossiping, State}.

handle_info({nodedown, Node}, StateName, #state{module=Module} = State) ->
    NodesLeft = lists:filter(fun(N) -> N =/= Node end, State#state.nodes),
    ?TRY(Module:expire(Node)),
    {next_state, StateName, State#state{nodes=NodesLeft}};

handle_info({nodeup, _}, StateName, State) ->
    % don't care about when a node is up, this is handled
    % when nodes gossip with eachother
    {next_state, StateName, State};

handle_info(tick, StateName, #state{module=Module} = State0) ->
    send_after(Module:gossip_freq(), tick),

    {ok, State1} = case get_peer(visible) of
        none_available ->
            {ok, State0};
        {ok, Node} ->
            send_gossip(Node, push, Module:digest(), State0)
    end,
    {next_state, StateName, State1}.

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
    AggregateProtocol = is_aggregation_protocol(Module),

    case Module:Token(Msg, From) of
        {ok, Reply} when Exported == true ->
            send_gossip(From, next(Token), Reply, State0);
        _ when AggregateProtocol == true ->
            % cycle ends when last message is received in gossip
            next_cycle(State0);
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

%% @doc
%% This handles cluster membership. We don't use the ErlangVM
%% to determine whether a node is taking part in a conversation.
%% The reason is because it would prevent aggregation-based protocols
%% from converging. If nodes are continuously joining a conversation
%% will never converge on an answer.
%% @end
reconcile_nodes(A, B, From, Module) ->
    Intersection = intersection(A, B),

    if
        A == B ->
            A;
        length(A) > length(B) andalso Intersection == [] ->
            union(A, [From]);
        length(A) < length(B) andalso Intersection == [] ->
            ?TRY(Module:join(B)),
            union(B, [node_name()]);
        length(Intersection) == 1 andalso length(B) > length(A) ->
            ?TRY(Module:join(B -- A)),
            union(B, [node_name()]);
        length(Intersection) > 0 ->
            union(A, B);
        A < B ->
            ?TRY(Module:join(B)),
            union(B, [node_name()]);
        true ->
            union(A, [From])
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

% if we implement an aggregation based protocol only
% count push when checking can_gossip/1
can_gossip(push, State) ->
    can_gossip(State);
can_gossip(_, State) ->
    case is_aggregation_protocol(State#state.module) of
        true -> {true, State};
        false -> can_gossip(State)
    end.

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
    {CanSend, State1} = can_gossip(Token, State0),
    case CanSend of
        false ->
            {ok, State1};
        true ->
            Epoch = State0#state.epoch,
            gen_fsm:send_event({?SERVER(Module), ToNode}, {Epoch, {Token, Data, node()}, Nodes}),
            {ok, State1}
    end.

is_aggregation_protocol(Module) ->
    erlang:function_exported(Module, cycles, 1).

union(L1, L2) ->
    sets:to_list(sets:union(sets:from_list(L1), sets:from_list(L2))).

intersection(A, B) ->
    sets:to_list(sets:intersection(sets:from_list(A), sets:from_list(B))).
