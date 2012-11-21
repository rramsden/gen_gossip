%% @doc
%% Behaviour module for egossip. egossip_server must be implemented by
%% the user. There's two modes for gossiping: aggregate and epidemic.
%%
%% Aggregation Protocols
%% ---------------------
%%
%% These protocols you want to converge at some point before reseting the round.
%% They will prevent other nodes from joining a round in progress. They do
%% this by keeping an ever increasing epoch counter which acts as a version number.
%% If two versions don't match up then nodes will not gossip with eachother. Lower
%% epoch nodes will wait to join higher epochs when the next round occurs.
%%
%% Epidemic Protocols
%% ------------------
%%
%% These don't have any kind of versioning; all nodes will always be able to
%% gossip with eachother.
%%
%% Implementing a module
%% ---------------------
%%
%% To use egossip_server you will need a user module
%% to implement it. You must define the following callbacks:
%%
%%  init(Args)
%%    ==> {ok, State}
%%
%%  gossip_freq()
%%    | Handles how frequently a gossip message is sent. Time is
%%    | in milliseconds
%%    ==> Integer
%%
%%  digest(State)
%%    | Message you want to be gossiped around cluster
%%    ==> {reply, Term, State}
%%
%%  join(Nodelist, State)
%%    | Notifies callback module when the CURRENT NODE joins another cluster
%%    ==> {noreply, State}
%%
%%  expire(Node, State)
%%    | Notifies callback module when a node leaves the cluster
%%    ==> {noreply, State}
%%
%%  push(Msg, From, State)
%%    | Called when we receive a push from another node
%%    ==> {reply, Reply, State} | {noreply, State}
%%
%%  symmetric_push(Msg, From, State)
%%    | Called when we receive a symmetric_push from another node
%%    ==> {reply, From, State} | {noreply, State}
%%
%%  commit(Msg, From, State)
%%    | Called when we receive a commit from another node
%%    ==> {noreply, State}
%%
%%  AGGREGATION CALLBACKS
%%
%%  round_finish(NodeCount, State)
%%    | User module is notified when a round finishes, passing
%%    | the number of nodes that were in on the current conversation
%%    ==> {noreply, State}
%%
%%  cycles(NodeCount)
%%    | You don't need to implement this if your building using epidemic mode.
%%    | This returns the number of cycles in each round needed for aggregation mode.
%%    ==> Integer
%%
%%
%%  Gossip Communication
%%  --------------------
%%
%%                   NODE A                     NODE B
%%                send push  ---------------->  Module:push/3
%%  Module:symmetric_push/3  <----------------  send symmetric_push
%%              send commit  ---------------->  Module:commit/3
%%
%% @end
-module(egossip_server).
-behaviour(gen_fsm).

-include("egossip.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1,
         gossiping/2,
         waiting/2,
         handle_info/3,
         handle_event/3,
         handle_sync_event/4,
         terminate/3,
         code_change/4]).

-define(SERVER(Module), list_to_atom("egossip_" ++ atom_to_list(Module))).
-define(TRY(Code), (catch begin Code end)).

-ifdef(TEST).
-export([reconcile_nodes/4, send_gossip/4]).
-define(mockable(Fun), ?MODULE:Fun).
-else.
-define(mockable(Fun), Fun).
-endif.

%%%===================================================================
%%% API
%%%===================================================================

-callback init(Args :: [any()]) ->
    {ok, module_state()}.
-callback gossip_freq() ->
    Tick :: pos_integer().
-callback digest(State :: any()) ->
    {reply, Reply :: any(), module_state()}.
-callback join(nodelist(), module_state()) ->
    {noreply, module_state()}.
-callback expire(node(), module_state()) ->
    {noreply, module_state()}.
-callback push(Msg :: any(), From :: node(), module_state()) ->
    {reply, Reply :: any(), module_state()} | {noreply, module_state()}.
-callback symmetric_push(Msg :: any(), From :: node(), module_state()) ->
    {reply, Reply :: any(), module_state()} | {noreply, module_state()}.
-callback commit(Msg :: any(), From :: node(), module_state()) ->
    {noreply, module_state()}.

%% @doc
%% Starts egossip server with registered handler module
%% @end

start_link(Module, Args, Mode) ->
    case lists:member(Mode, [aggregate, epidemic]) of
        true ->
            gen_fsm:start_link({local, ?SERVER(Module)}, ?MODULE, [Module, Args, Mode], []);
        false ->
            {error, invalid_mode}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Module, Args, Mode]) ->
    send_after(Module:gossip_freq(), tick),
    net_kernel:monitor_nodes(true),
    {ok, State} = Module:init(Args),
    {ok, gossiping, #state{module=Module, mode=Mode, mstate=State}}.

%% @doc
%% A node will transition into waiting state if there exists
%% a higher epoch converstation happening. The node will then
%% wait for (epoch + 1) to roll around to join in on the conversation
%% @end

-spec waiting({epoch(), message(), [node()]}, StateData :: term()) -> handle_event_ret().

waiting({R_Epoch, _, _} = Msg, #state{wait_for=R_Epoch} = State) ->
    gossiping(Msg, State#state{wait_for=undefined, max_wait=0, epoch=R_Epoch});
waiting({R_Epoch, _, _}, #state{wait_for=Epoch} = State0)
        when R_Epoch > Epoch ->
    % if there's a larger epoch around then wait for that one
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
%% 3. (epoch_remote > epoch_local) and nodelists don't match
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
        when R_Epoch > Epoch, R_Nodelist =/= Nodelist ->
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
            % wait twice the amount of cycles for nodes to join each other.
            % we do this because if the node we're waiting on crashes
            % we could end up waiting forever.
            ClusterSize = length(union(Nodelist, R_Nodelist)),
            MaxWait = Module:cycles(ClusterSize) * 2,
            {next_state, waiting, State0#state{max_wait = MaxWait, wait_for = (R_Epoch + 1)}};
        _NonEmpty ->
            {ok, State1} = next_round(R_Epoch, State0),
            {ok, State2} = reconcile_nodes(Nodelist, R_Nodelist, From, State1),
            {ok, State3} = do_gossip(Module, Token, Msg, From, State2),
            {next_state, gossiping, State3}
    end;
% 4.
gossiping({Epoch, {Token, Msg, From},  R_Nodelist},
        #state{module=Module, nodes=Nodelist, epoch=Epoch} = State0)
        when R_Nodelist =/= Nodelist ->
    {ok, State1} = reconcile_nodes(Nodelist, R_Nodelist, From, State0),
    {ok, State2} = do_gossip(Module, Token, Msg, From, State1),
    {next_state, gossiping, State2};
gossiping({_, _, _}, State) ->
    {next_state, gossiping, State}.

handle_info({nodedown, Node}, StateName, #state{mstate=MState0, module=Module} = State) ->
    NodesLeft = lists:filter(fun(N) -> N =/= Node end, State#state.nodes),
    {noreply, MState1} = Module:expire(Node, MState0),
    {next_state, StateName, State#state{nodes=NodesLeft, mstate=MState1}};

handle_info({nodeup, _}, StateName, State) ->
    % don't care about when a node is up, this is handled
    % when nodes gossip with eachother
    {next_state, StateName, State};

handle_info('$egossip_tick', StateName, #state{max_wait=MaxWait,
                                    mstate=MState0, module=Module} = State0) ->
    send_after(Module:gossip_freq(), '$egossip_tick'),

    {ok, State1} = case get_peer(visible) of
        none_available ->
            {ok, State0};
        {ok, Node} ->
            {reply, Digest, MState1} = Module:digest(MState0),
            ?mockable( send_gossip(Node, push, Digest, State0#state{mstate=MState1}) )
    end,

    % The MAX_WAIT counter is positive we're waiting to join a cluster.
    % the reason we set this is because a node could end up waiting forever
    % if the node it was waiting on crashed.
    case StateName == gossiping of
        true ->
            {ok, State2} = next_cycle(State1),
            {next_state, gossiping, State2};
        false ->
            case State1#state.max_wait == 0 of
                true ->
                    {next_state, gossiping, State1};
                false ->
                    {next_state, waiting, State1#state{max_wait=(MaxWait-1)}}
            end
    end.

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

do_gossip(Module, Token, Msg, From, #state{mstate=MState0} = State0) ->
    case Module:Token(Msg, From, MState0) of
        {reply, Reply, MState1} ->
            ?mockable( send_gossip(From, next(Token), Reply, State0#state{mstate=MState1}) );
        {noreply, MState1} ->
            {ok, State0#state{mstate=MState1}}
    end.

next(push) -> symmetric_push;
next(symmetric_push) -> commit;
next(_) -> undefined.

next_cycle(#state{mode=Mode} = State) when Mode =/= aggregate ->
    {ok, State};
next_cycle(#state{module=Module, cycle=Cycle, epoch=Epoch, nodes=Nodes} = State0) ->
    NextCycle = Cycle + 1,
    NodeCount = length(Nodes),

    case NextCycle > Module:cycles(NodeCount) of
        true ->
            next_round(Epoch + 1, State0);
        false ->
            {ok, State0#state{cycle=NextCycle}}
    end.

next_round(N, #state{module=Module, mstate=MState0} = State) ->
    NodeCount = length(State#state.nodes),
    {noreply, MState1} = Module:round_finish(NodeCount, MState0),
    {ok, State#state{epoch=N, cycle=0, mstate=MState1}}.

%% @doc
%% This handles cluster membership. We don't use the ErlangVM
%% to determine whether a node is taking part in a conversation.
%% The reason is because it would prevent aggregation-based protocols
%% from converging. If nodes are continuously joining a conversation
%% will never converge on an answer.
%% @end
reconcile_nodes(A, B, From, #state{mstate=MState0, module=Module} = State) ->
    Intersection = intersection(A, B),

    {Nodes, MState2} = if
        A == B ->
            {A, MState0};
        length(A) > length(B) andalso Intersection == [] ->
            {union(A, [From]), MState0};
        length(A) < length(B) andalso Intersection == [] ->
            {noreply, MState1} = Module:join(B, MState0),
            {union(B, [node_name()]), MState1};
        length(Intersection) == 1 andalso length(B) > length(A) ->
            {noreply, MState1} = Module:join(B -- A, MState0),
            {union(B, [node_name()]), MState1};
        length(Intersection) > 0 ->
            {union(A, B), MState0};
        A < B ->
            {noreply, MState1} = Module:join(B, MState0),
            {union(B, [node_name()]), MState1};
        true ->
            {union(A, [From]), MState0}
    end,

    {ok, State#state{mstate=MState2, nodes=Nodes}}.

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

send_gossip(ToNode, Token, Message, #state{module=Module, nodes=Nodelist} = State0) ->
    Payload = {State0#state.epoch, {Token, Message, node()}, Nodelist},
    gen_fsm:send_event({?SERVER(Module), ToNode}, Payload),
    {ok, State0}.

union(L1, L2) ->
    sets:to_list(sets:union(sets:from_list(L1), sets:from_list(L2))).

intersection(A, B) ->
    sets:to_list(sets:intersection(sets:from_list(A), sets:from_list(B))).
