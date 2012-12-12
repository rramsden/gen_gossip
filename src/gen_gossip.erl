%% @doc
%% Behaviour module for gen_gossip. gen_gossip must be implemented by
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
%% To use gen_gossip you will need a user module
%% to implement it. You must define the following callbacks:
%%
%%  init(Args)
%%    ==> {ok, State}
%%
%%  gossip_freq(State)
%%    | Handles how frequently a gossip message is sent. Time is
%%    | in milliseconds
%%    ==> {reply, Tick :: Integer, State}
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
%%  handle_push(Msg, From, State)
%%    | Called when we receive a push from another node
%%    ==> {reply, Reply, State} | {noreply, State}
%%
%%  handle_pull(Msg, From, State)
%%    | Called when we receive a pull from another node
%%    ==> {reply, From, State} | {noreply, State}
%%
%%  handle_commit(Msg, From, State)
%%    | Called when we receive a commit from another node
%%    ==> {noreply, State}
%%
%%  -- same as gen_server callbacks --
%%
%%  handle_info(Msg, State)
%%  handle_call(Msg, From, State)
%%  handle_cast(Msg, State)
%%  terminate(Reason, State)
%%  code_chnage(OldVsn, State, Extra)
%%
%%  AGGREGATION CALLBACKS
%%
%%  round_finish(NodeCount, State)
%%    | User module is notified when a round finishes, passing
%%    | the number of nodes that were in on the current conversation
%%    ==> {noreply, State}
%%
%%  round_length(NodeCount, State)
%%    | This returns the number of cycles in each round needed
%%    | to trigger round_finish.
%%    ==> Integer
%%
%%
%%  Gossip Communication
%%  --------------------
%%
%%                   NODE A                     NODE B
%%                send push  ---------------->  Module:handle_push/3
%%     Module:handle_pull/3  <----------------  send pull
%%              send commit  ---------------->  Module:handle_commit/3
%%
%% @end
-module(gen_gossip).
-behaviour(gen_fsm).

-include("gen_gossip.hrl").

%% API
-export([register_handler/3, call/2, cast/2]).

%% gen_server callbacks
-export([init/1,
         gossiping/2,
         waiting/2,
         handle_info/3,
         handle_event/3,
         handle_sync_event/4,
         terminate/3,
         code_change/4]).

-define(SERVER(Module), Module).
-define(TRY(Code), (catch begin Code end)).

-ifdef(TEST).
-export([reconcile_nodes/4, send_gossip/4, node_name/0]).
-define(mockable(Fun), ?MODULE:Fun).
-else.
-define(mockable(Fun), Fun).
-endif.

%%%===================================================================
%%% API
%%%===================================================================

-callback init(Args :: [any()]) ->
    {ok, module_state()}.
-callback gossip_freq(State :: term()) ->
    {reply, Tick :: pos_integer(), NewState :: term()}.
-callback digest(State :: term()) ->
    {reply, Reply :: term(), NewState :: term()}.
-callback join(nodelist(), State :: term()) ->
    {noreply, NewState :: term()}.
-callback expire(node(), NewState :: term()) ->
    {noreply, NewState :: term()}.
-callback handle_push(Msg :: term(), From :: node(), State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} | {noreply, NewState :: term()}.
-callback handle_pull(Msg :: term(), From :: node(), State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} | {noreply, NewState :: term()}.
-callback handle_commit(Msg :: any(), From :: node(), module_state()) ->
    {noreply, NewState :: term()}.
-callback handle_info(Info :: timeout() | term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: term()}.
-callback handle_call(Request :: term(), From :: {pid(), Tag :: term()},
                      State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} |
    {reply, Reply :: term(), NewState :: term(), timeout() | hibernate} |
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.
-callback handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: term()}.
-callback terminate(Reason, State) -> no_return() when
    Reason :: normal | shutdown | {shutdown, term()} | term(),
    State :: term().
-callback code_change(OldVsn :: (term() | {down, term()}), State :: term(), Extra :: term()) ->
    {ok, NewState :: term()} | {error, Reason :: term()}.

%% @doc
%% Starts gen_gossip server with registered handler module
%% @end
-spec register_handler(module(), list(atom()), Mode :: atom()) -> {error, Reason :: atom()} | {ok, pid()}.

register_handler(Module, Args, Mode) ->
    case lists:member(Mode, [aggregate, epidemic]) of
        true ->
            gen_fsm:start_link({local, ?SERVER(Module)}, ?MODULE, [Module, Args, Mode], []);
        false ->
            {error, invalid_mode}
    end.

%% @doc
%% Cals gen_fsm:sync_send_all_state_event/2
%% @end
-spec call(FsmRef :: pid(), Event :: term()) -> term().

call(FsmRef, Event) ->
    gen_fsm:sync_send_all_state_event(FsmRef, Event).

%% @doc
%% Calls gen_fsm:send_all_state_event/2
%% @end
-spec cast(FsmRef :: pid(), Event ::term()) -> term().

cast(FsmRef, Request) ->
    gen_fsm:send_all_state_event(FsmRef, Request).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Module, Args, Mode]) ->
    net_kernel:monitor_nodes(true),
    {ok, MState0} = Module:init(Args),

    State0 = #state{module=Module, mode=Mode, mstate=MState0},
    {reply, Tick, MState1} = Module:gossip_freq(MState0),

    send_after(Tick, '$gen_gossip_tick'),

    {ok, gossiping, State0#state{mstate=MState1}}.

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
    {ok, State1} = set_round(R_Epoch, State0),
    {ok, State2} = do_gossip(Module, Token, Msg, From, State1),
    {next_state, gossiping, State2};

% 3.
gossiping({R_Epoch, {Token, Msg, From}, R_Nodelist},
        #state{epoch=Epoch, module=Module, mstate=MState0, nodes=Nodelist} = State0)
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

            {reply, RoundLength, MState1} =  Module:round_length(ClusterSize, MState0),
            {next_state, waiting, State0#state{max_wait = (RoundLength * 2),
                                               mstate=MState1,
                                               wait_for = (R_Epoch + 1)}};
        _NonEmpty ->
            {ok, State1} = set_round(R_Epoch, State0),
            {MState1, NewNodes} = reconcile_nodes(Nodelist, R_Nodelist, From, State1),
            {ok, State2} = do_gossip(Module, Token, Msg, From, State1#state{mstate=MState1}),
            {next_state, gossiping, State2#state{nodes=NewNodes}}
    end;
% 4.
gossiping({Epoch, {Token, Msg, From},  R_Nodelist},
        #state{module=Module, nodes=Nodelist, epoch=Epoch} = State0)
        when R_Nodelist =/= Nodelist ->
    {MState1, NewNodes} = reconcile_nodes(Nodelist, R_Nodelist, From, State0),
    {ok, State1} = do_gossip(Module, Token, Msg, From, State0#state{mstate=MState1}),
    {next_state, gossiping, State1#state{nodes=NewNodes}};
gossiping({_, _, _}, State) ->
    {next_state, gossiping, State}.

handle_info({nodedown, Node} = Msg, StateName, #state{mstate=MState0, module=Module} = State) ->
    NodesLeft = lists:filter(fun(N) -> N =/= Node end, State#state.nodes),
    {noreply, MState1} = Module:expire(Node, MState0),
    {noreply, MState2} = Module:handle_info(Msg, MState1),
    {next_state, StateName, State#state{nodes=NodesLeft, mstate=MState2}};

    handle_info('$gen_gossip_tick', StateName, #state{max_wait=MaxWait,
                                    mstate=MState0, module=Module} = State0) ->
    {reply, Tick, MState1} = Module:gossip_freq(MState0),
    send_after(Tick, '$gen_gossip_tick'),

    case StateName == gossiping of
        true ->
            {ok, State1} = case get_peer(visible) of
                none_available ->
                    {ok, State0};
                {ok, Node} ->
                    {reply, Digest, MState2} = Module:digest(MState1),
                    ?mockable( send_gossip(Node, handle_push, Digest, State0#state{mstate=MState2}) )
            end,
            {ok, State2} = next_cycle(State1),
            {next_state, gossiping, State2};
        false ->
            case State0#state.max_wait == 0 of
                true ->
                    {next_state, gossiping, State0#state{mstate=MState1}};
                false ->
                    % The MAX_WAIT counter is positive we're waiting to join a cluster.
                    % the reason we set this is because a node could end up waiting forever
                    % if the node it was waiting on crashed.
                    {next_state, waiting, State0#state{mstate=MState1, max_wait=(MaxWait-1)}}
            end
    end;

handle_info(Msg, StateName, #state{module=Module, mstate=MState0} = State) ->
    Reply = Module:handle_info(Msg, MState0),
    handle_reply(Reply, StateName, State).

handle_event(Event, StateName, #state{module=Module, mstate=MState0} = State) ->
    Reply = Module:handle_cast(Event, MState0),
    handle_reply(Reply, StateName, State).

handle_sync_event(Event, From, StateName, #state{module=Module, mstate=MState0} = State) ->
    Reply = Module:handle_call(Event, From, MState0),
    handle_reply(Reply, StateName, State).

terminate(Reason, _StateName, #state{module=Module, mstate=MState0}) ->
    Module:terminate(Reason, MState0).

code_change(OldVsn, _StateName, #state{module=Module, mstate=MState} = State, Extra) ->
    case Module:code_change(OldVsn, MState, Extra) of
        {ok, NewState} ->
            {ok, State#state{mstate=NewState}};
        Error -> Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_reply(Msg, StateName, State) ->
    case Msg of
        {reply, Reply, MState0} ->
            {reply, Reply, StateName, State#state{mstate=MState0}};
        {reply, Reply, MState0, Extra} ->
            {reply, Reply, StateName, State#state{mstate=MState0}, Extra};
        {noreply, MState0} ->
            {next_state, StateName, State#state{mstate=MState0}};
        {noreply, MState0, Extra} ->
            {next_state, StateName, State#state{mstate=MState0}, Extra};
        {stop, Reason, Reply, MState0} ->
            {stop, Reason, Reply, State#state{mstate=MState0}};
        {stop, Reason, MState0} ->
            {stop, Reason, State#state{mstate=MState0}}
    end.

do_gossip(Module, Token, Msg, From, #state{mstate=MState0} = State0) ->
    case Module:Token(Msg, From, MState0) of
        {reply, Reply, MState1} ->
            ?mockable( send_gossip(From, next(Token), Reply, State0#state{mstate=MState1}) );
        {noreply, MState1} ->
            {ok, State0#state{mstate=MState1}}
    end.

next(handle_push) -> handle_pull;
next(handle_pull) -> handle_commit;
next(_) -> undefined.

next_cycle(#state{mode=Mode} = State) when Mode =/= aggregate ->
    {ok, State};
next_cycle(#state{module=Module, mstate=MState0, cycle=Cycle, epoch=Epoch, nodes=Nodes} = State0) ->
    NextCycle = Cycle + 1,
    NodeCount = length(Nodes),

    {reply, RoundLength, MState1} = Module:round_length(NodeCount, MState0),

    case NextCycle > RoundLength of
        true ->
            set_round(Epoch + 1, State0#state{mstate=MState1});
        false ->
            {ok, State0#state{cycle=NextCycle, mstate=MState1}}
    end.

set_round(N, #state{module=Module, mstate=MState0} = State) ->
    NodeCount = length(State#state.nodes),
    {noreply, MState1} = Module:round_finish(NodeCount, MState0),
    {ok, State#state{epoch=N, cycle=0, mstate=MState1}}.

%% @doc
%% Figures out how we join islands together. This is important because
%% this bit of code figures out which nodes should trigger Module:join
%% callbacks.
%% @end
reconcile_nodes(A, B, From, #state{mstate=MState0, module=Module}) ->
    NodeName = ?mockable( node_name() ),
    Intersection = intersection(A, B),
    TieBreaker = lists:sort(A) < lists:sort(B),
    LenA = length(A),
    LenB = length(B),

    if
        % if the intersection is one this means that the node in question has
        % left our island to join another. If the intersection is greater than
        % or equal to 2 this means we are in the process of forming a larger island
        % so we can simply union the two islands together.
        length(Intersection) >= 2 ->
            {MState0, union(A, B)};
        LenA == LenB ->
            case TieBreaker of
                true ->
                    {MState0, union(A, [From])};
                false ->
                    {noreply, MState1} = Module:join(B, MState0),
                    {MState1, union([NodeName], B)}
            end;
        LenA > LenB ->
            % if my island is bigger than the remotes i consume it
            {MState0, union(A, [From])};
        LenA < LenB ->
            % my island is smaller, I have to leave it and join the remotes
            {noreply, MState1} = Module:join(B, MState0),
            {MState1, union([NodeName], B)}
    end.

% mocked out when testing
node_name() ->
    node().

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
