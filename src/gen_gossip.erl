%% @doc
%% Behaviour module for gen_gossip. gen_gossip must be implemented by
%% the user. There's two gossiping modes:
%%
%% 1. Aggregation Protocols
%% ------------------------
%%
%% If you want to converge on a value over a period of time then you want to implement
%% an aggregation protocol.  These protocols will prevent nodes from joining a round in
%% progress. They do this by keeping a version number of the current conversation.
%% If two versions don't match then nodes will not gossip with eachother. Lower
%% versioned nodes will wait for the next version to join in when the next round rolls over.
%%
%% 2. Epidemic Protocols
%% ---------------------
%%
%% These don't have any kind of versioning. All nodes will always be able to
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
%%  join(Nodelist, State)
%%    | Notifies callback module when the CURRENT NODE joins another cluster
%%    ==> {noreply, State}
%%
%%  expire(Node, State)
%%    | Notifies callback module when a node leaves the cluster
%%    ==> {noreply, State}
%%
%%  digest(State)
%%    | Message you want to be gossiped around cluster
%%    ==> {reply, Reply, HandleToken State}
%%
%%  handle_gossip(Token, Msg, From, State)
%%    | Called when we receive a gossip message from another node
%%    ==> {reply, Reply, HandleToken, State} | {noreply, State}
%%
%%  You will need to implment the following if you're implementing an aggregation
%%  protocol:
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
%%  Optionally, you can also implement gen_server callbacks:
%%
%%  handle_info(Msg, State)
%%  handle_call(Msg, From, State)
%%  handle_cast(Msg, State)
%%  terminate(Reason, State)
%%  code_chnage(OldVsn, State, Extra)
%%
%%  Gossip Communication
%%  --------------------
%%
%%                   NODE A                     NODE B
%%                send digest -----------------> Module:handle_gossip/4
%%     Module:handle_gossip/4 <----------------- send pull
%%                send commit -----------------> Module:handle_commit/3
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
-export([reconcile_nodes/4, send_gossip/4, node_name/0, nodelist/0]).
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
-callback handle_gossip(HandleToken :: atom(), Msg :: term(), From :: node(), State :: term()) ->
    {reply, Reply :: term(), HandleToken :: atom(), NewState :: term()} | {noreply, NewState :: term()}.

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

handle_info('$gen_gossip_tick', StateName, #state{nodes=Nodelist0, max_wait=MaxWait,
                                mstate=MState0, module=Module} = State0) ->
    % schedule another tick
    {reply, Tick, MState1} = Module:gossip_freq(MState0),
    send_after(Tick, '$gen_gossip_tick'),

    {Nodelist1, MState2} = expire_downed_nodes(Nodelist0, Module, MState1),
    State1 = State0#state{nodes=Nodelist1, mstate=MState2},

    case StateName == gossiping of
        true ->
            {ok, State2} = gossip(State1),
            {ok, State3} = next_cycle(State2),
            {next_state, gossiping, State3};
        false ->
            % prevent a node from waiting forever on a crashed node
            ExceededMaxWait = (MaxWait == 0),
            case ExceededMaxWait of
                true ->
                    {next_state, gossiping, State1};
                false ->
                    {next_state, waiting, State1#state{max_wait=(MaxWait-1)}}
            end
    end;

handle_info(Msg, StateName, #state{module=Module, mstate=MState0} = State) ->
    case erlang:function_exported(Module, handle_info, 2) of
        true ->
            Reply = Module:handle_info(Msg, MState0),
            handle_reply(Reply, StateName, State);
        false ->
            {next_state, StateName, State}
    end.

handle_event(Event, StateName, #state{module=Module, mstate=MState0} = State) ->
    case erlang:function_exported(Module, handle_cast, 2) of
        true ->
            Reply = Module:handle_cast(Event, MState0),
            handle_reply(Reply, StateName, State);
        false ->
            {next_state, StateName, State}
    end.

handle_sync_event(Event, From, StateName, #state{module=Module, mstate=MState0} = State) ->
    case erlang:function_exported(Module, handle_call, 3) of
        true ->
            Reply = Module:handle_call(Event, From, MState0),
            handle_reply(Reply, StateName, State);
        false ->
            {next_state, StateName, State}
    end.

terminate(Reason, _StateName, #state{module=Module, mstate=MState0}) ->
    case erlang:function_exported(Module, terminate, 2) of
        true ->
            Module:terminate(Reason, MState0);
        false ->
            ok
    end.

code_change(OldVsn, _StateName, #state{module=Module, mstate=MState} = State, Extra) ->
    case erlang:function_exported(Module, code_change, 3) of
        true ->
            case Module:code_change(OldVsn, MState, Extra) of
                {ok, NewState} ->
                    {ok, State#state{mstate=NewState}};
                Error -> Error
            end;
        false ->
            {ok, State}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

gossip(#state{mstate=MState0, module=Module} = State) ->
    case get_peer(visible) of
        none_available ->
            {ok, State};
        {ok, Node} ->
            {reply, Digest, HandleToken, MState1} = Module:digest(MState0),
            ?mockable( send_gossip(Node, HandleToken, Digest, State#state{mstate=MState1}) )
    end.

expire_downed_nodes(Nodelist0, Module, MState0) ->
    Alive = ?mockable( nodelist() ),
    DownedNodes = subtract(Nodelist0, Alive),
    Nodelist1 = subtract(Nodelist0, DownedNodes),

    % expire nodes that have left the cluster
    MState1 = lists:foldl(fun(Node, Acc) ->
                {noreply, NewState} = Module:expire(Node, Acc),
                NewState
        end, MState0, DownedNodes),

    {Nodelist1, MState1}.

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
    case Module:handle_gossip(Token, Msg, From, MState0) of
        {reply, Reply, HandleToken, MState1} ->
            ?mockable( send_gossip(From, HandleToken, Reply, State0#state{mstate=MState1}) );
        {noreply, MState1} ->
            {ok, State0#state{mstate=MState1}}
    end.

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
reconcile_nodes(Local, Remote, From, #state{mstate=MState0, module=Module}) ->
    NodeName = ?mockable( node_name() ),
    Alive = ?mockable( nodelist() ),

    A = Local,
    B = intersection(Alive, Remote), % prevent dead nodes from being re-added

    LenA = length(A),
    LenB = length(B),
    Intersection = intersection(A, B),

    if
        % if the intersection is one this means that the node in question has
        % Left our island to join another. If the intersection is greater than
        % or equal to 2 this means we are in the process of forming a larger island
        % so we can simply union the two islands together.
        length(Intersection) >= 2 ->
            {MState0, union(A, B)};
        LenA == LenB ->
            TieBreaker = lists:sort(A) < lists:sort(B),
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

nodelist() ->
   [?mockable( node_name() ) | nodes()].

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

subtract(A, B) ->
    sets:to_list(sets:subtract(sets:from_list(A), sets:from_list(B))).
