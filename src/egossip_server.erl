-module(egossip_server).
-behaviour(gen_fsm).

%% API
-export([ start_link/1 ]).

%% gen_server callbacks
-export([init/1,
         gossiping/2,
         waiting/2,
         syncing/2,
         handle_info/3,
         handle_event/3,
         handle_sync_event/4,
         terminate/3,
         code_change/4]).

-ifdef(TEST).
-export([reconcile_nodes/4, can_gossip/1, reset_gossip/1]).
-endif.

-record(state, {
    cgossip = {0, {0,0,0}}, % current gossip count
    mgossip = {1, {1,1,1}}, % max gossip count
    nodecache = [node()],
    expired = [],

    % aggregation
    epoch = 0,
    cycle = 0,

    module
}).

% debug
-define(TIMESTAMP, begin {_, {Hour,Min,Sec}} = erlang:localtime(),
        lists:flatten(io_lib:format("~2..0B:~2..0B:~2..0B", [Hour, Min, Sec])) end).
-define(DEBUG, false).
-define(DEBUG_MSG(Str, Args), ?DEBUG andalso
        io:format("[~s] :: ~s", [?TIMESTAMP, lists:flatten(io_lib:format(Str, Args))])).

-define(RECHECK, 5).
-define(SERVER(Module), list_to_atom("egossip_" ++ atom_to_list(Module))).

-define(TRY(Code), (catch begin Code end)).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Module) ->
    gen_fsm:start_link({local, ?SERVER(Module)}, ?MODULE, [Module], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Module]) ->
    send_after(Module:gossip_freq(), tick),
    net_kernel:monitor_nodes(true),
    {ok, waiting, reset_gossip(#state{module=Module})}.

waiting({Epoch, _, _}, State0) ->
    {ok, State1} = next_epoch(Epoch + 1, State0),
    {next_state, syncing, State1};
waiting(_, State) ->
    ?DEBUG_MSG("waiting...~n", []),
    {next_state, syncing, State}.

syncing({Epoch, _, _} = Msg, #state{epoch=Epoch} = State) ->
    gossiping(Msg, State);
syncing({R_Epoch, _, _}, #state{epoch=Epoch} = State0)
        when R_Epoch > Epoch ->
    % This resolves a race condition. For example, if two nodes A and B
    % join to form a cluster. Perhaps Node A was trying to sync with B then all
    % of a sudden A and B join C and D which have a larger era will
    % cause B to increase his epoch leaving A stuck syncing for Epoch + 1 which
    % will never roll around
    {ok, State1} = next_epoch(R_Epoch + 1, State0),
    {next_state, syncing, State1};
syncing(_, State) ->
    ?DEBUG_MSG("syncing...~n", []),
    {next_state, syncing, State}.

gossiping({R_Epoch, _, _} = Msg, #state{epoch=Epoch} = State)
        when Epoch < R_Epoch ->
    % were not in the same era, change our era and reset gossip
    {ok, State1} = next_epoch(R_Epoch, State),
    gossiping(Msg, State1);

gossiping({_, {push, Msg, From}, RemoteNodes},
          #state{module=Module, nodecache=Nodes} = State0) when From =/= node() ->
    ?DEBUG_MSG("push from ~p~n", [From]),
    NewNodes = reconcile_nodes(Nodes, RemoteNodes, From, Module),
    Exported = erlang:function_exported(Module, symmetric_push, 2),

    {ok, State1} = case Module:push(Msg, From) of
        {ok, Reply} when Exported == true ->
            send_gossip(From, symmetric_push, Reply, State0);
        _ ->
            {ok, State0}
    end,
    {next_state, gossiping, State1#state{nodecache=NewNodes}};

gossiping({_, {symmetric_push, Msg, From}, RemoteNodes},
          #state{module=Module, nodecache=Nodes} = State0) when From =/= node() ->
    ?DEBUG_MSG("symmetric_push from ~p~n", [From]),
    NewNodes = reconcile_nodes(Nodes, RemoteNodes, From, Module),
    Exported = erlang:function_exported(Module, commit, 2),

    {ok, State1} = case Module:symmetric_push(Msg, From) of
        {ok, Reply} when Exported == true ->
            send_gossip(From, commit, Reply, State0);
        _ ->
            {ok, State0}
    end,
    {next_state, gossiping, State1#state{nodecache=NewNodes}};

gossiping({_, {commit, Msg, From}, _}, #state{module=Module} = State)
        when From =/= node() ->
    ?DEBUG_MSG("commit from ~p~n", [From]),
    Module:commit(Msg, From),
    {next_state, gossiping, State}.

handle_info({nodedown, Node}, StateName, #state{module=Module} = State) ->
    NodesLeft = lists:filter(fun(N) -> N =/= Node end, State#state.nodecache),
    ?TRY(Module:expire(Node)),
    {next_state, StateName, State#state{nodecache=NodesLeft}};

handle_info({nodeup, _}, StateName, State) ->
    % this is handled when nodes gossip
    {next_state, StateName, State};

handle_info(tick, StateName, #state{module=Module} = State0) ->
    send_after(Module:gossip_freq(), tick),

    {ok, State1} = case get_peer(visible) of
        none_available ->
            {ok, State0};
        {ok, Node} ->
           case Module:digest() of
                {ok, Digest} ->
                    send_gossip(Node, push, Digest, State0);
                noreply ->
                    {ok, State0}
            end
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

next_epoch(N, State) ->
    {ok, State#state{epoch=N, cycle=0}}.

reconcile_nodes(A, B, From, Module) ->
    Intersection = intersection(A, B),

    if
        A == B ->
            union(A, B);
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

send_gossip(ToNode, Token, Data, #state{module=Module, nodecache=Nodes} = State0) ->
    {CanSend, State1} = can_gossip(State0),
    case CanSend of
        false ->
            {ok, State1};
        true ->
            ?DEBUG_MSG("sending ~p to ~p~n", [Token, ToNode]),
            Epoch = State0#state.epoch,
            gen_fsm:send_event({?SERVER(Module), ToNode}, {Epoch, {Token, Data, node()}, Nodes}),
            {ok, State1}
    end.

union(L1, L2) ->
    sets:to_list(sets:union(sets:from_list(L1), sets:from_list(L2))).

intersection(A, B) ->
    sets:to_list(sets:intersection(sets:from_list(A), sets:from_list(B))).
