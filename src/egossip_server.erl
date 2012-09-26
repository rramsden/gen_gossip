-module(egossip_server).
-behaviour(gen_server).

%% API
-export([ start_link/1 ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ifdef(TEST).
-export([reconcile_nodes/4, can_gossip/1, reset_gossip/1]).
-endif.

-record(state, {
    cgossip = {0, {0,0,0}}, % current gossip count
    mgossip = {1, {1,1,1}}, % max gossip count
    nodes = [node()],
    expired = [],
    module
}).

% debug
-define(TIMESTAMP, begin {_, {Hour,Min,Sec}} = erlang:localtime(), lists:flatten(io_lib:format("~2..0B:~2..0B:~2..0B", [Hour, Min, Sec])) end).
-define(DEBUG, false).
-define(DEBUG_MSG(Str, Args), ?DEBUG andalso io:format("[~s] :: ~s", [?TIMESTAMP, lists:flatten(io_lib:format(Str, Args))])).

-define(RECHECK, 5).
-define(SERVER(Module), list_to_atom("egossip_" ++ atom_to_list(Module))).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Module) ->
    gen_server:start_link(?SERVER(Module), [Module], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Module]) ->
    send_after(Module:gossip_freq(), tick),
    net_kernel:monitor_nodes(true),
    {ok, reset_gossip(#state{module=Module})}.

handle_cast({push, RemoteNodes, From, Msg}, #state{module=Module, nodes=Nodes} = State0) when From =/= node() ->
    ?DEBUG_MSG("push from ~p~n", [From]),
    NewNodes = reconcile_nodes(Nodes, RemoteNodes, From, Module),

    {ok, State1} = case Module:push(Msg, From) of
        {ok, Reply} ->
            send_gossip(From, symmetric_push, Reply, State0);
        noreply ->
            {ok, State0}
    end,
    {noreply, State1#state{nodes=NewNodes}};

handle_cast({symmetric_push, RemoteNodes, From, Msg}, #state{module=Module, nodes=Nodes} = State0) when From =/= node() ->
    ?DEBUG_MSG("symmetric_push from ~p~n", [From]),
    NewNodes = reconcile_nodes(Nodes, RemoteNodes, From, Module),

    {ok, State1} = case Module:symmetric_push(Msg, From) of
        {ok, Reply} ->
            send_gossip(From, commit, Reply, State0);
        noreply ->
            {ok, State0}
    end,
    {noreply, State1#state{nodes=NewNodes}};

handle_cast({commit, RemoteNodes, From, Msg}, #state{module=Module, nodes=Nodes} = State) when From =/= node() ->
    ?DEBUG_MSG("commit from ~p~n", [From]),
    NewNodes = reconcile_nodes(Nodes, RemoteNodes, From, Module),
    Module:commit(Msg, From),
    {noreply, State#state{nodes=NewNodes}}.

handle_info({nodedown, Node}, #state{module=Module} = State) ->
    NodesLeft = lists:filter(fun(N) -> N =/= Node end, State#state.nodes),
    Module:expire(Node, NodesLeft),
    {noreply, State#state{nodes=NodesLeft}};

handle_info({nodeup, _}, State) ->
    % this is handled when nodes gossip
    {noreply, State};

handle_info(tick, #state{module=Module} = State0) ->
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
    {noreply, State1}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

reconcile_nodes(A, B, From, Module) ->
    Intersection = intersection(A, B),

    if
        A == B ->
            union(A, B);
        length(A) > length(B) andalso Intersection == [] ->
            union(A, [From]);
        length(A) < length(B) andalso Intersection == [] ->
            Module:join(B),
            union(B, [node_name()]);
        length(Intersection) == 1 andalso length(B) > length(A) ->
            Module:join(B -- A),
            union(B, [node_name()]);
        length(Intersection) > 0 ->
            union(A, B);
        A < B ->
            Module:join(B),
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

send_gossip(ToNode, Token, Data, #state{module=Module, nodes=Nodes} = State0) ->
    {CanSend, State1} = can_gossip(State0),
    case CanSend of
        false ->
            {ok, State1};
        true ->
            ?DEBUG_MSG("sending ~p to ~p~n", [Token, ToNode]),
            gen_server:cast({?SERVER(Module), ToNode}, {Token, Nodes, node(), Data}),
            {ok, State1}
    end.

union(L1, L2) ->
    sets:to_list(sets:union(sets:from_list(L1), sets:from_list(L2))).

intersection(A, B) ->
    sets:to_list(sets:intersection(sets:from_list(A), sets:from_list(B))).
