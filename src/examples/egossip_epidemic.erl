%% Simple epidemic based protocol. Gossips an ever increasing epoch
%% value around the cluster
%%
%% Usage:
%%
%%   (a@machine1)> egossip_epidemic:start_link().
%%   (b@machine1)> egossip_epidemic:start_link().
%%   (b@machine1)> net_adm:ping('a@machine1').
%%
-module(egossip_epidemic).
-behaviour(egossip_server).

%% api
-export([start_link/0]).

%% egossip callbacks
-export([init/1,
         gossip_freq/0,
         digest/1,
         join/2,
         expire/2,
         handle_pull/3,
         handle_push/3,
         handle_commit/3]).

-record(state, {
    epoch = 0
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    egossip_server:register_handler(?MODULE, [], epidemic).

%%%===================================================================
%%% egossip callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

% how often do we want to send a message? in milliseconds.
gossip_freq() ->
    1000.

% defines what we're gossiping
digest(#state{epoch=Epoch0} = State) ->
    Epoch1 = Epoch0 + 1,
    io:format("Epoch = ~p~n", [Epoch1]),
    {reply, State#state.epoch, State#state{epoch=Epoch1}}.

% received a push
handle_push(Epoch, _From, State) when Epoch >= State#state.epoch ->
    {noreply, State#state{epoch=Epoch}};
handle_push(_Epoch, _From, State) ->
    {reply, State#state.epoch, State}.

% received a symmetric push
handle_pull(Epoch, _From, State) ->
    {noreply, State#state{epoch=Epoch}}.

% received a commit
handle_commit(_, _, State) ->
    {noreply, State}.

% joined cluster
join(_Nodelist, State) ->
    {noreply, State}.

% node left
expire(_Node, State) ->
    {noreply, State}.
