%% Simple epidemic based protocol. Gossips an ever increasing epoch
%% value around the cluster
%%
%% Usage:
%%
%%   (a@machine1)> gen_gossip_epidemic:start_link().
%%   (b@machine1)> gen_gossip_epidemic:start_link().
%%   (b@machine1)> net_adm:ping('a@machine1').
%%
-module(gen_gossip_epidemic).
-behaviour(gen_gossip).

%% api
-export([start_link/0]).

%% gen_gossip callbacks
-export([init/1,
         gossip_freq/1,
         digest/1,
         join/2,
         expire/2,
         handle_gossip/4]).

-record(state, {
    epoch = 0
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_gossip:register_handler(?MODULE, [], epidemic).

%%%===================================================================
%%% gen_gossip callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

% how often do we want to send a message? in milliseconds.
gossip_freq(State) ->
    {reply, 1000, State}.

% defines what we're gossiping
digest(#state{epoch=Epoch0} = State) ->
    Epoch1 = Epoch0 + 1,
    Value = State#state.epoch,
    HandleToken = push,
    io:format("Epoch = ~p~n", [Epoch1]),
    {reply, Value, HandleToken, State#state{epoch=Epoch1}}.

% received a push
handle_gossip(push, Epoch, _From, State) when Epoch >= State#state.epoch ->
    {noreply, State#state{epoch=Epoch}};
handle_gossip(push, _Epoch, _From, State) ->
    {reply, State#state.epoch, _HandleToken = pull, State};

% received a symmetric push
handle_gossip(pull, Epoch, _From, State) ->
    {noreply, State#state{epoch=Epoch}}.

% joined cluster
join(_Nodelist, State) ->
    {noreply, State}.

% node left
expire(_Node, State) ->
    {noreply, State}.
