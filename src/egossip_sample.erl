-module(egossip_sample).
-behaviour(gen_server).

%% API
-export([start_link/0,
         set/1]).

%% egossip callbacks
-export([gossip_freq/0,
         digest/0,
         push/2,
         symmetric_push/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    value = 0
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

set(Number) ->
    gen_server:call(?MODULE, {set, Number}).

%%%===================================================================
%%% egossip callbacks
%%%===================================================================

gossip_freq() ->
    {1, 2}.

digest() ->
    gen_server:call(?MODULE, {get, digest}).

push(Msg, _From) ->
    gen_server:call(?MODULE, {push, Msg}).

symmetric_push(Msg, _From) ->
    gen_server:call(?MODULE, {symmetric_push, Msg}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    egossip_sup:start_child(?MODULE),
    {ok, #state{}}.

handle_call({get, digest}, _From, State) ->
    Reply = {ok, State#state.value},
    {reply, Reply, State};

handle_call({set, Number}, _From, State) ->
    {reply, ok, State#state{value=Number}};

handle_call({push, Value}, _From, State) ->
    NewValue = (Value + State#state.value) / 2,
    {reply, {ok, NewValue}, State#state{value=NewValue}};

handle_call({symmetric_push, Value}, _From, State) ->
    NewValue = (Value + State#state.value) / 2,
    {reply, {ok, NewValue}, State#state{value=NewValue}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
