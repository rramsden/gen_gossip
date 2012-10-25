%% @doc
%% Example gen_sever which implements egossip and calculates
%% a summation across mutliple nodes in a cluster.
%%
%% Usage:
%%
%%   (a@machine1)> egossip_sum:start_link(25).
%%   (b@machine1)> egossip_sum:start_link(25).
%%
%% @end

-module(egossip_sum).
-behaviour(gen_server).

%% API
-export([start_link/1, calculate_sum/0]).

%% egossip callbacks
-export([gossip_freq/0,
         round_finish/1,
         cycles/1,
         digest/0,
         join/1,
         expire/1,
         symmetric_push/2,
         push/2]).

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

start_link(Number) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Number], []).

calculate_sum() ->
    egossip:run(?MODULE).

%%%===================================================================
%%% egossip callbacks
%%%===================================================================

% @doc
% Defines the maximum threshold on messages that can be sent over
% the network. {10,30} = 10 message every 30 seconds
% @end
gossip_freq() ->
    {30, 100}.

% @doc
% The total number of cycles needed to reach convergence.
% Best to experiment and figure out how many cycles it takes
% your algorithm to reach convergence then assign that number
% @end
cycles(NodeCount) ->
    round(math:log(NodeCount * 5) + 1).

% @doc
% Callback signifiying end of a round
% @end
round_finish(NodeCount) ->
    io:format("round finished with ~p nodes~n", [NodeCount]),
    gen_server:call(?MODULE, calculate).

% @doc
% First message sent when talking to another node.
% @end
digest() ->
    io:format("polling digest~n"),
    gen_server:call(?MODULE, {get, digest}).

% @doc
% Callback giving you another nodes digest
% @end
push(Digest, _From) ->
    gen_server:call(?MODULE, {push, Digest}).

% @doc
% Callback triggered on the node that initiated
% the gossip
% @end
symmetric_push(Msg, _From) ->
    gen_server:call(?MODULE, {symmetric_push, Msg}).

% @doc
% Callback triggered when you join a cluster of nodes
% @end
join(Nodelist) ->
    io:format("Joined ~p~n", [Nodelist]).

% @doc
% Callback triggered when a node crashes
% @end
expire(_Node) ->
    do, nothing.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Number]) ->
    egossip:new(?MODULE),
    {ok, #state{value=Number}}.

handle_call({get, digest}, _From, State) ->
    Digest = State#state.value,
    {reply, Digest, State};

handle_call(calculate, _From, State) ->
    io:format("The answer is ~p~n", [State#state.value * (length(nodes()) + 1)]),
    {reply, ok, State};

handle_call({push, Value}, _From, State) ->
    NewValue = (Value + State#state.value) / 2,
    {reply, {ok, State#state.value}, State#state{value=NewValue}};

handle_call({symmetric_push, Value}, _From, State) ->
    NewValue = (Value + State#state.value) / 2,
    {reply, nil, State#state{value=NewValue}}.

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
