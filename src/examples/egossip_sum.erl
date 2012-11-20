%% @doc
%% Implements a simple aggregation-based summation protocol.
%% cycles/1 defines how long it takes to converge on the answer.
%% We calculate the sum of the cluster by taking the average of value
%% and multiplying it by the number of nodes in the conversation.
%%
%% Usage:
%%
%%   (a@machine1)> egossip_sum:start_link(25).
%%   (b@machine1)> egossip_sum:start_link(25).
%%   (b@machine1)> net_adm:ping('a@machine1').
%%
%% @end
-module(egossip_sum).
-behaviour(egossip_server).

%% API
-export([start_link/1]).

%% egossip callbacks
-export([init/1,
         gossip_freq/0,
         round_finish/2,
         cycles/1,
         digest/1,
         join/2,
         expire/2,
         symmetric_push/3,
         push/3,
         commit/3]).

-record(state, {
    value = 0
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Number) ->
    egossip_server:start_link(?MODULE, [Number],  aggregate).

%%%===================================================================
%%% egossip callbacks
%%%===================================================================

init([Number]) ->
    {ok, #state{value=Number}}.

% @doc
% Defines how frequently we want to send a gossip message.
% In milliseconds.
% @end
gossip_freq() ->
    1000.

% @doc
% The total number of cycles needed to reach convergence.
% Best to experiment and figure out how many cycles it takes
% your algorithm to reach convergence then assign that number
% @end
cycles(NodeCount) ->
    ceil(math:log(NodeCount * NodeCount)) + 1.

% @doc
% Callback signifiying end of a round
% @end
round_finish(NodeCount, State) ->
    io:format("=== end of round ===~n"),
    io:format(">>> SUM : ~p~n", [State#state.value * NodeCount]),
    {noreply, State}.

% @doc
% First message sent when talking to another node.
% @end
digest(State) ->
    {reply, State#state.value, State}.

% @doc
% Callback giving you another nodes digest
% @end
push(Value, _From, State) ->
    io:format("got push~n"),
    NewValue = (Value + State#state.value) / 2,
    {reply, State#state.value, State#state{value=NewValue}}.

% @doc
% Callback triggered on the node that initiated
% the gossip
% @end
symmetric_push(Value, _From, State) ->
    io:format("got sym push~n"),
    NewValue = (Value + State#state.value) / 2,
    {noreply, State#state{value=NewValue}}.

% @doc
% Doesn't get called in this example
% @end
commit(_, _, State) ->
    {noreply, State}.

% @doc
% Callback triggered when you join a cluster of nodes
% @end
join(Nodelist, State) ->
    io:format("Joined cluster ~p~n", [Nodelist]),
    {noreply, State}.

% @doc
% Callback triggered when a node crashes
% @end
expire(_Node, State) ->
    {noreply, State}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

ceil(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.
