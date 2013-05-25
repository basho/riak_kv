%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test for the sms streaming merge sort module code.
-module(sms_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-behaviour(eqc_statem).
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3]).

-record(state, {
        sms,
        sources :: undefined | [{integer(), [integer()]}],   % [{ClientId, [Num]}]
        received = [] :: [{integer(), [integer()]}] % Sorted received inputs [Num], a call to sms
        }).

-define(NUM_TESTS, 200).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

sms_eqc_test_() ->
    {timeout, 60,
     ?_assert(eqc:quickcheck(eqc:numtests(?NUM_TESTS, ?QC_OUT(?MODULE:sms_prop()))))}.

% Basicaly create one sms with a list of sources,
% then issue commands appending ordered data from those sources
% verifying at each point that it returns data that is safe to consume without
% breaking the merge sort.
sms_prop() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
            {H, S, R} = run_commands(?MODULE, Cmds),
            eqc_gen:with_parameter(show_states, true,
                                   pretty_commands(?MODULE, Cmds,
                                                   {H, S, R}, R == ok))
        end).

initial_state() ->
    #state{}.

gen_inputs(Inputs) ->
    sms:new([Source || {Source, _} <- Inputs]).

add_results(SMS, {Source, Items}, N) ->
    %% Adding some items when source is exhausted is translated to done msg.
    %% Adding zero items is a valid case.
    Arg = case Items == [] andalso N > 0 of
        true -> done;
        false -> lists:sublist(Items, N)
    end,
    sms:add_results(Source, Arg, SMS).

%% Pull merge sort ready results and compare to expected.
sms(SMS, Expected) ->
    {Values, NewSMS} = sms:sms(SMS),
    ?assertEqual(Expected, Values),
    NewSMS.

%% Call done, which should return everything received so far.
done(SMS, Received) ->
    L = lists:sort(lists:flatten([L || {_, L} <- Received])),
    Actual = sms:done(SMS),
    ?assertEqual(L, Actual).

pos_nat() ->
    ?LET(N, nat(), N+1).

%% Generates a list of sources, each with a random list of inputs.
input_gen() ->
    ?LET(NSources, pos_nat(),
         [{Source, non_empty(list(int()))}
          || Source <- lists:seq(1, NSources)]).

%% Initial state, so create sms, generate sources
command(#state{sources=undefined}) ->
    {call, ?MODULE, gen_inputs, [input_gen()]};
%% All sources exhausted, so start again or keep calling sms/done
%% expecting empty results.
command(#state{sms=SMS, received=[]}) ->
    oneof([{call, ?MODULE, gen_inputs, [input_gen()]},
           {call, ?MODULE, sms, [SMS, []]},
           {call, ?MODULE, done, [SMS, []]}]);
%% Normal operation with some received inputs and stuff.
command(#state{sms=SMS, sources=Sources,
               received=Received}) ->
    {Ready, _} = sms_ready(Received, Sources),
    oneof([{call, ?MODULE, sms, [SMS, Ready]},
           {call, ?MODULE, done, [SMS, Received]}]
          ++ [{call, ?MODULE, add_results, [SMS, elements(Sources), nat()]}
              || Sources /= []]
         ).

% Allow only initialization at first
precondition(#state{sms=undefined}, {call, _, gen_inputs, _}) ->
    true;
precondition(#state{sms=undefined}, _) ->
    false;
% Add results needs the source to exist.
precondition(#state{sources=Inputs},
             {call, _, add_results, [_SMS, {Source, _}, _]}) ->
    lists:keymember(Source, 1, Inputs);
% Other than that, it's all good.
precondition(#state{}, _Cmd) ->
    true.

%% We're only failing on eunit's assert macros inside the cmd functions.
postcondition(_S, _Cmd, _R) ->
    true.

%% Mimics the main sort merge operation, gathering all received inputs that can
%% safely be harvested because no overlapping keys could come from the sources.
sms_ready(Received, Sources) ->
    case Received == [] orelse lists:any(fun({_, L}) -> L == [] end, Received) of
        true ->
            % Buffer is missing some sources, so can't merge sort
            {[], Received};
        false ->
            % Find the smallest element common to all inputs to split by it
            MaxReady = lists:min([lists:last(L) || {_, L} <- Received]),
            % Split the values that are ready for merge and would be returned by sms
            Ready = lists:sort(lists:flatten([[E || E <- CL, E =< MaxReady]
                                              || {_, CL} <- Received])),
            % Take out the values that would be returned by sms
            Received2 = [{C, [E || E <- CL, E > MaxReady]} || {C, CL} <- Received],
            NotDone = fun({S,L})-> L /= [] orelse lists:keymember(S, 1, Sources) end,
            Received3 = lists:filter(NotDone, Received2),
            {Ready, Received3}
    end.

%% Capture the sources generated by eqc for gen_inputs and put them in the
%% state. Also, gen_inputs returns the sms object.
next_state(S = #state{}, V, {call, _, gen_inputs, [Inputs]}) ->
    SortedInputs = [{Source, lists:sort(L)} || {Source, L} <- Inputs],
    Received = [{Source, []} || {Source, L} <- Inputs, L /= []],
    S#state{sms=V, sources=SortedInputs, received=Received};
%% Trying to add some results when source is empty gets translated to a
%% source signaling it is done. Remove from sources and also from received
%% if that buffer is empty. Note that by removing from sources we are not
%% calling sms:done more than once, which could be a valid case.
next_state(S = #state{sources=Sources, received=Received},
           V,
           {call, _, add_results, [_, {Source, []}, N]}) when N > 0 ->
    NewSources = lists:keydelete(Source, 1, Sources),
    NewReceived =
    case lists:keyfind(Source, 1, Received) of
        {Source, []} ->
            lists:keydelete(Source, 1, Received);
        _ ->
            Received
    end,
    S#state{sms=V, sources=NewSources, received=NewReceived};
%% Adding some results, so remove those from sources, add to received.
next_state(S = #state{sources=Inputs, received=Received},
           V,
           {call, _, add_results, [_, {Source, Items}, N]}) ->
    ?assertEqual({Source, Items}, lists:keyfind(Source, 1, Inputs)),
    Is = lists:sublist(Items, N),
    NL = lists:nthtail(length(Is), Items),
    NewInputs = lists:keyreplace(Source, 1, Inputs, {Source, NL}),
    NewReceived = orddict:update(Source,
                                 fun(Val) -> Val ++ Is end,
                                 Is,
                                 Received),
    S#state{sources=NewInputs, received=NewReceived, sms=V};
%% Pulling sort merged results and removing from received buffers.
next_state(S = #state{received=Received, sources=Sources}, V, {call, _, sms, _}) ->
    {_, NewReceived} = sms_ready(Received, Sources),
    S#state{sms=V, received=NewReceived};
%% Done has no side effects, it should just return all buffered received values.
next_state(S, _V, {call, _, done,_}) ->
    S.

-endif. % EQC
