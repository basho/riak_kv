%% -------------------------------------------------------------------
%%
%% crdt_statem_eqc: Quickcheck statem test for riak_dt modules
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(crdt_statem_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{vnodes=[], mod_state, vnode_id=0, mod}).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

%% Initialize the state
initial_state() ->
    #state{}.

%% Command generator, S is the state
command(#state{vnodes=VNodes, mod=Mod}) ->
    oneof([{call, ?MODULE, create, [Mod]}] ++
           [{call, ?MODULE, update, [Mod, Mod:gen_op(), elements(VNodes)]} || length(VNodes) > 0] ++ %% If a vnode exists
           [{call, ?MODULE, merge, [Mod, elements(VNodes), elements(VNodes)]} || length(VNodes) > 0] ++
           [{call, ?MODULE, crdt_equals, [Mod, elements(VNodes), elements(VNodes)]} || length(VNodes) > 0]
).

%% Next state transformation, S is the current state
next_state(#state{vnodes=VNodes, mod=Mod, vnode_id=ID, mod_state=Expected0}=S,V,{call,?MODULE,create,_}) ->
    Expected = Mod:update_expected(ID, create, Expected0),
    S#state{vnodes=VNodes++[{ID, V}], vnode_id=ID+1, mod_state=Expected};
next_state(#state{vnodes=VNodes0, mod_state=Expected, mod=Mod}=S,V,{call,?MODULE, update, [Mod, Op, {ID, _C}]}) ->
    VNodes = lists:keyreplace(ID, 1, VNodes0, {ID, V}),
    S#state{vnodes=VNodes, mod_state=Mod:update_expected(ID, Op, Expected)};
next_state(#state{vnodes=VNodes0, mod_state=Expected0, mod=Mod}=S,V,{call,?MODULE, merge, [_Mod, {IDS, _C}=_Source, {ID, _C}=_Dest]}) ->
    VNodes = lists:keyreplace(ID, 1, VNodes0, {ID, V}),
    Expected = Mod:update_expected(ID, {merge, IDS}, Expected0),
    S#state{vnodes=VNodes, mod_state=Expected};
next_state(S, _V, _C) ->
    S.

%% Precondition, checked before command is added to the command sequence
precondition(_S,{call,_,_,_}) ->
    true.

%% Postcondition, checked after command has been evaluated
%% OBS: S is the state before next_state(S,_,<command>)
postcondition(_S,{call,?MODULE, crdt_equals, _},Res) ->
    Res == true;
postcondition(_S,{call,_,_,_},_Res) ->
    true.

prop_converge(InitialValue, NumTests, Mod) ->
    eqc:quickcheck(eqc:numtests(NumTests, ?QC_OUT(prop_converge(InitialValue, Mod)))).

prop_converge(InitialValue, Mod) ->
    ?FORALL(Cmds,commands(?MODULE, #state{mod=Mod, mod_state=InitialValue}),
            begin
                {H,S,Res} = run_commands(?MODULE,Cmds),
                Merged = merge_crdts(Mod, S#state.vnodes),
                MergedVal = Mod:value(Merged),
                ExpectedValue = Mod:eqc_state_value(S#state.mod_state),
                ?WHENFAIL(
                   %% History: ~p\nState: ~p\ H,S,
                   io:format("History: ~p\nState: ~p", [H,S]),
                   conjunction([{res, equals(Res, ok)},
                                {total, equals(sort(MergedVal), sort(ExpectedValue))}]))
            end).

merge_crdts(Mod, []) ->
    Mod:new();
merge_crdts(Mod, [{_ID, Crdt}|Crdts]) ->
    lists:foldl(fun({_ID0, C}, Acc) ->
                        Mod:merge(C, Acc) end,
                Crdt,
                Crdts).

%% Commands
create(Mod) ->
    Mod:new().

update(Mod, Op, {ID, C}) ->
    Mod:update(Op, ID, C).

merge(Mod, {_IDS, CS}, {_IDD, CD}) ->
    Mod:merge(CS, CD).

crdt_equals(Mod, {_IDS, CS}, {_IDD, CD}) ->
    Mod:equal(Mod:merge(CS, CD),
              Mod:merge(CD, CS)).

%% Helpers
%% The orset CRDT returns a list, it has no guarantees about order
%% list equality expects lists in order
sort(L) when is_list(L) ->
    lists:sort(L);
sort(Other) ->
    Other.

-endif. % EQC
