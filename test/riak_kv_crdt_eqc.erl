%% -------------------------------------------------------------------
%%
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_crdt_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_kv_types.hrl").
-include("../src/riak_kv_wm_raw.hrl").

-compile([export_all]).

-define(BUCKET, {<<"type">>, <<"b">>}).
-define(KEY, <<"k">>).

%% What are the properties that riak claims to offer for CRDTs?

%% On read sibling CRDTs will be merged into a single value

%% On downstream merge sibling CRDTs will be merged into a single
%% value Concurrent updates at two different replicas converge as per
%% the rules of the CRDT being updated

%% These two are the same property?

%% What about updates? What do we say about updates? What do we know?
%% Updates are applied to the local data in the order they are
%% received by the vnode, irrespective of the order they were sent.
%% Counter increments will _always_ cause the counter to be
%% incremented by the amount requested, regardless of the amount seen
%% by the user.  Adding an element to a set will always end up with
%% that element being in the set.  Removes come in two flavours, with
%% a context and without. And three types, removes from Sets, Removes
%% fields from a top leve Map, removes for elements in a Map Field
%% (these are really updates)


%% On an update, the system merges all siblings that were present locally
%% Then applies the new operation.
%% If there was no local value there will be single sibling, the empty type
%% If there was a local value there will be at least extra sibling, but maybe more
prop_remove_empty_fallback_no_ctx_fails() ->
    ?FORALL({ROBj, Actor, Type,  Op, Context}, gen_empty(),
            begin
                CRDTOp = ?CRDT_OP{mod=Type, op=Op, ctx=Context},
                Res = riak_kv_crdt:update(ROBj, Actor, CRDTOp),
                ExpectedError = not_present(first_remove_op(Op)),
                ?WHENFAIL(
                   begin
                       io:format("Riak Object ~p~n", [ROBj]),
                       io:format("Operation ~p~n", [Op]),
                       io:format("Context ~p~n", [Context])
                   end,
                collect(Type, equals({error,
                                      {precondition,
                                       {not_present, ExpectedError}}}, Res)))
            end).

gen_empty() ->
    ?LET({Actor, {Type, Op}}, {gen_actor(), gen_remove_op()},
         {gen_empty(Type), Actor, Type,  Op, undefined}).

gen_actor() ->
    binary(20).

gen_op() ->
    ?LET(Type, gen_type(), {Type, gen_op(Type)}).

gen_remove_op() ->
    ?LET(Type, oneof([riak_dt_map, riak_dt_orswot]),
         {Type, ?SUCHTHAT(Op, gen_op(Type), has_remove(Op))}).

gen_type() ->
    oneof([riak_dt_map, riak_dt_orswot, riak_dt_pncounter]).

gen_op(Type) ->
    Type:gen_op().

gen_empty(Mod) ->
    riak_kv_crdt:new(?BUCKET, ?KEY, Mod).

gen_context(riak_dt_pncounter, _Op) ->
    <<>>;
gen_context(Type, Op) ->
    case has_remove(Op) of
        true ->
            %% Half the time get a context that contains what's being removed
            frequency([{5, gen_containing_context(Type, Op)},
                       {5, <<>>}]);
        false ->
            <<>>
    end.

gen_containing_context(Type, Op) ->
    DT = Type:new(),
    {Removes, _Adds} = riak_kv_crdt:split_ops(Op),
    io:format("removes ~p~n", [Removes]),
    NewAdds = removes_to_adds(Removes),
    {ok, Val} = Type:update(NewAdds, <<1, 2, 3, 4>>, DT),
    Val.

has_remove([]) ->
    false;
has_remove(Ops) ->
    case riak_kv_crdt:split_ops(Ops) of
        {{update, []}, {update, _}} ->
            false;
        _ -> true
end.

removes_to_adds({update, Rems}) ->
    {update, removes_to_adds(Rems, [])}.

removes_to_adds([], Acc) ->
    lists:reverse(Acc);
removes_to_adds([{RemOp, Arg} | Rest], Acc) ->
    AddOp = remove_to_add(RemOp),
    removes_to_adds(Rest, [{AddOp, Arg} | Acc]);
removes_to_adds([{update, Field, {update, Arg}} | Rest], Acc) ->
    Adds = removes_to_adds({update, Arg}),
    removes_to_adds(Rest, [{update, Field, Adds} | Acc]);
removes_to_adds([{update, Field, {RemOp, Arg}} | Rest], Acc) ->
    AddOp = remove_to_add(RemOp),
    removes_to_adds(Rest, [{update, Field, {AddOp, Arg}} | Acc]).

remove_to_add(remove) ->
    add;
remove_to_add(remove_all) ->
    add_all.

first_remove_op(Op) ->
    {{update, Rems}, {update, _Adds}} = riak_kv_crdt:split_ops(Op),
    hd(Rems).

not_present({update, [H | _T]}) ->
    not_present(H);
not_present({_Op, Removed}) when is_list(Removed) ->
    hd(Removed);
not_present({_Op, Removed}) ->
    Removed;
not_present({update, _Field, Op}) ->
    not_present(Op).

-endif.
