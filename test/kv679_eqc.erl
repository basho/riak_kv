%% -------------------------------------------------------------------
%%
%% eqc model/tests for kv679 type bugs
%%
%% Copyright (c) 2017 bet365, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(kv679_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(P1, p1).
-define(REPLICAS, [p2, p3, f1, f2, f3]).

-record(state, {
          pfhead=?P1,
          %% Actor Ids for replicas in the system
          replicas= ?REPLICAS,
          %% grow list of values put with their contexts, to decide
          %% what values should be left at the end
          values=[],
          %% result of last get
          last_get=undefined
         }).

-record(replica, {
          id, %% The replica ID
          data=orddict:new() %% just use an orddict for now
         }).

-define(BUCKET, <<"b">>).
%% The set of keys, we want more than one, but not too many (but start
%% with one while developing)
-define(KEYS, [<<"A">>]).

-define(NUMTESTS, 100).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_test_() ->
    {setup,
     fun() ->
             meck:new(riak_core_bucket),
             meck:expect(riak_core_bucket, get_bucket,
                         fun(_Bucket) -> [dvv_enabled] end)
     end,
     fun(_) ->
             meck:unload(riak_core_bucket)
     end,
     {timeout, 120, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(50, ?QC_OUT(prop_merge()))))}
    }.

run() ->
    run(?NUMTESTS).

run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_merge())).

check() ->
    eqc:check(prop_merge()).

check(File) ->
    {ok, Bytes} = file:read_file(File),
    CE = binary_to_term(Bytes),
    eqc:check(prop_merge(), CE).

setup() ->
    meck:new(riak_core_bucket),
    meck:expect(riak_core_bucket, get_bucket,
                fun(_Bucket) -> [dvv_enabled] end).

teardown() ->
    meck:unload(riak_core_bucket).

initial_state() ->
    #state{}.

%% @doc <i>Optional callback</i>, Invariant, checked for each visited
%% state during test execution.
-spec invariant(S :: eqc_statem:dynamic_state()) -> boolean().
invariant(_S) ->
    %%
    true.

%% ------ Grouped operator: put

put_args(#state{replicas=Replicas, last_get=LastGet}) ->
    [
     elements(Replicas), %% Replica
     growingelements(?KEYS), %% key
     gen_val(), %% new value
     LastGet, %% result of last get operation
     bool() %% use or not use the last get
    ].

%% @doc PUT a value into "riak", maybe without a preceding get (bad
%% client, blind put, it happens!)
put(Replica, Key, Value, LastGet, UseLastGet) ->
    [#replica{id=Replica, data=RepData}=Rep] = ets:lookup(?MODULE, Replica),
    [#replica{id= ?P1, data=Data}=Coord] = ets:lookup(?MODULE, ?P1),

    %% here we just pretend to be riak. We read the local value for
    %% key, and we do what riak does with the incoming contextless
    %% value

    PutObj = generate_put_obj(Key, Value, LastGet, UseLastGet),
    PutCtx = riak_object:vclock(PutObj),

    Res = coord_put(?P1, orddict:find(Key, Data), PutObj),
    {ok, Dot} = vclock:get_dot(?P1, riak_object:vclock(Res)),

    Data2 = orddict:store(Key, Res, Data),

    RepRes = replica_put(Replica, orddict:find(Key, RepData), Res),
    RepData2 = orddict:store(Key, RepRes, RepData),

    ets:insert(?MODULE, Coord#replica{data=Data2}),
    ets:insert(?MODULE, Rep#replica{data=RepData2}),
    {Key, Value, Dot, PutCtx}.

%% @doc put_next - Add the `{K, V, Ctx, Dot}' to the `values' list so we
%% can figure out what the set of acked remaining siblings are at the
%% end of the run
-spec put_next(S :: eqc_statem:symbolic_state(),
               V :: eqc_statem:var(),
               Args :: [term()]) -> eqc_statem:symbolic_state().
put_next(S=#state{values=Values}, Val, [_Replica, _Key, _Value, _LastGet, _UseLastGet]) ->
    S#state{values=[Val | Values]}.

%% %% @doc add_post - Postcondition for put
%% -spec put_post(S, Args, Res) -> true | term()
%%     when S    :: eqc_state:dynamic_state(),
%%          Args :: [term()],
%%          Res  :: term().
%% put_post(_S, [Replica, Key, Value], _) ->
%%     [#replica{id=Replica,
%%               data=Data}] = ets:lookup(?MODULE, Replica),
%%     RO = orddict:fetch(Key, Data),
%%     lists:member(Value, riak_object:get_values(RO)),
%%     %% @TODO renable this when test runs at least
%%     true.

%% ------ Grouped operator: get
get_args(#state{pfhead=PFHead,replicas=Replicas}) ->
    [elements([PFHead] ++ Replicas),
     elements(?KEYS)].

%% @doc get_pre - only get if there are some values put
-spec get_pre(S :: eqc_statem:symbolic_state()) -> boolean().
get_pre(#state{values=Values}) ->
    Values /= [].

%% @TODO make this a random quorum
%% @doc perform a get from Replica,
%% may well be a not_found
get(Replica, Key) ->
    [#replica{id=Replica, data=Data}] = ets:lookup(?MODULE, Replica),

    case orddict:find(Key, Data) of
        error -> undefined;
        {ok, Value} -> Value
    end.


%% @doc get_next - Add read value to state
-spec get_next(S :: eqc_statem:symbolic_state(),
                               V :: eqc_statem:var(),
                               Args :: [term()]) -> eqc_statem:symbolic_state().
get_next(S, Val, [_Replica, _Key]) ->
    S#state{last_get=Val}.

%% ------ Grouped operator: replicate

%% @doc replicate_args - Choose source and target for
%% replication.
replicate_args(#state{replicas=Replicas}) ->
    [
     elements(?KEYS),
     elements([?P1] ++ Replicas),
     elements([?P1] ++ Replicas)
    ].

%% @doc replicate_pre - only replicate if there are some values put
-spec replicate_pre(S :: eqc_statem:symbolic_state()) -> boolean().
replicate_pre(#state{values=Values}) ->
    Values /= [].

%% @doc simulate replication by merging value at `From' with value at
%% `To' and store on `To'
replicate(Key, From, To) ->
    [#replica{id=From, data=FromData}] = ets:lookup(?MODULE, From),
    [#replica{id= To, data=ToData}=ToRep] = ets:lookup(?MODULE, To),

    case orddict:find(Key, FromData) of
        error ->
            ok;
        {ok, Value} ->
            ToRes= replica_put(To, orddict:find(Key, ToData), Value),
            ToData2 = orddict:store(Key, ToRes, ToData),
            ets:insert(?MODULE, ToRep#replica{data=ToData2}),
            ok
    end.

%% ------ Grouped operator: forget - the preflist head forgets
%% sometimes! Losing a node should not lose data in a replicated
%% system like riak

%% @doc forget_args -
forget_args(_S) ->
    [
     ?P1,
     elements(?KEYS)
    ].

%% @doc forget_pre - only forget if there is something to firget
-spec forget_pre(S :: eqc_statem:symbolic_state()) -> boolean().
forget_pre(#state{values=Values}) ->
    Values /= [].

%% @doc simulate forgetting by dropping the key from the data dict
forget(Replica, Key) ->
    [#replica{id=Replica, data=Data}=Rep] = ets:lookup(?MODULE, Replica),
    Data2 = orddict:erase(Key, Data),
    ets:insert(?MODULE, Rep#replica{data=Data2}).

%% @TODO decide on weights
weight(_S, _) ->
    1.

%% @doc check that no writes are lost
-spec prop_merge() -> eqc:property().
prop_merge() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                %% Store the state external to the statem for correct
                %% shrinking. This is best practice.
                (catch ets:delete(?MODULE)),
                ets:new(?MODULE, [public, named_table, set, {keypos, #replica.id}]),
                set_up_replicas(),

                {H, S=#state{values=Values}, Res} = run_commands(?MODULE,Cmds),

                MergedObjects =
                    lists:foldl(fun(#replica{data=Data}, Acc) ->
                                        orddict:merge(fun(_Key, O1, O2) ->
                                                              riak_object:reconcile([O1, O2], true)
                                                      end,
                                                      Data,
                                                      Acc)
                                end,
                                orddict:new(),
                                ets:tab2list(?MODULE)),

                ExpectedSiblingValues = derive_sibling_list(Values),

                ets:delete(?MODULE),

                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                aggregate(
                                  command_names(Cmds),
                                  conjunction([
                                               {result, Res == ok},
                                               {equal, results_equal(MergedObjects, ExpectedSiblingValues)}
                                              ]
                                             )
                                 )
                               )
            end).



%% generators
gen_val() ->
    %% just want unique value per put
    timestamp().

%% Helper functions

set_up_replicas() ->
    [ ets:insert(?MODULE, #replica{id=Id}) || Id <- ?REPLICAS ++ [?P1] ].

%% two dicts come in, a true or a dict comes out, you can't explain
%% that
-spec results_equal(orddict:orddict(), orddict:orddict()) ->
                           true | orddict:orddict().
results_equal(MergedObjects, ExpectedSiblingValues) ->
    MergedKeys = orddict:fetch_keys(MergedObjects),
    ExpectedKeys = orddict:fetch_keys(ExpectedSiblingValues),
    case MergedKeys == ExpectedKeys  of
        false ->
            {false, MergedKeys, ExpectedKeys};
        true ->
            orddict:fold(fun(K, V, Acc) ->
                                 Actual = riak_object:get_dotted_values(V),
                                 Expected = orddict_fetch(K, ExpectedSiblingValues, []),
                                 case lists:sort(Expected) == lists:sort(Actual) of
                                     true ->
                                         true;
                                     false ->
                                         update_equals_acc(Actual, Expected, K, Acc)
                                 end
                         end,
                         true,
                         MergedObjects)
    end.

%% the type of the Acc in results_equal/2 is either true | orddict,
%% this handles updating the accumulator, especially as it changes
%% type
update_equals_acc(Actual, Expected, Key, true) ->
    update_equals_acc(Actual, Expected, Key, orddict:new());
update_equals_acc(Actual, Expected, Key, Acc) ->
    orddict:store(Key, {actual, Actual, expected, Expected}, Acc).


%% given the state gathered list of value tuples, decide which should
%% be present as genuiune siblings
derive_sibling_list(Values) ->
    MergedCtxs = lists:foldl(fun({Key, _Value, _ValueDot, PutCtx}, Acc) ->
                                     orddict:update(Key, fun(Ctx) ->
                                                                 vclock:merge([Ctx, PutCtx])
                                                         end,
                                                    PutCtx,
                                                    Acc)
                             end,
                             orddict:new(),
                             Values),
    %% now traverse again, discarding any value whose dot was dominated by a put
    lists:foldl(fun({Key, Value, ValueDot, _PutCtx}, Acc) ->
                        Ctx = orddict_fetch(Key, MergedCtxs, vclock:fresh()),
                        case vclock:descends_dot(Ctx, ValueDot) of
                            true ->
                                Acc;
                            false ->
                                Res = {ValueDot, Value},
                                orddict:update(Key, fun(Vals) ->
                                                            [Res | Vals]
                                                    end,
                                               [Res],
                                               Acc)
                        end
                end,
                orddict:new(),
                Values).

%% fetch from an orddict with a default if absent
orddict_fetch(Key, Dict, Default) ->
    case orddict:find(Key, Dict) of
        error ->
            Default;
        {ok, Val} ->
            Val
    end.

generate_put_obj(Key, Value, LastGet, UseLastGet) when LastGet == undefined
                                                       orelse
                                                       UseLastGet == false ->
    new_object(Key, Value);
generate_put_obj(_Key, Value, LastGet, true) ->
    O = riak_object:update_value(LastGet, Value),
    O1= riak_object:update_last_modified(O),
    riak_object:apply_updates(O1).

new_object(K, V) ->
    riak_object:new(?BUCKET, K, V).

%% this is a cut down and (vastly!) simplified version of what riak
%% does when coordinating a put (without any of the Epoch stuff for
%% now!)
coord_put(VId, error, IncomingObject) ->
    StartTime = timestamp(),
    riak_object:increment_vclock(IncomingObject, VId, StartTime);
coord_put(VId, {ok, LocalObj}, IncomingObject) ->
    StartTime = timestamp(),
    riak_object:update(false, LocalObj, IncomingObject, VId, StartTime).

%% this is a cut down and (vastly!) simplified version of what riak
%% does when receiving a non-coorindating (replica, handoff, mdc, read
%% repair etc) put (without any of the Epoch stuff for now!)
replica_put(_VId, error, IncomingObject) ->
    IncomingObject;
replica_put(_Vid, {ok, LocalObj}, IncomingObject) ->
    riak_object:syntactic_merge(LocalObj, IncomingObject).

%% like vclock:timestamp() but monotinically increasing
timestamp() ->
    {A, B, C} = erlang:now(),
    A+B+C.

-endif.
