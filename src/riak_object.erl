%% -------------------------------------------------------------------
%%
%% riak_object: container for Riak data and metadata
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc container for Riak data and metadata

-module(riak_object).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include("riak_kv_wm_raw.hrl").

-export_type([riak_object/0, bucket/0, key/0, value/0]).

-type key() :: binary().
-type bucket() :: binary().
-type value() :: term().

-record(r_content, {
          metadata :: dict(),
          value :: term()
         }).

%% Opaque container for Riak objects, a.k.a. riak_object()
-record(r_object, {
          bucket :: bucket(),
          key :: key(),
          contents :: compactdvv:clock(), % [{id, count, [riak_content()]}]
          updatemetadata=dict:store(clean, true, dict:new()) :: dict(),
          updatevalue :: term()
         }).

-opaque riak_object() :: #r_object{}.
-opaque riak_content() :: #r_content{}.

-type index_op() :: add | remove.
-type index_value() :: integer() | binary().

-define(MAX_KEY_SIZE, 65536).

-export([new/3, new/4, ensure_robject/1, equal/2, new_vclock/0, equal_vclock/2]).
-export([increment_vclock/2, increment_vclock/3, update_vclock/4, update_vclock/3]).
-export([reconcile/2, descendant/2, strict_descendant/2, key/1]).
-export([get_metadata/1, get_metadatas/1, get_values/1, get_value/1]).
-export([get_vclock/2, update_value/2, update_metadata/2, bucket/1, value_count/1]).
-export([get_update_metadata/1, get_update_value/1, get_contents/1]).
-export([apply_updates/1, syntactic_merge/2, compare_content_dates/2, set_lww/1]).
-export([to_json/1, from_json/1]).
-export([index_specs/1, diff_index_specs/2]).
-export([set_contents/2, set_vclock/2]). %% INTERNAL, only for riak_*

%% @doc Constructor for new riak objects.
-spec new(Bucket::bucket(), Key::key(), Value::value()) -> riak_object().
new(B, K, V) when is_binary(B), is_binary(K) ->
    new(B, K, V, no_initial_metadata).

%% @doc Constructor for new riak objects with an initial content-type.
-spec new(Bucket::bucket(), Key::key(), Value::value(),
          string() | dict() | no_initial_metadata) -> riak_object().
new(B, K, V, C) when is_binary(B), is_binary(K), is_list(C) ->
    new(B, K, V, dict:from_list([{?MD_CTYPE, C}]));

%% @doc Constructor for new riak objects with an initial metadata dict.
%%
%% NOTE: Removed "is_tuple(MD)" guard to make Dialyzer happy.  The previous clause
%%       has a guard for string(), so this clause is OK without the guard.
new(B, K, V, MD) when is_binary(B), is_binary(K) ->
    case size(K) > ?MAX_KEY_SIZE of
        true ->
            throw({error,key_too_large});
        false ->
            case MD of
                no_initial_metadata ->
                    Contents = compactdvv:new([#r_content{metadata=dict:new(), value=V}]),
                    #r_object{bucket=B,key=K,
                              contents=Contents};
                _ ->
                    Contents = compactdvv:new([#r_content{metadata=MD, value=V}]),
                    #r_object{bucket=B,key=K,updatemetadata=MD,
                              contents=Contents}
            end
    end.

-spec new_vclock() -> compactdvv:clock().
new_vclock() -> compactdvv:new().

%% Ensure the incoming term is a riak_object.
-spec ensure_robject(any()) -> riak_object().
ensure_robject(Obj = #r_object{}) -> Obj.

-spec strict_descendant(riak_object(), riak_object()) -> boolean().
strict_descendant(#r_object{contents=C1},#r_object{contents=C2}) ->
    compactdvv:strict_descendant(C1,C2).

-spec descendant(riak_object(), riak_object()) -> boolean().
descendant(#r_object{contents=C1},#r_object{contents=C2}) ->
    compactdvv:equal(C1,C2) orelse compactdvv:strict_descendant(C1,C2).

-spec equal_vclock(riak_object(), riak_object()) -> boolean().
equal_vclock(#r_object{contents=C1},#r_object{contents=C2}) ->
    compactdvv:equal(C1,C2).

-spec equal(riak_object(), riak_object()) -> boolean().
%% @doc Deep (expensive) comparison of Riak objects.
equal(Obj1,Obj2) ->
    (Obj1#r_object.bucket =:= Obj2#r_object.bucket)
        andalso (Obj1#r_object.key =:= Obj2#r_object.key)
        andalso equal_vclock(Obj1,Obj2)
        andalso equal2(Obj1,Obj2).
equal2(Obj1,Obj2) ->
    UM1 = lists:keysort(1, dict:to_list(Obj1#r_object.updatemetadata)),
    UM2 = lists:keysort(1, dict:to_list(Obj2#r_object.updatemetadata)),
    (UM1 =:= UM2)
        andalso (Obj1#r_object.updatevalue =:= Obj2#r_object.updatevalue)
        andalso begin
                    Cont1 = lists:sort(get_contents(Obj1)),
                    Cont2 = lists:sort(get_contents(Obj2)),
                    equal_contents(Cont1,Cont2)
                end.
equal_contents([],[]) -> true;
equal_contents(_,[]) -> false;
equal_contents([],_) -> false;
equal_contents([{M1,V1}|R1],[{M2,V2}|R2]) ->
    MD1 = lists:keysort(1, dict:to_list(M1)),
    MD2 = lists:keysort(1, dict:to_list(M2)),
    (MD1 =:= MD2)
        andalso (V1 =:= V2)
        andalso equal_contents(R1,R2).



% @doc  Reconcile the object from the client and the object from the server. 
%       If AllowMultiple is true,
%       the riak_object returned may contain multiple values if Objects
%       contains sibling versions (objects that could not be syntactically
%       merged).   If AllowMultiple is false, the riak_object returned will
%       contain the value of the most-recently-updated object, as per the
%       X-Riak-Last-Modified header.
-spec reconcile([riak_object()], boolean()) -> riak_object().
reconcile(RObjs, AllowMultiple) ->
    %% filter clocks with null as id
    AllContents = 
        [lists:filter(fun ({I,_,_}) -> I =/= null end, O#r_object.contents) || O <- RObjs],
    AllContents2 = 
        case AllContents of 
            [] -> [O#r_object.contents || O <- RObjs]; 
            _ -> AllContents 
        end,
    SyncedContents = compactdvv:sync(AllContents2),
    Contents = case AllowMultiple of
                   false -> compactdvv:lww(SyncedContents);
                   true -> SyncedContents
               end,
    HdObj = hd(RObjs),
    HdObj#r_object{contents=Contents,
                   updatemetadata=dict:store(clean, true, dict:new()),
                   updatevalue=undefined}.

-spec syntactic_merge(riak_object(), riak_object()) -> riak_object().
syntactic_merge(CurrentObj, NewObj) ->
    %% Paranoia in case objects were incorrectly stored
    %% with update information.  Vclock is not updated
    %% but since no data is lost the objects will be
    %% fixed if rewritten.
    UCurr = update(CurrentObj),
    UNew = update(NewObj),
    reconcile([UNew, UCurr], true).


-spec set_lww(riak_object()) -> riak_object().
set_lww(Object=#r_object{contents=C}) ->
    set_contents(Object,most_recent_content(C)).

-spec most_recent_content(riak_content()) -> riak_content().
most_recent_content(Contents) ->
    compactdvv:lww(fun riak_object:compare_content_dates/2, Contents).

-spec compare_content_dates(riak_content(), riak_content()) -> boolean().
compare_content_dates(C1,C2) ->
    D1 = dict:fetch(<<"X-Riak-Last-Modified">>, C1#r_content.metadata),
    D2 = dict:fetch(<<"X-Riak-Last-Modified">>, C2#r_content.metadata),
    %% true if C1 was modifed later than C2
    Cmp1 = riak_core_util:compare_dates(D1, D2),
    %% true if C2 was modifed later than C1
    Cmp2 = riak_core_util:compare_dates(D2, D1),
    %% check for deleted objects
    Del1 = dict:is_key(<<"X-Riak-Deleted">>, C1#r_content.metadata),
    Del2 = dict:is_key(<<"X-Riak-Deleted">>, C2#r_content.metadata),

    SameDate = (Cmp1 =:= Cmp2),
    case {SameDate, Del1, Del2} of
        {false, _, _} ->
            Cmp1;
        {true, true, false} ->
            false;
        {true, false, true} ->
            true;
        _ ->
            %% Dates equal and either both present or both deleted, compare
            %% by opaque contents.
            C1 < C2
    end.


-spec is_updated(riak_object()) -> boolean().
is_updated(_Object=#r_object{updatemetadata=M,updatevalue=V}) ->
    case dict:find(clean, M) of
        error -> true;
        {ok,_} ->
            case V of
                undefined -> false;
                _ -> true
            end
    end.

-spec update(riak_object()) -> riak_object().
update(Obj) ->
    case is_updated(Obj) of
        true  -> apply_updates(Obj);
        false -> Obj
    end.


% @doc  Promote pending updates (made with the update_value() and
%       update_metadata() calls) to this riak_object.
-spec apply_updates(riak_object()) -> riak_object().
apply_updates(Object=#r_object{}) ->
    CurrentContents = get_vclock(Object,true),
    UpdatedContents = 
        case Object#r_object.updatevalue of
            undefined ->
                case dict:find(clean, Object#r_object.updatemetadata) of
                    {ok,_} -> CurrentContents; %% no changes in values or metadata
                    error -> 
                        NewMD = dict:erase(clean,Object#r_object.updatemetadata),
                        compactdvv:map_values(
                            fun (R) -> #r_content{metadata=NewMD, value=R#r_content.value} end,
                            CurrentContents)
                end;
            _ ->
                MD = case dict:find(clean, Object#r_object.updatemetadata) of
                        {ok,_} -> 
                            hd(get_metadatas(Object));
                        error -> 
                            dict:erase(clean,Object#r_object.updatemetadata)
                     end,
                VL = Object#r_object.updatevalue,
                NewR = #r_content{metadata=MD,value=VL},
                compactdvv:set_value(CurrentContents,NewR)
        end,
    Object#r_object{contents=UpdatedContents,
                    updatemetadata=dict:store(clean, true, dict:new()),
                    updatevalue=undefined}.

%% @doc Return the containing bucket for this riak_object.
-spec bucket(riak_object()) -> bucket().
bucket(#r_object{bucket=Bucket}) -> Bucket.

%% @doc  Return the key for this riak_object.
-spec key(riak_object()) -> key().
key(#r_object{key=Key}) -> Key.

%% @doc  Return the logical clock for this riak_object.
-spec get_vclock(riak_object(), boolean()) -> compactdvv:clock().
get_vclock(#r_object{contents=Clock}, _WithContents = true) -> Clock;
get_vclock(#r_object{contents=Clock}, _WithContents = false) -> compactdvv:join(Clock).


%% @doc  Return the number of values (siblings) of this riak_object.
-spec value_count(riak_object()) -> non_neg_integer().
value_count(#r_object{contents=Contents}) -> compactdvv:value_count(Contents).

%% @doc  Return the contents (a list of {metadata, value} tuples) for
%%       this riak_object.
-spec get_contents(riak_object()) -> [riak_content()].
get_contents(#r_object{contents=Contents}) -> 
    Rconts = compactdvv:get_values(Contents),
    [{C#r_content.metadata, C#r_content.value} ||
         C <- Rconts].

%% @doc  Assert that this riak_object has no siblings and return its associated
%%       metadata.  This function will fail with a badmatch error if the
%%       object has siblings (value_count() > 1).
-spec get_metadata(riak_object()) -> dict().
get_metadata(#r_object{contents=Contents}) ->
                                                % this blows up intentionally (badmatch) if more than one content value!
    1 = compactdvv:value_count(Contents),
    V = compactdvv:get_last_value(Contents),
    V#r_content.metadata.

%% @doc  Return a list of the metadata values for this riak_object.
-spec get_metadatas(riak_object()) -> [dict()].
get_metadatas(#r_object{contents=C}) ->
    [V#r_content.metadata || V <- compactdvv:get_values(C)].

%% @doc  Return a list of object values for this riak_object.
-spec get_values(riak_object()) -> [value()].
get_values(#r_object{contents=C}) ->
    [V#r_content.value || V <- compactdvv:get_values(C)].

%% @doc  Assert that this riak_object has no siblings and return its associated
%%       value.  This function will fail with a badmatch error if the object
%%       has siblings (value_count() > 1).
-spec get_value(riak_object()) -> value().
get_value(#r_object{contents=C}) ->
                                                % this blows up intentionally (badmatch) if more than one content value!
    1 = compactdvv:value_count(C),
    V = compactdvv:get_last_value(C),
    V#r_content.value.

%% @doc  Set the updated metadata of an object to M.
-spec update_metadata(riak_object(), dict()) -> riak_object().
update_metadata(Object=#r_object{}, M) ->
    Object#r_object{updatemetadata=dict:erase(clean, M)}.

%% @doc  Set the updated value of an object to V
-spec update_value(riak_object(), value()) -> riak_object().
update_value(Object=#r_object{}, V) -> Object#r_object{updatevalue=V}.

%% @doc  Return the updated metadata of this riak_object.
-spec get_update_metadata(riak_object()) -> dict().
get_update_metadata(#r_object{updatemetadata=UM}) -> UM.

%% @doc  Return the updated value of this riak_object.
-spec get_update_value(riak_object()) -> value().
get_update_value(#r_object{updatevalue=UV}) -> UV.

%% @doc  INTERNAL USE ONLY.  Set the vclock of riak_object O to V.
-spec set_vclock(riak_object(), compactdvv:clock()) -> riak_object().
set_vclock(Object=#r_object{contents=Contents}, Clock) -> 
    1 = compactdvv:value_count(Contents),
    V = compactdvv:get_last_value(Contents),
    C = compactdvv:set_value(Clock,V),
    Object#r_object{contents=C}.

%% @doc  INTERNAL USE ONLY.  Set the contents of riak_object 
%%       to the Contents. Normal clients should use the
%%       set_update_[value|metadata]() + apply_updates() method for changing
%%       object contents.
-spec set_contents(riak_object(), riak_content()) -> riak_object().
set_contents(Object=#r_object{}, Contents) ->
    Object#r_object{contents=Contents}.

%% @doc  Increment the entry for Id in O's vclock.
-spec increment_vclock(riak_object(), any(), non_neg_integer()) -> riak_object().
increment_vclock(Object=#r_object{}, Id, _TS) ->
    increment_vclock(Object, Id).

%% @doc  Increment the entry for Id in O's vclock (ignore timestamp since we are not pruning).
-spec increment_vclock(riak_object(), any()) -> riak_object().
increment_vclock(Object=#r_object{contents=Conts}, Id) ->
    C = case Conts of
        [{null,0,[V]}] -> compactdvv:syncupdate([], Id, V);
        _ -> 
            Value = #r_content{} = compactdvv:get_last_value(Conts),
            compactdvv:syncupdate(Conts, Id, Value)
    end,
    set_contents(Object, C).

-spec update_vclock(riak_object(), riak_object(), any()) -> riak_object().
update_vclock(ObjectC=#r_object{contents=ContC}, #r_object{contents=ContR}, Id) ->
    Value = #r_content{} = compactdvv:get_last_value(ContC),
    C = compactdvv:syncupdate(ContC, ContR, Id, Value),
    set_contents(ObjectC, C).

-spec update_vclock(riak_object(), riak_object(), any(), non_neg_integer()) -> riak_object().
update_vclock(ObjectC=#r_object{}, ObjectR=#r_object{}, Id, _TS) ->
    increment_vclock(ObjectC, ObjectR, Id).



%% @doc Prepare a list of index specifications
%% to pass to the backend. This function is for
%% the case where there is no existing object
%% stored for a key and therefore no existing
%% index data.
-spec index_specs(riak_object()) ->
                         [{index_op(), binary(), index_value()}].
index_specs(Obj) ->
    Indexes = index_data(Obj),
    assemble_index_specs(Indexes, add).

%% @doc Prepare a list of index specifications to pass to the
%% backend. If the object passed as the first parameter does not have
%% siblings, the function will assemble specs to replace all of its
%% indexes with the indexes specified in the object passed as the
%% second parameter. If there are siblings only the unique new
%% indexes are added.
-spec diff_index_specs(riak_object(), riak_object()) ->
                              [{index_op(), binary(), index_value()}].
diff_index_specs(Obj, OldObj) ->
    OldIndexes = index_data(OldObj),
    OldIndexSet = ordsets:from_list(OldIndexes),
    AllIndexes = index_data(Obj),
    AllIndexSet = ordsets:from_list(AllIndexes),
    NewIndexSet = ordsets:subtract(AllIndexSet, OldIndexSet),
    RemoveIndexSet =
        ordsets:subtract(OldIndexSet, AllIndexSet),
    NewIndexSpecs =
        assemble_index_specs(ordsets:subtract(NewIndexSet, OldIndexSet),
                             add),
    RemoveIndexSpecs =
        assemble_index_specs(RemoveIndexSet,
                             remove),
    NewIndexSpecs ++ RemoveIndexSpecs.

%% @doc Get a list of {Index, Value} tuples from the
%% metadata of an object.
-spec index_data(riak_object()) -> [{binary(), index_value()}].
index_data(undefined) ->
    [];
index_data(Obj) ->
    MetaDatas = get_metadatas(Obj),
    lists:flatten([dict:fetch(?MD_INDEX, MD)
                   || MD <- MetaDatas,
                      dict:is_key(?MD_INDEX, MD)]).

%% @doc Assemble a list of index specs in the
%% form of triplets of the form
%% {IndexOperation, IndexField, IndexValue}.
-spec assemble_index_specs([{binary(), binary()}], index_op()) ->
                                  [{index_op(), binary(), binary()}].
assemble_index_specs(Indexes, IndexOp) ->
    [{IndexOp, Index, Value} || {Index, Value} <- Indexes].


%% @doc Converts a riak_object into its JSON equivalent
-spec to_json(riak_object()) -> {struct, list(any())}.
to_json(Obj=#r_object{}) ->
    {struct, 
      [
        {<<"bucket">>, bucket(Obj)},
        {<<"key">>, key(Obj)},
        {<<"contents">>,
          [
            {struct,
              [
                {<<"id">>, riak_kv_wm_utils:encode_vclock(Id)},
                {<<"counter">>, riak_kv_wm_utils:encode_vclock(Counter)},
                {<<"values">>,
                  [
                    {struct, 
                      [
                        {<<"metadata">>, jsonify_metadata(Rcont#r_content.metadata)},
                        {<<"value">>, Rcont#r_content.value}
                      ]
                    } || Rcont <- Values
                  ]
                }
              ]
            } || {Id, Counter, Values} <- get_vclock(Obj,true)
          ]
        }
      ]
    }.

-spec from_json(any()) -> riak_object().
from_json({struct, Obj}) ->
    from_json(Obj);
from_json(Obj) ->
    Bucket = proplists:get_value(<<"bucket">>, Obj),
    Key = proplists:get_value(<<"key">>, Obj),
    Contents = proplists:get_value(<<"contents">>, Obj),
    RObj0 = new(Bucket, Key, <<"">>),
    set_contents(RObj0, dejsonify_contents(Contents, [])).

jsonify_metadata(MD) ->
    MDJS = fun({LastMod, Now={_,_,_}}) ->
                   %% convert Now to JS-readable time string
                   {LastMod, list_to_binary(
                               httpd_util:rfc1123_date(
                                 calendar:now_to_local_time(Now)))};
              %% When the user metadata is empty, it should still be a struct
              ({?MD_USERMETA, []}) ->
                   {?MD_USERMETA, {struct, []}};
              ({<<"Links">>, Links}) ->
                   {<<"Links">>, [ [B, K, T] || {{B, K}, T} <- Links ]};
              ({Name, List=[_|_]}) ->
                   {Name, jsonify_metadata_list(List)};
              ({Name, Value}) ->
                   {Name, Value}
           end,
    {struct, lists:map(MDJS, dict:to_list(MD))}.

%% @doc convert strings to binaries, and proplists to JSON objects
jsonify_metadata_list([]) -> [];
jsonify_metadata_list(List) ->
    Classifier = fun({Key,_}, Type) when (is_binary(Key) orelse is_list(Key)),
                                         Type /= array, Type /= string ->
                         struct;
                    (C, Type) when is_integer(C), C >= 0, C =< 256,
                                   Type /= array, Type /= struct ->
                         string;
                    (_, _) ->
                         array
                 end,
    case lists:foldl(Classifier, undefined, List) of
        struct -> {struct, jsonify_proplist(List)};
        string -> list_to_binary(List);
        array -> List
    end.

%% @doc converts a proplist with potentially multiple values for the
%%    same key into a JSON object with single or multi-valued keys.
jsonify_proplist([]) -> [];
jsonify_proplist(List) ->
    dict:to_list(lists:foldl(fun({Key, Value}, Dict) ->
                                     JSONKey = if is_list(Key) -> list_to_binary(Key);
                                                  true -> Key
                                               end,
                                     JSONVal = if is_list(Value) -> jsonify_metadata_list(Value);
                                                  true -> Value
                                               end,
                                     case dict:find(JSONKey, Dict) of
                                         {ok, ListVal} when is_list(ListVal) ->
                                             dict:append(JSONKey, JSONVal, Dict);
                                         {ok, Other} ->
                                             dict:store(JSONKey, [Other,JSONVal], Dict);
                                         _ ->
                                             dict:store(JSONKey, JSONVal, Dict)
                                     end
                             end, dict:new(), List)).


dejsonify_contents([], Accum) ->
    lists:reverse(Accum);
dejsonify_contents([{struct, [
                      {<<"id">>, Id},
                      {<<"counter">>, Counter}, 
                      {<<"values">>, Values}]} | T], Accum) ->
    V = dejsonify_values(Values),
    Id2 = binary_to_term(zlib:unzip(base64:decode(Id))),
    Counter2 = binary_to_term(zlib:unzip(base64:decode(Counter))),
    dejsonify_contents(T, [{Id2,Counter2,V}|Accum]).


dejsonify_values(V) -> dejsonify_values(V,[]).
dejsonify_values([], Accum) ->
    lists:reverse(Accum);
dejsonify_values([{struct, [
                    {<<"metadata">>, {struct, MD0}},
                    {<<"value">>, V}]} | T], Accum) ->
    Converter = fun({Key, Val}) ->
                        case Key of
                            <<"Links">> ->
                                {Key, [{{B, K}, Tag} || [B, K, Tag] <- Val]};
                            <<"X-Riak-Last-Modified">> ->
                                {Key, os:timestamp()};
                            _ ->
                                {Key, if
                                          is_binary(Val) ->
                                              binary_to_list(Val);
                                          true ->
                                              dejsonify_meta_value(Val)
                                      end}
                        end
                end,
    MD = dict:from_list([Converter(KV) || KV <- MD0]),
    dejsonify_values(T, [#r_content{metadata=MD, value=V}|Accum]).

%% @doc convert structs back into proplists
dejsonify_meta_value({struct, PList}) ->
    lists:foldl(fun({Key, List}, Acc) when is_list(List) ->
                        %% This reverses the {k,v},{k,v2} pattern that
                        %% is possible in multi-valued indexes.
                        [{Key, dejsonify_meta_value(L)} || L <- List] ++ Acc;
                    ({Key, V}, Acc) ->
                        [{Key, dejsonify_meta_value(V)}|Acc]
                end, [], PList);
dejsonify_meta_value(Value) -> Value.



-ifdef(TEST).

object_test() ->
    B = <<"buckets_are_binaries">>,
    K = <<"keys are binaries">>,
    V = <<"values are anything">>,
    O = riak_object:new(B,K,V),
    B = riak_object:bucket(O),
    K = riak_object:key(O),
    V = riak_object:get_value(O),
    1 = riak_object:value_count(O),
    1 = length(riak_object:get_values(O)),
    1 = length(riak_object:get_metadatas(O)),
    O.

update_test() ->
    O = object_test(),
    V2 = <<"testvalue2">>,
    O1 = riak_object:update_value(O, V2),
    O2 = riak_object:apply_updates(O1),
    V2 = riak_object:get_value(O2),
    {O,O2}.

reconcile_test() ->
    {O,O2} = update_test(),
    O3 = riak_object:increment_vclock(O2,self()),
    O3 = riak_object:reconcile([O,O3],true),
    O3 = riak_object:reconcile([O,O3],false),
    {O,O3}.

merge1_test() ->
    {O,O3} = reconcile_test(),
    O3 = riak_object:syntactic_merge(O,O3),
    {O,O3}.

merge2_test() ->
    B = <<"buckets_are_binaries">>,
    K = <<"keys are binaries">>,
    V = <<"testvalue2">>,
    O1 = riak_object:increment_vclock(object_test(), node1),
    O2 = riak_object:increment_vclock(riak_object:new(B,K,V), node2),
    O3 = riak_object:syntactic_merge(O1, O2),
    [node1, node2] = lists:sort([N || {N,_,_} <- riak_object:get_vclock(O3,false)]),
    2 = riak_object:value_count(O3).

merge3_test() ->
    O0 = riak_object:new(<<"test">>, <<"test">>, hi),
    O1 = riak_object:increment_vclock(O0, x),
    ?assertEqual(O1, riak_object:syntactic_merge(O1, O1)).

merge4_test() ->
    O0 = riak_object:new(<<"test">>, <<"test">>, hi),
    O1 = riak_object:increment_vclock(
           riak_object:update_value(O0, bye), x),
    OM = riak_object:syntactic_merge(O0, O1),
    ?assertNot(O0 =:= OM),
    ?assertNot(O1 =:= OM), %% vclock should have updated
    ?assertEqual(bye, riak_object:get_value(OM)),
    OMp = riak_object:syntactic_merge(O1, O0),
    ?assertEqual(OM, OMp), %% merge should be symmetric here
    {O0, O1}.

merge5_test() ->
    B = <<"buckets_are_binaries">>,
    K = <<"keys are binaries">>,
    V = <<"testvalue2">>,
    O1 = riak_object:increment_vclock(object_test(), node1),
    O2 = riak_object:increment_vclock(riak_object:new(B,K,V), node2),
    ?assertEqual(riak_object:syntactic_merge(O1, O2),
                 riak_object:syntactic_merge(O2, O1)).

equality1_test() ->
    MD0 = dict:new(),
    MD = dict:store("X-Riak-Test", "value", MD0),
    O1 = riak_object:new(<<"test">>, <<"a">>, "value"),
    O2 = riak_object:new(<<"test">>, <<"a">>, "value"),
    O3 = riak_object:increment_vclock(O1, self()),
    O4 = riak_object:increment_vclock(O2, self()),
    O5 = riak_object:update_metadata(O3, MD),
    O6 = riak_object:update_metadata(O4, MD),
    true = riak_object:equal(O5, O6).

inequality_value_test() ->
    O1 = riak_object:new(<<"test">>, <<"a">>, "value"),
    O2 = riak_object:new(<<"test">>, <<"a">>, "value1"),
    false = riak_object:equal(O1, O2).

inequality_multivalue_test() ->
    O1 = riak_object:new(<<"test">>, <<"a">>, "value"),
    [C] = riak_object:get_vclock(O1,true),
    O1p = riak_object:set_contents(O1, [C,C]),
    false = riak_object:equal(O1, O1p),
    false = riak_object:equal(O1p, O1).

inequality_metadata_test() ->
    O1 = riak_object:new(<<"test">>, <<"a">>, "value"),
    O2 = riak_object:new(<<"test">>, <<"a">>, "value"),
    O1p = riak_object:apply_updates(
            riak_object:update_metadata(
              O1, dict:store(<<"X-Riak-Test">>, "value",
                             riak_object:get_metadata(O1)))),
    false = riak_object:equal(O1p, O2).

inequality_key_test() ->
    O1 = riak_object:new(<<"test">>, <<"a">>, "value"),
    O2 = riak_object:new(<<"test">>, <<"b">>, "value"),
    false = riak_object:equal(O1, O2).

inequality_vclock_test() ->
    O1 = riak_object:new(<<"test">>, <<"a">>, "value"),
    false = riak_object:equal(O1, riak_object:increment_vclock(O1, foo)).

inequality_bucket_test() ->
    O1 = riak_object:new(<<"test1">>, <<"a">>, "value"),
    O2 = riak_object:new(<<"test">>, <<"a">>, "value"),
    false = riak_object:equal(O1, O2).

inequality_updatecontents_test() ->
    MD1 = dict:new(),
    MD2 = dict:store("X-Riak-Test", "value", MD1),
    MD3 = dict:store("X-Riak-Test", "value1", MD1),
    O1 = riak_object:new(<<"test">>, <<"a">>, "value"),
    O2 = riak_object:new(<<"test">>, <<"a">>, "value"),
    O3 = riak_object:update_metadata(O1, MD2),
    false = riak_object:equal(O3, riak_object:update_metadata(O2, MD3)),
    O5 = riak_object:update_value(O1, "value1"),
    false = riak_object:equal(O5, riak_object:update_value(O2, "value2")).

largekey_test() ->
    TooLargeKey = <<0:(65537*8)>>,
    try
        riak_object:new(<<"a">>, TooLargeKey, <<>>)
    catch throw:{error, key_too_large} ->
            ok
    end.

date_reconcile_test() ->
    {O,O3} = reconcile_test(),
    D = calendar:datetime_to_gregorian_seconds(
          httpd_util:convert_request_date(
            httpd_util:rfc1123_date())),
    O2 = apply_updates(
           riak_object:update_metadata(
             increment_vclock(O, date),
             dict:store(
               <<"X-Riak-Last-Modified">>,
               httpd_util:rfc1123_date(
                 calendar:gregorian_seconds_to_datetime(D)),
               get_metadata(O)))),
    O4 = apply_updates(
           riak_object:update_metadata(
             O3,
             dict:store(
               <<"X-Riak-Last-Modified">>,
               httpd_util:rfc1123_date(
                 calendar:gregorian_seconds_to_datetime(D+1)),
               get_metadata(O3)))),
    O5 = riak_object:reconcile([O2,O4], false),
    false = riak_object:equal(O2, O5),
    false = riak_object:equal(O4, O5).

get_update_value_test() ->
    O = riak_object:new(<<"test">>, <<"test">>, old_val),
    NewVal = new_val,
    ?assertEqual(NewVal,
                 riak_object:get_update_value(
                   riak_object:update_value(O, NewVal))).

get_update_metadata_test() ->
    O = riak_object:new(<<"test">>, <<"test">>, val),
    OldMD = riak_object:get_metadata(O),
    NewMD = dict:store(<<"X-Riak-Test">>, "testval", OldMD),
    ?assertNot(NewMD =:= OldMD),
    ?assertEqual(NewMD,
                 riak_object:get_update_metadata(
                   riak_object:update_metadata(O, NewMD))).

is_updated_test() ->
    O = riak_object:new(<<"test">>, <<"test">>, test),
    ?assertNot(is_updated(O)),
    OMu = riak_object:update_metadata(
            O, dict:store(<<"X-Test-Update">>, "testupdate",
                          riak_object:get_metadata(O))),
    ?assert(is_updated(OMu)),
    OVu = riak_object:update_value(O, testupdate),
    ?assert(is_updated(OVu)).

new_with_ctype_test() ->
    O = riak_object:new(<<"b">>, <<"k">>, <<"{\"a\":1}">>, "application/json"),
    ?assertEqual("application/json", dict:fetch(?MD_CTYPE, riak_object:get_metadata(O))).

new_with_md_test() ->
    O = riak_object:new(<<"b">>, <<"k">>, <<"abc">>, dict:from_list([{?MD_CHARSET,"utf8"}])),
    ?assertEqual("utf8", dict:fetch(?MD_CHARSET, riak_object:get_metadata(O))).

jsonify_multivalued_indexes_test() ->
    Indexes = [{<<"test_bin">>, <<"one">>},
               {<<"test_bin">>, <<"two">>},
               {<<"test2_int">>, 4}],
    ?assertEqual({struct, [{<<"test2_int">>,4},{<<"test_bin">>,[<<"one">>,<<"two">>]}]},
                 jsonify_metadata_list(Indexes)).

jsonify_round_trip_test() ->
    Links = [{{<<"b">>,<<"k2">>},<<"tag">>},
             {{<<"b2">>,<<"k2">>},<<"tag2">>}],
    Indexes = [{<<"test_bin">>, <<"one">>},
               {<<"test_bin">>, <<"two">>},
               {<<"test2_int">>, 4}],
    Meta = [{<<"foo">>, <<"bar">>}, {<<"baz">>, <<"quux">>}],
    MD = dict:from_list([{?MD_USERMETA, Meta},
                         {?MD_CTYPE, "application/json"},
                         {?MD_INDEX, Indexes},
                         {?MD_LINKS, Links}]),
    O = riak_object:new(<<"b">>, <<"k">>, <<"{\"a\":1}">>, MD),
    O2 = from_json(to_json(O)),
    ?assertEqual(bucket(O), bucket(O2)),
    ?assertEqual(key(O), key(O2)),
    ?assert(compactdvv:equal(get_vclock(O,true), get_vclock(O2,true))),
    ?assertEqual(lists:sort(Meta), lists:sort(dict:fetch(?MD_USERMETA, get_metadata(O2)))),
    ?assertEqual(Links, dict:fetch(?MD_LINKS, get_metadata(O2))),
    ?assertEqual(lists:sort(Indexes), lists:sort(index_data(O2))),
    ?assertEqual(get_contents(O), get_contents(O2)).

check_most_recent({V1, T1, D1}, {V2, T2, D2}) ->
    MD1 = dict:store(<<"X-Riak-Last-Modified">>, T1, D1),
    MD2 = dict:store(<<"X-Riak-Last-Modified">>, T2, D2),

    O1 = riak_object:new(<<"test">>, <<"a">>, V1, MD1),
    O2 = riak_object:new(<<"test">>, <<"a">>, V2, MD2),

    C1 = hd(O1#r_object.contents),
    C2 = hd(O2#r_object.contents),

    C3 = most_recent_content([C1, C2]),
    C4 = most_recent_content([C2, C1]),

    ?assertEqual(C3, C4),

    (compactdvv:get_last_value(C3))#r_content.value.


determinstic_most_recent_test() ->
    D = calendar:datetime_to_gregorian_seconds(
          httpd_util:convert_request_date(
            httpd_util:rfc1123_date())),

    TNow = httpd_util:rfc1123_date(
             calendar:gregorian_seconds_to_datetime(D)),
    TPast = httpd_util:rfc1123_date(
              calendar:gregorian_seconds_to_datetime(D-1)),

    Available = dict:new(),
    Deleted = dict:store(<<"X-Riak-Deleted">>, true, Available),

    %% Test all cases with equal timestamps
    ?assertEqual(<<"a">>, check_most_recent({<<"a">>, TNow, Available},
                                            {<<"b">>, TNow, Available})),

    ?assertEqual(<<"b">>, check_most_recent({<<"a">>, TNow, Deleted},
                                            {<<"b">>, TNow, Available})),

    ?assertEqual(<<"a">>, check_most_recent({<<"a">>, TNow, Available},
                                            {<<"b">>, TNow, Deleted})),

    ?assertEqual(<<"a">>, check_most_recent({<<"a">>, TNow, Deleted},
                                            {<<"b">>, TNow, Deleted})),

    %% Test all cases with different timestamp
    ?assertEqual(<<"b">>, check_most_recent({<<"a">>, TPast, Available},
                                            {<<"b">>, TNow, Available})),

    ?assertEqual(<<"b">>, check_most_recent({<<"a">>, TPast, Deleted},
                                            {<<"b">>, TNow, Available})),

    ?assertEqual(<<"b">>, check_most_recent({<<"a">>, TPast, Available},
                                            {<<"b">>, TNow, Deleted})),

    ?assertEqual(<<"b">>, check_most_recent({<<"a">>, TPast, Deleted},
                                            {<<"b">>, TNow, Deleted})).

-endif.
