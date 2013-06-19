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

-export_type([riak_object/0, bucket/0, key/0, value/0, binary_version/0]).

-type key() :: binary().
-type bucket() :: binary().
%% -type bkey() :: {bucket(), key()}.
-type value() :: term().

-record(r_content, {
          metadata :: dict(),
          value :: term()
         }).

%% Opaque container for Riak objects, a.k.a. riak_object()
-record(r_object, {
          bucket :: bucket(),
          key :: key(),
          contents :: [#r_content{}],
          vclock = vclock:fresh() :: vclock:vclock(),
          updatemetadata=dict:store(clean, true, dict:new()) :: dict(),
          updatevalue :: term()
         }).
-opaque riak_object() :: #r_object{}.

-type index_op() :: add | remove.
-type index_value() :: integer() | binary().
-type binary_version() :: v0 | v1.

-define(MAX_KEY_SIZE, 65536).

-define(LASTMOD_LEN, 29). %% static length of rfc1123_date() type. Hard-coded in Erlang.
-define(V1_VERS, 1).
-define(MAGIC, 53).      %% Magic number, as opposed to 131 for Erlang term-to-binary magic
                         %% Shanley's(11) + Joe's(42)
-define(EMPTY_VTAG_BIN, <<"e">>).

-export([new/3, new/4, ensure_robject/1, ancestors/1, reconcile/2, equal/2]).
-export([increment_vclock/2, increment_vclock/3]).
-export([key/1, get_metadata/1, get_metadatas/1, get_values/1, get_value/1]).
-export([hash/1, approximate_size/2]).
-export([vclock_encoding_method/0, vclock/1, vclock_header/1, encode_vclock/1, decode_vclock/1]).
-export([encode_vclock/2, decode_vclock/2]).
-export([update_value/2, update_metadata/2, bucket/1, value_count/1]).
-export([get_update_metadata/1, get_update_value/1, get_contents/1]).
-export([merge/2, apply_updates/1, syntactic_merge/2]).
-export([to_json/1, from_json/1]).
-export([index_specs/1, diff_index_specs/2]).
-export([to_binary/2, from_binary/3, to_binary_version/4, binary_version/1]).
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
                    Contents = [#r_content{metadata=dict:new(), value=V}],
                    #r_object{bucket=B,key=K,
                              contents=Contents,vclock=vclock:fresh()};
                _ ->
                    Contents = [#r_content{metadata=MD, value=V}],
                    #r_object{bucket=B,key=K,updatemetadata=MD,
                              contents=Contents,vclock=vclock:fresh()}
            end
    end.

%% @doc Ensure the incoming term is a riak_object.
-spec ensure_robject(any()) -> riak_object().
ensure_robject(Obj = #r_object{}) -> Obj.

%% @doc Deep (expensive) comparison of Riak objects.
-spec equal(riak_object(), riak_object()) -> true | false.
equal(Obj1,Obj2) ->
    (Obj1#r_object.bucket =:= Obj2#r_object.bucket)
        andalso (Obj1#r_object.key =:= Obj2#r_object.key)
        andalso vclock:equal(vclock(Obj1),vclock(Obj2))
        andalso equal2(Obj1,Obj2).
equal2(Obj1,Obj2) ->
    UM1 = lists:keysort(1, dict:to_list(Obj1#r_object.updatemetadata)),
    UM2 = lists:keysort(1, dict:to_list(Obj2#r_object.updatemetadata)),
    (UM1 =:= UM2)
        andalso (Obj1#r_object.updatevalue =:= Obj2#r_object.updatevalue)
        andalso begin
                    Cont1 = lists:sort(Obj1#r_object.contents),
                    Cont2 = lists:sort(Obj2#r_object.contents),
                    equal_contents(Cont1,Cont2)
                end.
equal_contents([],[]) -> true;
equal_contents(_,[]) -> false;
equal_contents([],_) -> false;
equal_contents([C1|R1],[C2|R2]) ->
    MD1 = lists:keysort(1, dict:to_list(C1#r_content.metadata)),
    MD2 = lists:keysort(1, dict:to_list(C2#r_content.metadata)),
    (MD1 =:= MD2)
        andalso (C1#r_content.value =:= C2#r_content.value)
        andalso equal_contents(R1,R2).


%% @doc  Given a list of riak_object()s, return the objects that are pure
%%       ancestors of other objects in the list, if any.  The changes in the
%%       objects returned by this function are guaranteed to be reflected in
%%       the other objects in Objects, and can safely be discarded from the list
%%       without losing data.
-spec ancestors([riak_object()]) -> [riak_object()].
ancestors(pure_baloney_to_fool_dialyzer) ->
    [#r_object{vclock = vclock:fresh()}];
ancestors(Objects) ->
    ToRemove = [[O2 || O2 <- Objects,
                       vclock:descends(O1#r_object.vclock,O2#r_object.vclock),
                       (vclock:descends(O2#r_object.vclock,O1#r_object.vclock) == false)]
                || O1 <- Objects],
    lists:flatten(ToRemove).

%% @doc  Reconcile a list of riak objects.  If AllowMultiple is true,
%%       the riak_object returned may contain multiple values if Objects
%%       contains sibling versions (objects that could not be syntactically
%%       merged).   If AllowMultiple is false, the riak_object returned will
%%       contain the value of the most-recently-updated object, as per the
%%       X-Riak-Last-Modified header.
-spec reconcile([riak_object()], boolean()) -> riak_object().
reconcile(Objects, AllowMultiple) ->
    RObjs = reconcile(Objects),
    AllContents = lists:flatten([O#r_object.contents || O <- RObjs]),
    Contents = case AllowMultiple of
                   false ->
                       [most_recent_content(AllContents)];
                   true ->
                       lists:usort(AllContents)
               end,
    VClock = vclock:merge([O#r_object.vclock || O <- RObjs]),
    HdObj = hd(RObjs),
    HdObj#r_object{contents=Contents,vclock=VClock,
                   updatemetadata=dict:store(clean, true, dict:new()),
                   updatevalue=undefined}.

-spec reconcile([riak_object()]) -> [riak_object()].
reconcile(Objects) ->
    All = sets:from_list(Objects),
    Del = sets:from_list(ancestors(Objects)),
    remove_duplicate_objects(sets:to_list(sets:subtract(All, Del))).

remove_duplicate_objects(Os) -> rem_dup_objs(Os,[]).
rem_dup_objs([],Acc) -> Acc;
rem_dup_objs([O|Rest],Acc) ->
    EqO = [AO || AO <- Acc, riak_object:equal(AO,O) =:= true],
    case EqO of
        [] -> rem_dup_objs(Rest,[O|Acc]);
        _ -> rem_dup_objs(Rest,Acc)
    end.

most_recent_content(AllContents) ->
    hd(lists:sort(fun compare_content_dates/2, AllContents)).

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

%% @doc  Merge the contents and vclocks of OldObject and NewObject.
%%       Note:  This function calls apply_updates on NewObject.
-spec merge(riak_object(), riak_object()) -> riak_object().
merge(OldObject, NewObject) ->
    NewObj1 = apply_updates(NewObject),
    OldObject#r_object{contents=lists:umerge(lists:usort(NewObject#r_object.contents),
                                             lists:usort(OldObject#r_object.contents)),
                       vclock=vclock:merge([OldObject#r_object.vclock,
                                            NewObj1#r_object.vclock]),
                       updatemetadata=dict:store(clean, true, dict:new()),
                       updatevalue=undefined}.

%% @doc  Promote pending updates (made with the update_value() and
%%       update_metadata() calls) to this riak_object.
-spec apply_updates(riak_object()) -> riak_object().
apply_updates(Object=#r_object{}) ->
    VL = case Object#r_object.updatevalue of
             undefined ->
                 [C#r_content.value || C <- Object#r_object.contents];
             _ ->
                 [Object#r_object.updatevalue]
         end,
    MD = case dict:find(clean, Object#r_object.updatemetadata) of
             {ok,_} ->
                 MDs = [C#r_content.metadata || C <- Object#r_object.contents],
                 case Object#r_object.updatevalue of
                     undefined -> MDs;
                     _ -> [hd(MDs)]
                 end;
             error ->
                 [dict:erase(clean,Object#r_object.updatemetadata) || _X <- VL]
         end,
    Contents = [#r_content{metadata=M,value=V} || {M,V} <- lists:zip(MD, VL)],
    Object#r_object{contents=Contents,
                    updatemetadata=dict:store(clean, true, dict:new()),
                    updatevalue=undefined}.

%% @doc Return the containing bucket for this riak_object.
-spec bucket(riak_object()) -> bucket().
bucket(#r_object{bucket=Bucket}) -> Bucket.

%% @doc  Return the key for this riak_object.
-spec key(riak_object()) -> key().
key(#r_object{key=Key}) -> Key.

%% @doc  Return the vector clock for this riak_object.
-spec vclock(riak_object()) -> vclock:vclock().
vclock(#r_object{vclock=VClock}) -> VClock.

%% @doc  Return the number of values (siblings) of this riak_object.
-spec value_count(riak_object()) -> non_neg_integer().
value_count(#r_object{contents=Contents}) -> length(Contents).

%% @doc  Return the contents (a list of {metadata, value} tuples) for
%%       this riak_object.
-spec get_contents(riak_object()) -> [{dict(), value()}].
get_contents(#r_object{contents=Contents}) ->
    [{Content#r_content.metadata, Content#r_content.value} ||
        Content <- Contents].

%% @doc  Assert that this riak_object has no siblings and return its associated
%%       metadata.  This function will fail with a badmatch error if the
%%       object has siblings (value_count() > 1).
-spec get_metadata(riak_object()) -> dict().
get_metadata(O=#r_object{}) ->
    % this blows up intentionally (badmatch) if more than one content value!
    [{Metadata,_V}] = get_contents(O),
    Metadata.

%% @doc  Return a list of the metadata values for this riak_object.
-spec get_metadatas(riak_object()) -> [dict()].
get_metadatas(#r_object{contents=Contents}) ->
    [Content#r_content.metadata || Content <- Contents].

%% @doc  Return a list of object values for this riak_object.
-spec get_values(riak_object()) -> [value()].
get_values(#r_object{contents=C}) -> [Content#r_content.value || Content <- C].

%% @doc  Assert that this riak_object has no siblings and return its associated
%%       value.  This function will fail with a badmatch error if the object
%%       has siblings (value_count() > 1).
-spec get_value(riak_object()) -> value().
get_value(Object=#r_object{}) ->
    % this blows up intentionally (badmatch) if more than one content value!
    [{_M,Value}] = get_contents(Object),
    Value.

%% @doc calculates the hash of a riak object
-spec hash(riak_object()) -> integer().
hash(Obj=#r_object{}) ->
    Vclock = vclock(Obj),
    UpdObj = riak_object:set_vclock(Obj, lists:sort(Vclock)),
    erlang:phash2(to_binary(v0, UpdObj)).

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
-spec set_vclock(riak_object(), vclock:vclock()) -> riak_object().
set_vclock(Object=#r_object{}, VClock) -> Object#r_object{vclock=VClock}.

%% @doc  Increment the entry for ClientId in O's vclock.
-spec increment_vclock(riak_object(), vclock:vclock_node()) -> riak_object().
%% @spec increment_vclock(riak_object(), vclock:vclock_node()) -> riak_object()
increment_vclock(Object=#r_object{}, ClientId) ->
    Object#r_object{vclock=vclock:increment(ClientId, Object#r_object.vclock)}.

%% @doc  Increment the entry for ClientId in O's vclock.
-spec increment_vclock(riak_object(), vclock:vclock_node(), vclock:timestamp()) -> riak_object().
increment_vclock(Object=#r_object{}, ClientId, Timestamp) ->
    Object#r_object{vclock=vclock:increment(ClientId, Timestamp, Object#r_object.vclock)}.

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

%% @doc  INTERNAL USE ONLY.  Set the contents of riak_object to the
%%       {Metadata, Value} pairs in MVs. Normal clients should use the
%%       set_update_[value|metadata]() + apply_updates() method for changing
%%       object contents.
-spec set_contents(riak_object(), [{dict(), value()}]) -> riak_object().
set_contents(Object=#r_object{}, MVs) when is_list(MVs) ->
    Object#r_object{contents=[#r_content{metadata=M,value=V} || {M, V} <- MVs]}.

%% @spec vclock_header(riak_object()) -> {Name::string(), Value::string()}
%% @doc Transform the Erlang representation of the document's vclock
%%      into something suitable for an HTTP header
vclock_header(Doc) ->
    VClock = riak_object:vclock(Doc),
    EncodedVClock = binary_to_list(base64:encode(encode_vclock(VClock))),
    {?HEAD_VCLOCK, EncodedVClock}.

%% @spec to_json(riak_object()) -> {struct, list(any())}
%% @doc Converts a riak_object into its JSON equivalent
-spec to_json(riak_object()) -> {struct, list(any())}.
to_json(Obj=#r_object{}) ->
    {_,Vclock} = vclock_header(Obj),
    {struct, [{<<"bucket">>, riak_object:bucket(Obj)},
              {<<"key">>, riak_object:key(Obj)},
              {<<"vclock">>, list_to_binary(Vclock)},
              {<<"values">>,
               [{struct,
                 [{<<"metadata">>, jsonify_metadata(MD)},
                  {<<"data">>, V}]}
                || {MD, V} <- riak_object:get_contents(Obj)
               ]}]}.

-spec from_json(any()) -> riak_object().
from_json({struct, Obj}) ->
    from_json(Obj);
from_json(Obj) ->
    Bucket = proplists:get_value(<<"bucket">>, Obj),
    Key = proplists:get_value(<<"key">>, Obj),
    VClock0 = proplists:get_value(<<"vclock">>, Obj),
    VClock = decode_vclock(base64:decode(VClock0)),
    [{struct, Values}] = proplists:get_value(<<"values">>, Obj),
    RObj0 = riak_object:new(Bucket, Key, <<"">>),
    RObj1 = riak_object:set_vclock(RObj0, VClock),
    riak_object:set_contents(RObj1, dejsonify_values(Values, [])).

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

dejsonify_values([], Accum) ->
    lists:reverse(Accum);
dejsonify_values([{<<"metadata">>, {struct, MD0}},
                  {<<"data">>, D}|T], Accum) ->
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
    dejsonify_values(T, [{MD, D}|Accum]).

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

is_updated(_Object=#r_object{updatemetadata=M,updatevalue=V}) ->
    case dict:find(clean, M) of
        error -> true;
        {ok,_} ->
            case V of
                undefined -> false;
                _ -> true
            end
    end.


-spec syntactic_merge(riak_object(), riak_object()) -> riak_object().
syntactic_merge(CurrentObject, NewObject) ->
    %% Paranoia in case objects were incorrectly stored
    %% with update information.  Vclock is not updated
    %% but since no data is lost the objects will be
    %% fixed if rewritten.
    UpdatedNew = case is_updated(NewObject) of
                     true  -> apply_updates(NewObject);
                     false -> NewObject
                 end,
    UpdatedCurr = case is_updated(CurrentObject) of
                      true  -> apply_updates(CurrentObject);
                      false -> CurrentObject
                  end,

    case ancestors([UpdatedCurr, UpdatedNew]) of
        [] -> merge(UpdatedCurr, UpdatedNew);
        [Ancestor] ->
            case equal(Ancestor, UpdatedCurr) of
                true  -> UpdatedNew;
                false -> UpdatedCurr
            end
    end.

%% @doc Get an approximation of object size by adding together the bucket, key,
%% vectorclock, and all of the siblings. This is more complex than
%% calling term_to_binary/1, but it should be easier on memory,
%% especially for objects with large values.
-spec approximate_size(binary_version(), riak_object()) -> integer().
approximate_size(Vsn, Obj=#r_object{bucket=Bucket,key=Key,vclock=VClock}) ->
    Contents = get_contents(Obj),
    size(Bucket) + size(Key) + size(term_to_binary(VClock)) + contents_size(Vsn, Contents).

contents_size(Vsn, Contents) ->
    lists:sum([metadata_size(Vsn, MD) + value_size(Val) || {MD, Val} <- Contents]).

metadata_size(v0, MD) ->
    size(term_to_binary(MD));
metadata_size(v1, MD) ->
    size(meta_bin(MD)).

value_size(Value) when is_binary(Value) -> size(Value);
value_size(Value) -> size(term_to_binary(Value)).

%% @doc Convert riak object to binary form
-spec to_binary(binary_version(), riak_object()) -> binary().
to_binary(v0, RObj) ->
    term_to_binary(RObj);
to_binary(v1, #r_object{contents=Contents, vclock=VClock}) ->
    new_v1(VClock, Contents).

%% @doc convert a binary encoded riak object to a different
%% encoding version. If the binary is already in the desired
%% encoding this is a no-op
-spec to_binary_version(binary_version(), bucket(), key(), binary()) -> binary().
to_binary_version(v0, _, _, <<131,_/binary>>=Bin) ->
    Bin;
to_binary_version(v1, _, _, <<?MAGIC:8/integer, 1:8/integer, _/binary>>=Bin) ->
    Bin;
to_binary_version(Vsn, B, K, Bin) when is_binary(Bin) ->
    to_binary(Vsn, from_binary(B, K, Bin));
to_binary_version(Vsn, _B, _K, Obj = #r_object{}) ->
    to_binary(Vsn, Obj).

%% @doc return the binary version the riak object binary is encoded in
-spec binary_version(binary()) -> binary_version().
binary_version(<<131,_/binary>>) -> v0;
binary_version(<<?MAGIC:8/integer, 1:8/integer, _/binary>>) -> v1.

%% @doc Convert binary object to riak object
-spec from_binary(bucket(),key(),binary()) -> riak_object().
from_binary(_B,_K,<<131, _Rest/binary>>=ObjTerm) ->
    binary_to_term(ObjTerm);
from_binary(B,K,<<?MAGIC:8/integer, 1:8/integer, Rest/binary>>=_ObjBin) ->
    %% Version 1 of binary riak object
    case Rest of
        <<VclockLen:32/integer, VclockBin:VclockLen/binary, SibCount:32/integer, SibsBin/binary>> ->
            Vclock = binary_to_term(VclockBin),
            Contents = sibs_of_binary(SibCount, SibsBin),
            #r_object{bucket=B,key=K,contents=Contents,vclock=Vclock};
        _Other ->
            {error, bad_object_format}
    end;
from_binary(_B, _K, Obj = #r_object{}) ->
    Obj.

sibs_of_binary(Count,SibsBin) ->
    sibs_of_binary(Count, SibsBin, []).

sibs_of_binary(0, <<>>, Result) -> lists:reverse(Result);
sibs_of_binary(0, _NotEmpty, _Result) ->
    {error, corrupt_contents};
sibs_of_binary(Count, SibsBin, Result) ->
    {Sib, SibsRest} = sib_of_binary(SibsBin),
    sibs_of_binary(Count-1, SibsRest, [Sib | Result]).

sib_of_binary(<<ValLen:32/integer, ValBin:ValLen/binary, MetaLen:32/integer, MetaBin:MetaLen/binary, Rest/binary>>) ->
    <<LMMega:32/integer, LMSecs:32/integer, LMMicro:32/integer, VTagLen:8/integer, VTag:VTagLen/binary, Deleted:1/binary-unit:8, MetaRestBin/binary>> = MetaBin,

    MDList0 = deleted_meta(Deleted, []),
    MDList1 = last_mod_meta({LMMega, LMSecs, LMMicro}, MDList0),
    MDList2 = vtag_meta(VTag, MDList1),
    MDList = meta_of_binary(MetaRestBin, MDList2),
    MD = dict:from_list(MDList),
    {#r_content{metadata=MD, value=decode_maybe_binary(ValBin)}, Rest}.

deleted_meta(<<1>>, MDList) ->
    [{?MD_DELETED, "true"} | MDList];
deleted_meta(_, MDList) ->
    MDList.

last_mod_meta({0, 0, 0}, MDList) ->
    MDList;
last_mod_meta(LM, MDList) ->
    [{?MD_LASTMOD, LM} | MDList].

vtag_meta(?EMPTY_VTAG_BIN, MDList) ->
    MDList;
vtag_meta(VTag, MDList) ->
    [{?MD_VTAG, binary_to_list(VTag)} | MDList].

meta_of_binary(<<>>, Acc) ->
    Acc;
meta_of_binary(<<KeyLen:32/integer, KeyBin:KeyLen/binary, ValueLen:32/integer, ValueBin:ValueLen/binary, Rest/binary>>, ResultList) ->
    Key = decode_maybe_binary(KeyBin),
    Value = decode_maybe_binary(ValueBin),
    meta_of_binary(Rest, [{Key, Value} | ResultList]).

%% V1 Riak Object Binary Encoding
%% -type binobj_header()     :: <<53:8, Version:8, VClockLen:32, VClockBin/binary,
%%                                SibCount:32>>.
%% -type binobj_flags()      :: <<Deleted:1, 0:7/bitstring>>.
%% -type binobj_umeta_pair() :: <<KeyLen:32, Key/binary, ValueLen:32, Value/binary>>.
%% -type binobj_meta()       :: <<LastMod:LastModLen, VTag:128, binobj_flags(),
%%                                [binobj_umeta_pair()]>>.
%% -type binobj_value()      :: <<ValueLen:32, ValueBin/binary, MetaLen:32,
%%                                [binobj_meta()]>>.
%% -type binobj()            :: <<binobj_header(), [binobj_value()]>>.
new_v1(Vclock, Siblings) ->
    VclockBin = term_to_binary(Vclock),
    VclockLen = byte_size(VclockBin),
    SibCount = length(Siblings),
    SibsBin = bin_contents(Siblings),
    <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer, VclockBin/binary, SibCount:32/integer, SibsBin/binary>>.

bin_content(#r_content{metadata=Meta, value=Val}) ->
    ValBin = encode_maybe_binary(Val),
    ValLen = byte_size(ValBin),
    MetaBin = meta_bin(Meta),
    MetaLen = byte_size(MetaBin),
    <<ValLen:32/integer, ValBin:ValLen/binary, MetaLen:32/integer, MetaBin:MetaLen/binary>>.

bin_contents(Contents) ->
    F = fun(Content, Acc) ->
                <<Acc/binary, (bin_content(Content))/binary>>
        end,
    lists:foldl(F, <<>>, Contents).

meta_bin(MD) ->
    {{VTagVal, Deleted, LastModVal}, RestBin} = dict:fold(fun fold_meta_to_bin/3,
                                                          {{undefined, <<0>>, undefined}, <<>>},
                                                          MD),
    VTagBin = case VTagVal of
                  undefined ->  ?EMPTY_VTAG_BIN;
                  _ -> list_to_binary(VTagVal)
              end,
    VTagLen = byte_size(VTagBin),
    LastModBin = case LastModVal of
                     undefined -> <<0:32/integer, 0:32/integer, 0:32/integer>>;
                     {Mega,Secs,Micro} -> <<Mega:32/integer, Secs:32/integer, Micro:32/integer>>
                 end,
    <<LastModBin/binary, VTagLen:8/integer, VTagBin:VTagLen/binary,
      Deleted:1/binary-unit:8, RestBin/binary>>.

fold_meta_to_bin(?MD_VTAG, Value, {{_Vt,Del,Lm},RestBin}) ->
    {{Value, Del, Lm}, RestBin};
fold_meta_to_bin(?MD_LASTMOD, Value, {{Vt,Del,_Lm},RestBin}) ->
     {{Vt, Del, Value}, RestBin};
fold_meta_to_bin(?MD_DELETED, true, {{Vt,_Del,Lm},RestBin})->
     {{Vt, <<1>>, Lm}, RestBin};
fold_meta_to_bin(?MD_DELETED, "true", Acc) ->
    fold_meta_to_bin(?MD_DELETED, true, Acc);
fold_meta_to_bin(?MD_DELETED, _, {{Vt,_Del,Lm},RestBin}) ->
    {{Vt, <<0>>, Lm}, RestBin};
fold_meta_to_bin(Key, Value, {{_Vt,_Del,_Lm}=Elems,RestBin}) ->
    ValueBin = encode_maybe_binary(Value),
    ValueLen = byte_size(ValueBin),
    KeyBin = encode_maybe_binary(Key),
    KeyLen = byte_size(KeyBin),
    MetaBin = <<KeyLen:32/integer, KeyBin/binary, ValueLen:32/integer, ValueBin/binary>>,
    {Elems, <<RestBin/binary, MetaBin/binary>>}.

encode_maybe_binary(Bin) when is_binary(Bin) ->
    <<1, Bin/binary>>;
encode_maybe_binary(Bin) ->
    <<0, (term_to_binary(Bin))/binary>>.

decode_maybe_binary(<<1, Bin/binary>>) ->
    Bin;
decode_maybe_binary(<<0, Bin/binary>>) ->
    binary_to_term(Bin).

%%
%% Helpers for managing vector clock encoding and related capability:
%%

%% Fetch the preferred vclock encoding method:
-spec vclock_encoding_method() -> atom().
vclock_encoding_method() ->
    riak_core_capability:get({riak_kv, vclock_data_encoding}, encode_zlib).

%% Encode a vclock in accordance with our capability setting:
encode_vclock(VClock) ->
    encode_vclock(vclock_encoding_method(), VClock).

-spec encode_vclock(atom(), VClock :: vclock:vclock()) -> binary().
encode_vclock(Method, VClock) ->
    case Method of
        %% zlib legacy support: we don't return standard encoding with metadata:
        encode_zlib -> zlib:zip(term_to_binary(VClock));
        _ -> embed_encoding_method(Method, VClock)
    end.

%% Return a vclock in a consistent format:
embed_encoding_method(Method, EncodedVClock) ->
    term_to_binary({ Method, EncodedVClock }).

-spec decode_vclock(binary()) -> vclock:vclock().
decode_vclock(EncodedVClock) ->
    % For later versions, we expect to simply read the embedded encoding
    % so switching will not be a problem. But upgrading from first version
    % without embedded encoding is handled by the try/catch. We should be
    % able to remove that expensive part a couple of releases after 1.4
    {Method, EncodedVClock2} = try binary_to_term(EncodedVClock)
    catch error:badarg -> {encode_zlib, EncodedVClock} end,
    decode_vclock(Method, EncodedVClock2).

%% Decode a vclock against our capability settings:
-spec decode_vclock(atom(), VClock :: term()) -> vclock:vclock().
decode_vclock(Method, VClock) ->
    case Method of
        encode_raw  -> VClock;
        encode_zlib -> binary_to_term(zlib:unzip(VClock));
        _           -> lager:error("Bad vclock encoding method ~p", [Method]),
                       throw(bad_vclock_encoding_method)
    end.

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

ancestor_test() ->
    {O,O2} = update_test(),
    O3 = riak_object:increment_vclock(O2,self()),
    [O] = riak_object:ancestors([O,O3]),
    {O,O3}.

reconcile_test() ->
    {O,O3} = ancestor_test(),
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
    [node1, node2] = [N || {N,_} <- riak_object:vclock(O3)],
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
    [C] = riak_object:get_contents(O1),
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

remove_duplicates_test() ->
    O0 = riak_object:new(<<"test">>, <<"test">>, zero),
    O1 = riak_object:new(<<"test">>, <<"test">>, one),
    ND = remove_duplicate_objects([O0, O0, O1, O1, O0, O1]),
    ?assertEqual(2, length(ND)),
    ?assert(lists:member(O0, ND)),
    ?assert(lists:member(O1, ND)).

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
    ?assert(vclock:equal(vclock(O), vclock(O2))),
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

    C3#r_content.value.

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

vclock_codec_test() ->
    VCs = [<<"BinVclock">>, {vclock, something, [], <<"blah">>}, vclock:fresh()],
    [ ?assertEqual({Method, VC}, {Method, decode_vclock(encode_vclock(Method, VC))})
     || VC <- VCs, Method <- [encode_raw, encode_zlib]].

-endif.
