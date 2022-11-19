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
-include("riak_object.hrl").

-export_type([riak_object/0, proxy_object/0, bucket/0, key/0, value/0, binary_version/0, index_value/0]).

-type key() :: binary().
-type bucket() :: binary() | {binary(), binary()}.
%% -type bkey() :: {bucket(), key()}.
-type value() :: term().
-type riak_object_dict() :: dict:dict().

-record(r_content, {
          metadata :: riak_object_dict(),
          value :: term()
         }).

%% Used by the merge function to accumulate contents
-record(merge_acc, {
          drop=orddict:new() :: orddict:orddict(),
          keep=ordsets:new() :: ordsets:ordset(#r_content{}),
          crdt=orddict:new() :: orddict:orddict(),
          error=[] :: list(binary())
         }).

%% Opaque container for Riak objects, a.k.a. riak_object()
-record(r_object, {
          bucket :: bucket(),
          key :: key(),
          contents :: list(r_content()),
          vclock = vclock:fresh() :: vclock:vclock(),
          updatemetadata=dict:store(clean, true, dict:new()) :: riak_object_dict(),
          updatevalue :: term()
         }).
-record(p_object, {
            r_object :: riak_object(),
            proxy :: tuple(),
            size = 0 :: integer()}).

-opaque riak_object() :: #r_object{}.
-opaque proxy_object() :: #p_object{}.

-type merge_acc() :: #merge_acc{}.
-type r_content() :: #r_content{}.
-type index_op() :: add | remove.
-type index_value() :: integer() | binary().
-type binary_version() :: v0 | v1.

-define(MAX_KEY_SIZE, 65536).

-define(LASTMOD_LEN, 29). %% static length of rfc1123_date() type. Hard-coded in Erlang.
-define(V1_VERS, 1).
-define(MAGIC, 53).      %% Magic number, as opposed to 131 for Erlang term-to-binary magic
                         %% Shanley's(11) + Joe's(42)
-define(EMPTY_VTAG_BIN, <<"e">>).

-export([new/3, new/4, ensure_robject/1, ancestors/1, reconcile/2, equal/2, remove_dominated/1]).
-export([increment_vclock/2, increment_vclock/3, prune_vclock/3, vclock_descends/2, all_actors/1]).
-export([actor_counter/2]).
-export([key/1, get_metadata/1, get_metadatas/1, get_values/1, get_value/1, get_dotted_values/1]).
-export([hash/1, hash/2, hash/4, approximate_size/2, proxy_size/1]).
-export([vclock_encoding_method/0, vclock/1, vclock_header/1, encode_vclock/1, decode_vclock/1]).
-export([encode_vclock/2, decode_vclock/2]).
-export([update/5, update_value/2, update_metadata/2, bucket/1, bucket_only/1, type/1, value_count/1]).
-export([get_update_metadata/1, get_update_value/1, get_contents/1]).
-export([merge/2, apply_updates/1, syntactic_merge/2]).
-export([to_json/1, from_json/1]).
-export([index_data/1, diff_index_data/2]).
-export([index_specs/1, diff_index_specs/2]).
-export([to_binary/2, from_binary/3, to_binary_version/4, binary_version/1]).
-export([nextgenrepl_encode/3, nextgenrepl_decode/1]).
-export([summary_from_binary/1, aae_from_object_binary/1,
            get_metadata_from_aae_binary/1, aae_fold_metabin/2,
            is_aae_object_deleted/2]).
-export([set_contents/2, set_vclock/2]). %% INTERNAL, only for riak_*
-export([is_robject/1, is_head/1]).
-export([update_last_modified/1, update_last_modified/2, get_last_modified/1]).
-export([strict_descendant/2, new_actor_epoch/2]).
-export([find_bestobject/1]).
-export([spoof_getdeletedobject/1]).
-export([delete_hash/1]).

-ifdef(TEST).
-export([convert_object_to_headonly/3]). % Used in unit testing of get_core
-endif.

%% @doc Constructor for new riak objects.
-spec new(Bucket::bucket(), Key::key(), Value::value()) -> riak_object().
new({T, B}, K, V) when is_binary(T), is_binary(B), is_binary(K) ->
    new_int({T, B}, K, V, no_initial_metadata);
new(B, K, V) when is_binary(B), is_binary(K) ->
    new_int(B, K, V, no_initial_metadata).

%% @doc Constructor for new riak objects with an initial content-type.
-spec new(Bucket::bucket(), Key::key(), Value::value(),
          string() | riak_object_dict() | no_initial_metadata) -> riak_object().
new({T, B}, K, V, C) when is_binary(T), is_binary(B), is_binary(K), is_list(C) ->
    new_int({T, B}, K, V, dict:from_list([{?MD_CTYPE, C}]));
new(B, K, V, C) when is_binary(B), is_binary(K), is_list(C) ->
    new_int(B, K, V, dict:from_list([{?MD_CTYPE, C}]));

%% @doc Constructor for new riak objects with an initial metadata dict.
%%
%% NOTE: Removed "is_tuple(MD)" guard to make Dialyzer happy.  The previous clause
%%       has a guard for string(), so this clause is OK without the guard.
new({T, B}, K, V, MD) when is_binary(T), is_binary(B), is_binary(K) ->
    new_int({T, B}, K, V, MD);
new(B, K, V, MD) when is_binary(B), is_binary(K) ->
    new_int(B, K, V, MD).

%% internal version after all validation has been done
new_int(B, K, V, MD) ->
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

-spec is_robject(any()) -> boolean()|proxy.
%% Is this a recognised riak object
%% true - riak object
%% proxy -  a proxy for a riak object
%% false - something else, hopefully just needs converting from binary
is_robject(#r_object{}) ->
    true;
is_robject(#p_object{}) ->
    proxy;
is_robject(_) ->
    false.

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
ancestors(Objects) ->
    [ O2 || O1 <- Objects, O2 <- Objects, strict_descendant(O1, O2) ].

%% @doc returns true if the `riak_object()' at `O1' is a strict
%% descendant of that at `O2'. This means that the first object
%% causally dominates the second.
%% @see vclock:dominates/2
-spec strict_descendant(riak_object(), riak_object()) -> boolean().
strict_descendant(O1, O2) ->
    vclock:dominates(riak_object:vclock(O1), riak_object:vclock(O2)).

%% @doc  Reconcile a list of riak objects.  If AllowMultiple is true,
%%       the riak_object returned may contain multiple values if Objects
%%       contains sibling versions (objects that could not be syntactically
%%       merged).   If AllowMultiple is false, the riak_object returned will
%%       contain the value of the most-recently-updated object, as per the
%%       X-Riak-Last-Modified header.
-spec reconcile([riak_object()], boolean()) -> riak_object().
reconcile(Objects, AllowMultiple) ->
    RObj = reconcile(remove_dominated(Objects)),
    case AllowMultiple of
        false ->
            Contents = [most_recent_content(RObj#r_object.contents)],
            RObj#r_object{contents=Contents};
        true ->
            RObj
    end.

%% @doc remove all Objects from the list that are causally
%% dominated by any other object in the list. Only concurrent /
%% conflicting objects will remain.
remove_dominated(Objects) ->
    All = sets:from_list(Objects),
    Del = sets:from_list(ancestors(Objects)),
    sets:to_list(sets:subtract(All, Del)).

%% @doc Take a list of {Idx, {ok, Object}} tuples that have been the
%% result of HEAD requests or GET requests. This list MUST be in the
%% reverse order that the results are received, so that the hd of the
%% list is the latest response received, and the last element the
%% first response received.
%%
%% Works out the best answers and returns {IdxObjList, IdxHeadList} where the
%% IdxHeadList is a list of indexes and headers that may need to be updated to
%% objects before the merge can be complete, and the IdxObjList is a list of
%% vnode indexes and objects which are ready to be merged
-spec find_bestobject(list({non_neg_integer(), {ok, riak_object()}})) -> 
                        {list({non_neg_integer(), {ok, riak_object()}}),
                            list({non_neg_integer(), {ok, riak_object()}})}.
find_bestobject(FetchedItems) ->
    % Find the best object.  Normally we expect this to be a score-draw in
    % terms of vector clock, so in this case 'best' means an object not a
    % head as the get-response can be used immediately.  Then the best is
    % the first received (the fastest responder) - as if there is a need
    % for a follow-up fetch, we should prefer the vnode that had responded
    % fastest to he HEAD (this may be local).
    ObjNotJustHeadFun =  fun({_Idx, Rsp}) ->  not is_head(Rsp) end,
    {Objects, Heads} = lists:partition(ObjNotJustHeadFun, FetchedItems),
    %% prefer full objects to heads
    FoldList = Heads ++ Objects,
    
    DescendsFun =
        fun(ObjClock, DescendsDirection) ->
            fun({_BestIdxSib, {ok, BestObjSib}}) ->
                case DescendsDirection of
                    from_obj ->
                        vclock:descends(vclock(BestObjSib), ObjClock);
                    from_best ->
                        vclock:descends(ObjClock, vclock(BestObjSib))
                end
            end
        end,
    
    FoldFun =
        % BestAnswers are a list of [{Idx, {ok, Obj}}] there are either the
        % best answer or a sibling of the best answer,  BestAnswers can also
        % be undefined at the start of the fold
        % 
        % If BestAnswers is undefined the object being folded over must now
        % be the head of the list of BestAnswers
        %
        % If all of the objects in the BestAnswers descend from the comparison
        % object, then the comparison object does not improve or conflict with
        % the BestAnswers, so the BestAnswers are unchanged
        %
        % Otherwise, if the Comparison Object descends from all the BestAnswers
        % then the Comparison Object represents a non-conflicting improvement
        % on the Best answers and becomes the single best answer
        %
        % If neither way represents a clean descent, then we consider
        % Comparison Object to be a sibling of at least one of the BestAnswers 
        fun({Idx, {ok, Obj}}, BestAnswers) ->
            case BestAnswers of
                undefined ->
                    [{Idx, {ok, Obj}}];
                _ ->
                    ObjClock = vclock(Obj),
                    AllBestAnswersDescendsObj =
                        lists:all(DescendsFun(ObjClock, from_obj),
                                    BestAnswers),
                    ObjDescendsAllBestAnswers =
                        lists:all(DescendsFun(ObjClock, from_best),
                                    BestAnswers),
                    case {AllBestAnswersDescendsObj,
                            ObjDescendsAllBestAnswers} of
                        {true, _} ->
                            BestAnswers;
                        {false, true} ->
                            [{Idx, {ok, Obj}}];
                        {false, false} ->
                            [{Idx, {ok, Obj}}|BestAnswers]
                    end
            end
        end,
    
    %% prefer the rightmost (fastest responder)
    BestAnswerList = lists:foldr(FoldFun, undefined, FoldList),

    % There may still be dominated siblings.  Whther there are dominated
    % siblings depends on the order that the siblings were discovered.  This
    % gives variation in behaviour base don order, to make this more
    % determenistic by removing all dominated siblings with this additional
    % check
    DominatedSibCheckFun = 
        fun({_, {ok, BA_Obj}}) ->
            not lists:any(fun({_, {ok, CheckObj}}) -> 
                                vclock:dominates(vclock(CheckObj),
                                                    vclock(BA_Obj))
                            end,
                            BestAnswerList)
        end,
    DominantList = lists:filter(DominatedSibCheckFun, BestAnswerList),

    % Return a list of sibling best answers split into two - first a list
    % where the object has already been fetched, and then a list where the
    % object it still to be fetched.
    %
    % In each list the list should be ordered from R to L i terms of fastest
    % responder
    lists:partition(ObjNotJustHeadFun, DominantList).



-spec is_head({ok, riak_object()}|riak_object()) -> boolean().
%% @private Check if an object is simply a head response
is_head({ok, #r_object{contents=[]}}) ->
    false;
is_head({ok, #r_object{contents=Contents}}) ->
    C0 = lists:nth(1, Contents),
    case C0#r_content.value of
        head_only ->
            true;
        _ ->
            false
    end;
is_head({ok, #p_object{}}) ->
    true;
is_head(Obj) ->
    is_head({ok, Obj}).

-spec spoof_getdeletedobject(riak_object()) -> riak_object().
%% @doc
%% If an object has been confirmed as deleted by riak_util:is_x_deleted, then
%% any head_only contents can be uplifted to a GET-equivalent by swapping the
%% head_only for an empty value.  There is no need to fetch values we know must
%% be empty
spoof_getdeletedobject(Obj) ->
    MapFun =
        fun(Content) ->
            V0 =
                case Content#r_content.value of
                    head_only -> <<>>;
                    V -> V
                end,
            Content#r_content{value = V0}
        end,
    Obj#r_object{contents = lists:map(MapFun, Obj#r_object.contents)}.

%% @private pairwise merge the objects in the list so that a single,
%% merged riak_object remains that contains all sibling values. Only
%% called with a list of conflicting objects. Use `remove_dominated/1'
%% to get such a list first.
reconcile(Objects) ->
    lists:foldl(fun syntactic_merge/2,
                hd(Objects),
                tl(Objects)).

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
%%       Note: This function calls apply_updates on NewObject.
%%       Depending on whether DVV is enabled or not, then may merge
%%       dropping dotted and dominated siblings, otherwise keeps all
%%       unique sibling values. NOTE: as well as being a Syntactic
%%       merge, this is also now a semantic merge for CRDTs.  Only
%%       call with concurrent objects. Use `syntactic_merge/2' if one
%%       object may strictly dominate another.
-spec merge(riak_object(), riak_object()) -> riak_object().
merge(OldObject=#r_object{}, NewObject=#r_object{}) ->
    NewObj1 = apply_updates(NewObject),
    Bucket = bucket(OldObject),
    case riak_kv_util:get_write_once(Bucket) of
        true ->
            merge_write_once(OldObject, NewObj1);
        _ ->
            DVV = dvv_enabled(Bucket),
            {Time,  {CRDT, Contents}} = timer:tc(fun merge_contents/3,
                                                 [NewObject, OldObject, DVV]),
            ok = riak_kv_stat:update({riak_object_merge, CRDT, Time}),
            OldObject#r_object{contents=Contents,
                vclock=vclock:merge([OldObject#r_object.vclock,
                    NewObj1#r_object.vclock]),
                updatemetadata=dict:store(clean, true, dict:new()),
                updatevalue=undefined}
    end.

%% @doc Special case write_once merge, in the case where the write_once property is
%%      set on the bucket (type).  In this case, take the lesser (in lexical order)
%%      of the SHA1 hash of each object.
%%
-spec merge_write_once(riak_object(), riak_object()) -> riak_object().
merge_write_once(OldObject, NewObject) ->
    ok = riak_kv_stat:update(write_once_merge),
    case crypto:hash(sha, term_to_binary(OldObject)) =< crypto:hash(sha, term_to_binary(NewObject)) of
        true ->
            OldObject;
        _ ->
            NewObject
    end.


%% @doc Merge the r_objects contents by converting the inner dict to
%%      a list, ensuring a sane order, and merging into a unique list.
merge_contents(NewObject, OldObject, false) ->
    Result = lists:umerge(fun compare/2,
                          lists:usort(fun compare/2, NewObject#r_object.contents),
                          lists:usort(fun compare/2, OldObject#r_object.contents)),
    {undefined, Result};

%% @private with DVV enabled, use event dots in sibling metadata to
%% remove dominated siblings and stop fake concurrency that causes
%% sibling explsion. Also, since every sibling is iterated over (some
%% twice!) why not merge CRDTs here, too?
merge_contents(NewObject, OldObject, true) ->
    Bucket = bucket(NewObject),
    Key = key(NewObject),
    MergeAcc0 = prune_object_siblings(OldObject, vclock(NewObject)),
    MergeAcc = prune_object_siblings(NewObject, vclock(OldObject), MergeAcc0),
    #merge_acc{crdt=CRDT, error=Error} = MergeAcc,
    riak_kv_crdt:log_merge_errors(Bucket, Key, CRDT, Error),
    merge_acc_to_contents(Bucket, MergeAcc).

%% Optimisation. To save converting every meta dict to a list sorting,
%% comparing, and coverting back again, we use this optimisation, that
%% shortcuts out the meta_data comparison, if the obects aren't equal,
%% we needn't compare meta_data at all. `compare/2' is an "ordering
%% fun". Return true if A =< B, false otherwise.
%%
%% @see lists:usort/2
compare(A=#r_content{value=VA}, B=#r_content{value=VB}) ->
    if VA < VB ->
            true;
       VA > VB ->
            false;
       true ->
            %% values are equal, compare metadata
            compare_metadata(A, B)
    end.

%% Optimisation. Used by `compare/2' ordering fun. Only convert, sort,
%% and compare the meta_data dictionaries if they are the same
%% size. Called by an ordering function, this too is an ordering
%% function.
%%
%% @see compare/2
%% @see lists:usort/3
compare_metadata(#r_content{metadata=MA}, #r_content{metadata=MB}) ->
    ASize = dict:size(MA),
    BSize = dict:size(MB),
    if ASize < BSize ->
            true;
       ASize > BSize ->
            false;
       true ->
            %% same size metadata, need to do actual compare
            lists:sort(dict:to_list(MA)) =< lists:sort(dict:to_list(MB))
    end.

%% @private de-duplicates, removes dominated siblings, merges CRDTs
-spec prune_object_siblings(riak_object(), vclock:vclock()) -> merge_acc().
prune_object_siblings(Object, Clock) ->
    prune_object_siblings(Object, Clock, #merge_acc{}).

-spec prune_object_siblings(riak_object(), vclock:vclock(), merge_acc())
                           -> merge_acc().
prune_object_siblings(Object, Clock, MergeAcc) ->
    lists:foldl(fun(Contents, Acc) ->
                        fold_contents(Contents, Acc, Clock)
                end,
                MergeAcc,
                Object#r_object.contents).

%% @private called once for each content entry for each object being
%% merged. Its job is to drop siblings that are causally dominated,
%% remove duplicate values, and merge CRDT values down to a single
%% value.
%%
%% When a Riak takes a PUT, a `dot' is generated. (See assign_dot/2)
%% for the PUT and placed in `riak_object' metadata. The cases below
%% use this `dot' to decide if a sibling value has been superceded,
%% and should therefore be dropped.
%%
%% Based on the work by Baquero et al:
%%
%% Efficient causality tracking in distributed storage systems with
%% dotted version vectors
%% http://doi.acm.org/10.1145/2332432.2332497
%% Nuno Preguiça, Carlos Bauqero, Paulo Sérgio Almeida, Victor Fonte,
%% and Ricardo Gonçalves. 2012
-spec fold_contents(r_content(), merge_acc(), vclock:vclock()) -> merge_acc().
fold_contents(C0, MergeAcc, Clock) ->
    #merge_acc{drop=Drop, keep=Keep, crdt=CRDT, error=Error} = MergeAcc,
    #r_content{metadata = Dict, value = Value} = C0,
    case get_dot(Dict) of
        {ok, {Dot, PureDot}} ->
            case {vclock:descends_dot(Clock, Dot),
                  is_drop_candidate(PureDot, Drop)} of
                {true, true} ->
                    %% When the exact same dot is present in both
                    %% objects siblings, we keep that value. Without
                    %% this merging an object with itself would result
                    %% in an object with no values at all, since an
                    %% object's vector clock always dominates all its
                    %% dots. However, due to riak_kv#679, and
                    %% backup-restore it is possible for two objects
                    %% to have dots with the same {id, count} but
                    %% different timestamps (see is_drop_candidate/2
                    %% below), and for the referenced values to be
                    %% _different_ too. We need to include both dotted
                    %% values, and let the list sort/merge ensure that
                    %% only one sibling for a pair of normal dots is
                    %% returned.  Hence get the other dots contents,
                    %% should be identical, but Skewed Dots mean we
                    %% can't guarantee that, the merge will dedupe for
                    %% us.
                    DC = get_drop_candidate(PureDot, Drop),
                    MergeAcc#merge_acc{keep=[C0, DC | Keep]};
                {true, false} ->
                    %% The `dot' is dominated by the other object's
                    %% vclock. This means that the other object has a
                    %% value that is the result of a PUT that
                    %% reconciled this sibling value, we can therefore
                    %% (potentialy, see above) drop this value.
                    MergeAcc#merge_acc{drop=add_drop_candidate(PureDot, C0,
                                                               Drop)};
                {false, _} ->
                    %% The other object's vclock does not contain (or
                    %% dominate) this sibling's `dot'. That means this
                    %% is a genuinely concurrent (or sibling) value
                    %% and should be kept for the user to reconcile.
                    MergeAcc#merge_acc{keep=[C0|Keep]}
            end;
        undefined ->
            %% Both CRDTs and legacy data don't have dots. Legacy data
            %% because it was written before DVV (or with DVV turned
            %% off.) That means keep this sibling. We cannot know if
            %% it is dominated. This is the pre-DVV behaviour.
            %%
            %% CRDTs values do not get a dot as they are merged by
            %% Riak, not by the client. A CRDT PUT _NEVER_ overwrites
            %% existing values in Riak, it is always merged with
            %% them. The resulting value then does not have a single
            %% event origin. Instead it is a merge of many events. In
            %% DVVSets (referenced above) there is an `anonymous list'
            %% for values that have no single event. In Riak we
            %% already have that, by having an `undefined' dot. We
            %% could store all the dots for a CRDT as a clock, but it
            %% would need complex logic to prune.
            %%
            %% Since there are no dots for CRDT values, there can be
            %% sibling values on disk. Which is a concern for sibling
            %% explosion scenarios. To avoid such scenarios we call
            %% out to CRDT logic to merge CRDTs into a single value
            %% here.
            case riak_kv_crdt:merge_value({Dict, Value}, {CRDT, [], []}) of
                {CRDT, [], [E]} ->
                    %% An error occurred trying to merge this sibling
                    %% value. This means it was CRDT with some
                    %% corruption of the binary format, or maybe a
                    %% user's opaque binary that happens to match the
                    %% CRDT binary start tag. Either way, gather the
                    %% error for later logging.
                    MergeAcc#merge_acc{error=[E | Error]};
                {CRDT, [{Dict, Value}], E} when is_list(E)->
                    %% The sibling value was not a CRDT, but a
                    %% (legacy?) un-dotted user opaque value. Add it
                    %% to the list of values to keep.
                    MergeAcc#merge_acc{keep=[C0|Keep], error=E ++ Error};
                {CRDT2, [], []} ->
                    %% The sibling was a CRDT and the CRDT accumulator
                    %% has been updated.
                    MergeAcc#merge_acc{crdt=CRDT2}
            end
    end.

%% Store a pure dot and it's contents as a possible candidate to be
%% dropped.
%%
%% @see is_drop_candidate/2
add_drop_candidate(Dot, Contents,  Dict) ->
    orddict:store(Dot, Contents, Dict).

%% Check if a pure dot is already present as a drop candidate.
is_drop_candidate(Dot, Dict) ->
    orddict:is_key(Dot, Dict).

%% fetch a drop candidate by it's pure dot
get_drop_candidate(Dot, Dict) ->
    orddict:fetch(Dot, Dict).

%% @private Transform a `merge_acc()' to a list of `r_content()'. The
%% merge accumulator contains a list of non CRDT (opaque) sibling
%% values (`keep') and merged CRDT values (`crdt'). Due to bucket
%% types it should really only ever contain one or the other (and the
%% accumulated CRDTs should have a single value), but it is better to
%% be safe.
-spec merge_acc_to_contents(riak_object:bucket(), merge_acc())
                           -> list(r_content()).
merge_acc_to_contents(Bucket, MergeAcc) ->
    #merge_acc{keep=Keep, crdt=CRDTs} = MergeAcc,
    %% Convert the non-CRDT sibling values back to dict metadata values.
    %%
    %% For improved performance, fold_contents/3 does not check for duplicates
    %% when constructing the "Keep" list (eg. using an ordset), but instead
    %% simply prepends kept siblings to the list. Here, we convert Keep into an
    %% ordset equivalent with reverse/unique sort.
    Keep2 = lists:usort(fun compare/2, lists:reverse(Keep)),
    %% Iterate the set of converged CRDT values and turn them into
    %% `r_content' entries.  by generating their metadata entry and
    %% binary encoding their contents. Bucket Types should ensure this
    %% accumulator only has one entry ever.

    case orddict:size(CRDTs) > 0 of
        true ->
            BProps = riak_core_bucket:get_bucket(Bucket),
            orddict:fold(
              fun(_Type, {Meta, CRDT0}, {_, Contents}) ->
                      CRDT = riak_kv_crdt:maybe_apply_props(BProps, CRDT0),
                      {riak_kv_crdt:to_mod(CRDT),
                       [{r_content, riak_kv_crdt:meta(Meta, CRDT),
                         riak_kv_crdt:to_binary(CRDT)} | Contents]}
              end,
              {undefined, Keep2},
              CRDTs);
        false ->
            {undefined, Keep2}
    end.

%% @private Get the dot from the passed metadata dict (if present and
%% valid). It turns out, due to weirdness, it is possible to have two
%% dots with the same id and counter, but different timestamps (backup
%% and restore, riak_kv#679). In that case, it is possible to drop
%% both dots, as both are dominted, but not equal. Here we strip the
%% timestamp component when comparing dots. A `dot' in riak_object is
%% just a {binary(), pos_integer()} pair.
%%
%% @see vclock:destructure_dot/1
-spec get_dot(riak_object_dict()) ->
        {ok, {vclock:dot(), vclock:pure_dot()}} | undefined.
get_dot(Dict) ->
    case dict:find(?DOT, Dict) of
        {ok, Dot} ->
            case vclock:valid_dot(Dot) of
                true ->
                    PureDot = vclock:pure_dot(Dot),
                    {ok, {Dot, PureDot}};
                false ->
                    undefined
            end;
        error ->
            undefined
    end.

%% @private used by get_dotted_values to re-use get_dot/1
get_vc_dot(Dict) ->
    case get_dot(Dict) of
        {ok, {Dot, _PDot}} ->
            Dot;
        _ -> undefined
    end.

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
-spec bucket(riak_object()|proxy_object()) -> bucket().
bucket(#r_object{bucket=Bucket}) -> Bucket;
bucket(#p_object{r_object=RObj}) -> bucket(RObj).

%% @doc Return the containing bucket for this riak_object without any type
%% information, if present.
-spec bucket_only(riak_object()|proxy_object()) -> binary().
bucket_only(#r_object{bucket={_Type, Bucket}}) -> Bucket;
bucket_only(#r_object{bucket=Bucket}) -> Bucket;
bucket_only(#p_object{r_object=RObj}) -> bucket_only(RObj).

%% @doc Return the containing bucket-type for this riak_object.
-spec type(riak_object()|proxy_object()) -> binary() | undefined.
type(#r_object{bucket={Type, _Bucket}}) -> Type;
type(#r_object{bucket=_Bucket}) -> undefined;
type(#p_object{r_object=RObj}) -> type(RObj).

%% @doc  Return the key for this riak_object.
-spec key(riak_object()|proxy_object()) -> key().
key(#r_object{key=Key}) -> Key;
key(#p_object{r_object=RObj}) -> key(RObj).

%% @doc  Return the vector clock for this riak_object.
-spec vclock(riak_object()|proxy_object()) -> vclock:vclock().
vclock(#r_object{vclock=VClock}) -> VClock;
vclock(#p_object{r_object=RObj}) -> vclock(RObj).


%% @doc  Return the number of values (siblings) of this riak_object.
-spec value_count(riak_object()|proxy_object()) -> non_neg_integer().
value_count(#r_object{contents=Contents}) -> length(Contents);
value_count(#p_object{r_object=RObj}) -> value_count(RObj).

%% @doc  Return the contents (a list of {metadata, value} tuples) for
%%       this riak_object.
-spec get_contents(riak_object()|proxy_object()) -> [{riak_object_dict(), value()}].
get_contents(#r_object{contents=Contents}) ->
    [{Content#r_content.metadata, Content#r_content.value} ||
        Content <- Contents];
get_contents(#p_object{r_object=RObj, proxy=P}) ->
    {FetchFun, Pid, FetchKey} = P,
    ObjBin = FetchFun(Pid, FetchKey),
    R0 = from_binary(RObj#r_object.bucket, RObj#r_object.key, ObjBin),
    get_contents(R0).

%% @doc  Assert that this riak_object has no siblings and return its associated
%%       metadata.  This function will fail with a badmatch error if the
%%       object has siblings (value_count() > 1).
-spec get_metadata(riak_object()|proxy_object()) -> riak_object_dict().
get_metadata(O=#r_object{}) ->
    % this blows up intentionally (badmatch) if more than one content value!
    [{Metadata,_V}] = get_contents(O),
    Metadata;
get_metadata(#p_object{r_object=RObj}) ->
    [Content] = RObj#r_object.contents,
    Content#r_content.metadata.

%% @doc  Return a list of the metadata values for this riak_object.
-spec get_metadatas(riak_object()|proxy_object()) -> [riak_object_dict()].
get_metadatas(#r_object{contents=Contents}) ->
    [Content#r_content.metadata || Content <- Contents];
get_metadatas(#p_object{r_object=RObj}) ->
    get_metadatas(RObj).

%% @doc  Return a list of object values for this riak_object.
%%       If the object is a proxy will need to fetch the actual object
-spec get_values(riak_object()|proxy_object()) -> [value()].
get_values(#r_object{contents=C}) ->
    [Content#r_content.value || Content <- C];
get_values(#p_object{r_object = RObj, proxy = P}) ->
    {FetchFun, Pid, FetchKey} = P,
    ObjBin = FetchFun(Pid, FetchKey),
    R0 = from_binary(RObj#r_object.bucket, RObj#r_object.key, ObjBin),
    get_values(R0).

%% @doc  Return a list of object values and their dots for this riak_object.
-spec get_dotted_values(riak_object()) -> [{vclock:dot(), value()}].
get_dotted_values(#r_object{contents=C}) ->
    [{get_vc_dot(Content#r_content.metadata) , Content#r_content.value} || Content <- C].

%% @doc  Assert that this riak_object has no siblings and return its associated
%%       value.  This function will fail with a badmatch error if the object
%%       has siblings (value_count() > 1).
%%       If the object is a proxy will need to fetch the actual object.
-spec get_value(riak_object()|proxy_object()) -> value().
get_value(RObj=#r_object{}) ->
    % this blows up intentionally (badmatch) if more than one content value!
    [{_M,Value}] = get_contents(RObj),
    Value;
get_value(#p_object{r_object=RObj, proxy = P}) ->
    {FetchFun, Pid, FetchKey} = P,
    ObjBin = FetchFun(Pid, FetchKey),
    R0 = from_binary(RObj#r_object.bucket, RObj#r_object.key, ObjBin),
    get_value(R0).

%% @doc calculates the canonical hash of a riak object
%%      Old API which uses the version .
%% DEPRECATED
-spec hash(riak_object()) -> binary().
hash(Obj=#r_object{}) ->
    case riak_kv_entropy_manager:get_version() of
        0 ->
            vclock_hash(Obj);
        legacy ->
            legacy_hash(Obj)
    end.

-spec hash(bucket(), key(), 
            riak_object()|proxy_object()|binary(), 
            non_neg_integer()|legacy) -> binary().
%% @doc calculates the canonical hash of a riak object depending on version
%% May accept as input either a real object or a proxy object, or an object
%% that is still serialised in a binary form (where that serialised object 
%% could be either a proxy or a riak object)
hash(_Bucket, _Key, RObj=#r_object{}, Version) ->
    hash(RObj, Version);
hash(_Bucket, _Key, PObj=#p_object{}, Version) ->
    hash(PObj#p_object.r_object, Version);
hash(Bucket, Key, ObjBin, Version) when is_binary(ObjBin) ->
    hash(Bucket, Key, riak_object:from_binary(Bucket, Key, ObjBin), Version).


%% @doc calculates the canonical hash of a riak object depending on version
-spec hash(riak_object(), non_neg_integer() | legacy) -> binary().
hash(Obj=#r_object{}, _Version=0) ->
    vclock_hash(Obj);
hash(Obj=#r_object{}, _Version) ->
    legacy_hash(Obj).

%% @private return the legacy full object hash of the riak_object
-spec legacy_hash(riak_object()) -> binary().
legacy_hash(Obj=#r_object{}) ->
    % Blow up if we ever try performing a legacy hash on a proxy
    % object.  
    UpdObj = riak_object:set_vclock(Obj, lists:sort(vclock(Obj))),
    Hash = erlang:phash2(to_binary(v0, UpdObj)),
    term_to_binary(Hash).

%% @private return the hash of the vclock
-spec vclock_hash(riak_object()) -> binary().
vclock_hash(Obj=#r_object{}) ->
    Hash = erlang:phash2(lists:sort(vclock(Obj))),
    term_to_binary(Hash).

%% @doc  Set the updated metadata of an object to M.
-spec update_metadata(riak_object(), riak_object_dict()) -> riak_object().
update_metadata(Object=#r_object{}, M) ->
    Object#r_object{updatemetadata=dict:erase(clean, M)}.

%% @doc  Set the updated value of an object to V
-spec update_value(riak_object(), value()) -> riak_object().
update_value(Object=#r_object{}, V) -> Object#r_object{updatevalue=V}.

%% @doc  Return the updated metadata of this riak_object.
-spec get_update_metadata(riak_object()) -> riak_object_dict().
get_update_metadata(#r_object{updatemetadata=UM}) -> UM.

%% @doc  Return the updated value of this riak_object.
-spec get_update_value(riak_object()) -> value().
get_update_value(#r_object{updatevalue=UV}) -> UV.

%% @doc  INTERNAL USE ONLY.  Set the vclock of riak_object O to V.
-spec set_vclock(riak_object(), vclock:vclock()) -> riak_object().
set_vclock(Object=#r_object{}, VClock) -> Object#r_object{vclock=VClock}.

%% @doc vnode uses to add a new actor epoch to the vclock, a replica
%% write showed local amnesia
-spec new_actor_epoch(riak_object(), vclock:vclock_node()) -> riak_object().
new_actor_epoch(Object=#r_object{vclock=VC}, ClientId) ->
    NewClock = vclock:increment(ClientId, VC),
    Object#r_object{vclock=NewClock}.

%% @doc  Increment the entry for ClientId in O's vclock.
-spec increment_vclock(riak_object(), vclock:vclock_node()) -> riak_object().
increment_vclock(Object=#r_object{bucket=B}, ClientId) ->
    NewClock = vclock:increment(ClientId, Object#r_object.vclock),
    {ok, Dot} = vclock:get_dot(ClientId, NewClock),
    assign_dot(Object#r_object{vclock=NewClock}, Dot, dvv_enabled(B)).

%% @doc  Increment the entry for ClientId in O's vclock.
-spec increment_vclock(riak_object(), vclock:vclock_node(), vclock:timestamp()) -> riak_object().
increment_vclock(Object=#r_object{bucket=B}, ClientId, Timestamp) ->
    NewClock = vclock:increment(ClientId, Timestamp, Object#r_object.vclock),
    {ok, Dot} = vclock:get_dot(ClientId, NewClock),
    %% If it is true that we only ever increment the vclock to create
    %% a frontier object, then there must only ever be a single value
    %% when we increment, so add the dot here.
    assign_dot(Object#r_object{vclock=NewClock}, Dot, dvv_enabled(B)).

%% @doc Prune vclock
-spec prune_vclock(riak_object(), vclock:timestamp(), [proplists:property()]) ->
                          riak_object().
prune_vclock(Obj=#r_object{vclock=VC}, PruneTime, BucketProps) ->
    VC2 = vclock:prune(VC, PruneTime, BucketProps),
    Obj#r_object{vclock=VC2}.

%% @doc Does the `riak_object' descend the provided `vclock'?
-spec vclock_descends(riak_object(), vclock:vclock()) -> boolean().
vclock_descends(#r_object{vclock=ObjVC}, VC) ->
    vclock:descends(ObjVC, VC).

%% @doc get the list of all actors that have touched this object.
-spec all_actors(riak_object()) -> [binary()] | [].
all_actors(#r_object{vclock=VC}) ->
    vclock:all_nodes(VC).

%%$ @doc get the counter for the given actor, 0 if not present
-spec actor_counter(vclock:vclock_node(), riak_object()) ->
                           non_neg_integer().
actor_counter(Actor, #r_object{vclock=VC}) ->
    vclock:get_counter(Actor, VC).

%% @private assign the dot to the value only if DVV is enabled. Only
%% call with a valid dot. Only assign dot when there is a single value
%% in contents.
-spec assign_dot(riak_object(), vclock:dot(), boolean()) -> riak_object().
assign_dot(Object=#r_object{}, Dot, true) ->
    #r_object{contents=[C=#r_content{metadata=Meta0}]} = Object,
    Object#r_object{contents=[C#r_content{metadata=dict:store(?DOT, Dot, Meta0)}]};
assign_dot(Object, _Dot, _DVVEnabled) ->
    Object.

%% @private is dvv enabled on this node?
-spec dvv_enabled(bucket()) -> boolean().
dvv_enabled(Bucket) ->
    BProps = riak_core_bucket:get_bucket(Bucket),
    %% default to `true`, since legacy buckets should have `false` by
    %% default, and typed should have `true` and `undefined` is not a
    %% valid return.
    proplists:get_value(dvv_enabled, BProps, true).

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
-spec diff_index_specs(undefined | riak_object(), undefined | riak_object()) ->
    [{index_op(), binary(), index_value()}].
diff_index_specs(Obj, OldObj) ->
    OldIndexes = index_data(OldObj),
    AllIndexes = index_data(Obj),
    diff_index_data(OldIndexes, AllIndexes).

-spec diff_index_data([{binary(), index_value()}],
                      [{binary(), index_value()}]) ->
    [{index_op(), binary(), index_value()}].
diff_index_data(OldIndexes, AllIndexes) ->
    OldIndexSet = ordsets:from_list(OldIndexes),
    AllIndexSet = ordsets:from_list(AllIndexes),
    diff_specs_core(AllIndexSet, OldIndexSet).


diff_specs_core(AllIndexSet, OldIndexSet) ->
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
-spec index_data(riak_object() | undefined) -> [{binary(), index_value()}].
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
-spec set_contents(riak_object(), [{riak_object_dict(), value()}]) -> riak_object().
set_contents(Object=#r_object{}, MVs) when is_list(MVs) ->
    Object#r_object{contents=[#r_content{metadata=M,value=V} || {M, V} <- MVs]}.

-spec vclock_header(riak_object()) -> {Name::string(), Value::string()}.
%% @doc Transform the Erlang representation of the document's vclock
%%      into something suitable for an HTTP header
vclock_header(Doc) ->
    VClock = riak_object:vclock(Doc),
    EncodedVClock = binary_to_list(base64:encode(encode_vclock(VClock))),
    {?HEAD_VCLOCK, EncodedVClock}.

%% @doc Converts a riak_object into its JSON equivalent
%% @deprecated use `riak_object_json:encode' directly
-spec to_json(riak_object()) -> {struct, list(any())}.
to_json(Obj) ->
    lager:warning("Change uses of riak_object:to_json/1 to riak_object_json:encode/1"),
    riak_object_json:encode(Obj).

%% @deprecated Use `riak_object_json:decode' now.
from_json(JsonObj) ->
    lager:warning("Change uses of riak_object:from_json/1 to riak_object_json:decode/1"),
    riak_object_json:decode(JsonObj).

is_updated(_Object=#r_object{updatemetadata=M,updatevalue=V}) ->
    case dict:find(clean, M) of
        error -> true;
        {ok,_} ->
            case V of
                undefined -> false;
                _ -> true
            end
    end.

%% @doc a Put merge. Update a stored riak_object with the value from a
%% new riak_object that has been put. Must be serialised by the
%% `Actor' provided.
-spec update(LWW :: boolean(), LocalObj :: riak_object(),
             NewObj :: riak_object(), Actor ::vclock:vclock_node(),
             TimeStamp :: vclock:timestamp()) ->
                    riak_object().
update(true, _OldObject, NewObject=#r_object{}, Actor, Timestamp) ->
    increment_vclock(NewObject, Actor, Timestamp);
update(false, OldObject=#r_object{}, NewObject=#r_object{}, Actor, Timestamp) ->
    %% Get the vclock we have for the local / old object
    LocalVC = vclock(OldObject),
    %% get the vclock from the new object
    PutVC = vclock(NewObject),

    %% Optimisation: if the new object's vclock descends from the old
    %% object's vclock, then don't merge values, just increment the
    %% clock and overwrite.
    case vclock:descends(PutVC, LocalVC) of
        true ->
            increment_vclock(NewObject, Actor, Timestamp);
        false ->
            %% The new object is concurrent with some other value, so
            %% merge the new object and the old object.
            MergedClock = vclock:merge([PutVC, LocalVC]),
            FrontierClock = vclock:increment(Actor, Timestamp, MergedClock),
            {ok, Dot} = vclock:get_dot(Actor, FrontierClock),
            %% Assign an event to the new value
            Bucket = bucket(OldObject),
            DottedPutObject = assign_dot(NewObject, Dot, dvv_enabled(Bucket)),
            MergedObject = merge(DottedPutObject, OldObject),
            set_vclock(MergedObject, FrontierClock)
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

%% @doc Get the actual size of the object if it is a proxy object, otherwise
%% return 0
-spec proxy_size(proxy_object()) -> integer().
proxy_size(Obj) ->
    Obj#p_object.size.

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

%% @doc Encode for nextgen_repl
-spec nextgenrepl_encode(repl_v1, riak_object(), boolean()) -> binary().
nextgenrepl_encode(repl_v1, RObj, ToCompress) ->
    B = riak_object:bucket(RObj),
    K = riak_object:key(RObj),
    KS = byte_size(K),
    ObjBK =
        case B of
            {T, B0} ->
                TS = byte_size(T),
                B0S = byte_size(B0),
                <<TS:32/integer, T/binary, B0S:32/integer, B0/binary,
                    KS:32/integer, K/binary>>;
            B0 ->
                B0S = byte_size(B0),
                <<0:32/integer, B0S:32/integer, B0/binary,
                    KS:32/integer, K/binary>>
        end,
    {Version, ObjBin} =
        case ToCompress of
            true ->
                {<<1:4/integer, 1:1/integer, 0:3/integer>>,
                    zlib:compress(riak_object:to_binary(v1, RObj))};
            false ->
                {<<1:4/integer, 0:1/integer, 0:3/integer>>,
                    riak_object:to_binary(v1, RObj)}
        end,
    <<Version/binary, ObjBK/binary, ObjBin/binary>>.

%% @doc Deocde for nextgen_repl
-spec nextgenrepl_decode(binary()) -> riak_object().
nextgenrepl_decode(<<1:4/integer, C:1/integer, _:3/integer,
                        0:32/integer, BL:32/integer, B:BL/binary,
                        KL:32/integer, K:KL/binary,
                        ObjBin/binary>>) ->
    nextgenrepl_decode(B, K, C == 1, ObjBin);
nextgenrepl_decode(<<1:4/integer, C:1/integer, _:3/integer,
                        TL:32/integer, T:TL/binary, BL:32/integer, B:BL/binary,
                        KL:32/integer, K:KL/binary,
                        ObjBin/binary>>) ->
    nextgenrepl_decode({T, B}, K, C == 1, ObjBin).

nextgenrepl_decode(B, K, true, ObjBin) ->
    nextgenrepl_decode(B, K, false, zlib:uncompress(ObjBin));
nextgenrepl_decode(B, K, false, ObjBin) ->
    riak_object:from_binary(B, K, ObjBin).

%% @doc Convert binary object to riak object
-spec from_binary(bucket(),key(),binary()) ->
    riak_object() | {error, 'bad_object_format'} | proxy_object().
from_binary(B, K, <<131, _Rest/binary>>=ObjTerm) ->
    case binary_to_term(ObjTerm) of
        {proxy_object, HeadBin, ObjSize, Fetcher} ->
            RObj = from_binary(B, K, HeadBin),
            #p_object{r_object = RObj,
                        proxy = Fetcher,
                        size = ObjSize};
        T ->
            T
    end;
from_binary(B, K, <<?MAGIC:8/integer, 1:8/integer, Rest/binary>>=_ObjBin) ->
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


-spec summary_from_binary(binary()) -> 
        {vclock:vclock(), integer(), integer(),
            list(erlang:timestamp())|undefined, binary()}.
%% @doc
%% Extract only sumarry infromation from the binary - the vector, the object 
%% size and the sibling count
summary_from_binary(<<131, _Rest/binary>>=ObjBin) ->
    case binary_to_term(ObjBin) of 
        {proxy_object, HeadBin, ObjSize, _Fetcher} ->
            summary_from_binary(HeadBin, ObjSize);
        T ->
            {vclock(T), byte_size(ObjBin), value_count(T),
                undefined, <<>>}
            % Legacy object version will end up with dummy details
    end;
summary_from_binary(ObjBin) when is_binary(ObjBin) ->
    summary_from_binary(ObjBin, byte_size(ObjBin));
summary_from_binary(Object = #r_object{}) ->
    % Unexpected scenario but included for parity with from_binary
    % Calculating object size is expensive (relatively to other branches)
    summary_from_binary(to_binary(v1, Object)).


-spec summary_from_binary(binary(), integer()) ->
    {vclock:vclock(), non_neg_integer(), non_neg_integer(),
        list(erlang:timestamp()), binary()}.
%% @doc 
%% Return afrom a version 1 binary the vector clock and siblings
summary_from_binary(ObjBin, ObjSize) ->
    <<?MAGIC:8/integer, 
        1:8/integer, 
        VclockLen:32/integer, VclockBin:VclockLen/binary, 
        SibCount:32/integer, 
        SibsBin/binary>> = ObjBin,
    {LastMods, SibBin} = 
        case SibCount of
            SC when is_integer(SC) ->
                get_metadata_from_siblings(SibsBin,
                                            SibCount,
                                            [],
                                            [])
        end,
    {binary_to_term(VclockBin), ObjSize, SibCount, LastMods, SibBin}.

%% @doc
%% Function used to split objects in parallel AAE store
-spec aae_from_object_binary(boolean()) ->
        fun((binary()) -> 
            {integer(), integer(), integer(),
                list(erlang:timestamp()), binary()}).
aae_from_object_binary(true) ->
    fun(ObjBin) ->
        {_Clock, Size, SibCount, LastMods, SibsBin} =
            summary_from_binary(ObjBin),
        {Size, SibCount, 0, LastMods, SibsBin}
    end;
aae_from_object_binary(false) ->
    fun(ObjBin) ->
        {_Clock, Size, SibCount, LastMods, _SibsBin} =
            summary_from_binary(ObjBin),
        {Size, SibCount, 0, LastMods, term_to_binary(null)}
    end.


-spec get_metadata_from_siblings(binary(), integer(),
                                    list(erlang:timestamp()), list()) ->
                                    {list(erlang:timestamp()), binary()}.
%% @doc
%% Split the metadata and the last modification date away from the rest of the
%% object
get_metadata_from_siblings(<<>>, 0, LastMods, SibMDList) ->
    {LastMods, term_to_binary(SibMDList)};
get_metadata_from_siblings(<<ValLen:32/integer, Rest0/binary>>,
                            SibCount,
                            LastMods,
                            SibMDList) ->
    <<_ValBin:ValLen/binary, MetaLen:32/integer, Rest1/binary>> = Rest0,
    SibBin = <<0:32/integer, MetaLen:32/integer, Rest1/binary>>,
    {SibMetaData, LastMod, Remainder} = sib_of_binary(SibBin),
    get_metadata_from_siblings(Remainder,
                                SibCount - 1,
                                [LastMod|LastMods],
                                [SibMetaData|SibMDList]).

%% @doc Where sibs_of_binary has been used to create a value-free binary of
%% an object - reverse it here.
-spec get_metadata_from_aae_binary(binary()) -> list().
get_metadata_from_aae_binary(SiblingBinary) ->
    case binary_to_term(SiblingBinary) of
        null ->
            [];
        ContentList ->
            lists:map(fun(S) -> S#r_content.metadata end, ContentList)
    end.

%% @doc
%% AAE folds need to cope with metadata being returned in native form, only
%% with and empty value - but also as stored in parallel mode using
%% get_metadata_from_siblngs
-spec aae_fold_metabin(binary(), list()) -> list().
aae_fold_metabin(<<>>, SibMDAcc) ->
    SibMDAcc;
aae_fold_metabin(<<0:32/integer, Rest/binary>>, SibMDAcc) ->
    %% Native mode
    {SibMetaData, _LastMod, OtherSibs} =
        sib_of_binary(<<0:32/integer, Rest/binary>>),
    aae_fold_metabin(OtherSibs, [SibMetaData#r_content.metadata|SibMDAcc]);
aae_fold_metabin(Bin, _SibMDAcc) ->
    %% Parallel mode
    get_metadata_from_aae_binary(Bin).

%% @doc
%% Given the MD result of an AAE Fold - determine if the object is a Riak
%% tombstone.  The MD result is subtly different for parallel and native mode
%% and this is handle within aae_fold_metabin.
%% Second argument is a boolean to indicate if the applictaion is interested
%% in processing the metadata after determining the tombstone status - if true
%% the de-serialised metadata list is returned along with the deleted status.
-spec is_aae_object_deleted(binary()|list(), boolean()) ->
                                                {boolean(), list()|undefined}.
is_aae_object_deleted(<<0:32, _Rest/binary>> = MetaBin, false) ->
    {is_binary_deleted(MetaBin, true), undefined};
is_aae_object_deleted(MetaBin, ReturnMD) when is_binary(MetaBin) ->
    is_aae_object_deleted(aae_fold_metabin(MetaBin, []), ReturnMD);
is_aae_object_deleted([], ReturnMD) ->
    case ReturnMD of
        true ->
            {false, []};
        false ->
            {false, undefined}
    end;
is_aae_object_deleted(MDs, ReturnMD) ->
    PredFun = 
        fun(M) ->
            dict:is_key(<<"X-Riak-Deleted">>, M)
        end,
    IsDeleted = lists:all(PredFun, MDs),
    case ReturnMD of
        true ->
            {IsDeleted, MDs};
        false ->
            {IsDeleted, undefined}
    end.

%% @doc
%% In the case that the binary is the native object binary format - can find
%% if it is deleted by pattern matching the binary, there is no need to process
%% all the metadata
-spec is_binary_deleted(binary(), boolean()) -> boolean().
is_binary_deleted(<<>>, IsDeleted) ->
    IsDeleted;
is_binary_deleted(<<ValLen:32/integer, _ValBin:ValLen/binary,
                    MetaLen:32/integer, MetaBin:MetaLen/binary,
                    Rest/binary>>, true) ->
    <<_LMD:12/binary, VTagLen:8/integer, _VTag:VTagLen/binary,
        Deleted:1/binary-unit:8, _MetaRestBin/binary>> = MetaBin,
    is_binary_deleted(Rest, Deleted == <<1>>);
is_binary_deleted(_SibBin, false) ->
    false.


sibs_of_binary(Count,SibsBin) ->
    sibs_of_binary(Count, SibsBin, []).

sibs_of_binary(0, <<>>, Result) -> lists:reverse(Result);
sibs_of_binary(0, _NotEmpty, _Result) ->
    {error, corrupt_contents};
sibs_of_binary(Count, SibsBin, Result) ->
    {Sib, _LastMod, SibsRest} = sib_of_binary(SibsBin),
    sibs_of_binary(Count-1, SibsRest, [Sib | Result]).

sib_of_binary(<<ValLen:32/integer, ValBin:ValLen/binary, MetaLen:32/integer, MetaBin:MetaLen/binary, Rest/binary>>) ->
    <<LMMega:32/integer, LMSecs:32/integer, LMMicro:32/integer, VTagLen:8/integer, VTag:VTagLen/binary, Deleted:1/binary-unit:8, MetaRestBin/binary>> = MetaBin,
    LastModDate = {LMMega, LMSecs, LMMicro},
    MDList0 = deleted_meta(Deleted, []),
    MDList1 = last_mod_meta(LastModDate, MDList0),
    MDList2 = vtag_meta(VTag, MDList1),
    MDList3 = val_encoding_meta(ValBin, MDList2),
    MDList = meta_of_binary(MetaRestBin, MDList3),
    MD = dict:from_list(MDList),
    {#r_content{metadata=MD, value=decode_maybe_binary(ValBin)}, LastModDate, Rest}.

val_encoding_meta(<<>>, MDList) ->
    MDList;
val_encoding_meta(<<0, _Rest/binary>>, MDList) ->
    MDList;
val_encoding_meta(<<1, _Rest/binary>>, MDList) ->
    MDList;
val_encoding_meta(<<Other:8, _Rest/binary>>, MDList) ->
    [{?MD_VAL_ENCODING, Other} | MDList].

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

bin_content(#r_content{metadata=Meta0, value=Val}) ->
    {TypeTag, Meta} = determine_binary_type(Val, Meta0),
    ValBin = encode_maybe_binary(Val, TypeTag),
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

encode_maybe_binary(Value) when is_binary(Value) ->
    encode_maybe_binary(Value, 1);
encode_maybe_binary(Value) when not is_binary(Value) ->
    encode_maybe_binary(Value, 0).
encode_maybe_binary(Value, TypeTag) when is_binary(Value) ->
    <<TypeTag, Value/binary>>;
encode_maybe_binary(Value, 0) when not is_binary(Value) ->
    <<0, (term_to_binary(Value))/binary>>.

determine_binary_type(Val, Meta) when is_binary(Val) ->
    case dict:find(?MD_VAL_ENCODING, Meta) of
        error -> {1, Meta};
        {ok, TypeTag} -> {TypeTag, dict:erase(?MD_VAL_ENCODING, Meta)}
    end;
determine_binary_type(_Val, Meta) ->
    {0, Meta}.

decode_maybe_binary(<<>>) ->
    head_only;
decode_maybe_binary(<<1, Bin/binary>>) ->
    Bin;
decode_maybe_binary(<<0, Bin/binary>>) ->
    binary_to_term(Bin);
decode_maybe_binary(<<_Other:8, Bin/binary>>) ->
    Bin.

%% Update X-Riak-VTag and X-Riak-Last-Modified in the object's metadata, if
%% necessary.
update_last_modified(RObj) ->
    update_last_modified(RObj, os:timestamp()).

%% Update X-Riak-VTag and X-Riak-Last-Modified in the object's metadata, if
%% necessary with an external timestamp passed in.
update_last_modified(RObj, TS) ->
    MD0 = case dict:find(clean, riak_object:get_update_metadata(RObj)) of
              {ok, true} ->
                  %% There have been no changes to updatemetadata. If we stash the
                  %% last modified in this dict, it will cause us to lose existing
                  %% metadata (bz://508). If there is only one instance of metadata,
                  %% we can safely update that one, but in the case of multiple siblings,
                  %% it's hard to know which one to use. In that situation, use the update
                  %% metadata as is.
                  case riak_object:get_metadatas(RObj) of
                      [MD] ->
                          MD;
                      _ ->
                          riak_object:get_update_metadata(RObj)
                  end;
               _ ->
                  riak_object:get_update_metadata(RObj)
          end,
    %% Post-0.14.2 changed vtags to be generated from node/now rather the vclocks.
    %% The vclock has not been updated at this point.  Vtags/etags should really
    %% be an external interface concern and are only used for sibling selection
    %% and if-modified type tests so they could be generated on retrieval instead.
    %% This changes from being a hash on the value to a likely-to-be-unique value
    %% which should serve the same purpose.  It was possible to generate two
    %% objects with the same vclock on 0.14.2 if the same clientid was used in
    %% the same second.  It can be revisited post-1.0.0.
    NewMD = dict:store(?MD_VTAG, riak_kv_util:make_vtag(TS),
                       dict:store(?MD_LASTMOD, TS, MD0)),
    riak_object:update_metadata(RObj, NewMD).

%% Get the last modified date from the metadata
get_last_modified(MD) ->
    case dict:find(?MD_LASTMOD, MD) of
        error ->
            {0, 0, 0};
        {ok, TS} ->
            TS
    end.

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

%% @doc Need to hash the object, so that when a final_delete is processed
%% it can be compared against the current object state to ensure that the
%% deletion is not of an updated object
-spec delete_hash(riak_object()|vclock:vclock()) -> non_neg_integer().
delete_hash(ObjOrClock) ->
    %% This could be the same as vclock_hash, but kept different should the
    %% two hashes need to diverge in the future.  Extra collision protection
    %% from using 2^32 range as with previous riak_kv_vnode:delete_hash/1, but
    %% this should not be necessary given addition of is_x_deleted check before
    %% comparison (and changes to vector clocks e.g. addition of epochs) since
    %% delete_hash/1 was introduced.
    case is_robject(ObjOrClock) of
        false ->
            erlang:phash2(lists:sort(ObjOrClock), 4294967296);
        _ ->
            delete_hash(vclock(ObjOrClock))
    end.


-ifdef(TEST).

%% See https://github.com/basho/riak_kv/issues/1707
object_with_empty_contents_test() ->
    B = <<"buckets_are_binaries">>,
    K = <<"keys are binaries">>,

    Contents = [],
    O = #r_object{bucket=B,key=K,
                  contents=Contents,vclock=vclock:fresh()},

    ?assertEqual(false, is_head(O)).

object_test() ->
    B = <<"buckets_are_binaries">>,
    K = <<"keys are binaries">>,
    V = <<"values are anything">>,
    O = riak_object:new(B, K, V),
    B = riak_object:bucket(O),
    K = riak_object:key(O),
    V = riak_object:get_value(O),
    1 = riak_object:value_count(O),
    1 = length(riak_object:get_values(O)),
    1 = length(riak_object:get_metadatas(O)),
    O.

val_encoding_term_test() ->
    B = <<"buckets are binaries">>,
    K = <<"keys are binaries">>,
    V = {a, tuple, is, a, valid, value},
    Object = riak_object:new(B, K, V),
    Binary = to_binary(v1, Object),
    {FirstBinaryByte, _Meta} = get_binary_type_tag_and_metadata_from_full_binary(Binary),
    %% term_to_binary format is 0
    ?assertEqual(0, FirstBinaryByte).

val_encoding_bin_test() ->
    B = <<"buckets are binaries">>,
    K = <<"keys are binaries">>,
    V = <<"Some Binary Data">>,
    Object = riak_object:new(B, K, V),
    Binary = to_binary(v1, Object),
    {FirstBinaryByte, _Meta} = get_binary_type_tag_and_metadata_from_full_binary(Binary),
    %% arbitrary binary format is 1
    ?assertEqual(1, FirstBinaryByte).

val_encoding_with_metadata_test() ->
    B = <<"buckets are binaries">>,
    K = <<"keys are binaries">>,
    V = <<"Some Binary Data">>,
    Object = riak_object:new(B, K, V, dict:from_list([{?MD_VAL_ENCODING, 2}, {<<"X-Foo_MetaData">>, "Foo"}])),
    Binary = to_binary(v1, Object),
    {FirstBinaryByte, Meta} = get_binary_type_tag_and_metadata_from_full_binary(Binary),
    %% When specified in metadata, use the val_encoding version
    ?assertEqual(2, FirstBinaryByte),
    %% Make sure <<"X-Riak-Val-Encoding">> metadata was removed, as it's encoded in the binary itself
    %% and would be superfluous
    ?assertNot(dict:is_key(?MD_VAL_ENCODING, Meta)).

get_binary_type_tag_and_metadata_from_full_binary(Binary) ->
    <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer, _VclockBin:VclockLen/binary, _SibCount:32/integer, SibsBin/binary>> = Binary,
    <<ValLen:32/integer, ValBin:ValLen/binary, MetaLen:32/integer, MetaBinRest:MetaLen/binary>> = SibsBin,
    <<_LMMega:32/integer, _LMSecs:32/integer, _LMMicro:32/integer, VTagLen:8/integer, _VTag:VTagLen/binary, _Deleted:1/binary-unit:8, MetaBin/binary>> = MetaBinRest,
    <<FirstBinaryByte:8, _Rest/binary>> = ValBin,
    {FirstBinaryByte, dict:from_list(meta_of_binary(MetaBin, []))}.


convert_object_to_headonly(B, K, Object) ->
    % Use in unit tests to simulate an object returned as a result of a HEAD
    % request
    Binary = to_binary(v1, Object),
    <<?MAGIC:8/integer,
        ?V1_VERS:8/integer,
        VclockLen:32/integer,
        VclockBin:VclockLen/binary,
        SibCount:32/integer, SibsBin/binary>> = Binary,
    <<ValLen:32/integer,
        _ValBin:ValLen/binary,
        MetaLen:32/integer,
        MetaBinRest:MetaLen/binary>> = SibsBin,
    SibsBin0 = <<0:32/integer,
                    MetaLen:32/integer,
                    MetaBinRest:MetaLen/binary>>,
    HeadBin = <<?MAGIC:8/integer,
                ?V1_VERS:8/integer,
                VclockLen:32/integer,
                VclockBin:VclockLen/binary,
                SibCount:32/integer, SibsBin0/binary>>,
    from_binary(B, K, HeadBin).

val_decoding_headresponse_test() ->
    % An empty binary as a value results in the content value being marked as
    % head_only
    B = <<"buckets are binaries">>,
    K = <<"keys are binaries">>,
    V = <<"Some value">>,
    InObject = riak_object:new(B, K, V,
                                dict:from_list([{?MD_VAL_ENCODING, 2},
                                {<<"X-Foo_MetaData">>, "Foo"}])),
    OutObject = convert_object_to_headonly(B, K, InObject),
    C0 = lists:nth(1, OutObject#r_object.contents),
    ?assertMatch(head_only, C0#r_content.value).

find_bestobject_equal_test() ->
    % three identical objects, but one is head_only
    B = <<"buckets are binaries">>,
    K = <<"keys are binaries">>,
    V = <<"Some value">>,
    InObject = riak_object:new(B, K, V,
                                dict:from_list([{?MD_VAL_ENCODING, 2},
                                {<<"X-Foo_MetaData">>, "Foo"}])),
    Obj1 = InObject,
    Obj2 = InObject,
    Obj3 = convert_object_to_headonly(B, K, InObject),
    ?assertMatch(true, is_head({ok, Obj3})),
    ?assertMatch(false, is_head({ok, Obj1})),
    % The response is:
    % {[{Idx, {ok, ObjG}}], [{Idx, {ok, ObjH}}] where heads need to be fetched
    % and gets are ready to be merged
    ?assertMatch({[{1, {ok, Obj1}}], []},
                    find_bestobject([{3, {ok, Obj3}},
                                        {2, {ok, Obj2}},
                                        {1, {ok, Obj1}}])),
    % Shuffle and get alterate (RH) result
    ?assertMatch({[{2, {ok, Obj2}}], []},
                    find_bestobject([{1, {ok, Obj1}},
                                        {2, {ok, Obj2}},
                                        {3, {ok, Obj3}}])),
    % Shuffle and get alterate (RH) result
    ?assertMatch({[{1, {ok, Obj1}}], []},
                    find_bestobject([{2, {ok, Obj2}},
                                        {1, {ok, Obj1}},
                                        {3, {ok, Obj3}}])).

find_bestobject_ancestor() ->
    % one object is behind, and one of the dominant objects is head_only
    B = <<"buckets_are_binaries">>,
    K = <<"keys are binaries">>,
    {Obj1, Obj2} = ancestor(),
    Obj3 = convert_object_to_headonly(B, K, Obj2),
    ?assertMatch({[{2, {ok, Obj2}}], []},
                    find_bestobject([{3, {ok, Obj3}},
                                        {2, {ok, Obj2}},
                                        {1, {ok, Obj1}}])),
    % Change ordering - still works
    ?assertMatch({[{2, {ok, Obj2}}], []},
                    find_bestobject([{2, {ok, Obj2}},
                                        {3, {ok, Obj3}},
                                        {1, {ok, Obj1}}])).

find_bestobject_reconcile() ->
    B = <<"buckets_are_binaries">>,
    K = <<"keys are binaries">>,
    {Obj1, UpdO} = update_test(),
    Obj2 = riak_object:increment_vclock(UpdO, one_pid),
    Obj3 = riak_object:increment_vclock(UpdO, alt_pid),
    Obj4 = convert_object_to_headonly(B, K, Obj2),
    Obj5 = convert_object_to_headonly(B, K, Obj1),
    ?assertMatch({[{2, {ok, Obj2}},
                        {3, {ok, Obj3}}],
                    [{4, {ok, Obj4}}]},
                    find_bestobject([{1, {ok, Obj1}},
                                        {2, {ok, Obj2}},
                                        {3, {ok, Obj3}},
                                        {4, {ok, Obj4}}])),
    % due to change in ordering knows that 1 is dominated
    ?assertMatch({[{2, {ok, Obj2}},
                        {3, {ok, Obj3}}],
                    [{4, {ok, Obj4}}]},
                    find_bestobject([{4, {ok, Obj4}},
                                        {2, {ok, Obj2}},
                                        {3, {ok, Obj3}},
                                        {1, {ok, Obj1}}])),
    ?assertMatch({[{2, {ok, Obj2}},
                        {3, {ok, Obj3}}],
                    []},
                    find_bestobject([{1, {ok, Obj1}},
                                        {2, {ok, Obj2}},
                                        {3, {ok, Obj3}},
                                        {5, {ok, Obj5}}])),
    ?assertMatch({[{3, {ok, Obj3}}],
                    [{4, {ok, Obj4}}]},
                    find_bestobject([{1, {ok, Obj1}},
                                        {4, {ok, Obj4}},
                                        {3, {ok, Obj3}},
                                        {5, {ok, Obj5}}])),
                                        
    Obj6 = riak_object:increment_vclock(Obj2, two_pid),
    Hdr6 = convert_object_to_headonly(B, K, Obj6),

    ?assertMatch({[{3, {ok, Obj3}}, {6, {ok, Obj6}}], []},
                    find_bestobject([{3, {ok, Obj3}},
                                        {6, {ok, Obj6}},
                                        {2, {ok, Obj2}}])),
    ?assertMatch({[{3, {ok, Obj3}}, {6, {ok, Obj6}}], []},
                    find_bestobject([{2, {ok, Obj2}},
                                        {3, {ok, Obj3}},
                                        {6, {ok, Obj6}}])),
                                        
    ?assertMatch({[{6, {ok, Obj6}}], []},
                    find_bestobject([{2, {ok, Obj2}},
                                    {4, {ok, Obj4}},
                                    {6, {ok, Obj6}}])),
    ?assertMatch({[{7, {ok, Obj6}}], []},
                    find_bestobject([{2, {ok, Obj2}},
                                    {4, {ok, Obj4}},
                                    {6, {ok, Obj6}},
                                    {7, {ok, Obj6}}])),
    ?assertMatch({[{7, {ok, Obj6}}], []},
                    find_bestobject([{2, {ok, Obj2}},
                                    {4, {ok, Obj4}},
                                    {6, {ok, Obj6}},
                                    {7, {ok, Obj6}},
                                    {8, {ok, Hdr6}}])),
    
    Hdr3 = convert_object_to_headonly(B, K, Obj3),
    ?assertMatch({[{7, {ok, Obj6}}], [{3, {ok, Hdr3}}]},
                    find_bestobject([{2, {ok, Obj2}},
                                    {3, {ok, Hdr3}},
                                    {4, {ok, Obj4}},
                                    {6, {ok, Obj6}},
                                    {7, {ok, Obj6}},
                                    {8, {ok, Hdr6}}])).


update_test() ->
    O = object_test(),
    V2 = <<"testvalue2">>,
    O1 = riak_object:update_value(O, V2),
    O2 = riak_object:apply_updates(O1),
    V2 = riak_object:get_value(O2),
    {O,O2}.

bucket_prop_needers_test_() ->
    {setup,
     fun() ->
             meck:new(riak_core_bucket),
             meck:expect(riak_core_bucket, get_bucket,
                         fun(_) ->
                                 []
                         end)
     end,
     fun(_) ->
             meck:unload(riak_core_bucket)
     end,
     [{"Ancestor", fun ancestor/0},
        {"Ancestor Weird Clocks", fun ancestor_weird_clocks/0},
        {"Reconcile", fun reconcile/0},
        {"Merge 1", fun merge1/0},
        {"Merge 2", fun merge2/0},
        {"Merge 3", fun merge3/0},
        {"Merge 4", fun merge4/0},
        {"Merge 5", fun merge5/0},
        {"Inequality", fun inequality1/0},
        {"Inequality Vclock", fun inequality_vclock/0},
        {"Date Reconcile", fun date_reconcile/0},
        {"Dotted values reconcile", fun dotted_values_reconcile/0},
        {"Weird Clocks, Weird Dots", fun weird_clocks_weird_dots/0},
        {"Mixed Merge", fun mixed_merge/0},
        {"Mixed Merge 2", fun mixed_merge2/0},
        {"Find Object Ancestor", fun find_bestobject_ancestor/0},
        {"Find Object Reconcile", fun find_bestobject_reconcile/0},
        {"Test Summary Bin Extract", fun summary_binary_extract/0},
        {"Next Gen Repl Encode/Decode", fun nextgenrepl/0}]
    }.

ancestor() ->
    Actor = self(),
    {O,O2} = update_test(),
    O3 = riak_object:increment_vclock(O2, Actor),
    [O] = riak_object:ancestors([O,O3]),
    MD = riak_object:get_metadata(O3),
    ?assertMatch({Actor, {1, _}}, dict:fetch(?DOT, MD)),
    {O,O3}.

ancestor_weird_clocks() ->
    %% make objects with dots / clocks that are causally the same but
    %% with different timestamps (backup - restore, riak_kv#679)
    {B, K} = {<<"b">>, <<"k">>},
    A = riak_object:increment_vclock(riak_object:new(B, K, <<"a">>), a, 100),
    AWat = riak_object:increment_vclock(riak_object:new(B, K, <<"b">>), a, 1000),
    %% reconcile should neither as neither dominates the other (even
    %% though they're not equal)
    Ancestors = ancestors([A, AWat]),
    ?assertEqual(0, length(Ancestors)).

reconcile() ->
    {O,O3} = ancestor(),
    O3 = riak_object:reconcile([O,O3],true),
    O3 = riak_object:reconcile([O,O3],false),
    {O,O3}.

merge1() ->
    {O,O3} = reconcile(),
    O3 = riak_object:syntactic_merge(O,O3),
    {O,O3}.

merge2() ->
    B = <<"buckets_are_binaries">>,
    K = <<"keys are binaries">>,
    V = <<"testvalue2">>,
    O1 = riak_object:increment_vclock(object_test(), node1),
    O2 = riak_object:increment_vclock(riak_object:new(B,K,V), node2),
    O3 = riak_object:syntactic_merge(O1, O2),
    [node1, node2] = [N || {N,_} <- riak_object:vclock(O3)],
    2 = riak_object:value_count(O3).

merge3() ->
    O0 = riak_object:new(<<"test">>, <<"test">>, hi),
    O1 = riak_object:increment_vclock(O0, x),
    ?assertEqual(O1, riak_object:syntactic_merge(O1, O1)).

merge4() ->
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

merge5() ->
    B = <<"buckets_are_binaries">>,
    K = <<"keys are binaries">>,
    V = <<"testvalue2">>,
    O1 = riak_object:increment_vclock(object_test(), node1),
    O2 = riak_object:increment_vclock(riak_object:new(B,K,V), node2),
    ?assertEqual(riak_object:syntactic_merge(O1, O2),
                 riak_object:syntactic_merge(O2, O1)).

inequality1() ->
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

inequality_vclock() ->
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

date_reconcile() ->
    {O,O3} = reconcile(),
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

dotted_values_reconcile() ->
    {B, K} = {<<"b">>, <<"k">>},
    A = riak_object:increment_vclock(riak_object:new(B, K, <<"a">>), a),
    C = riak_object:increment_vclock(riak_object:new(B, K, <<"c">>), c),
    Z = riak_object:increment_vclock(riak_object:new(B, K, <<"z">>), z),
    A1 = riak_object:increment_vclock(riak_object:apply_updates(riak_object:update_value(A, <<"a1">>)), a),
    C1 = riak_object:increment_vclock(riak_object:apply_updates(riak_object:update_value(C, <<"c1">>)), c),
    Z1 = riak_object:increment_vclock(riak_object:apply_updates(riak_object:update_value(Z, <<"z1">>)), z),
    ACZ = riak_object:reconcile([A, C, Z, A1, C1, Z1], true),
    ?assertEqual(3, riak_object:value_count(ACZ)),
    Contents = lists:sort(riak_object:get_contents(ACZ)),
    verify_contents(Contents, [{{a, 2}, <<"a1">>}, {{c, 2}, <<"c1">>}, {{z, 2}, <<"z1">>}]),
    Vclock = riak_object:vclock(ACZ),
    ?assertEqual(2, vclock:get_counter(a, Vclock)),
    ?assertEqual(2, vclock:get_counter(c, Vclock)),
    ?assertEqual(2, vclock:get_counter(z, Vclock)),
    ?assertEqual(3, length(Vclock)).

weird_clocks_weird_dots() ->
    %% make objects with dots / clocks that are causally the same but
    %% with different timestamps (backup - restore, riak_kv#679)
    {B, K} = {<<"b">>, <<"k">>},
    A = riak_object:increment_vclock(riak_object:new(B, K, <<"a">>), a, 100),
    AWat = riak_object:increment_vclock(riak_object:new(B, K, <<"a">>), a, 1000),
    O = riak_object:syntactic_merge(A, AWat),
    ?assertEqual(2, riak_object:value_count(O)).

%% Test the case where an object with undotted values is merged with
%% an object with dotted values.
mixed_merge() ->
    {B, K} = {<<"b">>, <<"k">>},
    A_VC = vclock:fresh(a, 3),
    Z_VC = vclock:fresh(b, 2),
    A = riak_object:set_vclock(riak_object:new(B, K, <<"a">>), A_VC),
    C = riak_object:increment_vclock(riak_object:new(B, K, <<"c">>), c),
    Z = riak_object:set_vclock(riak_object:new(B, K, <<"b">>), Z_VC),
    ACZ = riak_object:reconcile([A, C, Z], true),
    ?assertEqual(3, riak_object:value_count(ACZ)).

mixed_merge2() ->
    {B, K} = {<<"b">>, <<"k">>},
    A = riak_object:new(B, K, <<"a">>),
    A1 = riak_object:increment_vclock(A, a),
    Z = riak_object:increment_vclock(A, z),
    AZ = riak_object:merge(A1, Z),
    AZ2 = riak_object:merge(AZ, AZ),
    ?assertEqual(AZ2, AZ),
    ?assertEqual(2, riak_object:value_count(AZ2)),
    AZ3 = riak_object:set_contents(AZ2, [{dict:new(), <<"undotted">>} | riak_object:get_contents(AZ2)]),
    AZ4 = riak_object:syntactic_merge(AZ3, AZ2),
    ?assertEqual(3, riak_object:value_count(AZ4)),
    ?assert(equal(AZ3, AZ4)).

nextgenrepl() ->
    {B, K} = {<<"b">>, <<"k">>},
    A_VC = vclock:fresh(a, 3),
    Z_VC = vclock:fresh(b, 2),
    A = riak_object:set_vclock(riak_object:new(B, K, <<"a">>), A_VC),
    C = riak_object:increment_vclock(riak_object:new(B, K, <<"c">>), c),
    Z = riak_object:set_vclock(riak_object:new(B, K, <<"b">>), Z_VC),
    ACZ = riak_object:reconcile([A, C, Z], true),
    ACZ0 = nextgenrepl_decode(nextgenrepl_encode(repl_v1, ACZ, false)),
    ACZ0 = nextgenrepl_decode(nextgenrepl_encode(repl_v1, ACZ, true)),
    ?assertEqual(ACZ0, ACZ).


verify_contents([], []) ->
    ?assert(true);
verify_contents([{MD, V} | Rest], [{{Actor, Count}, V} | Rest2]) ->
    ?assertMatch({Actor, {Count, _}}, dict:fetch(?DOT, MD)),
    verify_contents(Rest, Rest2).


head_binary(VC1) ->
    head_binary(VC1, false).

head_binary(VC1, IsDeleted) ->
    DelBin = 
        case IsDeleted of
            true -> <<1>>;
            false -> <<0>>
        end,
    MetaBin =
        <<0:32/integer,
            0:32/integer,
            0:32/integer,
            0:8/integer,
            DelBin:1/binary>>,

    MetaSize = byte_size(MetaBin),
    SibsBin =
        <<0:32/integer, MetaSize:32/integer, MetaBin/binary,
            0:32/integer, MetaSize:32/integer, MetaBin/binary>>,
    VclockLen = byte_size(VC1),

    RObjBin =
        <<?MAGIC:8/integer, ?V1_VERS:8/integer,
            VclockLen:32/integer, VC1/binary,
            2:32/integer, SibsBin/binary>>,
    RObjBin.


from_binary_headonly_test() ->
    % Confirm that from_binary works as expected for a response to a head
    % request, which will have no r_content.value
    Bucket = <<"B">>,
    Key = <<"K1">>,
    VC1 = term_to_binary(vclock:fresh(a, 3)),
    
    RObjBin = head_binary(VC1, false),
    RObj = riak_object:from_binary(Bucket, Key, RObjBin),

    ?assertMatch(true, is_robject(RObj)),
    ?assertMatch(VC1, term_to_binary(riak_object:vclock(RObj))),
    ?assertMatch(true, is_head(RObj)),
    ?assertMatch(false, riak_kv_util:is_x_deleted(RObj)),
    
    RObjBinD = head_binary(VC1, true),
    RObjD = riak_object:from_binary(Bucket, Key, RObjBinD),
    ?assertMatch(true, is_robject(RObjD)),
    ?assertMatch(VC1, term_to_binary(riak_object:vclock(RObjD))),
    ?assertMatch(true, is_head(RObjD)),
    ?assertMatch(true, riak_kv_util:is_x_deleted(RObjD)).


fetch_value(clone, "JournalKey") ->
    term_to_binary(#r_object{contents = [#r_content{value = "V1"}]}).

from_binary_foldheads_test() ->
    % the response to fold_heads request will be a head binary, but
    % wrapped up in a tuple
    Bucket = <<"B">>,
    Key = <<"K1">>,
    VC1 = term_to_binary(vclock:fresh(a, 3)),
    MDBin = head_binary(VC1),
    HeadObj =
        term_to_binary(
            {proxy_object, MDBin, 100,
                {fun fetch_value/2, clone, "JournalKey"}}),

    RObj = riak_object:from_binary(Bucket, Key, HeadObj),
    ?assertMatch(proxy, is_robject(RObj)),
    ?assertMatch(VC1, term_to_binary(riak_object:vclock(RObj))),
    ?assertMatch(true, is_head(RObj)),
    ?assertMatch("V1", get_value(RObj)),
    ?assertMatch(["V1"], get_values(RObj)),
    ?assertMatch(Bucket, bucket(RObj)),
    ?assertMatch(Key, key(RObj)).

summary_from_binary_foldheads_test() ->
    % the response to fold_heads request will be a head binary, but
    % wrapped up in a tuple
    VC1 = term_to_binary(vclock:fresh(a, 3)),
    MDBin = head_binary(VC1),
    HeadObj =
        term_to_binary(
            {proxy_object, MDBin, 100,
                {fun fetch_value/2, clone, "JournalKey"}}),

    {VC_Summ, BS_Summ, SC_Summ, _LMD, _SibBin} = summary_from_binary(HeadObj),
    ?assertMatch(VC1, term_to_binary(VC_Summ)),
    ?assertMatch(100, BS_Summ),
    ?assertMatch(2, SC_Summ).

summary_binary_extract() ->
    B = <<"buckets are binaries">>,
    K = <<"keys are binaries">>,
    V = <<"Some Binary Data">>,
    ObjectA0 = riak_object:new(B, K, V),
    ObjectA = riak_object:increment_vclock(ObjectA0, a),
    Binary = to_binary(v1, ObjectA),
    {Clock, Size, SibCount, LMD, SibBin} = summary_from_binary(Binary),
    ?assertMatch(1, SibCount),
    ?assertMatch(1, length(Clock)),
    ?assertMatch(true, Size > 0),
    {Clock, Size, SibCount, LMD, SibBin} = summary_from_binary(ObjectA),
    DeletedM = dict:store(<<"X-Riak-Deleted">>, true, dict:new()),
    ObjectB0 = riak_object:new(B, K, V, DeletedM),
    ObjectB = riak_object:increment_vclock(ObjectB0, b),
    {_ClockB, _SizeB, 1, _LMDB, SibBinB} = summary_from_binary(ObjectB),
    DeletedMDLB = aae_fold_metabin(SibBinB, []),
    ?assertMatch({true, undefined},
                    is_aae_object_deleted(DeletedMDLB, false)),
    ObjectC = merge(ObjectA, ObjectB),
    {_ClockC, _SizeC, 2, _LMDC, SibBinC} = summary_from_binary(ObjectC),
    DeletedMDLC = aae_fold_metabin(SibBinC, []),
    ?assertMatch({false, undefined},
                    is_aae_object_deleted(DeletedMDLC, false)),
    ObjectD0 = riak_object:new(B, K, V, DeletedM),
    ObjectD = riak_object:increment_vclock(ObjectD0, d),
    ObjectE = merge(ObjectB, ObjectD),
    {_ClockE, _SizeE, 2, _LMDE, SibBinE} = summary_from_binary(ObjectE),
    DeletedMDLE = aae_fold_metabin(SibBinE, []),
    ?assertMatch(true,
                    element(1, is_aae_object_deleted(DeletedMDLE, true))),

    % Should be able to see is_deleted status straight from binary
    % although in the case of parallel this will convert
    ?assertMatch(true, element(1, is_aae_object_deleted(SibBinB, true))),
    ?assertMatch(false, element(1, is_aae_object_deleted(SibBinC, true))),
    ?assertMatch({true, undefined}, is_aae_object_deleted(SibBinE, false)),
    
    ObjBinA = trim_value_frombinary(to_binary(v1, ObjectA)),
    ObjBinB = trim_value_frombinary(to_binary(v1, ObjectB)),
    ObjBinC = trim_value_frombinary(to_binary(v1, ObjectC)),
    ObjBinD = trim_value_frombinary(to_binary(v1, ObjectD)),
    ObjBinE = trim_value_frombinary(to_binary(v1, ObjectE)),
    
    % Prove that we cna extract metadata - and see is deleted status
    MDLA = aae_fold_metabin(ObjBinA, []),
    MDLB = aae_fold_metabin(ObjBinB, []),
    MDLC = aae_fold_metabin(ObjBinC, []),
    MDLD = aae_fold_metabin(ObjBinD, []),
    MDLE = aae_fold_metabin(ObjBinE, []),
    ?assertMatch({false, undefined}, is_aae_object_deleted(MDLA, false)),
    ?assertMatch({true, undefined}, is_aae_object_deleted(MDLB, false)),
    ?assertMatch({false, undefined}, is_aae_object_deleted(MDLC, false)),
    ?assertMatch({true, undefined}, is_aae_object_deleted(MDLD, false)),
    ?assertMatch({true, undefined}, is_aae_object_deleted(MDLE, false)),
    
    % Should be able to see is_deleted status straight from binary
    ?assertMatch({false, undefined}, is_aae_object_deleted(ObjBinA, false)),
    ?assertMatch({true, undefined}, is_aae_object_deleted(ObjBinB, false)),
    ?assertMatch({false, undefined}, is_aae_object_deleted(ObjBinC, false)),
    ?assertMatch({true, undefined}, is_aae_object_deleted(ObjBinD, false)),
    ?assertMatch({true, undefined}, is_aae_object_deleted(ObjBinE, false)),
    ?assertMatch(false, element(1, is_aae_object_deleted(ObjBinA, true))),
    ?assertMatch(true, element(1, is_aae_object_deleted(ObjBinB, true))),
    ?assertMatch(false, element(1, is_aae_object_deleted(ObjBinC, true))),
    ?assertMatch(true, element(1, is_aae_object_deleted(ObjBinD, true))),
    ?assertMatch(true, element(1, is_aae_object_deleted(ObjBinE, true))).


trim_value_frombinary(<<?MAGIC:8/integer, 1:8/integer,
                        VclockLen:32/integer, _VclockBin:VclockLen/binary,
                        _SibCount:32/integer, SibsBin/binary>>) ->
    trim_values(SibsBin, <<>>).

trim_values(<<>>, AccBin) ->
    AccBin;
trim_values(<<ValueLen:32/integer, _ValueBin:ValueLen/binary,
            MetaLen:32/integer, MetaBin:MetaLen/binary, Rest/binary>>, AccBin) ->
    trim_values(Rest,
                <<AccBin/binary,
                    0:32/integer, MetaLen:32/integer, MetaBin/binary>>).

-endif.
