%% -------------------------------------------------------------------
%%
%% sms: Streaming merge sort
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

-module(sms).

-define(DICTMODULE, orddict).

-export([new/1,
         add_results/3,
         done/1,
         sms/1]).

-export_type([sms/0]).

-opaque sms() :: ?DICTMODULE:?DICTMODULE().

%% @doc create a new sms buffer for the given covering set
%% of `Vnodes'
-spec new([non_neg_integer()]) -> sms().
new(Vnodes) ->
    DictList = [{VnodeID, {active,[]}} || VnodeID <- Vnodes],
    ?DICTMODULE:from_list(DictList).

%% @doc Append `Results' to existing buffer for `VnodeID' in
%% `Data'
-spec add_results(non_neg_integer(), list(), sms()) -> sms().
add_results(VnodeID, done, Data) ->
    UpdateFun = fun({_, Prev}) -> {done, Prev} end,
    update(VnodeID, UpdateFun, Data);
add_results(VnodeID, Results, Data) ->
    UpdateFun = fun ({active, Prev}) -> {active, Prev ++ Results} end,
    update(VnodeID, UpdateFun, Data).

%% @private
update(VnodeID, UpdateFun, Data) ->
    ?DICTMODULE:update(VnodeID, UpdateFun, Data).

%% @doc get all data in buffer, for all vnodes, merged
-spec done(sms()) -> [term()].
done(Data) ->
    Values = values(Data),
    lists:merge(Values).


%% @doc perform the streaming merge sort over given `Data:sms()'
%% returns a two tuple of {`MergedReadyToSendResults::[term()], sms()},
%% where the first element is the merge-sorted data from all vnodes that can
%% be consumed by the client, and `sms()' is a buffer of remaining results.
-spec sms(sms()) -> {[term()] | [], sms()}.
sms(Data) ->
    Vals = values(Data),
    case any_empty(Vals) orelse Vals == [] of
        true ->
            {[], Data};
        false ->
            unsafe_sms(Data)
    end.

%% @private, perform the merge
unsafe_sms(Data) ->
    MinOfLastsOfLists = lists:min([lists:last(List) || List <- values(Data)]),
    SplitFun = fun (Elem) -> Elem =< MinOfLastsOfLists end,
    Split = ?DICTMODULE:map(fun (_Key, {Status, V}) -> {Status, lists:splitwith(SplitFun, V)} end, Data),
    LessThan = ?DICTMODULE:map(fun (_Key, {Status, V}) -> {Status, element(1, V)} end, Split),
    GreaterThan = ?DICTMODULE:map(fun (_Key, {Status, V}) -> {Status, element(2, V)} end, Split),
    Merged = lists:merge(values(LessThan)),
    {Merged, GreaterThan}.

%% @private
values(Data) ->
    %% Don't make the SMS wait forever for vnodes that are done
    [V || {_Key, {_Status, V}=T} <- ?DICTMODULE:to_list(Data), T /= {done, []}].

%% @private
empty([]) -> true;
empty(_) -> false.

%% @private
any_empty(Lists) ->
    lists:any(fun empty/1, Lists).
