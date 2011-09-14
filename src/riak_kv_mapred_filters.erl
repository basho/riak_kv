%% -------------------------------------------------------------------
%%
%% riak_kv_mapper: Executes map functions on input batches
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
-module(riak_kv_mapred_filters).
-author('John Muellerleile <johnm@basho.com>').

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([build_filter/1]).

-export([int_to_string/1,
         string_to_int/1,
         float_to_string/1,
         string_to_float/1,
         to_upper/1,
         to_lower/1,
         tokenize/1,
         urldecode/1]).

-export([greater_than/1,
         less_than/1,
         greater_than_eq/1,
         less_than_eq/1,
         between/1,
         matches/1,
         neq/1,
         eq/1,
         set_member/1,
         similar_to/1,
         starts_with/1,
         ends_with/1]).

-export([logical_and/1,
         logical_or/1,
         logical_not/1]).

-export([compose/1, resolve_name/1, build_exprs/1]).

-export([to_string/1, levenshtein/2]).

%% @doc Convert a list of Filter expressions into a list of MFA tuples.
-spec build_filter([[string()]]) ->
         {ok, [{Module::atom(), Function::atom(), Args::list()}]}
        |{error, {bad_filter, string()}}.
build_filter(Exprs) ->
    build_filter(Exprs, []).

build_filter([], Accum) ->
    {ok, lists:reverse(Accum)};
build_filter([[FunName|Args]|T], Accum) ->
    case resolve_name(FunName) of
        error ->
            {error, {bad_filter, FunName}};
        Fun ->
            build_filter(T, [{?MODULE, Fun, Args}|Accum])
    end.

%% Transform functions

int_to_string(_) ->
    fun(V) -> list_to_binary(integer_to_list(V)) end.

string_to_int(_) ->
    fun(V) -> list_to_integer(to_string(V)) end.

float_to_string(_) ->
    fun(V) -> list_to_binary(float_to_list(V)) end.

string_to_float(_) ->
    fun(V) -> list_to_float(to_string(V)) end.

to_upper(_) ->
    fun(V) -> string:to_upper(to_string(V)) end.

to_lower(_) ->
    fun(V) -> string:to_lower(to_string(V)) end.

tokenize(Args) ->
    Seps = riak_kv_mapred_filters:to_string(lists:nth(1, Args)),
    TokenNum = lists:nth(2, Args),
    fun(V) -> list_to_binary(lists:nth(TokenNum, string:tokens(to_string(V), Seps))) end.

urldecode(_) ->
    fun(V) -> {_, V1} = hd(mochiweb_util:parse_qs("x=" ++ to_string(V))), V1 end.

%% Filter functions

greater_than([Value|_]) ->
    fun(V) -> V > Value end.

less_than([Value|_]) ->
    fun(V) -> V < Value end.

greater_than_eq([Value|_]) ->
    fun(V) -> V >= Value end.

less_than_eq([Value|_]) ->
    fun(V) -> V =< Value end.

between(Args) ->
    LowValue = lists:nth(1, Args),
    HighValue = lists:nth(2, Args),
    case length(Args) == 3 of
        true ->
            case lists:nth(3, Args) of
                true -> Inclusive = true;
                false -> Inclusive = false
            end;
        false ->
            Inclusive = true
    end,
    case Inclusive of
        true ->
            fun(V) -> V >= LowValue andalso V =< HighValue end;
        false ->
            fun(V) -> V > LowValue andalso V < HighValue end
    end.

matches([MS|_]) ->
    {ok, CMS} = re:compile(MS),
    fun(V) ->
        case re:run(V, CMS) of
            nomatch -> false;
            {match, _} -> true
        end
    end.

neq([Value|_]) ->
    fun(V) -> V =/= Value end.

eq([Value|_]) ->
    fun(V) -> V =:= Value end.

set_member(Args) ->
    Args1 = lists:map(fun(Arg) -> riak_kv_mapred_filters:to_string(Arg) end, Args),
    VSet = sets:from_list(Args1),
    fun(V) -> sets:is_element(riak_kv_mapred_filters:to_string(V), VSet) end.

similar_to(Args) ->
    Str = riak_kv_mapred_filters:to_string(lists:nth(1, Args)),
    MaxDist = lists:nth(2, Args),
    fun(V) -> riak_kv_mapred_filters:levenshtein(
                riak_kv_mapred_filters:to_string(V), Str) =< MaxDist end.

starts_with([Str|_]) ->
    fun(V) -> lists:prefix(riak_kv_mapred_filters:to_string(Str), riak_kv_mapred_filters:to_string(V)) end.

ends_with([Str|_]) ->
    Str1 = lists:reverse(riak_kv_mapred_filters:to_string(Str)),
    fun(V) -> lists:prefix(Str1, lists:reverse(riak_kv_mapred_filters:to_string(V))) end.

%% Logical functions

logical_and(Args) ->
    Funs = lists:map(fun(FilterDef) ->
        {ok, FilterDef1} = build_exprs(FilterDef),
        compose(FilterDef1) end, Args),
    fun(V) ->
        lists:all(fun(Fun) -> Fun(V) end, Funs) end.

logical_or(Args) ->
    Funs = lists:map(fun(FilterDef) ->
        {ok, FilterDef1} = build_exprs(FilterDef),
        compose(FilterDef1) end, Args),
    fun(V) ->
        lists:any(fun(Fun) -> Fun(V) end, Funs) end.

logical_not([FilterDef|_]) ->
    {ok, FilterDef1} = build_exprs(FilterDef),
    FilterFun = compose(FilterDef1),
    fun(V) -> case FilterFun(V) of
        true -> false;
        false -> true end end.

%% Composer

compose([]) ->
    fun(_) -> true end;
compose(Filters) ->
    compose(Filters, fun(V) -> V end).

compose([], F0) -> F0;
compose([Filter1|Filters], F0) ->
    {FilterMod, FilterFun, Args} = Filter1,
    Fun1 = FilterMod:FilterFun(Args),
    F1 = fun(CArgs) -> Fun1(F0(CArgs)) end,
    compose(Filters, F1).

build_exprs(Exprs) ->
    build_exprs(Exprs, []).

build_exprs([], Accum) ->
    {ok, lists:reverse(Accum)};
build_exprs([[FunName|Args]|T], Accum) ->
    case riak_kv_mapred_filters:resolve_name(FunName) of
        error ->
            {error, {bad_filter, FunName}};
        Fun ->
            build_exprs(T, [{riak_kv_mapred_filters, Fun, Args}|Accum])
    end.

%% Resolver

resolve_name(<<"int_to_string">>) ->
    int_to_string;

resolve_name(<<"string_to_int">>) ->
    string_to_int;

resolve_name(<<"string_to_float">>) ->
    string_to_float;

resolve_name(<<"float_to_string">>) ->
    float_to_string;

resolve_name(<<"greater_than">>) ->
    greater_than;

resolve_name(<<"less_than">>) ->
    less_than;

resolve_name(<<"greater_than_eq">>) ->
    greater_than_eq;

resolve_name(<<"less_than_eq">>) ->
    less_than_eq;

resolve_name(<<"between">>) ->
    between;

resolve_name(<<"similar_to">>) ->
    similar_to;

resolve_name(<<"matches">>) ->
    matches;

resolve_name(<<"to_upper">>) ->
    to_upper;

resolve_name(<<"to_lower">>) ->
    to_lower;

resolve_name(<<"set_member">>) ->
    set_member;

resolve_name(<<"neq">>) ->
    neq;

resolve_name(<<"eq">>) ->
    eq;

resolve_name(<<"tokenize">>) ->
    tokenize;

resolve_name(<<"urldecode">>) ->
    urldecode;

resolve_name(<<"sounds_like">>) ->
    sounds_like;

resolve_name(<<"starts_with">>) ->
    starts_with;

resolve_name(<<"ends_with">>) ->
    ends_with;

resolve_name(<<"and">>) ->
    logical_and;

resolve_name(<<"or">>) ->
    logical_or;

resolve_name(<<"not">>) ->
    logical_not;

resolve_name(_Err) ->
    error.


%% Internal

to_string(V) when is_binary(V) ->
    binary_to_list(V);

to_string(V) when is_list(V) ->
    V;

to_string(V) when is_integer(V) ->
    integer_to_list(V);

to_string(V) when is_float(V) ->
    float_to_list(V);

to_string(V) ->
    io_lib:format("~p", [V]).

%%
%% Levenshtein code by Adam Lindberg, Fredrik Svensson via
%% http://www.trapexit.org/String_similar_to_(Levenshtein)
%%
%%------------------------------------------------------------------------------
%% @spec levenshtein(StringA :: string(), StringB :: string()) -> integer()
%% @doc Calculates the Levenshtein distance between two strings
%% @end
%%------------------------------------------------------------------------------
levenshtein(Samestring, Samestring) -> 0;
levenshtein(String, []) -> length(String);
levenshtein([], String) -> length(String);
levenshtein(Source, Target) ->
	levenshtein_rec(Source, Target, lists:seq(0, length(Target)), 1).

%% Recurses over every character in the source string and calculates a list of distances
levenshtein_rec([SrcHead|SrcTail], Target, DistList, Step) ->
	levenshtein_rec(SrcTail, Target, levenshtein_distlist(Target, DistList, SrcHead, [Step], Step), Step + 1);
levenshtein_rec([], _, DistList, _) ->
	lists:last(DistList).

%% Generates a distance list with distance values for every character in the target string
levenshtein_distlist([TargetHead|TargetTail], [DLH|DLT], SourceChar, NewDistList, LastDist) when length(DLT) > 0 ->
	Min = lists:min([LastDist + 1, hd(DLT) + 1, DLH + dif(TargetHead, SourceChar)]),
	levenshtein_distlist(TargetTail, DLT, SourceChar, NewDistList ++ [Min], Min);
levenshtein_distlist([], _, _, NewDistList, _) ->
	NewDistList.

% Calculates the difference between two characters or other values
dif(C, C) -> 0;
dif(_, _) -> 1.

-ifdef(TEST).

% transforms

int_to_string_test() ->
    F = compose([{riak_kv_mapred_filters, int_to_string, []}]),
    F(42) =:= "42".

string_to_int_test() ->
    F = compose([{riak_kv_mapred_filters, string_to_int, []}]),
    F("42") =:= 42.

to_upper_test() ->
    F = compose([{riak_kv_mapred_filters, to_upper, []}]),
    F("basho") =:= "BASHO" andalso F("BASHO") =:= "BASHO".

to_lower_test() ->
    F = compose([{riak_kv_mapred_filters, to_lower, []}]),
    F("BASHO") =:= "basho" andalso F("basho") =:= "basho".

tokenize_test() ->
    F = compose([{riak_kv_mapred_filters, tokenize, ["-", 3]}]),
    F("12-20-2010") =:= "2010".

% filters

greater_than_test() ->
    F = compose([{riak_kv_mapred_filters, greater_than, [2009]}]),
    F(2010) =:= true andalso F(2000) =:= false.

less_than_test() ->
    F = compose([{riak_kv_mapred_filters, less_than, [2009]}]),
    F(2008) =:= true andalso F(2020) =:= false.

greater_than_eq_test() ->
    F = compose([{riak_kv_mapred_filters, greater_than_eq, [2009]}]),
    F(2009) =:= true andalso F(2000) =:= false.

less_than_eq_test() ->
    F = compose([{riak_kv_mapred_filters, less_than_eq, [2009]}]),
    F(2009) =:= true andalso F(2020) =:= false.

between_implicit_inclusive_test() ->
    F = compose([{riak_kv_mapred_filters, between, [2009, 2010]}]),
    F(2009) =:= true andalso F(2020) =:= false.

between_inclusive_test() ->
    F = compose([{riak_kv_mapred_filters, between, [2009, 2010, true]}]),
    F(2009) =:= true andalso F(2020) =:= false.

between_exclusive_test() ->
    F = compose([{riak_kv_mapred_filters, between, [2008, 2010, false]}]),
    F(2009) =:= true andalso F(2010) =:= false.

matches_test() ->
    F = compose([{riak_kv_mapred_filters, matches, ["johnm.*"]}]),
    F("johnm@basho.com") =:= true andalso F("earl@basho.com") =:= false.

neq_test() ->
    F = compose([{riak_kv_mapred_filters, neq, ["12-20-2010"]}]),
    F("01-08-1901") =:= true andalso F("12-20-2010") =:= false.

eq_test() ->
    F = compose([{riak_kv_mapred_filters, neq, ["12-20-2010"]}]),
    F("01-08-1901") =:= false andalso F("12-20-2010") =:= true.

set_member_test() ->
    F = compose([{riak_kv_mapred_filters, set_member, ["12-20-2010", "12-21-2010"]}]),
    F("12-21-2010") =:= true andalso F("12-22-2010") =:= false.

similar_to_test() ->
    F = compose([{riak_kv_mapred_filters, similar_to, ["basho", 1]}]),
    F("baysho") =:= true andalso F("baysho!") =:= false.

starts_with_test() ->
    F = compose([{riak_kv_mapred_filters, starts_with, ["12-20"]}]),
    F("12-20-2010") =:= true andalso F("11-20-2010") =:= false.

ends_with_test() ->
    F = compose([{riak_kv_mapred_filters, ends_with, ["2010"]}]),
    F("12-20-2010") =:= true andalso F("12-20-2011") =:= false.

compose2_test() ->
    F = compose([{riak_kv_mapred_filters, to_lower, []},
                 {riak_kv_mapred_filters, similar_to, ["basho", 3]}]),
    F("BAYSHO") =:= true andalso F("lolcaTS") =:= false.

compose3_test() ->
    F = compose([{riak_kv_mapred_filters, tokenize, ["-", 3]},
                 {riak_kv_mapred_filters, string_to_int, []},
                 {riak_kv_mapred_filters, between, [2009, 2010]}]),
    F("12-20-2009") =:= true andalso F("12-20-2011") =:= false.

build_exprs_test() ->
    Expr = [[<<"tokenize">>, "-", 3]],
    FilterDef = build_exprs(Expr),
    FilterDef =:= [{riak_kv_mapred_filters, tokenize, ["-", 3]}].

logical_and_test() ->
    F = compose([{riak_kv_mapred_filters, logical_and, [
        [[<<"to_lower">>],
         [<<"starts_with">>, "hello"]],
        [[<<"to_lower">>],
         [<<"ends_with">>, "dolly"]]]}]),
    F("hello dolly") =:= true andalso F("dolly hello") =:= false.

logical_or_test() ->
    F = compose([{riak_kv_mapred_filters, logical_or, [
        [[<<"to_lower">>],
         [<<"starts_with">>, "hello"]],
        [[<<"to_lower">>],
         [<<"starts_with">>, "cloned"]]]}]),
    F("hello dolly") =:= true andalso F("cloned sheep") =:= true.

logical_not_test() ->
    F = compose([{riak_kv_mapred_filters, logical_not, [
        [[<<"ends_with">>, "2010"]]]}]),
    F("12-20-2010") =:= false andalso F("12-20-2011") =:= true.

-endif.
