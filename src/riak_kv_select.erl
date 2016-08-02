%%%-------------------------------------------------------------------
%%%
%%% riak_kv_select: Upgrade and downgrade for the riak_select_v* records.
%%%
%%% Copyright (C) 2016 Basho Technologies, Inc. All rights reserved
%%%
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
%%%
%%%-------------------------------------------------------------------

-module(riak_kv_select).

-export([convert/2]).
-export([current_version/0]).
-export([first_version/0]).
-export([is_sql_select_record/1]).

-include("riak_kv_ts.hrl").

%% Return the version as an integer that the select record is currently using.
-spec current_version() -> integer().
current_version() ->
    select_record_version(?SQL_SELECT_RECORD_NAME).

%% Return the first version that was declared for the select record.
-spec first_version() -> integer().
first_version() ->
    1.

%% Convert a select record to a different version.
-spec convert(integer(), Select1::tuple()) -> Select2::tuple().
convert(Version, Select) when is_integer(Version) ->
    CurrentVersion = select_record_version(element(1, Select)),
    if
        Version == CurrentVersion ->
            Select;
        Version > CurrentVersion ->
            VersionSteps = lists:seq(CurrentVersion, Version),
            upgrade_select(VersionSteps, Select);
        Version < CurrentVersion ->
            VersionSteps = lists:seq(CurrentVersion, Version, -1),
            downgrade_select(VersionSteps, Select)
    end.

%% Iterate over the versions and upgrade the record, from 1 to 2, 2 to 3 etc.
upgrade_select([_], Select) ->
    Select;
upgrade_select([1,2 = To|Tail], Select1) ->
    Select2 = #riak_select_v2{
        'SELECT'      = Select1#riak_select_v1.'SELECT',
        'FROM'        = Select1#riak_select_v1.'FROM',
        'WHERE'       = Select1#riak_select_v1.'WHERE',
        'ORDER BY'    = Select1#riak_select_v1.'ORDER BY',
        'LIMIT'       = Select1#riak_select_v1.'LIMIT',
        helper_mod    = Select1#riak_select_v1.helper_mod,
        partition_key = Select1#riak_select_v1.partition_key,
        is_executable = Select1#riak_select_v1.is_executable,
        type          = Select1#riak_select_v1.type,
        cover_context = Select1#riak_select_v1.cover_context,
        local_key     = Select1#riak_select_v1.local_key,
        group_by      = ?GROUP_BY_DEFAULT
    },
    upgrade_select([To|Tail], Select2).


%% Iterate over the versions backwards to downgrade, from 3 to 2 then 2 to 1 etc.
downgrade_select([_], Select) ->
    Select;
downgrade_select([2,1=To|Tail], Select1) ->
    Select2 = #riak_select_v1{
        'SELECT'      = Select1#riak_select_v2.'SELECT',
        'FROM'        = Select1#riak_select_v2.'FROM',
        'WHERE'       = Select1#riak_select_v2.'WHERE',
        'ORDER BY'    = Select1#riak_select_v2.'ORDER BY',
        'LIMIT'       = Select1#riak_select_v2.'LIMIT',
        helper_mod    = Select1#riak_select_v2.helper_mod,
        partition_key = Select1#riak_select_v2.partition_key,
        is_executable = Select1#riak_select_v2.is_executable,
        type          = Select1#riak_select_v2.type,
        cover_context = Select1#riak_select_v2.cover_context,
        local_key     = Select1#riak_select_v2.local_key
    },
    downgrade_select([To|Tail], Select2).

%%
select_record_version(RecordName) ->
    case RecordName of
        riak_select_v1 -> 1;
        riak_select_v2 -> 2
    end.

%%
-spec is_sql_select_record(tuple()) -> boolean().
is_sql_select_record(#riak_select_v1{ }) -> true;
is_sql_select_record(#riak_select_v2{ }) -> true;
is_sql_select_record(_)                  -> false.

%%%
%%% TESTS
%%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

new_select_v1() ->
    {riak_select_v1, undefined, << >>, [], [], [], undefined, none, false,
     sql, undefined, undefined}.

new_select_v2() ->
    {riak_select_v2, undefined, << >>, [], [], [], undefined, none, false,
     sql, undefined, undefined, ?GROUP_BY_DEFAULT}.

upgrade_select_1_2_test() ->
    ?assertEqual(
        new_select_v2(),
        convert(2, new_select_v1())
    ).

downgrade_select_2_1_test() ->
    ?assertEqual(
        new_select_v1(),
        convert(1, new_select_v2())
    ).

convert_when_equal_1_1_test() ->
    Select = setelement(2,new_select_v1(),oko),
    ?assertEqual(
        Select,
        convert(1, Select)
    ).

convert_when_equal_2_2_test() ->
    Select = setelement(2,new_select_v2(),oko),
    ?assertEqual(
        Select,
        convert(2, Select)
    ).

-endif.
