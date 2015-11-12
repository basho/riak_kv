%% Copyright (c) 2011-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% Used as configuration for sweep participants.
-record(sweep_participant, 
        {
         description :: string(),   %% Human readeble description of the user.
         module :: atom(),          %% module where the sweep call back lives.
         fun_type :: integer(),     %% ?DELETE_FUN, ?MODIFY_FUN or ?OBSERV_FUN
         sweep_fun :: fun(),        %%
         run_interval :: integer() | fun(), %% Defines how often participant wants to run.
         acc :: any(),
         options = [] :: list(),
         errors = 0 :: integer(),
         fail_reason
        }).

%% fun_type used to be able to sort the
%% participating funs.
-define(DELETE_FUN, 1).
-define(MODIFY_FUN, 3).
-define(OBSERV_FUN, 5).

-record(sweep,
        {
         index,
         state = idle :: idle | running,
         pid :: pid() | undefined,
         worker_pid :: pid() | undefined,
         results = dict:new(),
         active_participants,  %% Active in current run
         start_time :: erlang:timestamp(),
         end_time :: erlang:timestamp(),
         estimated_keys :: {non_neg_integer(), erlang:timestamp()},
         swept_keys :: non_neg_integer() | undefined
        }).

%% Sweep accumulator
-record(sa,
        {index,
         bucket_props = dict:new(),
         active_p,
         failed_p = [],
         succ_p = [],
         estimated_keys = 0  :: non_neg_integer(),
         swept_keys = 0  :: non_neg_integer()
         }).

