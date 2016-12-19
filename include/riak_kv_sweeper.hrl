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
         fun_type :: riak_kv_sweeper:fun_type(), %% delete_fun | modify_fun | observe_fun
         sweep_fun :: fun(),        %%
         run_interval :: integer() | fun(), %% Defines how often participant wants to run.
         acc :: any(),
         options = [] :: list(),    %% optional values that will be added during sweep
         errors = 0 :: integer(),
         fail_reason
        }).

-record(sweep,
        {
         index,
         state = idle :: idle | running | restart,
         pid :: pid() | undefined,
         results = dict:new(),
         active_participants,  %% Active in current run
         start_time :: erlang:timestamp(),
         end_time :: erlang:timestamp(),
         queue_time :: erlang:timestamp(),
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
         swept_keys = 0  :: non_neg_integer(),
         num_mutated = 0 :: non_neg_integer(),
         num_deleted = 0 :: non_neg_integer(),
         throttle = riak_kv_sweeper_fold:get_sweep_throttle(),
         total_obj_size = 0,

         %% Stats counters to track and report metrics about a sweep
         stat_mutated_objects_counter = 0 :: non_neg_integer(),
         stat_deleted_objects_counter = 0 :: non_neg_integer(),
         stat_swept_keys_counter = 0 :: non_neg_integer(),
         stat_obj_size_counter = 0 :: non_neg_integer()
         }).
