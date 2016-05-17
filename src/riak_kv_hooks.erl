%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_hooks).

%% API
-export([add_conditional_postcommit/1,
         del_conditional_postcommit/1,
         get_conditional_postcommit/2]).

-export([add_timeseries_postcommit/1,
         del_timeseries_postcommit/1,
         call_timeseries_postcommit/3]).

%% Exported for internal use by `riak_kv_sup'
-export([create_table/0]).

%% Types
-type hook()      :: {module(), atom()}.
-type hook_type() :: conditional_postcommit|timeseries_postcommit.
-type bucket() :: riak_object:bucket().
-type bucket_props() :: riak_kv_bucket:props().

%%%===================================================================

%% @doc
%% Called by {@link riak_kv_sup} to create the public ETS table used to
%% track registered hooks. Having `riak_kv_sup' own the table ensures
%% that the table exists aslong as riak_kv is running.
-spec create_table() -> ok.
create_table() ->
    ?MODULE = ets:new(?MODULE, [named_table, public, bag,
                                {write_concurrency, true},
                                {read_concurrency, true}]),
    restore_state(),
    ok.

%% @doc
%% Add a global conditional postcommit hook that is called for each
%% PUT operation. The hook is of the form `{Module, Fun}'. The specified
%% function is called with the relevent bucket, key, and bucket properties
%% at the time of the PUT operation and is expected to return `false' or
%% a normal postcommit hook specification that should be invoked.
-spec add_conditional_postcommit(hook()) -> ok.
add_conditional_postcommit(Hook) ->
    add_hook(conditional_postcommit, Hook).

%% @doc Remove a previously registered conditional postcommit hook
-spec del_conditional_postcommit(hook()) -> ok.
del_conditional_postcommit(Hook) ->
    del_hook(conditional_postcommit, Hook).

%% @doc
%% This function invokes each registered conditional postcommit
%% hook. Each hook will return either `false' or a list of active
%% hooks. This function then returns the combined list of active hooks.
-spec get_conditional_postcommit(bucket(), bucket_props()) -> [any()].
get_conditional_postcommit({BucketType, Bucket}, BucketProps) ->
    Hooks = get_hooks(conditional_postcommit),
    ActiveHooks =
        [ActualHook || {Mod, Fun} <- Hooks,
                       ActualHook <- [Mod:Fun(BucketType, Bucket, BucketProps)],
                       ActualHook =/= false],
    lists:flatten(ActiveHooks);
get_conditional_postcommit(_BKey, _BucketProps) ->
    %% For now, we only support typed buckets.
    [].

%% @doc
%% Add a global timeseries postcommit hook that is called for each
%% timeseries PUT operation. The hook is of the form `{Module, Fun}'. The specified
%% function will be called with three arguments:
%%    {PartitionKey, ListOfEncodedValues}, {BucketType, Bucket}, BucketProps
%%
%% Unlike conditional postcommits, this *is* the postcommit hook. If a
%% table/bucket type should not be subject to realtime replication,
%% the bucket type properties should include `{repl, false}` or
%% `{repl, fullsync}`.
-spec add_timeseries_postcommit(hook()) -> ok.
add_timeseries_postcommit(Hook) ->
    add_hook(timeseries_postcommit, Hook).

%% @doc Remove a previously registered timeseries postcommit hook
-spec del_timeseries_postcommit(hook()) -> ok.
del_timeseries_postcommit(Hook) ->
    del_hook(timeseries_postcommit, Hook).

%% @doc
%% This function invokes each registered timeseries postcommit
%% hook.
-spec call_timeseries_postcommit(tuple(), bucket(), list()) -> ok.
call_timeseries_postcommit({_PartitionIdx, _Data}=Batch, Bucket, BucketProps) ->
    Hooks = get_hooks(timeseries_postcommit),
    lists:foreach(fun({Mod, Fun}) ->
                          Mod:Fun(Batch, Bucket, BucketProps)
                  end, Hooks),
    ok.

%%%===================================================================

-spec add_hook(hook_type(), hook()) -> ok.
add_hook(Type, Hook) ->
    ets:insert(?MODULE, {Type, Hook}),
    save_state(),
    ok.

-spec del_hook(hook_type(), hook()) -> ok.
del_hook(Type, Hook) ->
    ets:delete_object(?MODULE, {Type, Hook}),
    save_state(),
    ok.

-spec get_hooks(hook_type()) -> [hook()].
get_hooks(Type) ->
    [Hook || {_, Hook} <- ets:lookup(?MODULE, Type)].

-ifdef(set_env_options).
-define(SETENV(Application, Par, Val, Opts),
        application:set_env(Application, Par, Val, [{timeout, Opts}])).
-else.
-define(SETENV(Application, Par, Val, Opts),
        application:set_env(Application, Par, Val, Opts)).
-endif.

%% Backup the current ETS state to the application environment just in case
%% riak_kv_sup dies and the ETS table is lost.
-spec save_state() -> ok.
save_state() ->
    Hooks = ets:tab2list(?MODULE),
    ok = ?SETENV(riak_kv, riak_kv_hooks, Hooks, infinity),
    ok.

%% Restore registered hooks in the unlikely case that riak_kv_sup died and
%% the ETS table was lost/recreated.
-spec restore_state() -> ok.
restore_state() ->
    case application:get_env(riak_kv, riak_kv_hooks) of
        undefined ->
            ok;
        {ok, Hooks} ->
            true = ets:insert_new(?MODULE, Hooks),
            ok
    end.
