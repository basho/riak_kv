%% -------------------------------------------------------------------
%%
%% riak_console: interface for Riak admin commands
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

%% @doc interface for Riak admin commands

-module(riak_kv_console).

-export([join/1,
         staged_join/1,
         leave/1,
         remove/1,
         status/1,
         vnode_status/1,
         reip/1,
         ringready/1,
         cluster_info/1,
         down/1,
         aae_status/1,
         repair_2i/1,
         reformat_indexes/1,
         reformat_objects/1,
         reload_code/1,
         bucket_type_status/1,
         bucket_type_activate/1,
         bucket_type_create/1,
         bucket_type_update/1,
         bucket_type_reset/1,
         bucket_type_list/1]).

-export([ensemble_status/1]).

%% Reused by Yokozuna for printing AAE status.
-export([aae_exchange_status/1,
         aae_repair_status/1,
         aae_tree_status/1]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

join([NodeStr]) ->
    join(NodeStr, fun riak_core:join/1,
         "Sent join request to ~s~n", [NodeStr]).

staged_join([NodeStr]) ->
    Node = list_to_atom(NodeStr),
    join(NodeStr, fun riak_core:staged_join/1,
         "Success: staged join request for ~p to ~p~n", [node(), Node]).

join(NodeStr, JoinFn, SuccessFmt, SuccessArgs) ->
    try
        case JoinFn(NodeStr) of
            ok ->
                io:format(SuccessFmt, SuccessArgs),
                ok;
            {error, not_reachable} ->
                io:format("Node ~s is not reachable!~n", [NodeStr]),
                error;
            {error, different_ring_sizes} ->
                io:format("Failed: ~s has a different ring_creation_size~n",
                          [NodeStr]),
                error;
            {error, unable_to_get_join_ring} ->
                io:format("Failed: Unable to get ring from ~s~n", [NodeStr]),
                error;
            {error, not_single_node} ->
                io:format("Failed: This node is already a member of a "
                          "cluster~n"),
                error;
            {error, self_join} ->
                io:format("Failed: This node cannot join itself in a "
                          "cluster~n"),
                error;
            {error, _} ->
                io:format("Join failed. Try again in a few moments.~n", []),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Join failed ~p:~p", [Exception, Reason]),
            io:format("Join failed, see log for details~n"),
            error
    end.


leave([]) ->
    try
        case riak_core:leave() of
            ok ->
                io:format("Success: ~p will shutdown after handing off "
                          "its data~n", [node()]),
                ok;
            {error, already_leaving} ->
                io:format("~p is already in the process of leaving the "
                          "cluster.~n", [node()]),
                ok;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [node()]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [node()]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Leave failed ~p:~p", [Exception, Reason]),
            io:format("Leave failed, see log for details~n"),
            error
    end.

remove([Node]) ->
    try
        case riak_core:remove(list_to_atom(Node)) of
            ok ->
                io:format("Success: ~p removed from the cluster~n", [Node]),
                ok;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [Node]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Remove failed ~p:~p", [Exception, Reason]),
            io:format("Remove failed, see log for details~n"),
            error
    end.

down([Node]) ->
    try
        case riak_core:down(list_to_atom(Node)) of
            ok ->
                io:format("Success: ~p marked as down~n", [Node]),
                ok;
            {error, is_up} ->
                io:format("Failed: ~s is up~n", [Node]),
                error;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [Node]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Down failed ~p:~p", [Exception, Reason]),
            io:format("Down failed, see log for details~n"),
            error
    end.

-spec(status([]) -> ok).
status([]) ->
    try
        Stats = riak_kv_status:statistics(),
        StatString = format_stats(Stats,
                    ["-------------------------------------------\n",
                     io_lib:format("1-minute stats for ~p~n",[node()])]),
        io:format("~s\n", [StatString])
    catch
        Exception:Reason ->
            lager:error("Status failed ~p:~p", [Exception,
                    Reason]),
            io:format("Status failed, see log for details~n"),
            error
    end.

-spec(vnode_status([]) -> ok).
vnode_status([]) ->
    try
        case riak_kv_status:vnode_status() of
            [] ->
                io:format("There are no active vnodes.~n");
            Statuses ->
                io:format("~s~n-------------------------------------------~n~n",
                          ["Vnode status information"]),
                print_vnode_statuses(lists:sort(Statuses))
        end
    catch
        Exception:Reason ->
            lager:error("Backend status failed ~p:~p", [Exception,
                    Reason]),
            io:format("Backend status failed, see log for details~n"),
            error
    end.

reip([OldNode, NewNode]) ->
    try
        %% reip is called when node is down (so riak_core_ring_manager is not running),
        %% so it has to use the basic ring operations.
        %%
        %% Do *not* convert to use riak_core_ring_manager:ring_trans.
        %%
        case application:load(riak_core) of
            %% a process, such as cuttlefish, may have already loaded riak_core
            {error,{already_loaded,riak_core}} -> ok;
            ok -> ok
        end,
        RingStateDir = app_helper:get_env(riak_core, ring_state_dir),
        {ok, RingFile} = riak_core_ring_manager:find_latest_ringfile(),
        BackupFN = filename:join([RingStateDir, filename:basename(RingFile)++".BAK"]),
        {ok, _} = file:copy(RingFile, BackupFN),
        io:format("Backed up existing ring file to ~p~n", [BackupFN]),
        Ring = riak_core_ring_manager:read_ringfile(RingFile),
        NewRing = riak_core_ring:rename_node(Ring, OldNode, NewNode),
        ok = riak_core_ring_manager:do_write_ringfile(NewRing),
        io:format("New ring file written to ~p~n",
            [element(2, riak_core_ring_manager:find_latest_ringfile())])
    catch
        Exception:Reason ->
            io:format("Reip failed ~p:~p", [Exception, Reason]),
            error
    end.

%% Check if all nodes in the cluster agree on the partition assignment
-spec(ringready([]) -> ok | error).
ringready([]) ->
    try
        case riak_core_status:ringready() of
            {ok, Nodes} ->
                io:format("TRUE All nodes agree on the ring ~p\n", [Nodes]);
            {error, {different_owners, N1, N2}} ->
                io:format("FALSE Node ~p and ~p list different partition owners\n", [N1, N2]),
                error;
            {error, {nodes_down, Down}} ->
                io:format("FALSE ~p down.  All nodes need to be up to check.\n", [Down]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Ringready failed ~p:~p", [Exception,
                    Reason]),
            io:format("Ringready failed, see log for details~n"),
            error
    end.

cluster_info([OutFile|Rest]) ->
    try
        case lists:reverse(atomify_nodestrs(Rest)) of
            [] ->
                cluster_info:dump_all_connected(OutFile);
            Nodes ->
                cluster_info:dump_nodes(Nodes, OutFile)
        end
    catch
        error:{badmatch, {error, eacces}} ->
            io:format("Cluster_info failed, permission denied writing to ~p~n", [OutFile]);
        error:{badmatch, {error, enoent}} ->
            io:format("Cluster_info failed, no such directory ~p~n", [filename:dirname(OutFile)]);
        error:{badmatch, {error, enotdir}} ->
            io:format("Cluster_info failed, not a directory ~p~n", [filename:dirname(OutFile)]);
        Exception:Reason ->
            lager:error("Cluster_info failed ~p:~p",
                [Exception, Reason]),
            io:format("Cluster_info failed, see log for details~n"),
            error
    end.

reload_code([]) ->
    case app_helper:get_env(riak_kv, add_paths) of
        List when is_list(List) ->
            _ = [ reload_path(filename:absname(Path)) || Path <- List ],
            ok;
        _ -> ok
    end.

reload_path(Path) ->
    {ok, Beams} = file:list_dir(Path),
    [ reload_file(filename:absname(Beam, Path)) || Beam <- Beams, ".beam" == filename:extension(Beam) ].

reload_file(Filename) ->
    Mod = list_to_atom(filename:basename(Filename, ".beam")),
    case code:is_loaded(Mod) of
        {file, Filename} ->
            code:soft_purge(Mod),
            {module, Mod} = code:load_file(Mod),
            io:format("Reloaded module ~w from ~s.~n", [Mod, Filename]);
        {file, Other} ->
            io:format("CONFLICT: Module ~w originally loaded from ~s, won't reload from ~s.~n", [Mod, Other, Filename]);
        _ ->
            io:format("Module ~w not yet loaded, skipped.~n", [Mod])
    end.

aae_status([]) ->
    ExchangeInfo = riak_kv_entropy_info:compute_exchange_info(),
    aae_exchange_status(ExchangeInfo),
    io:format("~n"),
    TreeInfo = riak_kv_entropy_info:compute_tree_info(),
    aae_tree_status(TreeInfo),
    io:format("~n"),
    aae_repair_status(ExchangeInfo).

aae_exchange_status(ExchangeInfo) ->
    io:format("~s~n", [string:centre(" Exchanges ", 79, $=)]),
    io:format("~-49s  ~-12s  ~-12s~n", ["Index", "Last (ago)", "All (ago)"]),
    io:format("~79..-s~n", [""]),
    _ = [begin
         Now = os:timestamp(),
         LastStr = format_timestamp(Now, LastTS),
         AllStr = format_timestamp(Now, AllTS),
         io:format("~-49b  ~-12s  ~-12s~n", [Index, LastStr, AllStr]),
         ok
     end || {Index, LastTS, AllTS, _Repairs} <- ExchangeInfo],
    ok.

aae_repair_status(ExchangeInfo) ->
    io:format("~s~n", [string:centre(" Keys Repaired ", 79, $=)]),
    io:format("~-49s  ~s  ~s  ~s~n", ["Index",
                                      string:centre("Last", 8),
                                      string:centre("Mean", 8),
                                      string:centre("Max", 8)]),
    io:format("~79..-s~n", [""]),
    _ = [begin
         io:format("~-49b  ~s  ~s  ~s~n", [Index,
                                           string:centre(integer_to_list(Last), 8),
                                           string:centre(integer_to_list(Mean), 8),
                                           string:centre(integer_to_list(Max), 8)]),
         ok
     end || {Index, _, _, {Last,_Min,Max,Mean}} <- ExchangeInfo],
    ok.

aae_tree_status(TreeInfo) ->
    io:format("~s~n", [string:centre(" Entropy Trees ", 79, $=)]),
    io:format("~-49s  Built (ago)~n", ["Index"]),
    io:format("~79..-s~n", [""]),
    _ = [begin
         Now = os:timestamp(),
         BuiltStr = format_timestamp(Now, BuiltTS),
         io:format("~-49b  ~s~n", [Index, BuiltStr]),
         ok
     end || {Index, BuiltTS} <- TreeInfo],
    ok.

format_timestamp(_Now, undefined) ->
    "--";
format_timestamp(Now, TS) ->
    riak_core_format:human_time_fmt("~.1f", timer:now_diff(Now, TS)).

parse_int(IntStr) ->
    try
        list_to_integer(IntStr)
    catch
        error:badarg ->
            undefined
    end.

index_reformat_options([], Opts) ->
    Defaults = [{concurrency, 2}, {batch_size, 100}],
    AddIfAbsent =
        fun({Name,Val}, Acc) ->
            case lists:keymember(Name, 1, Acc) of
                true ->
                    Acc;
                false ->
                    [{Name, Val} | Acc]
            end
        end,
    lists:foldl(AddIfAbsent, Opts, Defaults);
index_reformat_options(["--downgrade"], Opts) ->
    [{downgrade, true} | Opts];
index_reformat_options(["--downgrade" | More], _Opts) ->
    io:format("Invalid arguments after downgrade switch : ~p~n", [More]),
    undefined;
index_reformat_options([IntStr | Rest], Opts) ->
    HasConcurrency = lists:keymember(concurrency, 1, Opts),
    HasBatchSize = lists:keymember(batch_size, 1, Opts),
    case {parse_int(IntStr), HasConcurrency, HasBatchSize} of
        {_, true, true} ->
            io:format("Expected --downgrade instead of ~p~n", [IntStr]),
            undefined;
        {undefined, _, _ } ->
            io:format("Expected integer parameter instead of ~p~n", [IntStr]),
            undefined;
        {IntVal, false, false} ->
            index_reformat_options(Rest, [{concurrency, IntVal} | Opts]);
        {IntVal, true, false} ->
            index_reformat_options(Rest, [{batch_size, IntVal} | Opts])
    end;
index_reformat_options(_, _) ->
    undefined.

reformat_indexes(Args) ->
    Opts = index_reformat_options(Args, []),
    case Opts of
        undefined ->
            io:format("Expected options: <concurrency> <batch size> [--downgrade]~n"),
            ok;
        _ ->
            start_reformat(riak_kv_util, fix_incorrect_index_entries, [Opts]),
            io:format("index reformat started with options ~p ~n", [Opts]),
            io:format("check console.log for status information~n"),
            ok
    end.

reformat_objects([KillHandoffsStr]) ->
    reformat_objects([KillHandoffsStr, "2"]);
reformat_objects(["true", ConcurrencyStr | _Rest]) ->
    reformat_objects([true, ConcurrencyStr]);
reformat_objects(["false", ConcurrencyStr | _Rest]) ->
    reformat_objects([false, ConcurrencyStr]);
reformat_objects([KillHandoffs, ConcurrencyStr]) when is_atom(KillHandoffs) ->
    case parse_int(ConcurrencyStr) of
        C when C > 0 ->
            start_reformat(riak_kv_reformat, run,
                           [v0, [{concurrency, C}, {kill_handoffs, KillHandoffs}]]),
            io:format("object reformat started with concurrency ~p~n", [C]),
            io:format("check console.log for status information~n");
        _ ->
            io:format("ERROR: second argument must be an integer greater than zero.~n"),
            error

    end;
reformat_objects(_) ->
    io:format("ERROR: first argument must be either \"true\" or \"false\".~n"),
    error.

start_reformat(M, F, A) ->
    spawn(fun() -> run_reformat(M, F, A) end).

run_reformat(M, F, A) ->
    try erlang:apply(M, F, A)
    catch
        Err:Reason ->
            lager:error("index reformat crashed with error type ~p and reason: ~p",
                        [Err, Reason])
    end.

bucket_type_status([TypeStr]) ->
    Type = unicode:characters_to_binary(TypeStr, utf8, utf8),
    Return = bucket_type_print_status(Type, riak_core_bucket_type:status(Type)),
    bucket_type_print_props(bucket_type_raw_props(Type)),
    Return.


bucket_type_raw_props(<<"default">>) ->
    riak_core_bucket_props:defaults();
bucket_type_raw_props(Type) ->
    riak_core_claimant:get_bucket_type(Type, undefined, false).

bucket_type_print_status(Type, undefined) ->
    io:format("~ts is not an existing bucket type~n", [Type]),
    {error, undefined};
bucket_type_print_status(Type, created) ->
    io:format("~ts has been created but cannot be activated yet~n", [Type]),
    {error, not_ready};
bucket_type_print_status(Type, ready) ->
    io:format("~ts has been created and may be activated~n", [Type]),
    ok;
bucket_type_print_status(Type, active) ->
    io:format("~ts is active~n", [Type]),
    ok.

bucket_type_print_props(undefined) ->
    ok;
bucket_type_print_props(Props) ->
    io:format("~n"),
    [io:format("~p: ~p~n", [K, V]) || {K, V} <- Props].

bucket_type_activate([TypeStr]) ->
    Type = unicode:characters_to_binary(TypeStr, utf8, utf8),
    IsFirst = bucket_type_is_first(),
    bucket_type_print_activate_result(Type, riak_core_bucket_type:activate(Type), IsFirst).

bucket_type_print_activate_result(Type, ok, IsFirst) ->
    io:format("~ts has been activated~n", [Type]),
    case IsFirst of
        true ->
            io:format("~n"),
            io:format("WARNING: Nodes in this cluster can no longer be~n"
                      "downgraded to a version of Riak prior to 2.0~n");
        false ->
            ok
    end;
bucket_type_print_activate_result(Type, {error, undefined}, _IsFirst) ->
    bucket_type_print_status(Type, undefined);
bucket_type_print_activate_result(Type, {error, not_ready}, _IsFirst) ->
    bucket_type_print_status(Type, created).

bucket_type_create([TypeStr, ""]) ->
    Type = unicode:characters_to_binary(TypeStr, utf8, utf8),
    EmptyProps = {struct, [{<<"props">>, {struct, []}}]},
    CreateTypeFn = fun riak_core_bucket_type:create/2,
    bucket_type_create(CreateTypeFn, Type, EmptyProps);
bucket_type_create([TypeStr, PropsStr]) ->
    Type = unicode:characters_to_binary(TypeStr, utf8, utf8),
    CreateTypeFn =
        fun(Props) ->
            Result = riak_core_bucket_type:create(Type, Props),
            bucket_type_print_create_result(Type, Result)
        end,
    bucket_type_create(CreateTypeFn, Type, catch mochijson2:decode(PropsStr)).

-spec bucket_type_create(
        CreateTypeFn :: fun(([proplists:property()]) -> ok),
        Type :: binary(),
        JSON :: any()) -> ok | error.
bucket_type_create(CreateTypeFn, Type, {struct, Fields}) ->
    case Fields of
        [{<<"props", _/binary>>, {struct, Props1}}] ->
            {ok, Props2} = maybe_parse_table_def(Type, Props1),
            Props3 = [riak_kv_wm_utils:erlify_bucket_prop(P) || P <- Props2],
            CreateTypeFn(Props3);
        _ ->
            io:format("Cannot create bucket type ~ts: no props field found in json~n", [Type]),
            error
    end;
bucket_type_create(_, Type, _) ->
    io:format("Cannot create bucket type ~ts: invalid json~n", [Type]),
    error.

%%
-spec maybe_parse_table_def(BucketType :: binary(),
                            Props :: list(proplists:property())) -> 
        {ok, Props2 :: [proplists:property()]} | {error, any()}.
maybe_parse_table_def(BucketType, Props) ->
    case lists:keytake(<<"table_def">>, 1, Props) of
        false ->
            {ok, Props};
        {value, {<<"table_def">>, TableDef}, PropsNoDef} ->
            case riak_ql_parser:parse(riak_ql_lexer:get_tokens(binary_to_list(TableDef))) of
                {ok, DDL} ->
                    ok = assert_type_and_table_name_same(BucketType, DDL),
                    ok = try_compile_ddl(DDL),
                    {ok, [{<<"ddl">>, DDL} | PropsNoDef]};
                {error, _} = E ->
                    E
            end
    end.

%%
assert_type_and_table_name_same(BucketType, #ddl_v1{ bucket = BucketType }) ->
    ok;
assert_type_and_table_name_same(BucketType, #ddl_v1{ bucket = TableName }) ->
    io:format(
        "Error, the bucket type could not be the created. The bucket type and table name must be the same~n"
        "    bucket type was: ~s~n"
        "    table name was:  ~s~n",
        [BucketType, TableName]),
    {error, {bucket_type_and_table_name_different, BucketType, TableName}}.

%% Attempt to compile the DDL but don't do anything with the output, this is
%% catch failures as early as possible. Also the error messages are easy to
%% return at this point.
try_compile_ddl(DDL) ->
    {_, AST} = riak_ql_ddl_compiler:compile(DDL),
    {ok, _, _} = compile:forms(AST),
    ok.

bucket_type_print_create_result(Type, ok) ->
    io:format("~ts created~n", [Type]),
    case bucket_type_is_first() of
        true ->
            io:format("~n"),
            io:format("WARNING: After activating ~ts, nodes in this cluster~n"
                      "can no longer be downgraded to a version of Riak "
                      "prior to 2.0~n", [Type]);
        false ->
            ok
    end;
bucket_type_print_create_result(Type, {error, Reason}) ->
    io:format("Error creating bucket type ~ts:~n", [Type]),
    io:format(bucket_error_xlate(Reason)),
    io:format("~n"),
    error.

bucket_type_update([TypeStr, PropsStr]) ->
    Type = unicode:characters_to_binary(TypeStr, utf8, utf8),
    bucket_type_update(Type, catch mochijson2:decode(PropsStr)).

bucket_type_update(Type, {struct, Fields}) ->
    case proplists:get_value(<<"props">>, Fields) of
        {struct, Props} ->
            ErlProps = [riak_kv_wm_utils:erlify_bucket_prop(P) || P <- Props],
            bucket_type_print_update_result(Type, riak_core_bucket_type:update(Type, ErlProps));
        _ ->
            io:format("Cannot create bucket type ~ts: no props field found in json~n", [Type]),
            error
    end;
bucket_type_update(Type, _) ->
    io:format("Cannot update bucket type: ~ts: invalid json~n", [Type]),
    error.

bucket_type_print_update_result(Type, ok) ->
    io:format("~ts updated~n", [Type]);
bucket_type_print_update_result(Type, {error, Reason}) ->
    io:format("Error updating bucket type ~ts:~n", [Type]),
    io:format(bucket_error_xlate(Reason)),
    io:format("~n"),
    error.

bucket_type_reset([TypeStr]) ->
    Type = unicode:characters_to_binary(TypeStr, utf8, utf8),
    bucket_type_print_reset_result(Type, riak_core_bucket_type:reset(Type)).

bucket_type_print_reset_result(Type, ok) ->
    io:format("~ts reset~n", [Type]);
bucket_type_print_reset_result(Type, {error, Reason}) ->
    io:format("Error updating bucket type ~ts: ~p~n", [Type, Reason]),
    error.

bucket_type_list([]) ->
    It = riak_core_bucket_type:iterator(),
    io:format("default (active)~n"),
    bucket_type_print_list(It).

bucket_type_print_list(It) ->
    case riak_core_bucket_type:itr_done(It) of
        true ->
            riak_core_bucket_type:itr_close(It);
        false ->
            {Type, Props} = riak_core_bucket_type:itr_value(It),
            ActiveStr = case proplists:get_value(active, Props, false) of
                            true -> "active";
                            false -> "not active"
                        end,

            io:format("~ts (~s)~n", [Type, ActiveStr]),
            bucket_type_print_list(riak_core_bucket_type:itr_next(It))
    end.

bucket_type_is_first() ->
    It = riak_core_bucket_type:iterator(),
    bucket_type_is_first(It, false).

bucket_type_is_first(It, true) ->
    %% found an active bucket type
    riak_core_bucket_type:itr_close(It),
    false;
bucket_type_is_first(It, false) ->
    case riak_core_bucket_type:itr_done(It) of
        true ->
            %% no active bucket types found
            ok = riak_core_bucket_type:itr_close(It),
            true;
        false ->
            {_, Props} = riak_core_bucket_type:itr_value(It),
            Active = proplists:get_value(active, Props, false),
            bucket_type_is_first(riak_core_bucket_type:itr_next(It), Active)
    end.

repair_2i(["status"]) ->
    try
        Status = riak_kv_2i_aae:get_status(),
        Report = riak_kv_2i_aae:to_report(Status),
        io:format("2i repair status is running:\n~s", [Report]),
        ok
    catch
        exit:{noproc, _NoProcErr} ->
            io:format("2i repair is not running\n", []),
            ok
    end;
repair_2i(["kill"]) ->
    case whereis(riak_kv_2i_aae) of
        Pid when is_pid(Pid) ->
            try
                riak_kv_2i_aae:stop(60000)
            catch
                _:_->
                    lager:warning("Asking nicely did not work."
                                  " Will try a hammer"),
                    ok
            end,
            Mon = monitor(process, riak_kv_2i_aae),
            exit(Pid, kill),
            receive
                {'DOWN', Mon, _, _, _} ->
                    lager:info("2i repair process has been killed by user"
                               " request"),
                    io:format("The 2i repair process has ceased to be.\n"
                              "Since it was killed forcibly, you may have to "
                              "wait some time\n"
                              "for all internal locks to be released before "
                              "trying again\n", []),
                    ok
            end;
        undefined ->
            io:format("2i repair is not running\n"),
            ok
    end;
repair_2i(Args) ->
    case validate_repair_2i_args(Args) of
        {ok, IdxList, DutyCycle} ->
            case length(IdxList) < 5 of
                true ->
                    io:format("Will repair 2i on these partitions:\n", []),
                    _ = [io:format("\t~p\n", [Idx]) || Idx <- IdxList],
                    ok;
                false ->
                    io:format("Will repair 2i data on ~p partitions\n",
                              [length(IdxList)]),
                    ok
            end,
            Ret = riak_kv_2i_aae:start(IdxList, DutyCycle),
            case Ret of
                {ok, _Pid} ->
                    io:format("Watch the logs for 2i repair progress reports\n", []),
                    ok;
                {error, {lock_failed, not_built}} ->
                    io:format("Error: The AAE tree for that partition has not been built yet\n", []),
                    error;
                {error, {lock_failed, LockErr}} ->
                    io:format("Error: Could not get a lock on AAE tree for"
                              " partition ~p : ~p\n", [hd(IdxList), LockErr]),
                    error;
                {error, already_running} ->
                    io:format("Error: 2i repair is already running. Check the logs for progress\n", []),
                    error;
                {error, early_exit} ->
                    io:format("Error: 2i repair finished immediately. Check the logs for details\n", []),
                    error;
                {error, Reason} ->
                    io:format("Error running 2i repair : ~p\n", [Reason]),
                    error
            end;
        {error, aae_disabled} ->
            io:format("Error: AAE is currently not enabled\n", []),
            error;
        {error, Reason} ->
            io:format("Error: ~p\n", [Reason]),
            io:format("Usage: riak-admin repair-2i [--speed [1-100]] <Idx> ...\n", []),
            io:format("Speed defaults to 100 (full speed)\n", []),
            io:format("If no partitions are given, all partitions in the\n"
                      "node are repaired\n", []),
            error
    end.

ensemble_status([]) ->
    riak_kv_ensemble_console:ensemble_overview();
ensemble_status(["root"]) ->
    riak_kv_ensemble_console:ensemble_detail(root);
ensemble_status([Str]) ->
    N = parse_int(Str),
    case N of
        undefined ->
            io:format("No such ensemble: ~s~n", [Str]);
        _ ->
            riak_kv_ensemble_console:ensemble_detail(N)
    end.

%%%===================================================================
%%% Private
%%%===================================================================

validate_repair_2i_args(Args) ->
    case riak_kv_entropy_manager:enabled() of
        true ->
            parse_repair_2i_args(Args);
        false ->
            {error, aae_disabled}
    end.

parse_repair_2i_args(["--speed", DutyCycleStr | Partitions]) ->
    DutyCycle = parse_int(DutyCycleStr),
    case DutyCycle of
        undefined ->
            {error, io_lib:format("Invalid speed value (~s). It should be a " ++
                                  "number between 1 and 100",
                                  [DutyCycleStr])};
        _ ->
            parse_repair_2i_args(DutyCycle, Partitions)
    end;
parse_repair_2i_args(Partitions) ->
    parse_repair_2i_args(100, Partitions).

parse_repair_2i_args(DutyCycle, Partitions) ->
    case get_2i_repair_indexes(Partitions) of
        {ok, IdxList} ->
            {ok, IdxList, DutyCycle};
        {error, Reason} ->
            {error, Reason}
    end.

get_2i_repair_indexes([]) ->
    AllVNodes = riak_core_vnode_manager:all_vnodes(riak_kv_vnode),
    {ok, [Idx || {riak_kv_vnode, Idx, _} <- AllVNodes]};
get_2i_repair_indexes(IntStrs) ->
    {ok, NodeIdxList} = get_2i_repair_indexes([]),
    F =
    fun(_, {error, Reason}) ->
            {error, Reason};
       (IntStr, Acc) ->
            case parse_int(IntStr) of
                undefined ->
                    {error, io_lib:format("~s is not an integer\n", [IntStr])};
                Int ->
                    case lists:member(Int, NodeIdxList) of
                        true ->
                            Acc ++ [Int];
                        false ->
                            {error,
                             io_lib:format("Partition ~p does not belong"
                                           ++ " to this node\n",
                                           [Int])}
                    end
            end
    end,
    IdxList = lists:foldl(F, [], IntStrs),
    case IdxList of
        {error, Reason} ->
            {error, Reason};
        _ ->
            {ok, IdxList}
    end.

format_stats([], Acc) ->
    lists:reverse(Acc);
format_stats([{Stat, V}|T], Acc) ->
    format_stats(T, [io_lib:format("~p : ~p~n", [Stat, V])|Acc]).

atomify_nodestrs(Strs) ->
    lists:foldl(fun("local", Acc) -> [node()|Acc];
                   (NodeStr, Acc) -> try
                                         [list_to_existing_atom(NodeStr)|Acc]
                                     catch error:badarg ->
                                         io:format("Bad node: ~s\n", [NodeStr]),
                                         Acc
                                     end
                end, [], Strs).

print_vnode_statuses([]) ->
    ok;
print_vnode_statuses([{VNodeIndex, StatusData} | RestStatuses]) ->
    io:format("VNode: ~p~n", [VNodeIndex]),
    print_vnode_status(StatusData),
    io:format("~n"),
    print_vnode_statuses(RestStatuses).

print_vnode_status([]) ->
    ok;
print_vnode_status([{backend_status,
                     Backend,
                     StatusItem} | RestStatusItems]) ->
    if is_binary(StatusItem) ->
            StatusString = binary_to_list(StatusItem),
            io:format("Backend: ~p~nStatus: ~n~s~n",
                      [Backend, string:strip(StatusString)]);
       true ->
            io:format("Backend: ~p~nStatus: ~n~p~n",
                      [Backend, StatusItem])
    end,
    print_vnode_status(RestStatusItems);
print_vnode_status([StatusItem | RestStatusItems]) ->
    if is_binary(StatusItem) ->
            StatusString = binary_to_list(StatusItem),
            io:format("Status: ~n~s~n",
                      [string:strip(StatusString)]);
       true ->
            io:format("Status: ~n~p~n", [StatusItem])
    end,
    print_vnode_status(RestStatusItems).

bucket_error_xlate(Errors) when is_list(Errors) ->
    string:join(
      lists:map(fun bucket_error_xlate/1, Errors),
      "~n");
bucket_error_xlate({Property, not_integer}) ->
    [atom_to_list(Property), " must be an integer"];

%% `riak_kv_bucket:coerce_bool/1` allows for other values but let's
%% not encourage bad behavior
bucket_error_xlate({Property, not_boolean}) ->
    [atom_to_list(Property), " should be \"true\" or \"false\""];

bucket_error_xlate({Property, not_valid_quorum}) ->
    [atom_to_list(Property), " must be an integer or (one|quorum|all)"];
bucket_error_xlate({_Property, Error}) when is_list(Error)
                                            orelse is_binary(Error) ->
    Error;
bucket_error_xlate({Property, Error}) when is_atom(Error) ->
    [atom_to_list(Property), ": ", atom_to_list(Error)];
bucket_error_xlate({Property, Error}) ->
    [atom_to_list(Property), ": ", io_lib:format("~p", [Error])];
bucket_error_xlate(X) ->
    io_lib:format("~p", [X]).

%%%
%%% Unit tests
%%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

json_props(Props) ->
    lists:flatten(mochijson2:encode([{props, Props}])).

bucket_type_create_no_timeseries_test() ->
    Ref = make_ref(),
    JSON = json_props([{bucket_type, my_type}]),
    bucket_type_create(
        fun(Props) -> put(Ref, Props) end,
        <<"my_type">>,
        mochijson2:decode(JSON)
    ),
    ?assertEqual(
        [{bucket_type, <<"my_type">>}],
        get(Ref)
    ).

bucket_type_create_with_timeseries_table_test() ->
    Ref = make_ref(),
    TableDef =
        <<"CREATE TABLE my_type ",
          "(time TIMESTAMP NOT NULL, ",
	  "user varchar not null, ",
          " PRIMARY KEY ((quantum(time, 15, m)), time, user))">>,
    JSON = json_props([{bucket_type, my_type}, 
                       {table_def, TableDef}]),
    bucket_type_create(
        fun(Props) -> put(Ref, Props) end,
        <<"my_type">>,
        mochijson2:decode(JSON)
    ),
    ?assertMatch(
        [{ddl, _}, {bucket_type, <<"my_type">>}],
        get(Ref)
    ).

bucket_type_and_table_name_must_match_test() ->
    Ref = make_ref(),
    TableDef =
        <<"CREATE TABLE times ",
          "(time TIMESTAMP NOT NULL, ",
	  "user varchar not null, ",
          " PRIMARY KEY (time, user))">>,
    JSON = json_props([{bucket_type, my_type}, 
                       {table_def, TableDef}]),
    % if this error changes slightly it is not so important, as long as
    % the bucket type is not allowed to be created.
    ?assertError(
        {badmatch,
            {error,
                {bucket_type_and_table_name_different,
                    <<"my_type">>,<<"times">>}}},
        bucket_type_create(
            fun(Props) -> put(Ref, Props) end,
            <<"my_type">>,
            mochijson2:decode(JSON)
        )
    ).

-endif.
