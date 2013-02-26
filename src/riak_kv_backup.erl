%% -------------------------------------------------------------------
%%
%% riak_backup: utilities for backup and restore of Riak nodes and clusters
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

%% @doc utilities for backup and restore of Riak nodes and clusters.

%%      Note that if you want to restore to exactly the contents of
%%      a dump, you should restore to an empty cluster.  Otherwise,
%%      restore will reconcile values with the existing data.

-module(riak_kv_backup).
-export ([backup/3, restore/2]).
-define (TABLE, riak_kv_backup_table).

%%% BACKUP %%%

%% @doc 
%% Connect to the cluster of which EntryNode is a member, 
%% read data from the cluster, and save the data in the specified file.
backup(EntryNode, BaseFilename, Mode) -> 
    % Make sure we can reach the node...
    ensure_connected(EntryNode),

    % Get a list of nodes...
    {ok, Ring} = rpc:call(EntryNode, riak_core_ring_manager, get_raw_ring, []),
    Members = riak_core_ring:all_members(Ring),

    FileName = 
        case Mode of
            "all" ->
                io:format("Backing up (all nodes) to '~s'.~n", [BaseFilename]),
                io:format("...from ~p~n", [Members]),
                BaseFilename;
            "node" ->
                io:format("Backing up (node ~p) to '~s'.~n", [EntryNode, 
                                                              BaseFilename++"-"++atom_to_list(EntryNode)]),
                BaseFilename ++ "-" ++ EntryNode
        end,

    % Make sure all nodes in the cluster agree on the ring...
    ensure_synchronized(Ring, Members),

    % Backup the data...
    {ok, ?TABLE} = disk_log:open([{name, ?TABLE},
                                  {file, FileName},
                                  {mode, read_write},
                                  {type, halt}]),


    case Mode of
        "all" ->
            [backup_node(Node, Ring) || Node <- Members];
        "node" ->
            backup_node(EntryNode, Ring)
    end,
    io:format("syncing and closing log~n"),
    ok = disk_log:sync(?TABLE),
    ok = disk_log:close(?TABLE),
    
    % Make sure the nodes are still synchronized...
    ensure_synchronized(Ring, Members),
    ok.
    
backup_node(Node, Ring) ->
    Partitions = [I || {I,N} <- riak_core_ring:all_owners(Ring), N =:= Node],
    backup_vnodes(Partitions, Node).

backup_vnodes([], Node) ->
    io:format("Backup of ~p complete~n", [Node]);
backup_vnodes([Partition|T], Node) ->
    Self = self(),
    Pid = spawn_link(fun() -> result_collector(Self) end),
    riak_kv_vnode:fold({Partition, Node}, fun backup_folder/3, Pid),
    Pid ! stop,
    receive stop -> stop end,
    backup_vnodes(T, Node).

backup_folder(K, V, Pid) ->
    Pid ! {backup, {K, V}},
    Pid.

result_collector(PPid) ->
    receive
        stop -> 
            PPid ! stop;
        {backup, {{B, K}, M}} when is_binary(M) ->
            %% make sure binary is encoded using term_to_binary (v0)
            %% not v1 format. restore does not have access to bucket/key
            %% so must include them in encoded format, which v1 does not
            ObjBin = riak_object:to_binary_version(v0, B, K, M),
            disk_log:log(?TABLE, ObjBin),
            result_collector(PPid)
    end.

%%% RESTORE %%%

%% @doc
%% Read data from the specified file created by backup/2,
%% and write it to the cluster of which EntryNode is a member.
restore(EntryNode, Filename) ->
    io:format("Restoring from '~s' to cluster to which '~s' belongs.~n", [Filename, EntryNode]),
    
    % Connect to the node...
    {ok, Client} = riak:client_connect(EntryNode),
    
    % Open the table, write it out, close the table...
    {ok, ?TABLE} = disk_log:open([{name, ?TABLE},
                                  {file, Filename},
                                  {mode, read_only},
                                   {type, halt}]),
    Count = traverse_backup(
                disk_log:chunk(?TABLE, start), 
                fun(Entry) -> read_and_restore_function(Client, Entry) end, 0),
    ok = disk_log:close(?TABLE),
    io:format("Restored ~p records.~n", [Count]),
    ok.

traverse_backup(eof, _VisitorFun, Count) ->
    Count;
traverse_backup({Cont, Terms}, VisitorFun, Count) when is_list(Terms) ->
    [VisitorFun(T) || T <- Terms],
    traverse_backup(disk_log:chunk(?TABLE, Cont), 
                    VisitorFun, Count+length(Terms)).
    

read_and_restore_function(Client, BinTerm) ->
    Obj = binary_to_term(BinTerm),
    Bucket = riak_object:bucket(Obj),
    Key = riak_object:key(Obj),
    % Data Cleaning...
    Obj1 = make_binary_bucket(Bucket, Key, Obj),

    %% Store the object; be sure to tell the FSM not to update last modified!
    Response = Client:put(Obj1,1,1,1200000, [asis,{update_last_modified, false}]),
    {continue, Response}.
   
%%% DATA CLEANING %%% 
    
%% If the bucket name is an atom, convert it to a binary...
make_binary_bucket(Bucket, Key, OriginalObj) when is_atom(Bucket) ->
    Bucket1 = list_to_binary(atom_to_list(Bucket)),
    OriginalContents = riak_object:get_contents(OriginalObj),
    OriginalVClock = riak_object:vclock(OriginalObj),

    % We can't change the bucket name without creating a new object...
    NewObj = riak_object:new(Bucket1, Key, placeholder),
    NewObj1 = riak_object:set_contents(NewObj, OriginalContents),
    _NewObj2 = riak_object:set_vclock(NewObj1, OriginalVClock);
    
%% If the bucket name is a binary, just pass it on through...
make_binary_bucket(Bucket, _Key, Obj) when is_binary(Bucket) -> Obj.

%% @private
%% Try to reach the specified node, throw exception on failure.
ensure_connected(Node) ->
    case net_adm:ping(Node) of
        pang -> throw({could_not_reach_node, Node});
        pong -> ok
    end.

%% @private
%% Make sure that rings of all members are synchronized, 
%% throw exception on failure.
ensure_synchronized(Ring, Members) ->
    F = fun(Node) ->
        {ok, Ring2} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
        riak_core_ring:equal_rings(Ring, Ring2)
    end,
    case lists:all(F, Members) of
        true -> ok;
        false -> throw({nodes_not_synchronized, Members})
    end.

% pmap(Fun, List) ->
%     Workers = [spawn_worker(self(), Pred, Data) || X <- List],
%     [wait_result(Worker) || Worker <- Workers].
% 
% spawn_worker(Parent, Fun, Data) ->
%     erlang:spawn_monitor(fun() -> Parent ! {self(), Fun(Data)} end).
% 
% wait_result({Pid,Ref}) ->
%     receive
%         {'DOWN', Ref, _, _, normal} -> receive {Pid,Result} -> Result end;
%         {'DOWN', Ref, _, _, Reason} -> exit(Reason)
%     end.
