%% -------------------------------------------------------------------
%%
%% riak_kv_changeset: Track which bucket/key pairs have changed over
%%                    boxed time intervals
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_changeset).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([object_changed/3,
         async_object_changed/3,
         bloom_since/2
        ]).

%% Exporting for testing
-export([new_bloom/0, write_bloom_to_file/2, read_bloom_from_file/1]).

%% Export for intercepted testing.
-export([get_timebox_interval/0]).

-record(state, {
          partition,
          bloom,
          file,
          interval
         }).

-define(DEFAULT_BLOOM_SIZE, 10000000).
-define(DEFAULT_TIMEBOX_INTERVAL, (60 * 60 * 1000)). %% one hour

%%---------------------------------------
%% API Functions
%%---------------------------------------

%% @doc Start a changeset tracker for the given partition on a vnode.
start_link(Partition) ->
    gen_server:start_link(?MODULE, [Partition], []).

%% @doc Return an aggregate bloom filter reference suitable for testing
%%      inclusion in a changeset since Timestamp.
bloom_since(Timestamp, Pid) ->
    gen_server:call(Pid, {bloom_since, Timestamp}, infinity).

%% @doc
%% Inform the changeset that the object with {Bucket, Key} has changed;
%% this will cause a bit to get set in the bloom for that BKey for the
%% current timebox interval. This function does not block the caller.
async_object_changed(BKey, Val, Pid) ->
    gen_server:cast(Pid, {changed, BKey, Val}).

%% @doc
%% Same as async_object_changed/3, except that it does block the caller.
%% This ensures that all previous updates are logged; and thus creates
%% backpressure on the caller to avoid a huge mailbox.
object_changed(BKey, Val, Pid) ->
    catch gen_server:call(Pid, {changed, BKey, Val}, infinity).

%% gen server

init([Partition]) ->
    {File, Bloom} = create_file_backed_bloom(Partition),
    Interval = get_timebox_interval(),
    erlang:send_after(Interval, self(), rotate),
    {ok, #state{file=File, bloom=Bloom, interval=Interval, partition=Partition}}.

handle_call({changed, BKey, Val}, _From, State=#state{file=File, bloom=Bloom}) ->
    Bloom2 = do_object_changed(BKey, Val, File, Bloom, true),
    {reply, ok, State#state{bloom=Bloom2}};
handle_call({bloom_since, Timestamp}, _From, State=#state{partition=Partition,
                                                          file=File, bloom=Bloom}) ->
    write_bloom_to_file(Bloom, File),
    Result = do_get_bloom_since(Timestamp, Partition),
    {reply, Result, State};
handle_call(Msg, _From, State) ->
    lager:warning("ignored handle_call ~p", [Msg]),
    {reply, ok, State}.

handle_cast({changed, BKey, Val}, State=#state{file=File, bloom=Bloom}) ->
    Bloom2 = do_object_changed(BKey, Val, File, Bloom, false),
    {noreply, State#state{bloom=Bloom2}};
handle_cast(Msg, State) ->
    lager:warning("ignored handle_cast ~p", [Msg]),
    {noreply, State}.

handle_info(rotate, State=#state{interval=Interval}) ->
    %% io:format("Rotating bloom files~n"),
    erlang:send_after(Interval, self(), rotate),
    State2 = do_rotate_bloom(State),
    {noreply, State2};
handle_info(Msg, State) ->
    lager:warning("ignored handle_info ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, #state{file=Fd}) ->
    file:close(Fd),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------------------
%% Internal Functions
%%---------------------------------------

%% @doc Return the number of milliseconds between bloom filter rotations.
get_timebox_interval() ->
    case application:get_env(riak_kv, changeset_interval) of
        {ok, Interval} ->
            Interval;
        undefined ->
            ?DEFAULT_TIMEBOX_INTERVAL
    end.

do_rotate_bloom(State=#state{file=Fd, partition=Partition, bloom=Bloom}) ->
    write_bloom_to_file(Bloom, Fd),
    file:close(Fd),
    {NewFd, NewBloom} = create_file_backed_bloom(Partition),
    State#state{file=NewFd, bloom=NewBloom}.

%% @doc Update the bloom filter with the bucket and key that changed
%% and write the bloom to disk.
do_object_changed({Bucket, Key}, _Val, File, Bloom, true) ->
    %% io:format("do_object_changed and write: ~p/~p~n", [Bucket, Key]),
    ebloom:insert(Bloom, <<Bucket/binary, Key/binary>>),
    write_bloom_to_file(Bloom, File),
    Bloom;
do_object_changed({Bucket, Key}, _Val, _File, Bloom, false) ->
    %% io:format("do_object_changed no write: ~p/~p~n", [Bucket, Key]),
    ebloom:insert(Bloom, <<Bucket/binary, Key/binary>>),
    Bloom.

write_bloom_to_file(Bloom, Fd) ->
    Bin = ebloom:serialize(Bloom),
    Sz = size(Bin),
    Data = <<Sz:32, Bin/binary>>,
    file:position(Fd, 0),
    case file:write(Fd, [Data]) of
	ok ->
	    ok;
	{error, Reason} ->
	    io:format("Cant't write bloom file because ~p~n", [Reason]),
	    lager:warning("Cant't write bloom file because ~p", [Reason]),
	    {error, Reason}
    end.

%% @private
%% @doc Create a bloom filter and a file to write it to. The filename
%% is based on the partition, current time.
%% Returns {File, Bloom}
create_file_backed_bloom(Partition) ->
    Bloom = new_bloom(),
    case create_bloom_file(Partition) of
        {ok, File} ->
            {File, Bloom};
        Error ->
            lager:warning("Unable to create changeset file for partition: ~p because ~p",
                          [Partition, Error]),
            {undefined, Bloom}
    end.

new_bloom() ->
    NumKeys = ?DEFAULT_BLOOM_SIZE,
    {ok, Bloom} = ebloom:new(NumKeys, 0.01, random:uniform(1000)),
    Bloom.

create_bloom_file(Partition) ->
    case determine_data_root() of
        undefined ->
            lager:warning("Neither riak_kv/changeset_data_dir or "
                          "riak_core/platform_data_dir are defined. "
                          "Changesets will not persist node restarts."),
            undefined;
        Root ->
            Dir = filename:join(Root, integer_to_list(Partition)),
            Filename = utc_timestamp_name(os:timestamp()),
            Filepath = filename:join(Dir, Filename),
            case filelib:ensure_dir(Filepath) of
                ok ->
                    case file:open(Filepath, [write]) of
                        {ok, File} ->
%%                            io:format("Created ~p~n", [Filepath]),
                            {ok, File};
                        {error, Reason} ->
                            lager:warning("Could not create bloom changeset file: ~p because: ",
                                          [Filepath, Reason]),
                            undefined
                    end;
                {error, Reason} ->
                    lager:warning("Could not create bloom changeset directory for: ~p because: ",
                                  [Filepath, Reason]),
                    undefined
            end
    end.

utc_timestamp_name(TS) ->
    {{Year,Month,Day},{Hour,Minute,Second}} = 
	calendar:now_to_universal_time(TS),
    io_lib:format("~w-~w-~wT~w~2..0w.~2..0w.bloom",
		  [Year, Month, Day,Hour,Minute,Second]).

determine_data_root() ->
    case application:get_env(riak_kv, changeset_data_dir) of
        {ok, ChangeSetRoot} ->
            ChangeSetRoot;
        undefined ->
            case application:get_env(riak_core, platform_data_dir) of
                {ok, PlatformRoot} ->
                    Root = filename:join(PlatformRoot, "changeset"),
                    lager:warning("Config riak_kv/changeset_data_dir is "
                                  "missing. Defaulting to: ~p", [Root]),
                    application:set_env(riak_kv, changeset_data_dir, Root),
                    Root;
                undefined ->
                    undefined
            end
    end.

read_bloom_from_file(Filename) ->
%%    io:format("Reading bloom file: ~p~n", [Filename]),
    case file:read_file(Filename) of
        {ok, Bin} ->
            case Bin of
                <<BloomSize:32, SerializedBloom:BloomSize/binary, _Rest/binary>> ->
                    {ok, Bloom} = ebloom:deserialize(SerializedBloom),
%%                    io:format("Deserialized ~p bytes from file.~n", [BloomSize+4]),
                    Bloom;
                <<>> ->
                    %% It's empty, which is ok since it might have just been created.
                    new_bloom();
                Other ->
                    io:format("Corrupted bloom file ~p contains: ~p~n",
                              [Filename, Other]),
                    new_bloom()
            end;
        Error ->
            io:format("Unable to open bloom file for read: ~p because: ~p~n",
                          [Filename, Error]),
            lager:warning("Unable to open bloom file for read: ~p because: ~p",
                          [Filename, Error]),
            new_bloom()
    end.

%% @doc
%% Return an ebloom filter representing all changes since Timestamp.
%% Does not run in the gen_server, so it won't slow down the write path.
%% If no bloom filters are found for the given timestamp, then 'undefined'
%% is returned.
do_get_bloom_since(Timestamp, Partition) ->
    case [read_bloom_from_file(File) || File <- files_since(Timestamp, Partition)] of
        [] -> undefined;
        [Bloom] -> {ok, Bloom}; %% no need to union with an empty bloom.
        [Bloom | Blooms] ->
            [ebloom:union(Bloom, B1) || B1 <- Blooms],
            {ok, Bloom}
            %% {ok, lists:foldl(fun(B1,Acc) ->
            %%                          ebloom:union(Acc, B1),
            %%                          Acc end,
            %%                  Bloom, Blooms)}
    end.

%% @private
%% doc
%% Return a list of filesnames that represent bloom filters written since the
%% given os:timestamp() time.
files_since(Timestamp, Partition) ->
    %% Filenames are sortable.
    %% Get a list of all files in the changeset data dir, sort them, and split
    %% the list on a Marker name created for the given timestamp.
    %% Toss the front and keep the back.
    Datadir = determine_data_root(),
    Dir = filename:join(Datadir, integer_to_list(Partition)),
    case file:list_dir(Dir) of
        {ok, Files} ->
            case Files of
                [OneFile] -> [filename:join(Dir, OneFile)];
                AllFiles ->
                    Filename = lists:flatten(utc_timestamp_name(Timestamp)),
                    Sorted = lists:sort(AllFiles),
                    {Front, Back} = lists:splitwith(fun(F) -> F < Filename end, Sorted),
                    %% io:format("Filename: ~p~n", [Filename]),
                    %% io:format("Back: ~p~n", [Back]),
                    %% io:format("Front: ~p~n", [Front]),
                    %% Filenames mark the beginning of their time interval. We have
                    %% sorted by an imaginary filename, which probably doesn't exist,
                    %% so it might appear before the break in the split list.
                    Keepers = case Back /= [] andalso hd(Back) == Filename of
                                  true ->
                                      %% The requested start time is exactly at the
                                      %% beginning of the first file in "back".
                                      Back;
                                  false ->
                                      %% The start time is in the file prior to the
                                      %% first element of back.
                                      [hd(lists:reverse(Front)) | Back]
                              end,
                    %% io:format("Keepers: ~p~n", [Keepers]),
                    [filename:join(Dir, File) || File <- Keepers]
            end;
        Error ->
            lager:warning("Can't list directory contents for ~p because ~p",
                          [Dir, Error]),
            []
    end.


-ifdef(TEST).
roundtrip_test() ->
    Interval = 2000,
    Partition = 1234567890,
    Val = ignored_riak_obj_value,
    application:set_env(riak_kv, changeset_data_dir, "/tmp/riak_kv_data_dir"),
    application:set_env(riak_kv, changeset_interval, Interval),

    Bucket = <<"adventure">>,

    Key1 = <<"key">>,
    Key2 = <<"grate">>,
    Key3 = <<"lantern">>,
    Key4 = <<"ermeralds">>,
    Key5 = <<"xyzzy">>,
    Key6 = <<"plough">>,

    BK1 = <<Bucket/binary, Key1/binary>>,
    BK2 = <<Bucket/binary, Key2/binary>>,
    BK3 = <<Bucket/binary, Key3/binary>>,
    BK4 = <<Bucket/binary, Key4/binary>>,
    BK5 = <<Bucket/binary, Key5/binary>>,
    BK6 = <<Bucket/binary, Key6/binary>>,

    {ok, Pid} = start_link(Partition),

    TS1 = os:timestamp(),
    object_changed({Bucket, Key1}, Val, Pid),
    object_changed({Bucket, Key2}, Val, Pid),
    object_changed({Bucket, Key3}, Val, Pid),

    %% wait until the next interval has rotated.
    timer:sleep(Interval + 250),

    TS2 = os:timestamp(),
    async_object_changed({Bucket, Key4}, Val, Pid),
    async_object_changed({Bucket, Key5}, Val, Pid),
    async_object_changed({Bucket, Key6}, Val, Pid),

    {ok, Bloom1} = bloom_since(TS1, Pid),
    
    ?assertEqual(true, ebloom:contains(Bloom1, BK1)),
    ?assertEqual(true, ebloom:contains(Bloom1, BK2)),
    ?assertEqual(true, ebloom:contains(Bloom1, BK3)),

    ?assertEqual(false, ebloom:contains(Bloom1, BK4)),
    ?assertEqual(false, ebloom:contains(Bloom1, BK5)),
    ?assertEqual(false, ebloom:contains(Bloom1, BK6)),

    {ok, Bloom2} = bloom_since(TS2, Pid),

    ?assertEqual(true, ebloom:contains(Bloom2, BK4)),
    ?assertEqual(true, ebloom:contains(Bloom2, BK5)),
    ?assertEqual(true, ebloom:contains(Bloom2, BK6)),

    ?assertEqual(false, ebloom:contains(Bloom2, BK1)),
    ?assertEqual(false, ebloom:contains(Bloom2, BK2)),
    ?assertEqual(false, ebloom:contains(Bloom2, BK3)).

-endif.
