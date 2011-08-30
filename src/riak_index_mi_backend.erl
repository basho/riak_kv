%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_index_mi_backend).
-behavior(riak_index_backend).
-export([
         start/2,
         stop/1,
         index/2,
         delete/2,
         lookup_sync/4,
         fold_index/4,
         drop/1,
         callback/3
        ]).

-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% A size of 'all' tells merge_index to ignore the size parameter when
%% performing a range query.
-define(SIZE, all).

%% @type state() = term().
-record(state, {partition, pid}).

%% @type posting() :: {Index::binary(), Field::term(), Term::term(),
%%                     Value::term(), Properties::term(), Timestamp::Integer}.

%% @spec start(Partition :: integer(), Config :: proplist()) ->
%%          {ok, state()} | {{error, Reason :: term()}, state()}
%%
%% @doc Start this backend.
start(Partition, Config) ->
    %% Get the data root directory
    DataRoot =
        case proplists:get_value(data_root_2i, Config) of
            undefined ->
                case application:get_env(merge_index, data_root_2i) of
                    {ok, Dir} ->
                        Dir;
                    _ ->
                        riak:stop("merge_index data_root_2i unset, failing.")
                end;
            Value ->
                Value
        end,

    PartitionStr = lists:flatten(io_lib:format("~p", [Partition])),

    %% Setup actual merge_index dir for this partition
    PartitionRoot = filename:join([DataRoot, PartitionStr]),
    {ok, Pid} = merge_index:start_link(PartitionRoot),
    {ok, #state { partition=Partition, pid=Pid }}.

%% @spec stop(state()) -> ok | {error, Reason :: term()}
%%
%% @doc Stop this backend.
stop(State) ->
    Pid = State#state.pid,
    ok = merge_index:stop(Pid).

%% @spec index(State :: state(), Postings :: [posting()]) -> ok.
%%
%% @doc Store the specified postings in the index. Postings are a
%%      6-tuple of the form {Index, Field, Term, Value, Properties,
%%      Timestamp}. All fields can be any kind of Erlang term. If the
%%      Properties field is 'undefined', then it tells the system to
%%      delete any existing postings found with the same
%%      Index/Field/Term/Value.
index(State, Postings) ->
    Pid = State#state.pid,
    merge_index:index(Pid, Postings).

%% @spec delete(State :: state(), Postings :: [posting()]) -> ok.
%%
%% @doc Delete the specified postings in the index. Postings are a
%%      6-tuple of the form {Index, Field, Term, Value, Properties,
%%      Timestamp}.
delete(State, Postings) ->
    Pid = State#state.pid,
    %% Merge_index deletes a posting when you send it into the system
    %% with properties set to 'undefined'.
    F = fun ({I,F,T,V,_,K}) -> {I,F,T,V,undefined,K};
            ({I,F,T,V,K}) -> {I,F,T,V,undefined,K}
        end,
    Postings1 = [F(X) || X <- Postings],
    merge_index:index(Pid, Postings1).


%% @spec lookup_sync(State :: state(), Index::term(), Field::term(), Term::term()) ->
%%           [{Value::term(), Props::term()}].
%%
%% @doc Return a list of matching values stored under the provided
%%      Index/Field/Term. Results are of the form {Value, Properties}.
lookup_sync(State, Index, Field, Term) ->
    Pid = State#state.pid,
    FilterFun = fun(_Value, _Props) -> true end,
    merge_index:lookup_sync(Pid, Index, Field, Term, FilterFun).

%% @spec fold_index(FoldFun :: function(), Acc :: term(),
%%                   Opts :: [term()], State :: state()) -> term().
fold_index(FoldFun, Acc, Opts, State) ->
    case lists:keyfind(index, 1, Opts) of
        false ->
            %% At this point a check has already been done to verify
            %% that a tuple of the form {index, Bucket, Query} or
            %% {bucket, Bucket, Query} exists so we are safe in
            %% assuming this call to keyfind will return the tuple.
            {_, Bucket, Query} = lists:keyfind(bucket, 1, Opts);
        {_, Bucket, Query} ->
            ok
    end,                
    Pid = State#state.pid,
    FilterFun = fun(_Value, _Props) -> true end,

    %% Run a synchronous query against merge_index. In the future,
    %% this will run more complicated queries, and will run them
    %% asynchronously.
    Results = case Query of
                  [{eq, Field, Term}] ->
                      merge_index:lookup_sync(Pid, Bucket, Field, Term, FilterFun);
                  [{gte, Field, StartTerm}, {lte, Field, EndTerm}] ->
                      merge_index:range_sync(Pid, Bucket, Field, StartTerm, EndTerm, ?SIZE, FilterFun);
                  [{all, _Field}] ->
                      %% Don't think merge_index currently supports
                      %% a way to use a wildcard to collect all
                      %% results.
                      %% @TODO Decide what to do about returning
                      %% all keys for a given index.
                      {error, {unsupported_query_element, Query}};
                  _ ->
                      {error, {unknown_query_element, Query}}
              end,

    case Results of 
        {error, Reason} ->
            {error, Reason};
        _ ->
            FoldKeysFun = fold_keys_fun(FoldFun, Bucket),
            FoldResults = lists:foldl(FoldKeysFun, Acc, Results),
            {ok, FoldResults}
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldFun, Bucket) ->
    fun({Key, _}, Acc) ->
            FoldFun(Bucket, Key, Acc)
    end.

%% @spec drop(State::state()) -> ok.
%%
%% @doc Delete all values from the index.
drop(State) ->
    Pid = State#state.pid,
    merge_index:drop(Pid).

%% Ignore callbacks for other backends so multi backend works
callback(_State, _Ref, _Msg) ->
    ok.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test() ->
    ?assertCmd("rm -rf test/mi-backend"),
    application:start(merge_index),
    application:set_env(merge_index, data_root_2i, "test/mi-backend"),
    riak_index_backend:standard_test(?MODULE, []),
    ok.

custom_config_test() ->
    ?assertCmd("rm -rf test/mi-backend"),
    application:start(merge_index),
    application:set_env(merge_index, data_root_2i, ""),
    riak_index_backend:standard_test(?MODULE, [{data_root_2i, "test/mi-backend"}]).

-endif.
