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
         fold_index/6,
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
        case proplists:get_value(data_root, Config) of
            undefined ->
                case application:get_env(merge_index, data_root) of
                    {ok, Dir} ->
                        Dir;
                    _ ->
                        riak:stop("riak_index data_root unset, failing.")
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

%% @spec fold_index(State :: state(), Bucket :: binary(), Query :: riak_index:query_elements(),
%%                  SKFun :: function(), Acc :: term(), FinalFun :: function()) -> term().
fold_index(State, Index, Query, SKFun, Acc, FinalFun) ->
    Pid = State#state.pid,
    FilterFun = fun(_Value, _Props) -> true end,

    %% Run a synchronous query against merge_index. In the future,
    %% this will run more complicated queries, and will run them
    %% asynchronously.
    Results = case Query of
                  [{eq, Field, [Term]}] ->
                      merge_index:lookup_sync(Pid, Index, Field, Term, FilterFun);

                  [{lt, Field, [Term]}] ->
                      merge_index:range_sync(Pid, Index, Field, undefined, inc(Term, -1), ?SIZE, FilterFun);

                  [{gt, Field, [Term]}] ->
                      merge_index:range_sync(Pid, Index, Field, inc(Term, 1), undefined, ?SIZE, FilterFun);

                  [{lte, Field, [Term]}] ->
                      merge_index:range_sync(Pid, Index, Field, undefined, Term, ?SIZE, FilterFun);

                  [{gte, Field, [Term]}] ->
                      merge_index:range_sync(Pid, Index, Field, Term, undefined, ?SIZE, FilterFun);

                  [{range, Field, [StartTerm, EndTerm]}] ->
                      merge_index:range_sync(Pid, Index, Field, StartTerm, EndTerm, ?SIZE, FilterFun);

                  _ ->
                      {error, {unknown_query_element, Query}}
              end,


    %% If Results is a list of results, then call SKFun to accumulate
    %% the results.
    Results1 = case is_list(Results) of
                   true -> SKFun({results, Results}, Acc);
                   false -> {ok, Results}
               end,

    %% Call FinalFun on the results.
    case Results1 of
        {ok, NewAcc} ->
            FinalFun(done, NewAcc);
        {error, Reason} ->
            FinalFun({error, Reason}, Acc);
        done ->
            FinalFun(done, Acc);
        Other ->
            FinalFun({error, {unexpected_result, Other}}, Acc)
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


%% @private
%% @spec inc(term(), Amt :: integer()) -> term().
%%
%% @doc Increments or decrements an Erlang data type by one
%%      position. Used during construction of exclusive range searches.
inc(Term, Amt)  when is_integer(Term) ->
    Term + Amt;
inc(Term, _Amt) when is_float(Term) ->
    Term; %% Doesn't make sense to have exclusive range on floats.
inc(Term, Amt)  when is_list(Term) ->
    NewTerm = inc(list_to_binary(Term), Amt),
    binary_to_list(NewTerm);
inc(Term, Amt)  when is_binary(Term) ->
    Bits = size(Term) * 8,
    <<Int:Bits/integer>> = Term,
    NewInt = inc(Int, Amt),
    <<NewInt:Bits/integer>>;
inc(Term, _) ->
    throw({unhandled_type, binary_inc, Term}).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test() ->
    ?assertCmd("rm -rf test/mi-backend"),
    application:start(merge_index),
    application:set_env(merge_index, data_root, "test/mi-backend"),
    riak_index_backend:standard_test(?MODULE, []),
    ok.

custom_config_test() ->
    ?assertCmd("rm -rf test/mi-backend"),
    application:start(merge_index),
    application:set_env(merge_index, data_root, ""),
    riak_index_backend:standard_test(?MODULE, [{data_root, "test/mi-backend"}]).

-endif.
