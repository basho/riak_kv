%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
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

%% @doc Riak KV MapReduce / Riak Pipe Compatibility
%%
%% == About using `{modfun, Mod, Fun, Arg}' generator to a MapReduce job ==
%%
%% The six methods of specifying input for a MapReduce job are:
%%
%% <ol>
%% <li> Specify a bucket name (to emit all bucket/key pairs for that
%%      bucket) </li>
%% <li> Specify a bucket name and keyfilter spec, `{Bucket,
%%      KeyFilter}' </li>
%% <li> Specify an explicit list of bucket/key pairs </li>
%% <li> Specify `{index, Bucket, Index, Key}' or `{index, Bucket,
%%      Index, StartKey, EndKey}' to query secondary indexes and send
%%      matching keys into the MR as inputs. </li>
%% <li> Specify `{search, Bucket, Query}' or `{search, Bucket, Query,
%%      Filter}' to query Riak Search and send matching keys into the
%%      MR as inputs. </li>
%% <li> Specify `{modfun, Mod, Fun, Arg}' to generate the raw input
%%      data for the rest of the workflow </li>
%% </ol>
%%
%% For the final method, "raw input data" means that the output of the
%% function will be used as-is by the next item MapReduce workflow.
%% If that next item is a map phase, then that item's input is
%% expected to be a bucket/key pair.  If the next item is a reduce
%% phase, then the input can be an arbitrary term.
%%
%% The type specification for a `{modfun, Mod, Fun, Arg}' generator
%% function is:
%% ```
%% -spec generator_func(Pipe::riak_pipe:pipe(),
%%                      Arg::term(),
%%                      Timeout::integer() | 'infinity').
%% '''
%%
%% This generator function is responsible for using {@link
%% riak_pipe:queue_work/2} to send any data to the pipe, and it is
%% responsible for calling {@link riak_pipe:eoi/2} to signal the end
%% of input.
%%
%% == About reduce phase compatibility ==
%%
%% An Erlang reduce phase is defined by the tuple:
%% `{reduce, Fun::function(2), Arg::term(), Keep::boolean()}'.
%%
%% <ul>
%% <li> `Fun' takes the form of `Fun(InputList, Arg)' where `Arg' is
%%       the argument specified in the definition 4-tuple above.
%%
%%       NOTE: Unlike a fold function (e.g., `lists:foldl/3'), the
%%       `Arg' argument is constant for each iteration of the reduce
%%       function. </li>
%% <li> The `Arg' may be any term, as the caller sees fit.  However, if
%%      the caller wishes to have more control over the reduce phase,
%%      then `Arg' must be a property list.  The control knobs that may
%%      be specified are:
%%      <ul>
%%      <li> `reduce_phase_only_1' will buffer all inputs to the reduce
%%           phase fitting and only call the reduce function once.
%%
%%           NOTE: Use with caution to avoid excessive memory use. </li>
%%      <li> `{reduce_phase_batch_size, Max::integer()}' will buffer all
%%           inputs to the reduce phase fitting and call the reduce function
%%           after `Max' items have been buffered. </li>
%%      </ul>
%%      If neither `reduce_phase_only_1' nor
%%      `{reduce_phase_batch_size, Max}' are present, then the
%%      batching size will default to the value of the application
%%      environment variable `mapred_reduce_phase_batch_size' in the
%%      `riak_kv' application.
%%
%%      NOTE: This mixing of user argument data and MapReduce
%%      implementation metadata is suboptimal, but to separate the two
%%      types of data would require a change that is incompatible with
%%      the current Erlang MapReduce input specification, e.g., a
%%      5-tuple such as `{reduce, Fun, Arg, Keep, MetaData}' or else a
%%      custom wrapper around the 3rd arg, e.g. `{reduce, Fun,
%%      {magic_tag, Arg, Metadata}, Keep}'.
%% </li>
%% <li> If `Keep' is `true', then the output of this phase will be returned
%%      to the caller (i.e. the output will be "kept"). </li>
%% </ul>

-module(riak_kv_mrc_pipe).

%% TODO: Stolen from old-style MapReduce interface, but is 60s a good idea?
-define(DEFAULT_TIMEOUT, 60000).

-define(SINK_SYNC_PERIOD_DEFAULT, 10).

-export([
         mapred/2,
         mapred/3,
         mapred_stream/1,
         mapred_stream/2,
         send_inputs/2,
         send_inputs/3,
         send_inputs_async/2,
         send_inputs_async/3,
         collect_outputs/2,
         collect_outputs/3,
         group_outputs/2,
         mapred_stream_sink/3,
         collect_sink/1,
         receive_sink/1,
         destroy_sink/1,
         cleanup_sink/1,
         error_exists/1,
         mapred_plan/1,
         mapred_plan/2,
         compile_string/1,
         compat_fun/1,
         sink_sync_period/0
        ]).
%% NOTE: Example functions are used by EUnit tests
-export([example/0, example_bucket/0, example_reduce/0,
         example_setup/0, example_setup/1]).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("riak_pipe/include/riak_pipe_log.hrl").
-include("riak_kv_mrc_sink.hrl").

-export_type([map_query_fun/0,
              reduce_query_fun/0,
              key_input/0,
              link_match/0]).

%% All of the types of Input allowed for a MapReduce
-type input() :: [key_input()]
               | bucket_input()
               | index_input()
               | search_input()
               | modfun_input().
-type key_input() :: riak_kv_pipe_get:input().
-type bucket_input() :: binary()
                      | {Bucket :: binary(), KeyFilter :: [keyfilter()]}.
-type keyfilter() :: [string()].
-type index_input() :: {index, Bucket :: binary(), Index :: binary(),
                        Key :: term()}
                     | {index, Bucket :: binary(), Index :: binary(),
                        Start :: term(), End :: term()}.
-type search_input() :: {search, Bucket :: binary(), Query :: binary()}
                      | {search, Bucket :: binary(), Query :: binary(),
                         Filter :: [keyfilter()]}.
-type modfun_input() :: {modfun, Module :: atom(), Function :: atom(),
                         Arg :: term()}.

%% All of the Query syntax allowed
-type query_part() :: {map, map_query_fun(),
                       Arg :: term(), Keep :: boolean()}
                    | {reduce, reduce_query_fun(),
                       Arg :: term(), Keep :: boolean()}
                    | {link,
                       BucketMatch :: link_match(),
                       TagMatch :: link_match(),
                       Keep :: boolean()}.
-type map_query_fun() ::
        {qfun, fun( (Input :: term(),
                     KeyData :: term(),
                     PhaseArg :: term()) -> [term()] )}
      | query_fun().
-type reduce_query_fun() ::
        {qfun, fun( (Input :: [term()],
                     PhaseArg :: term()) -> [term()] )}
      | query_fun().
-type query_fun() ::
        {modfun, Module :: atom(), Function :: atom()}
      | {strfun, {Bucket :: binary(), Key :: binary()}}
      | {strfun, Source :: string()|binary()}
      | {jsanon, {Bucket :: binary(), Key :: binary()}}
      | {jsfun, Name :: binary()}
      | {jsanon, Source :: binary()}.
-type link_match() :: binary() | '_'.

%% The output of collect_outputs/2,3, group_outputs/2, and collect_sink/1
-type ungrouped_results() :: [{From :: non_neg_integer(), Result :: term()}].
-type grouped_results() :: [Results :: list()]
                         | list().

%% The error reasons returned from collect_sink/1
-type receive_sink_error() :: {sender_died, Reason::term()}
                            | {sink_died, Reason::term()}
                            | timeout
                            | {From::non_neg_integer(), Info::term()}.

%% @equiv mapred(Inputs, Query, 60000)
mapred(Inputs, Query) ->
    mapred(Inputs, Query, ?DEFAULT_TIMEOUT).

%% @doc Perform a MapReduce `Query' over `Inputs' and return the
%% result.  `Timeout' here is the maximum time to wait between the
%% delivery of each output, not an overall timeout.
-spec mapred(input(), [query_part()], timeout()) ->
         {ok, grouped_results()} | {error, Reason :: term()}
        |{error, Reason :: term(),
          {ok, grouped_results()} | {error, Reason :: term()}}.
mapred(Inputs, Query, Timeout) ->
    case mapred_stream_sink(Inputs, Query, Timeout) of
        {ok, Ctx} ->
            case collect_sink(Ctx) of
                {ok, _}=Success ->
                    cleanup_sink(Ctx),
                    Success;
                {error, _}=Error ->
                    destroy_sink(Ctx),
                    Error
            end;
        Error ->
            {error, Error}
    end.

%% @equiv mapred_stream(Query, [])
-spec mapred_stream([query_part()]) ->
         {{ok, riak_pipe:pipe()}, NumKeeps :: integer()}.
mapred_stream(Query) ->
    mapred_stream(Query, []).

%% @doc Setup the MapReduce plumbing, preparted to receive inputs.
%% The caller should then use {@link send_inputs/2} or {@link
%% send_inputs/3} to give the query inputs to process.
%%
%% The second element of the return tuple is the number of phases that
%% requested to keep their inputs, and will need to be passed to
%% {@link collect_outputs/3} or {@link group_outputs/2} to get labels
%% compatible with HTTP and PB interface results.
-spec mapred_stream([query_part()], list()) ->
         {{ok, riak_pipe:pipe()}, NumKeeps :: integer()}.
mapred_stream(Query, Options) when is_list(Options) ->
    NumKeeps = count_keeps_in_query(Query),
    {riak_pipe:exec(mr2pipe_phases(Query),
                    [{log, sink},{trace,[error]}]++Options),
     NumKeeps}.

%% @doc Setup the MapReduce plumbing, including separate process to
%% receive output (the sink) and send input (the async sender), and a
%% delayed `pipe_timeout' message. This call returns a context record
%% containing details for each piece. Monitors are setup in the
%% process that calls this function, watching the sink and sender.
%%
%% See {@link receive_sink/1} for details about how to use this
%% context.
-spec mapred_stream_sink(input(), [query_part()], timeout()) ->
         {ok, #mrc_ctx{}} | {error, term()}.
mapred_stream_sink(Inputs, Query, Timeout) ->
    {ok, Sink} = riak_kv_mrc_sink:start(self(), []),
    Options = [{sink, #fitting{pid=Sink}},
               {sink_type, {fsm, sink_sync_period(), infinity}}],
    try mapred_stream(Query, Options) of
        {{ok, Pipe}, NumKeeps} ->
            %% catch just in case the pipe or sink has already died
            %% for any reason - we'll get a DOWN from the monitor later
            catch riak_kv_mrc_sink:use_pipe(Sink, Pipe),
            SinkMon = erlang:monitor(process, Sink),
            PipeRef = (Pipe#pipe.sink)#fitting.ref,
            Timer = erlang:send_after(Timeout, self(),
                                      {pipe_timeout, PipeRef}),
            {Sender, SenderMon} =
                riak_kv_mrc_pipe:send_inputs_async(Pipe, Inputs),
            {ok, #mrc_ctx{ref=PipeRef,
                          pipe=Pipe,
                          sink={Sink,SinkMon},
                          sender={Sender,SenderMon},
                          timer={Timer,PipeRef},
                          keeps=NumKeeps}}
    catch throw:{badarg, Fitting, Reason} ->
            riak_kv_mrc_sink:stop(Sink),
            {error, {Fitting, Reason}}
    end.
    

%% The plan functions are useful for seeing equivalent (we hope) pipeline.

%% @doc Produce the pipe spec that will implement the given MapReduce
%% query.  <strong>Intended for debugging only.</strong>
-spec mapred_plan([query_part()]) -> [ riak_pipe:fitting_spec() ].
mapred_plan(Query) ->
    mr2pipe_phases(Query).

%% @doc Produce the pipe spec that will implement the given MapReduce
%% query, and prepend a tuple of the form `{bkeys, [key_input()]}'.
%% If `BucketOrList' is a binary bucket name, this function will list
%% the keys in the bucket to return in this tuple.  <strong>Intended
%% for debugging only.</strong>
-spec mapred_plan([key_input()]|binary(), [query_part()]) ->
         [{bkeys, [key_input()]} | riak_pipe:fitting_spec() ].
mapred_plan(BucketOrList, Query) ->
    BKeys = if is_list(BucketOrList) ->
                    BucketOrList;
               is_binary(BucketOrList) ->
                    {ok, C} = riak:local_client(),
                    {ok, Keys} = C:list_keys(BucketOrList),
                    [{BucketOrList, Key} || Key <- Keys]
            end,
    [{bkeys, BKeys}|mapred_plan(Query)].

%% @doc Convert a MapReduce query into a list of Pipe fitting specs.
-spec mr2pipe_phases([query_part()]) -> [ riak_pipe:fitting_spec() ].
mr2pipe_phases([]) ->
    [#fitting_spec{name=0,
                   module=riak_pipe_w_pass,
                   chashfun=follow}];
mr2pipe_phases(Query) ->
    ConstantHash = random_constant_hash(),

    %% first convert phase
    QueryT = list_to_tuple(Query),
    Numbered = lists:zip(Query, lists:seq(0, length(Query)-1)),
    Fittings0 = lists:flatten([mr2pipe_phase(P,I,ConstantHash,QueryT) ||
                                  {P,I} <- Numbered]),

    %% clean up naive 'keep' translationg
    Fs = fix_final_fitting(Fittings0),
    case lists:last(Query) of
        {_, _, _, false} ->
            %% The default action is to send results down to the next
            %% fitting in the pipe.  However, the last MapReduce query
            %% doesn't want those results.  So, add a "black hole"
            %% fitting that will stop all work items from getting to
            %% the sink and thus polluting our expected results.
            Fs ++ [#fitting_spec{name=black_hole,
                                 module=riak_pipe_w_pass,
                                 arg=black_hole,
                                 chashfun=follow}];
        _ ->
            Fs
    end.

-spec mr2pipe_phase(query_part(),
                    Index :: integer(),
                    ConstantHash :: chash:index(),
                    Query :: tuple()) ->
         [ riak_pipe:fitting_spec() ].
mr2pipe_phase({map,FunSpec,Arg,Keep}, I, _ConstHash, QueryT) ->
    map2pipe(FunSpec, Arg, Keep, I, QueryT);
mr2pipe_phase({reduce,FunSpec,Arg,Keep}, I, ConstHash, _QueryT) ->
    reduce2pipe(FunSpec, Arg, Keep, I, ConstHash);
mr2pipe_phase({link,Bucket,Tag,Keep}, I, _ConstHash, QueryT)->
    link2pipe(Bucket, Tag, Keep, I, QueryT).

%% @doc Covert a map phase to its pipe fitting specs.
%%
%% Map converts to:
%% <ol>
%%    <li>A required {@link riak_kv_pipe_get} to fetch the data for
%%    the input key.</li>
%%    <li>A required {@link riak_kv_mrc_map} to run the given query
%%    function on that data.</li>
%%    <li>An optional {@link riak_pipe_w_tee} if `keep=true'.</li>
%%    <li>An optional {@link riak_kv_w_reduce} if it is determined
%%    that results should be prereduced before being sent on.</li>
%% </ol>
%%
%% Prereduce logic: add pre_reduce fittings to the pipe line if the
%% current item is a map (if you're calling this func, yes it is) and
%% if the next item in the query is a reduce and if the map's arg or
%% system config wants us to use prereduce.  Remember: `I' starts
%% counting at 0, but the element BIF starts at 1, so the element of
%% the next item is I+2.
-spec map2pipe(map_query_fun(), term(), boolean(),
               Index :: integer(), Query :: tuple()) ->
         [ riak_pipe:fitting_spec() ].
map2pipe(FunSpec, Arg, Keep, I, QueryT) ->
    PrereduceP = I+2 =< size(QueryT) andalso
        query_type(I+2, QueryT) == reduce andalso
        want_prereduce_p(I+1, QueryT),
    SafeArg = case FunSpec of
                  {JS, _} when (JS == jsfun orelse JS == jsanon),
                               is_list(Arg) ->
                      %% mochijson cannot encode these properties,
                      %% so remove them from the argument list
                      lists:filter(
                        fun(do_prereduce)     -> false;
                           ({do_prereduce,_}) -> false;
                           (_)                -> true
                        end,
                        Arg);
                  _ ->
                      Arg
              end,
    [#fitting_spec{name={kvget_map,I},
                   module=riak_kv_pipe_get,
                   chashfun={riak_kv_pipe_get, bkey_chash},
                   nval={riak_kv_pipe_get, bkey_nval}},
     #fitting_spec{name={xform_map,I},
                   module=riak_kv_mrc_map,
                   arg={FunSpec, SafeArg},
                   chashfun=follow}]
     ++
     [#fitting_spec{name=I,
                    module=riak_pipe_w_tee,
                    arg=sink,
                    chashfun=follow} || Keep]
     ++
     if PrereduceP ->
             {reduce, R_FunSpec, R_Arg, _Keep} = element(I+2, QueryT),
             [#fitting_spec{name={prereduce,I},
                            module=riak_kv_w_reduce,
                            arg={rct,
                                 riak_kv_w_reduce:reduce_compat(R_FunSpec),
                                 R_Arg},
                            chashfun=follow}];
        true ->
             []
     end.              

%% @doc Examine query and application options to determine if
%% prereduce is appropriate.
-spec want_prereduce_p(Index :: integer(), Query :: tuple()) ->
         boolean().
want_prereduce_p(Idx, QueryT) ->
    {map, _FuncSpec, Arg, _Keep} = element(Idx, QueryT),
    Props = case Arg of
                L when is_list(L) -> L;         % May or may not be a proplist
                {struct, L}       -> L;         % mochijson form
                _                 -> []
            end,
    AppDefault = app_helper:get_env(riak_kv, mapred_always_prereduce, false),
    true =:= proplists:get_value(
               <<"do_prereduce">>, Props,       % mochijson form
               proplists:get_value(do_prereduce, Props, AppDefault)).

-spec query_type(integer(), tuple()) -> map | reduce | link.
query_type(Idx, QueryT) ->
    element(1, element(Idx, QueryT)).

%% @doc Convert a reduce phase to its equivalent pipe fittings.
%%
%% Reduce converts to:
%% <ol>
%%    <li>A required {@link riak_kv_w_reduce} to run the given query
%%    function on the input data.</li>
%%    <li>An optional {@link riak_pipe_w_tee} if `keep=true'.</li>
%% </ol>
%%
%% A constant has is used to get all of the inputs for the reduce to
%% the same vnode, without caring about which specific vnode that is.
-spec reduce2pipe(reduce_query_fun(), term(), boolean(),
                  Index :: integer(), ConstantHash :: chash:index()) ->
         [ riak_pipe:fitting_spec() ].
reduce2pipe(FunSpec, Arg, Keep, I, Hash) ->
    [#fitting_spec{name={reduce,I},
                   module=riak_kv_w_reduce,
                   arg={rct,
                        riak_kv_w_reduce:reduce_compat(FunSpec),
                        Arg},
                   chashfun=Hash}
     |[#fitting_spec{name=I,
                     module=riak_pipe_w_tee,
                     arg=sink,
                     chashfun=follow}
       ||Keep]].

%% @doc Convert a link phase to its equivalent pipe fittings.
%%
%% Link converts to:
%% Map converts to:
%% <ol>
%%    <li>A required {@link riak_kv_pipe_get} to fetch the data for
%%    the input key.</li>
%%    <li>A required {@link riak_pipe_w_xform} to perform the link
%%    extraction</li>
%%    <li>An optional {@link riak_pipe_w_tee} if `keep=true'.</li>
%% </ol>
-spec link2pipe(link_match(), link_match(), boolean(),
                Index :: integer(), Query :: tuple()) ->
         [ riak_pipe:fitting_spec() ].
link2pipe(Bucket, Tag, Keep, I, _QueryT) ->
    [#fitting_spec{name={kvget_map,I},
                   module=riak_kv_pipe_get,
                   chashfun={riak_kv_pipe_get, bkey_chash},
                   nval={riak_kv_pipe_get, bkey_nval}},
     #fitting_spec{name={xform_map,I},
                   module=riak_kv_mrc_map,
                   arg={{modfun, riak_kv_mrc_map, link_phase},
                        {Bucket, Tag}},
                   chashfun=follow}|
     [#fitting_spec{name=I,
                    module=riak_pipe_w_tee,
                    arg=sink,
                    chashfun=follow} || Keep]].

%% @doc Strip extra 'tee' fittings, and correct fitting names used by
%% the naive converters.
-spec fix_final_fitting([ riak_pipe:fitting_spec() ]) ->
         [ riak_pipe:fitting_spec() ].
fix_final_fitting(Fittings) ->
    case lists:reverse(Fittings) of
        [#fitting_spec{module=riak_pipe_w_tee,
                       name=Int},
         #fitting_spec{}=RealFinal|Rest]
          when is_integer(Int) ->
            %% chop off tee so we don't get double answers
            lists:reverse([RealFinal#fitting_spec{name=Int}|Rest]);
        [#fitting_spec{name={_Type,Int}}=Final|Rest]
          when is_integer(Int) ->
            %% fix final name so outputs look like old API
            lists:reverse([Final#fitting_spec{name=Int}|Rest])
    end.

%% @doc How many phases have `keep=true'?
-spec count_keeps_in_query([query_part()]) -> non_neg_integer().
count_keeps_in_query(Query) ->
    lists:foldl(fun({_, _, _, true}, Acc) -> Acc + 1;
                   (_, Acc)                 -> Acc
                end, 0, Query).

%% @equiv send_inputs_async(Pipe, Inputs, 60000)
send_inputs_async(Pipe, Inputs) ->
    send_inputs_async(Pipe, Inputs, ?DEFAULT_TIMEOUT).

%% @doc Spawn a process to send inputs to the MapReduce pipe.  If
%% sending completes without error, the process will exit normally.
%% If errors occur, the process exits with the error as its reason.
%%
%% The process links itself to the pipeline (via the builder), so if
%% the pipeline shutsdown before sending inputs finishes, the process
%% will be torn down automatically.  This also means that an error
%% sending inputs will automatically tear down the pipe (because the
%% process will exit abnormally).
%%
%% It's a good idea to prefer sending inputs and receiving outputs in
%% different processes, especially if you're both sending a large
%% number of inputs (a large bucket list, for instance) and expecting
%% to receive a large number of outputs.  The mailbox for a process
%% doing both is likely to be a point of contention, otherwise.
-spec send_inputs_async(riak_pipe:pipe(), input(), timeout()) ->
         {Sender::pid(), MonitorRef::reference()}.
send_inputs_async(Pipe, Inputs, Timeout) ->
    spawn_monitor(
      fun() ->
              %% tear this process down if the pipeline goes away;
              %% also automatically tears down the pipeline if feeding
              %% it inputs fails (which is what the users of this
              %% function, riak_kv_pb_socket and riak_kv_wm_mapred, want)
              erlang:link(Pipe#pipe.builder),
              case send_inputs(Pipe, Inputs, Timeout) of
                  ok ->
                      %% monitoring process sees a 'normal' exit
                      %% (and linked builder is left alone)
                      ok;
                  Error ->
                      %% monitoring process sees an 'error' exit
                      %% (and linked builder dies)
                      exit(Error)
              end
      end).

%% @equiv send_inputs(Pipe, Inputs, 60000)
send_inputs(Pipe, Inputs) ->
    send_inputs(Pipe, Inputs, ?DEFAULT_TIMEOUT).

%% @doc Send inputs into the MapReduce pipe.  This function handles
%% setting up the bucket-listing, index-querying, searching, or
%% modfun-evaluating needed to produce keys, if the input is not just
%% a list of keys.
-spec send_inputs(riak_pipe:pipe(), input(), timeout()) ->
         ok | term().
send_inputs(Pipe, BucketKeyList, _Timeout) when is_list(BucketKeyList) ->
    try [ok = riak_pipe:queue_work(Pipe, BKey)
         || BKey <- BucketKeyList] of
        _ ->
            riak_pipe:eoi(Pipe),
            ok
    catch error:{badmatch,{error,_}=Error} ->
            Error
    end;
send_inputs(Pipe, Bucket, Timeout) when is_binary(Bucket) ->
    riak_kv_pipe_listkeys:queue_existing_pipe(Pipe, Bucket, Timeout);
send_inputs(Pipe, {Bucket, FilterExprs}, Timeout) ->
    case riak_kv_mapred_filters:build_filter(FilterExprs) of
        {ok, Filters} ->
            riak_kv_pipe_listkeys:queue_existing_pipe(
              Pipe, {Bucket, Filters}, Timeout);
        Error ->
            Error
    end;
send_inputs(Pipe, {index, Bucket, Index, Key}, Timeout) ->
    Query = {eq, Index, Key},
    case riak_core_capability:get({riak_kv, mapred_2i_pipe}, false) of
        true ->
            riak_kv_pipe_index:queue_existing_pipe(
              Pipe, Bucket, Query, Timeout);
        _ ->
            %% must use modfun form if there are 1.0 nodes in the cluster,
            %% because they do not have the riak_kv_pipe_index module
            NewInput = {modfun, riak_index, mapred_index, [Bucket, Query]},
            send_inputs(Pipe, NewInput, Timeout)
    end;
send_inputs(Pipe, {index, Bucket, Index, StartKey, EndKey}, Timeout) ->
    Query = {range, Index, StartKey, EndKey},
    case riak_core_capability:get({riak_kv, mapred_2i_pipe}, false) of
        true ->
            riak_kv_pipe_index:queue_existing_pipe(
              Pipe, Bucket, Query, Timeout);
        _ ->
            NewInput = {modfun, riak_index, mapred_index, [Bucket, Query]},
            send_inputs(Pipe, NewInput, Timeout)
    end;
send_inputs(Pipe, {search, Bucket, Query}, Timeout) ->
    NewInput = {modfun, riak_search, mapred_search, [Bucket, Query, []]},
    send_inputs(Pipe, NewInput, Timeout);
send_inputs(Pipe, {search, Bucket, Query, Filter}, Timeout) ->
    NewInput = {modfun, riak_search, mapred_search, [Bucket, Query, Filter]},
    send_inputs(Pipe, NewInput, Timeout);
send_inputs(Pipe, {modfun, Mod, Fun, Arg} = Modfun, Timeout) ->
    try Mod:Fun(Pipe, Arg, Timeout) of
        {ok, Bucket, ReqId} ->
            send_key_list(Pipe, Bucket, ReqId);
        Other ->
            Other
    catch
        X:Y ->
            {Modfun, X, Y, erlang:get_stacktrace()}
    end.

%% @doc Helper function used to redirect the results of
%% index/search/etc. queries into the MapReduce pipe.  The function
%% expects to receive zero or more messages of the form `{ReqId,
%% {keys, Keys}}' or `{ReqId, {results, Results}}', followed by one
%% message of the form `{ReqId, done}'.
-spec send_key_list(riak_pipe:pipe(), binary(), term()) ->
         ok | term().
send_key_list(Pipe, Bucket, ReqId) ->
    receive
        {ReqId, {keys, Keys}} ->
            %% Get results from list keys operation.
            try [ok = riak_pipe:queue_work(Pipe, {Bucket, Key})
                 || Key <- Keys] of
                _ ->
                    send_key_list(Pipe, Bucket, ReqId)
            catch error:{badmatch,{error,_}=Error} ->
                    Error
            end;

        {ReqId, {results, Results}} ->
            %% Get results from 2i operation. Handle both [Keys] and [{Key,
            %% Props}] formats. If props exists, use it as keydata.
            F = fun
                    ({Key, Props}) ->
                        riak_pipe:queue_work(Pipe, {{Bucket, Key}, Props});
                    (Key) ->
                        riak_pipe:queue_work(Pipe, {Bucket, Key})
                end,
            try [ok = F(X) || X <- Results] of
                _ ->
                    send_key_list(Pipe, Bucket, ReqId)
            catch error:{badmatch,{error,_}=Error} ->
                    Error
            end;

        {ReqId, {error, Reason}} ->
            {error, Reason};

        {ReqId, done} ->
            %% Operation has finished.
            riak_pipe:eoi(Pipe),
            ok
    end.

%% @equiv collect_outputs(Pipe, NumKeeps, 60000)
collect_outputs(Pipe, NumKeeps) ->
    collect_outputs(Pipe, NumKeeps, ?DEFAULT_TIMEOUT).

%% @doc Receive the results produced by the MapReduce pipe (directly,
%% with no sink process between here and there), grouped by the phase
%% they came from.  See {@link group_outputs/2} for details on that
%% grouping.
-spec collect_outputs(riak_pipe:pipe(), non_neg_integer(), timeout()) ->
         {ok, grouped_results()}
       | {error, {Reason :: term(), Outputs :: ungrouped_results()}}.
collect_outputs(Pipe, NumKeeps, Timeout) ->
    {Result, Outputs, []} = riak_pipe:collect_results(Pipe, Timeout),
    case Result of
        eoi ->
            %% normal result
            {ok, group_outputs(Outputs, NumKeeps)};
        Other ->
            {error, {Other, Outputs}}
    end.

%% @doc Group the outputs of the MapReduce pipe by the phase that
%% produced them.  To be used with {@link collect_outputs/3}. If
%% `NumKeeps' is 2 or more, the return value is a list of result
%% lists, `[Results :: list()]', in the same order as the phases that
%% produced them.  If `NumKeeps' is less than 2, the return value is
%% just a list (possibly empty) of results, `Results :: list()'.
-spec group_outputs(ungrouped_results(), non_neg_integer()) ->
         grouped_results().
group_outputs(Outputs, NumKeeps) when NumKeeps < 2 -> % 0 or 1
    %% this path trusts that outputs are from only one phase;
    %% if NumKeeps lies, all phases will be grouped together;
    %% this is much faster than using dict:append/3 for a single key
    %% when length(Outputs) is large
    [ O || {_, O} <- Outputs ];
group_outputs(Outputs, _NumKeeps) ->
    Group = fun({I,O}, Acc) ->
                    %% it is assumed that the number of phases
                    %% producing outputs is small, so a linear search
                    %% through phases we've seen is not too taxing
                    case lists:keytake(I, 1, Acc) of
                        {value, {I, IAcc}, RAcc} ->
                            [{I,[O|IAcc]}|RAcc];
                        false ->
                            [{I,[O]}|Acc]
                    end
            end,
    Merged = lists:foldl(Group, [], Outputs),
    [ lists:reverse(O) || {_, O} <- lists:keysort(1, Merged) ].

%% @doc Receive the results produced by the MapReduce pipe, via the
%% sink started in {@link mapred_stream_sink/3}, grouped by the phase
%% they came from. If `NumKeeps' is 2 or more, the return value is a
%% list of result lists, `[Results :: list()]', in the same order as
%% the phases that produced them.  If `NumKeeps' is less than 2, the
%% return value is just a list (possibly empty) of results, `Results
%% :: list()'.
-spec collect_sink(#mrc_ctx{}) ->
         {ok, grouped_results()}
       | {error, receive_sink_error()}.
collect_sink(#mrc_ctx{keeps=NumKeeps}=Ctx) ->
    case collect_sink_loop(Ctx, []) of
        {ok, Outputs} ->
            {ok, remove_fitting_names(Outputs, NumKeeps)};
        {error, Reason, _}->
            {error, Reason}
    end.

%% collect everything the pipe has to offer
collect_sink_loop(Ctx, Acc) ->
    case receive_sink(Ctx) of
        {ok, false, Output} ->
            collect_sink_loop(Ctx, [Output|Acc]);
        {ok, true, Output} ->
            {ok, riak_kv_mrc_sink:merge_outputs([Output|Acc])};
        {error, Reason, Outputs} ->
            {error, Reason, Outputs}
    end.

%% @doc Receive any output generated by the system set up in {@link
%% mapred_stream_sink/3}. This will include any of the following:
%%
%% <ul>
%% <li>`#kv_mrc_sink{}'</li>
%% <li>`DOWN' for `#mrc_ctx.sender' (the async sender)</li>
%% <li>`DOWN' for `#mrc_ctx.sink'</li>
%% <li>`{pipe_timeout, #mrc_ctx.ref}'</li>
%% </ul>
%%
%% An `{ok, Done::boolean(), Results::orddict()}' tuple is returned if
%% a `#kv_mrc_sink{}' message is recieved with no error logs. An
%% `{error, Reason::term(), PartialResults::orddict()}' tuple is
%% returned if any of the following are received: `#kv_mrc_sink{}'
%% message with an error log, a `DOWN' for the async sender with
%% non-`normal' reason, a `DOWN' for the sink, or the `pipe_timeout'.
%%
%% Note that this function calls {@link riak_kv_mrc_sink:next/1}, so
%% your code should not also call it.
-spec receive_sink(#mrc_ctx{}) ->
          {ok, Done::boolean(), Results::grouped_results()}
        | {error, receive_sink_error(), PartialResults::grouped_results()}.
receive_sink(#mrc_ctx{sink={Sink,_}}=Ctx) ->
    %% the sender-DOWN-normal case loops to ignore that message, but
    %% we only want to send our next-request once
    riak_kv_mrc_sink:next(Sink),
    receive_sink_helper(Ctx).

receive_sink_helper(#mrc_ctx{ref=PipeRef,
                             sink={Sink, SinkMon},
                             sender={Sender, SenderMon}}=Ctx) ->
    receive
        #kv_mrc_sink{ref=PipeRef, results=Results, logs=Logs, done=Done} ->
            case error_exists(Logs) of
                {true, From, Info} ->
                    {error, {From, Info}, Results};
                false ->
                    {ok, Done, Results}
            end;
        {'DOWN', SenderMon, process, Sender, normal} ->
            %% sender dying normal just means it finished
            receive_sink_helper(Ctx);
        {'DOWN', SenderMon, process, Sender, Reason} ->
            {error, {sender_died, Reason}, []};
        {'DOWN', SinkMon, process, Sink, Reason} ->
            {error, {sink_died, Reason}, []};
        {pipe_timeout, PipeRef} ->
            {error, timeout, []}
    end.

%% MR is supposed to return just a list of results if there was only
%% one phase being "kept", but a list of result lists (one per phase)
%% if multiple phases were kept.
remove_fitting_names([{_,Outputs}], NumKeeps) when NumKeeps < 2 ->
    Outputs;
remove_fitting_names(Outputs, _NumKeeps) ->
    [O || {_, O} <- Outputs].

%% @doc Destroy the pipe, and call {@link cleanup_sink/1}.
-spec destroy_sink(#mrc_ctx{}) -> ok.
destroy_sink(#mrc_ctx{pipe=Pipe}=Ctx) ->
    riak_pipe:destroy(Pipe),
    cleanup_sink(Ctx).

%% @doc Tear down the async sender, sink, and timer pieces setup by
%% {@link mapred_stream_sink/3}, and collect any messages they might
%% have been delivering.
-spec cleanup_sink(#mrc_ctx{}|{pid(),reference()}|undefined) -> ok.
cleanup_sink(#mrc_ctx{sender=Sender, sink=Sink, timer=Timer}) ->
    cleanup_sender(Sender),
    cleanup_sink(Sink),
    cleanup_timer(Timer);
cleanup_sink({SinkPid, SinkMon}) when is_pid(SinkPid),
                                      is_reference(SinkMon) ->
    erlang:demonitor(SinkMon, [flush]),
    %% killing the sink should tear down the pipe
    riak_kv_mrc_sink:stop(SinkPid),
    %% receive just in case the sink had sent us one last response
    receive #kv_mrc_sink{} -> ok after 0 -> ok end;
cleanup_sink(undefined) ->
    ok.

%% Destroying the pipe via riak_pipe_builder:destroy/1 does not kill
%% the sender immediately, because it causes the builder to exit with
%% reason `normal', so no exit signal is sent. The sender will
%% eventually receive `worker_startup_error's from vnodes that can no
%% longer find the fittings, but to help the process along, we kill
%% them immediately here.
cleanup_sender({SenderPid, SenderMon}) when is_pid(SenderPid),
                                            is_reference(SenderMon) ->
    erlang:demonitor(SenderMon, [flush]),
    exit(SenderPid, kill),
    ok;
cleanup_sender(undefined) ->
    ok.

%% don't let timer messages leak
cleanup_timer({Tref, PipeRef}) when is_reference(Tref),
                                    is_reference(PipeRef) ->
    case erlang:cancel_timer(Tref) of
        false ->
            receive
                {pipe_timeout, PipeRef} ->
                    ok
            after 0 ->
                    ok
            end;
        _ ->
            ok
    end;
cleanup_timer(undefined) ->
    ok.

%% @doc Look through the logs the pipe produced, and determine if any
%% of them signal an error. Return the details about the first error
%% found.
%%
%% Each log should be of the form: `{#pipe_log.from, #pipe_log.msg}'
-spec error_exists(list()) -> {true, term(), term()} | false.
error_exists(Logs) ->
    case [ {F, I} || {F, {trace, [error], {error, I}}} <- Logs ] of
        [{From, Info}|_] ->
            {true, From, Info};
        [] ->
            false
    end.

%% @doc Produce an Erlang term from a string containing Erlang code.
%% This is used by {@link riak_kv_mrc_map} and {@link
%% riak_kv_w_reduce} to compile functions specified as `{strfun,
%% Source}'.
-spec compile_string(string()|binary()) -> {ok, term()}
                                | {ErrorType :: term, Reason :: term}.
compile_string(Binary) when is_binary(Binary) ->
    compile_string(binary_to_list(Binary));
compile_string(String) when is_list(String) ->
    try
        {ok, Tokens, _} = erl_scan:string(String),
        {ok, [Form]} = erl_parse:parse_exprs(Tokens),
        {value, Value, _} = erl_eval:expr(Form, erl_eval:new_bindings()),
        {ok, Value}
    catch Type:Error ->
            {Type, Error}
    end.

%% choose sink sync period, given Options, app env, default
-spec sink_sync_period() -> integer() | infinity.
sink_sync_period() ->
    case application:get_env(riak_kv, mrc_sink_sync_period) of
        {ok, Size} when is_integer(Size); Size == infinity ->
            Size;
        _ ->
            ?SINK_SYNC_PERIOD_DEFAULT
    end.

%%%

%% @doc Use a MapReduce query to get the value of the `foo/bar'
%% object.  See {@link example_setup/1} for details of what should be
%% in `foo/bar'.
-spec example() -> {ok, [binary()]}
                 | {error, term()} | {error, term(), term()}.
example() ->
    mapred([{<<"foo">>, <<"bar">>}],
           [{map, {modfun, riak_kv_mapreduce, map_object_value},
             none, true}]).

%% @doc Use a MapReduce query to get the values of the objects in the
%% `foo' bucket.  See {@link example_setup/1} for details of what
%% should be in `foo/*'.
-spec example_bucket() -> {ok, [binary()]}
                        | {error, term()} | {error, term(), term()}.
example_bucket() ->
    mapred(<<"foo">>,
           [{map, {modfun, riak_kv_mapreduce, map_object_value},
             none, true}]).

%% @doc Use a MapReduce query to sum the values of the objects in the
%% `foonum' bucket.  See {@link example_setup/1} for details of what
%% should be in `foonum/*'.
%%
%% This function asks to keep the results of both the map phase and
%% the reduce phase, so the output should be a list containing two
%% lists.  The first sublist should contain all of the values of the
%% objects in the bucket.  The second sublist should contain only one
%% element, equal to the sum of the elements in the first sublist.
%% For example, `[[1,2,3,4,5],[15]]'.
-spec example_reduce() -> {ok, [[integer()]]}
                        | {error, term()} | {error, term(), term()}.
example_reduce() ->
    mapred(<<"foonum">>,
           [{map, {modfun, riak_kv_mapreduce, map_object_value},
             none, true},
            {reduce, {qfun, fun(Inputs, _) -> [lists:sum(Inputs)] end},
             none, true}]).

%% @equiv example_setup(5)
example_setup() ->
    example_setup(5).

%% @doc Store some example data for the other example functions.
%%
%% WARNING: This function is used by riak_test mapred_*
%% tests. Changing what it creates will break those tests.
%%
%% Objects stored:
%% <dl>
%%   <dt>`foo/bar'</dt>
%%   <dd>Stores the string "what did you expect?"</dd>
%%
%%   <dt>`foo/bar1' .. `foo/barNum'</dt>
%%   <dd>Each stores the string "bar val INDEX"</dd>
%%
%%   <dt>`foonum/bar1' .. `foo/barNum'</dt>
%%   <dd>Each stores its index as an integer</dd>
%%</dl>
-spec example_setup(pos_integer()) -> ok.
example_setup(Num) when Num > 0 ->
    {ok, C} = riak:local_client(),
    C:put(riak_object:new(<<"foo">>, <<"bar">>, <<"what did you expect?">>)),
    [C:put(riak_object:new(<<"foo">>,
                           list_to_binary("bar"++integer_to_list(X)),
                           list_to_binary("bar val "++integer_to_list(X))))
     || X <- lists:seq(1, Num)],
    [C:put(riak_object:new(<<"foonum">>,
                           list_to_binary("bar"++integer_to_list(X)),
                           X)) ||
        X <- lists:seq(1, Num)],
    ok.

%% @doc For Riak 1.0 compatibility, provide a translation from old
%% anonymous functions to new ones.  This function should have a
%% limited-use lifetime: it will only be evaluated while a cluster is
%% in the middle of a rolling-upgrade from 1.0.x to 1.1.
%%
%% Yes, the return value is a new anonymous function.  This shouldn't
%% be a problem with a future upgrade, though, as no one should be
%% running a cluster that includes three Riak versions.  Therefore, the
%% node that spread this old Fun around the cluster should have been
%% stopped, along with the pipe defined by the old fun before this new
%% fun would itself be considered old.
compat_fun(Fun) ->
    {uniq, Uniq} = erlang:fun_info(Fun, uniq),
    {index, I} = erlang:fun_info(Fun, index),
    compat_fun(Uniq, I, Fun).

%% Riak 1.0.1 and 1.0.2 funs
compat_fun(120571329, 1, _Fun) ->
    %% chash used for kv_get in map
    {ok, fun riak_kv_pipe_get:bkey_chash/1};
compat_fun(112900629, 2, _Fun) ->
    %% nval used for kv_get in map
    {ok, fun riak_kv_pipe_get:bkey_nval/1};
compat_fun(19126064, 3, Fun) ->
    %% constant chash used for reduce
    {env, [Hash]} = erlang:fun_info(Fun, env),
    {ok, fun(_) -> Hash end};
compat_fun(29992360, 4, _Fun) ->
    %% chash used for kv_get in link
    {ok, fun riak_kv_pipe_get:bkey_chash/1};
compat_fun(22321692, 5, _Fun) ->
    %% nval used for kv_get in link
    {ok, fun riak_kv_pipe_get:bkey_nval/1};
compat_fun(66856669, 6, Fun) ->
    %% link extraction function
    %% Yes, the env really does have bucket and tag the reverse of the spec
    {env, [Tag, Bucket]} = erlang:fun_info(Fun, env),
    {ok, fun({ok, Input, _Keydata}, Partition, FittingDetails) ->
                 Results = riak_kv_mrc_map:link_phase(
                             Input, undefined, {Bucket, Tag}),
                 [ riak_pipe_vnode_worker:send_output(
                     R, Partition, FittingDetails)
                   || R <- Results ],
                 ok;
            ({{error, _},_,_}, _, _) ->
                 ok
         end};

%% dunno
compat_fun(_, _, _) ->
    error.

%% @doc Generate a constant hash for a vnode we know is up. This is
%% used to route all reduce inputs to the same vnode.
random_constant_hash()->
    %% using now/0 as a randomizer, so that we don't overload just one
    %% riak node with reduce work
    Random = chash:key_of(now()),

    %% the trick here: generate a random hash, then find the first
    %% vnode alive in its preflist, then re-generate a hash that puts
    %% the live vnode at the head of its preflist (since we'll be
    %% using N=1 for reduce)
    %%
    %% one problem with this strategy is that it makes the burden of
    %% reduce a little unfair on those vnodes that are successors to
    %% down vnodes (because they will now receive all of their own
    %% work, plus all work assigned to their predecessor); perhaps
    %% something that also skips a random number of up vnodes in the
    %% next version?
    {Partition, _Node} = riak_core_apl:first_up(Random, riak_pipe),
    riak_pipe_vnode:hash_for_partition(Partition).
