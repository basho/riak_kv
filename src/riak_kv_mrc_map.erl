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

%% @doc A pipe fitting that applies a function to a Riak object, and
%% sends its results downstream.
%%
%% This module is intended as the second half of the emulation layer
%% for running Riak KV MapReduce on top of Riak Pipe.  An upstream
%% fitting should read the object out of Riak KV, and then send it to
%% this fitting as a 3-tuple of the form `{ok, RiakObject, KeyData}'.
%% If there was an error reading the object, that can be sent to this
%% fitting as a 3-tuple of the form `{{error, Reason}, {Bucket, Key},
%% KeyData}'.  (The {@link riak_kv_pipe_get} fitting conforms to this
%% interface.)
%%
%% This module expects a 2-tuple, `{PhaseSpec, PhaseArg}' as argument.
%% Both elements come directly from the phase definition in the
%% MapReduce query: `{map, PhaseSpec, PhaseArg, Keep}'.
%%
%% When an `ok' input arrives, the function defined by `PhaseSpec' is
%% evalutated as, in pseudocode: `PhaseSpec(RiakObject, KeyData,
%% PhaseArg)'.  The result of the function is expected to be a list.
%% Each element of the result list is sent downstream as an output.
%%
%% When an `error' input arrives, the behavior of this fitting depends
%% on whether `PhaseSpec' specifies an Erlang function or a Javascript
%% function.  In the case of an Erlang function, `PhaseSpec' will be
%% evaluated as `PhaseSpec({error, Reason}, KeyData, PhaseArg)'.  In
%% the case of a Javascript function, if the `Reason' is `not_found',
%% then the output `{not_found, {Bucket, Key}, KeyData}' is sent
%% downstream as output.  Other error reasons cause Javascript
%% evaluation to fail.
-module(riak_kv_mrc_map).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/3,
         done/1,
         link_phase/3,
         validate_arg/1]).

-include_lib("riak_kv_js_pools.hrl").

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("riak_pipe/include/riak_pipe_log.hrl").

-record(state, {p :: riak_pipe_vnode:partition(),
                fd :: riak_pipe_fitting:details(),
                phase :: riak_kv_mrc_pipe:map_query_fun(),
                arg :: term()}).
-opaque state() :: #state{}.

-define(DEFAULT_JS_RESERVE_ATTEMPTS, 10).

%% @doc Init verifies the phase spec, and then stashes it away with
%% `Partition' and the rest of `FittingDetails' for use during
%% processing.
-spec init(riak_pipe_vnode:partition(), riak_pipe_fitting:details()) ->
         {ok, state()} | {error, Reason :: term()}.
init(Partition, #fitting_details{arg={Phase, Arg}}=FittingDetails) ->
    case init_phase(Phase) of
        {ok, LocalPhase} ->
            {ok, #state{p=Partition, fd=FittingDetails,
                        phase=LocalPhase, arg=Arg}};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Perform any one-time initialization of the phase that's
%% possible/needed.  This currently includes looking up functions from
%% Riak KV (for `{jsanon, {Bucket, Key}}' and `{strfun, {Bucket,
%% Key}}', as well as compiling `strfun' specs to Erlang functions.
%%
%% <ul>
%%   <li>`{jsanon, {Bucket, Key}}' is converted to `{jsanon, Source}'</li>
%%
%%   <li>`{strfun, {Bucket, Key}}' and `{strfun, Source}' are both
%%   converted to `{qfun, Fun}' after compiling</li>
%% </ul>
-spec init_phase(PhaseSpec :: term()) ->
         {ok, PhaseSpec :: term()} | {error, Reason :: term()}.
init_phase({Anon, {Bucket, Key}})
  when Anon =:= jsanon; Anon =:= strfun ->
    %% lookup source for stored functions only at fitting worker startup
    {ok, C} = riak:local_client(),
    case C:get(Bucket, Key, 1) of
        {ok, Object} ->
            case riak_object:get_value(Object) of
                Source when Anon =:= jsanon, is_binary(Source) ->
                    {ok, {jsanon, Source}};
                Source when Anon =:= strfun,
                            (is_binary(Source) orelse is_list(Source)) ->
                    init_phase({strfun, Source});
                Value ->
                    {error, {Anon, {invalid, Value}}}
            end;
        {error, notfound} ->
            {error, {Anon, {notfound, {Bucket, Key}}}}
    end;
init_phase({strfun, Source}) ->
    case app_helper:get_env(riak_kv, allow_strfun, false) of
        true ->
            case riak_kv_mrc_pipe:compile_string(Source) of
                {ok, Fun} when is_function(Fun, 3) ->
                    {ok, {qfun, Fun}};
                Error ->
                    {error, {strfun, {compile_error, Error}}}
            end;
        _ ->
            {error, {strfun, not_allowed}}
    end;        
init_phase(Other) ->
    %% other types need no initialization
    {ok, Other}.

%% @doc Process evaluates the fitting's argument function, and sends
%%      output downstream.
-spec process(term(), boolean(), state())
         -> {ok | forward_preflist, state()}.
process(Input, _Last,
        #state{fd=_FittingDetails, phase=Phase, arg=Arg}=State) ->
    ?T(_FittingDetails, [map], {mapping, Input}),
    case map(Phase, Arg, Input) of
        {ok, Results} when is_list(Results) ->
            ?T(_FittingDetails, [map], {produced, Results}),
            send_results(Results, State);
        {ok, _NonListResults} ->
            ?T(_FittingDetails, [map, error],
               {error, {non_list_result, Input}}),
            {ok, State};
        {forward_preflist, Reason} ->
            ?T(_FittingDetails, [map], {forward_preflist, Reason}),
            {forward_preflist, State};
        {error, Error} ->
            ?T(_FittingDetails, [map, error], {error, {Error, Input}}),
            {ok, State}
    end.
        
%% @doc Evaluate the map function.
-spec map(riak_kv_mrc_pipe:map_query_fun(), term(), term())
         -> {ok, [term()]}
          | {forward_preflist, Reason :: term()}
          | {error, Reason :: term()}.
map({modfun, Module, Function}, Arg, Input0) ->
    Input = erlang_input(Input0),
    KeyData = erlang_keydata(Input0),
    {ok, Module:Function(Input, KeyData, Arg)};
map({qfun, Fun}, Arg, Input0) ->
    Input = erlang_input(Input0),
    KeyData = erlang_keydata(Input0),
    {ok, Fun(Input, KeyData, Arg)};
%% {strfun, Source} is converted to {qfun, Fun} in init
%% {strfun, {Bucket, Key}} is converted to {qfun, Fun} in init
%% {jsanon, {Bucket, Key}} is converted to {jsanon, Source} in init
map({jsfun, Name}, Arg, Input) ->
    map_js({jsfun, Name}, Arg, Input);
map({jsanon, Source}, Arg, Input) ->
    map_js({jsanon, Source}, Arg, Input).

%% select which bit of the input to hand to the map function
erlang_input({ok, Input, _})          -> Input;
erlang_input({{error,_}=Input, _, _}) -> Input.

%% extract keydata from the input
erlang_keydata({_OkError, _Input, KeyData}) -> KeyData.

%% @doc Evaluate Javascript map functions ... if the input is ok.
map_js(_JS, _Arg, {{error, notfound}, {Bucket, Key}, KeyData}) ->
    {ok, [{not_found,
           {Bucket, Key},
           KeyData}]};
map_js(JS, Arg, {ok, Input, KeyData}) ->
    JSArgs = [riak_object:to_json(Input), KeyData, Arg],
    JSCall = {JS, JSArgs},
    case riak_kv_js_manager:blocking_dispatch(
           ?JSPOOL_MAP, JSCall, ?DEFAULT_JS_RESERVE_ATTEMPTS) of
        {ok, Results}   -> {ok, Results};
        {error, no_vms} -> {forward_preflist, no_js_vms};
        {error, Error}  -> {error, Error}
    end.

%% @doc Function to do link extraction via this module.  The function
%% will extract all links matching Bucket and Tag from an input
%% object, and send them as fitting output.
%%
%% Note: This function was added in Riak 1.0.2, but is not used there.
%% It is intended to smooth the transition from 1.0.2 to 1.1.0.
-spec link_phase(Object::term(), KeyData::term(),
                 {Bucket::riak_kv_mrc_pipe:link_match(),
                  Tag::riak_kv_mrc_pipe:link_match()})
         -> [riak_kv_mrc_pipe:key_input()].
link_phase({error, notfound}, _, _) ->
    [];
link_phase(Input, _KeyData, {Bucket, Tag}) ->
    LinkFun = bucket_linkfun(riak_object:bucket(Input)),
    Results = LinkFun(Input, none, {Bucket, Tag}),
    Results.

%% @doc Find the link-extraction function for the bucket.
-spec bucket_linkfun(binary()) ->
         fun( (Object::term(), KeyData::term(),
               {Bucket::riak_kv_mrc_pipe:link_match(),
                Tag::riak_kv_mrc_pipe:link_match()})
            -> [riak_kv_mrc_pipe:key_input()] ).
bucket_linkfun(Bucket) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    {_, {modfun, Module, Function}} = lists:keyfind(linkfun, 1, BucketProps),
    erlang:make_fun(Module, Function, 3).

%% @doc Send results to the next fitting.
-spec send_results([term()], state()) -> {ok | {error, term()}, state()}.
send_results([], State) ->
    {ok, State};
send_results([Result | Results], #state{p=P, fd=FD} = State) ->
    case riak_pipe_vnode_worker:send_output(Result, P, FD) of
        ok ->
            send_results(Results, State);
        ER ->
            {ER, State}
    end.

%% @doc Unused.
-spec done(state()) -> ok.
done(_State) ->
    ok.

%% @doc Check that the argument is a 2-tuple, with the first element
%%      being a valid map phase specification.  For `modfun' and
%%      `qfun' phases, also check that the specified function exists,
%%      and is arity-3 (see {@link riak_pipe_v_validate_function/3}).
-spec validate_arg(term()) -> ok | {error, iolist()}.
validate_arg({Phase, _Arg}) ->
    case Phase of
        {modfun, Module, Function} ->
            riak_pipe_v:validate_function(
              "PhaseSpec", 3, erlang:make_fun(Module, Function, 3));
        {qfun, Fun} ->
            riak_pipe_v:validate_function("PhaseSpec", 3, Fun);
        {Anon, {Bucket, Key}} when Anon =:= jsanon; Anon =:= strfun->
            if is_binary(Bucket), is_binary(Key) -> ok;
               true ->
                    {error, io_lib:format(
                              "~p requires that the {Bucket,Key} of a ~p"
                              " request be a {binary,binary}, not {~p,~p}",
                              [?MODULE, Anon,
                               riak_pipe_v:type_of(Bucket),
                               riak_pipe_v:type_of(Key)])}
            end;
        {jsfun, Name} ->
            if is_binary(Name) -> ok; %% TODO: validate name somehow?
               true ->
                    {error, io_lib:format(
                              "~p requires that the Name of a jsfun"
                              " request be a binary, not a ~p",
                              [?MODULE, riak_pipe_v:type_of(Name)])}
            end;
        {jsanon, Source} ->
            if is_binary(Source) -> ok; %% TODO: validate JS code somehow?
               true ->
                    {error, io_lib:format(
                              "~p requires that the Source of a jsanon"
                              " request be a binary, not a ~p",
                              [?MODULE, riak_pipe_v:type_of(Source)])}
            end;
        {strfun, Source} ->
            if is_binary(Source); is_list(Source) ->
                    ok;
               true ->
                    {error, io_lib:format(
                              "~p requires that the Source of a strfun"
                              " request be a binary or list, not a ~p",
                              [?MODULE, riak_pipe_v:type_of(Source)])}
            end;
        _ ->
            {error, io_lib:format(
                      "The PhaseSpec part of the argument for ~p"
                      " must be of one of the following forms:~n"
                      "   {modfun, Module :: atom(), Function :: atom()}~n"
                      "   {qfun, Function :: function()}~n"
                      "   {jsanon, {Bucket :: binary(), Key :: binary()}}~n"
                      "   {jsanon, Source :: binary()}~n"
                      "   {jsfun, Name :: binary()}~n"
                      "   {strfun, Source :: string()}~n"
                      "   {strfun, {Bucket :: binary(), Key :: binary()}}~n",
                      [?MODULE])}
    end;
validate_arg(Other) ->
    {error, io_lib:format("~p requires a 2-tuple of {PhaseSpec, StaticArg}"
                          " as argument, not a ~p",
                          [?MODULE, riak_pipe_v:type_of(Other)])}.
