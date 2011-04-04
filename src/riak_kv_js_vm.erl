%% -------------------------------------------------------------------
%%
%% riak_js_vm: interaction with JavaScript VMs
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

%% @doc interaction with JavaScript VMs

-module(riak_kv_js_vm).
-author('Kevin Smith <kevin@basho.com>').
-author('John Muellerleile <johnm@basho.com>').

-behaviour(gen_server).

-define(MAX_ANON_FUNS, 25).

%% API
-export([start_link/2, dispatch/4, blocking_dispatch/3, reload/1,
         batch_blocking_dispatch/2, start_batch/1, finish_batch/1, batch_dispatch/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {manager, pool, ctx, next_funid=1, anon_funs=[], in_batch=false}).

start_link(Manager, PoolName) ->
    gen_server:start_link(?MODULE, [Manager, PoolName], []).

start_batch(VMPid) ->
    gen_server:call(VMPid, start_batch, infinity).

finish_batch(VMPid) ->
    gen_server:call(VMPid, finish_batch, infinity).

dispatch(VMPid, Requestor, JobId, JSCall) ->
    gen_server:cast(VMPid, {dispatch, Requestor, JobId, JSCall}).

blocking_dispatch(VMPid, JobId, JSCall) ->
    gen_server:call(VMPid, {dispatch, JobId, JSCall}, infinity).

batch_dispatch(VMPid, JobId, JSCall) ->
    gen_server:cast(VMPid, {batch_dispatch, JobId, JSCall}).

batch_blocking_dispatch(VMPid, JSCall) ->
    gen_server:call(VMPid, {batch_dispatch, JSCall}, infinity).

reload(VMPid) ->
    gen_server:cast(VMPid, reload).

init([Manager, PoolName]) ->
    HeapSize = read_config(js_max_vm_mem, 8),
    StackSize = read_config(js_thread_stack, 8),
    %% Dialyzer: js_driver:new throws on failure- we can match on {ok,Ctx} here.
    {ok, Ctx} =  new_context(StackSize, HeapSize),
    error_logger:info_msg("Spidermonkey VM (thread stack: ~pMB, max heap: ~pMB, pool: ~p) host starting (~p)~n", [StackSize, HeapSize, PoolName, self()]),
    riak_kv_js_manager:add_vm(PoolName),
    erlang:monitor(process, Manager),
    {ok, #state{manager=Manager, pool=PoolName, ctx=Ctx}}.


handle_call(start_batch, _From, State) ->
    {reply, ok, State#state{in_batch=true}};

handle_call(finish_batch, _From, State) ->
    NewState = State#state{in_batch=false},
    maybe_idle(NewState),
    {reply, ok, NewState};

%% Batch synchronous dispatching

%% Blocking batch map phase with named function
handle_call({batch_dispatch, _JobId, {_Sender, {map, {jsanon, JS}, Reduced, Arg}},
                                      _Value, _KeyData, _BKey}, _From, State) ->
    {Reply, UpdatedState} = define_invoke_anon_js(JS, [Reduced, Arg], State),
    {reply, Reply, UpdatedState};

%% Blocking batch map phase with named function
handle_call({batch_dispatch, _JobId, {_Sender, {map, {jsfun, JS}, _Reduced, Arg},
                                      Value, KeyData, _BKey}},
                                     _From, #state{ctx=Ctx}=State) ->
    JsonValue = riak_object:to_json(Value),
    JsonArg = jsonify_arg(Arg),
    Reply = invoke_js(Ctx, JS, [JsonValue, KeyData, JsonArg]),
    {reply, Reply, State};

%% Blocking Batch general dispatch function for anonymous function with variable number of arguments
handle_call({batch_dispatch, {map, {jsanon, Source}, Args}}, _From,
            State) when is_list(Args) ->
    {Reply, UpdatedState} = define_invoke_anon_js(Source, Args, State),
    {reply, Reply, UpdatedState};
%% Blocking Batch general dispatch function for named function with variable number of arguments
handle_call({batch_dispatch, {map, {jsfun, JS}, Args}}, _From,
            #state{ctx=Ctx}=State) when is_list(Args) ->
    Reply = invoke_js(Ctx, JS, Args),
    {reply, Reply, State};

%% Non-batch synchronous dispatching

%% Reduce phase with anonymous function
handle_call({dispatch, _JobId, {{jsanon, JS}, Reduced, Arg}}, _From, State) ->
    {Reply, UpdatedState} = define_invoke_anon_js(JS, [Reduced, Arg], State),
    maybe_idle(State),
    {reply, Reply, UpdatedState};
%% Reduce phase with named function
handle_call({dispatch, _JobId, {{jsfun, JS}, Reduced, Arg}}, _From, #state{ctx=Ctx}=State) ->
    Reply = invoke_js(Ctx, JS, [Reduced, Arg]),
    maybe_idle(State),
    {reply, Reply, State};
%% General dispatch function for anonymous function with variable number of arguments
handle_call({dispatch, _JobId, {{jsanon, Source}, Args}}, _From,
            State) when is_list(Args) ->
    {Reply, UpdatedState} = define_invoke_anon_js(Source, Args, State),
    maybe_idle(State),
    {reply, Reply, UpdatedState};
%% General dispatch function for named function with variable number of arguments
handle_call({dispatch, _JobId, {{jsfun, JS}, Args}}, _From,
            #state{ctx=Ctx}=State) when is_list(Args) ->
    Reply = invoke_js(Ctx, JS, Args),
    maybe_idle(State),
    {reply, Reply, State};
%% Pre-commit hook with named function
handle_call({dispatch, _JobId, {{jsfun, JS}, Obj}}, _From, #state{ctx=Ctx}=State) ->
    Reply = invoke_js(Ctx, JS, [riak_object:to_json(Obj)]),
    maybe_idle(State),
    {reply, Reply, State};
handle_call(Request, _From, State) ->
    io:format("Request: ~p~n", [Request]),
    {reply, ignore, State}.

handle_cast(reload, #state{ctx=Ctx, pool=Pool}=State) ->
    init_context(Ctx),
    error_logger:info_msg("Spidermonkey VM (pool: ~p) host reloaded (~p)~n", [Pool, self()]),
    {noreply, State};

%% Batch map phase with anonymous function
handle_cast({batch_dispatch, JobId, {Sender, {map, {jsanon, JS}, Arg, _Acc},
                                            Value,
                                            KeyData, _BKey}}, State) ->
    JsonValue = riak_object:to_json(Value),
    JsonArg = jsonify_arg(Arg),
    {Result, UpdatedState} = define_invoke_anon_js(JS, [JsonValue, KeyData, JsonArg], State),
    FinalState = case Result of
                     {ok, ReturnValue} ->
                         Sender ! {mapexec_reply, JobId, ReturnValue},
                         UpdatedState;
                     ErrorResult ->
                         Sender ! {mapexec_error_noretry, JobId, ErrorResult},
                         State
                 end,
    {noreply, FinalState};

%% Batch map phase with named function
handle_cast({batch_dispatch, JobId, {Sender, {map, {jsfun, JS}, Arg, _Acc},
                                            Value,
                                            KeyData, _BKey}}, #state{ctx=Ctx}=State) ->
    JsonValue = riak_object:to_json(Value),
    JsonArg = jsonify_arg(Arg),
    case invoke_js(Ctx, JS, [JsonValue, KeyData, JsonArg]) of
        {ok, R} ->
            Sender ! {mapexec_reply, JobId, R};
        Error ->
            Sender ! {mapexec_error_noretry, JobId, Error}
    end,
    {noreply, State};

%% Map phase with anonymous function
handle_cast({dispatch, _Requestor, JobId, {Sender, {map, {jsanon, JS}, Arg, _Acc},
                                            Value,
                                            KeyData, _BKey}}, State) ->
    JsonValue = riak_object:to_json(Value),
    JsonArg = jsonify_arg(Arg),
    {Result, UpdatedState} = define_invoke_anon_js(JS, [JsonValue, KeyData, JsonArg], State),
    FinalState = case Result of
                     {ok, ReturnValue} ->
                         riak_core_vnode:send_command(Sender, {mapexec_reply, JobId, ReturnValue}),
                         UpdatedState;
                     ErrorResult ->
                         riak_core_vnode:send_command(Sender, {mapexec_error_noretry, JobId, ErrorResult}),
                         State
                 end,
    maybe_idle(State),
    {noreply, FinalState};

%% Map phase with named function
handle_cast({dispatch, _Requestor, JobId, {Sender, {map, {jsfun, JS}, Arg, _Acc},
                                            Value,
                                            KeyData, _BKey}}, #state{ctx=Ctx}=State) ->
    JsonValue = riak_object:to_json(Value),
    JsonArg = jsonify_arg(Arg),
    case invoke_js(Ctx, JS, [JsonValue, KeyData, JsonArg]) of
        {ok, R} ->
            %% Requestor should be the dispatching vnode
            %%riak_kv_vnode:mapcache(Requestor, BKey, {JS, Arg, KeyData}, R),
            riak_core_vnode:send_command(Sender, {mapexec_reply, JobId, R});
        Error ->
            riak_core_vnode:send_command(Sender, {mapexec_error_noretry, JobId, Error})
    end,
    maybe_idle(State),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, _Type, Manager, _Info}, #state{manager=Manager}=State) ->
    {stop, normal, State#state{manager=undefined}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{pool=Pool, ctx=Ctx}) ->
    js_driver:destroy(Ctx),
    error_logger:info_msg("Spidermonkey VM (pool: ~p) host stopping (~p)~n", [Pool, self()]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
define_invoke_anon_js(JS, Args, #state{ctx=Ctx}=State) ->
    case define_anon_js(JS, State) of
        {ok, FunName, NewState} ->
            case invoke_js(Ctx, FunName, Args) of
                {ok, R} ->
                    {{ok, R}, NewState};
                Error ->
                    {Error, State}
            end;
        {Error, undefined, NewState} ->
            {Error, NewState}
    end.

invoke_js(Ctx, Js, Args) ->
    try
        case js:call(Ctx, Js, Args) of
            {ok, {struct, R}} ->
                case proplists:get_value(<<"lineno">>, R) of
                    undefined ->
                        {ok, R};
                    _ ->
                        {error, R}
                end;
            R ->
                R
        end
    catch
        exit: {ucs, {bad_utf8_character_code}} ->
            error_logger:error_msg("Error JSON encoding arguments: ~p~n", [Args]),
            {error, bad_encoding};
        exit: {json_encode, _} ->
            {error, bad_json};
        throw:invalid_utf8 ->
            {error, bad_encoding}
    end.

define_anon_js(JS, #state{ctx=Ctx, anon_funs=AnonFuns, next_funid=NextFunId}=State) ->
    Hash = erlang:phash2(JS),
    case proplists:get_value(Hash, AnonFuns) of
        undefined ->
            FunId = case NextFunId > ?MAX_ANON_FUNS of
                        true ->
                            1;
                        false ->
                            NextFunId
                    end,
            FunName = list_to_binary("riakAnon" ++ integer_to_list(FunId)),
            case js:define(Ctx, list_to_binary([<<"var ">>, FunName, <<"=">>, JS, <<";">>])) of
                ok ->
                    {ok, FunName, State#state{anon_funs=[{Hash, FunName}|AnonFuns], next_funid=NextFunId + 1}};
                Error ->
                    error_logger:warning_msg("Error defining anonymous Javascript function: ~p~n", [Error]),
                    {Error, undefined, State}
            end;
        FunName ->
            {ok, FunName, State}
    end.

new_context(ThreadStack, HeapSize) ->
    InitFun = fun(Ctx) -> init_context(Ctx) end,
    js_driver:new(ThreadStack, HeapSize, InitFun).

init_context(Ctx) ->
    load_user_builtins(Ctx),
    load_mapred_builtins(Ctx).

priv_dir() ->
    %% Hacky workaround to handle running from a standard app directory
    %% and .ez package
    case code:priv_dir(riak_kv) of
        {error, bad_name} ->
            filename:join([filename:dirname(code:which(?MODULE)), "..", "priv"]);
        Dir ->
            Dir
    end.

load_user_builtins(Ctx) ->
    case app_helper:get_env(riak_kv, js_source_dir, undefined) of
        undefined ->
            ok;
        Path ->
            Files = filelib:wildcard("*.js", Path),
            lists:foreach(fun(File) ->
                                  {ok, Contents} = file:read_file(filename:join([Path, File])),
                                  js:define(Ctx, Contents) end, Files)
    end.

load_mapred_builtins(Ctx) ->
    {ok, Contents} = file:read_file(filename:join([priv_dir(), "mapred_builtins.js"])),
    js:define(Ctx, Contents).

jsonify_arg({Bucket,Tag}) when (Bucket == '_' orelse is_binary(Bucket)),
                               (Tag == '_' orelse is_binary(Tag)) ->
    %% convert link match syntax
    {struct, [{<<"bucket">>,Bucket},
              {<<"tag">>,Tag}]};
jsonify_arg([H|_]=Other) when is_tuple(H);
                              is_atom(H) ->
    {struct, Other};
jsonify_arg(Other) ->
    Other.

read_config(Param, Default) ->
    case app_helper:get_env(riak_kv, Param, 8) of
        N when is_integer(N) ->
            N;
        _ ->
            Default
    end.

maybe_idle(#state{in_batch=false, pool=Pool}) ->
    riak_kv_js_manager:mark_idle(Pool);
maybe_idle(#state{in_batch=true}) ->
    ok.
