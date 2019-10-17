%% --------------------------------------------------------------------------
%%
%% riak_repl_rtenqueue - common client code for PB/HTTP to use for rtq enqueue
%%
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
%% --------------------------------------------------------------------------

%% @doc riak local client style command for putting Riak bucket/key
%% onto the realtime repl queue
%%

-module(riak_repl_rtenqueue).

-export([rt_enqueue/4]).

%% @doc Users of Riak sometimes want Riak to realtime replicate an
%% object on demand (dropped messages, external divergence detection
%% etc) This function gets the object stored under `Bucket' `Key',
%% with the get options from `Options' and calls the postcommit
%% function(s) defined for realtime IF realtime is enabled for
%% `Bucket'. Returns `ok' if all went well, or `{error, Reason}' is
%% there was a failure. Riak seems to support "hybrid" configurations
%% with the possibility that two types of realtime queue are in use,
%% therefore error may obscure partial success.
-spec rt_enqueue(Bucket, Key, Options, Client) ->
                        ok | Error
                            when
      Bucket::riak_object:bucket(),
      Key::riak_object:key(),
      Options::riak_kv_get_fsm:options(),
      Client::riak_client:client(),
      Error::{error, Reason},
      Reason::term().
rt_enqueue(Bucket, Key, Options, Client) ->
    GetRes = Client:get(Bucket, Key, Options),
    case GetRes of
        {ok, Object} ->
            rt_enqueue_object(Object);
        GetErr ->
            GetErr
    end.

%% @private used by rt_equeue, once the object has been got.
-spec rt_enqueue_object(riak_object:object()) ->
                               ok | {error, ErrReason::term()}.
rt_enqueue_object(Object) ->
    BucketProps = get_bucket_props(Object),
    %% If repl is a thing, then get the postcommit hook(s) from the
    %% util (not the props) as the props does not name hooks, and may
    %% have many we don't care about. See riak_repl:fixup/2
    RTEnabled = app_helper:get_env(riak_repl, rtenabled, false),

    case proplists:get_value(repl, BucketProps) of
        Val when (Val==true orelse Val==realtime orelse Val==both),
                 RTEnabled == true  ->
            Hooks = riak_repl_util:get_hooks_for_modes(),
            run_rt_hooks(Object, Hooks, []);
        _ ->
            {error, realtime_not_enabled}
    end.


-spec run_rt_hooks(riak_object:riak_object(), Hooks::list(), ResAcc::list()) ->
                          ok | {error, Err::term()}.
run_rt_hooks(_Object, _Hooks=[], Acc) ->
    lists:foldl(fun rt_repl_results/2, ok, Acc);
run_rt_hooks(Object, [{struct, Hook} | Hooks], Acc) ->
    %% Bit jankey as it duplicates code in riak_kv_fsm postcommit
    ModBin = proplists:get_value(<<"mod">>, Hook),
    FunBin = proplists:get_value(<<"fun">>, Hook),

    Res =
        try
            Mod = binary_to_atom(ModBin, utf8),
            Fun = binary_to_atom(FunBin, utf8),
            Mod:Fun(Object)
        catch
             Class:Exception ->
                {error, {Hook, Class, Exception}}
        end,
    run_rt_hooks(Object, Hooks, [Res | Acc]).

-spec rt_repl_results(Res, ResAcc) -> ResAcc when
      Res :: ok | Err,
      Err :: any(),
      ResAcc:: ok | {error, list(Err)}.
rt_repl_results(_Res=ok, _ResAcc=ok) ->
    ok;
rt_repl_results(ErrRes, _ResAcc=ok) ->
    {error, [ErrRes]};
rt_repl_results(ErrRes, {error, Errors}) ->
    {error, [ErrRes | Errors]}.

%% @doc get the properties for a riak_kv_object's bucket. This code
%% appears in a few places, and I was about to cut and paste it to yet
%% another, and decided to give it home.
-spec get_bucket_props(riak_object:riak_object()) -> list({atom(), any()}).
get_bucket_props(RObj) ->
    Bucket = riak_object:bucket(RObj),
    {ok, DefaultProps} = application:get_env(riak_core,
                                             default_bucket_props),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    %% typed buckets never fall back to defaults
    case is_tuple(Bucket) of
        false ->
            lists:keymerge(1, lists:keysort(1, BucketProps),
                           lists:keysort(1, DefaultProps));
        true ->
            BucketProps
    end.
