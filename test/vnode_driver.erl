%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2014, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 18 Dec 2014 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(vnode_driver).

-behaviour(riak_kv_backend).

%% API
-export([api_version/0, capabilities/1, capabilities/2,
        start/2, stop/1, get/3, put/5, delete/4, drop/1, fold_buckets/4,
         fold_keys/4, fold_objects/4, is_empty/1, status/1, callback/3]).

-compile(export_all).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("riak_kv_vnode.hrl").

-define(MAX_INT, ((1 bsl 32) -1)).

-define(B, <<"b">>).
-define(K, <<"K">>).
-define(BKEY1, {?B, ?K}).
-define(V1, <<"v1">>).
-define(NEW_OBJ, riak_object:new(?B, ?K, ?V1)).
-define(DEFAULT_OPTIONS, []).

-ifdef(TEST).

simple_test_() ->
    riak_kv_backend:standard_test(?MODULE, []).

first_write_new_epoch_test_() ->
    {spawn, [
             {setup, local,
              fun setup/0,
              fun cleardown/1,
              fun(VnodeState) ->
                      [fwt(VnodeState)]
              end}]}.

fwt(VnodeState) ->
    {"Epoch",
     fun() ->
             Req = ?KV_PUT_REQ{bkey=?BKEY1,
                               req_id=epoch_test,
                               object=?NEW_OBJ,
                               start_time=100,
                               options=?DEFAULT_OPTIONS},
             riak_kv_vnode:handle_command(Req, undefined, VnodeState),
             Rep = meck:capture(first, riak_core_vnode, reply, '_', 2),
             ?assertEqual({w, 1, epoch_test}, Rep),
             ?debugFmt("Mgr Pid ~p~n", [manager_pid(VnodeState)]),
             Backend = backend_status(VnodeState),
             ?debugFmt("BE ~p~n", [Backend]),
             ?assertEqual({ok, ?V1}, orddict:find(?BKEY1, Backend)),
             ?assertEqual(false, true)
     end}.

setup() ->
    meck:new(app_helper),
    meck:new(riak_core_vnode),
    meck:new(riak_core_bg_manager),
    meck:new(riak_core_bucket),
    meck:expect(app_helper, get_env, fun(riak_kv, storage_backend) ->
                                             vnode_driver;
                                        (_, _) -> undefined
                                     end),

    meck:expect(app_helper, get_env, fun(_) -> [] end),
    meck:expect(app_helper, get_env, fun(riak_kv, async_folds, true) ->
                                             false;
                                        (_, _, Def) ->
                                             Def end),

    meck:expect(riak_core_vnode, reply, fun(_,_) -> ok end),
    meck:expect(riak_core_bg_manager, use_bg_mgr, fun() -> false end),
    meck:expect(riak_core_bucket, get_bucket, fun(_) ->
                                                      riak_core_bucket_type:defaults()
                                              end),
    {ok, Status} = riak_kv_vnode:init([1]),
    Status.

cleardown(Status) ->
    MgrPid = manager_pid(Status),
    riak_kv_vnode_status_mgr:clear_vnodeid(MgrPid),
    riak_kv_vnode_status_mgr:stop(MgrPid),
    meck:unload(riak_core_vnode),
    meck:unload(app_helper).


manager_pid(Status) ->
    element(20, Status). %% eugh, sorry!

backend_status(Status) ->
    element(4, Status).

-endif.
%%%===================================================================
%%% Backend API
%%%===================================================================

api_version() ->
    1.

capabilities(_) ->
    {ok, [async_fold, size]}.

capabilities(_, _) ->
    {ok, [async_fold, size]}.

start(_, _) ->
    {ok, orddict:new()}.

stop(_Dict) ->
    ok.

get(Bucket, Key, Dict) ->
    case orddict:find({Bucket, Key}, Dict) of
        {ok, Value} ->
            {ok, Value, Dict};
        error ->
            {error, not_found, Dict}
    end.

put(Bucket, Key, _IndexSpecs, Val, Dict) ->
    Dict2 = orddict:store({Bucket, Key}, Val, Dict),
    {ok, Dict2}.

delete(Bucket, Key, _Specs, Dict) ->
    {ok, orddict:erase({Bucket, Key}, Dict)}.

drop(_) ->
    {ok, orddict:new()}.

fold_buckets(Fun, Acc, Opts, Dict) ->
    Fold = fun() ->
                   orddict:fold(fun({B, _K}, _V, FAcc) ->
                                        Fun(B, FAcc)
                                end,
                                Acc,
                                Dict)
           end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, Fold};
        _ ->
            Res = Fold(),
            {ok, Res}
    end.

fold_keys(Fun, Acc, Opts, Dict) ->
    Bucket =  proplists:get_value(bucket, Opts),
    Fold = fun() ->
                   orddict:fold(fun({B, K}, _V, FAcc) ->
                                        case Bucket of
                                            undefined ->
                                                Fun(B, K, FAcc);
                                            B ->
                                                Fun(B, K, FAcc);
                                            _ ->
                                                FAcc
                                        end
                                end,
                                Acc,
                                Dict)
           end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, Fold};
        _ ->
            Res = Fold(),
            {ok, Res}
    end.

fold_objects(Fun, Acc, Opts, Dict) ->
    Bucket =  proplists:get_value(bucket, Opts),
    Fold = fun() ->
                   orddict:fold(fun({B, K}, V, FAcc) ->
                                        case Bucket of
                                            undefined ->
                                                Fun(B, K, V, FAcc);
                                            B ->
                                                Fun(B, K, V, FAcc);
                                            _ ->
                                                FAcc
                                        end
                                end,
                                Acc,
                                Dict)
           end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, Fold};
        _ ->
            Res = Fold(),
            {ok, Res}
    end.

is_empty([]) ->
    true;
is_empty(_) ->
    false.

status(_) ->
    [].

%% wha?
callback(_Ref, _Msg, Dict) ->
    {ok, Dict}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================



