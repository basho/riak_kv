%% @author mikael
%% @doc This module keep sweeper callbacks for object TTL.


-module(riak_kv_object_ttl).

%% ====================================================================
%% API functions
%% ====================================================================
-export([participate_in_sweep/2,
         successfull_sweep/2,
         failed_sweep/2]).

%% riak_kv_sweeper callbacks
participate_in_sweep(_Index, _Pid) ->
    InitialAcc = 0,
    {ok, ttl_fun(), InitialAcc}.

ttl_fun() ->
    fun({_BKey, RObj}, Acc, [{bucket_props, BucketProps}]) ->
            case expired(RObj, BucketProps) of
                true ->
                    {deleted, Acc};
                _ ->
                    {ok, Acc}
            end
    end.

successfull_sweep(Index, _FinalAcc) ->
    lager:info("successfull_sweep ~p", [Index]),
    ok.

failed_sweep(Index, Reason) ->
    lager:info("failed_sweep ~p ~p", [Index, Reason]),
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

expired(_RObj, _BucketProps) ->
    true.
