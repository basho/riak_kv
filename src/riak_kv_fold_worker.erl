-module(riak_kv_fold_worker).

-behaviour(riak_core_vnode_worker).

-export([init_worker/3,
         handle_work/3]).

-include_lib("riak_kv_vnode.hrl").

-record(state, {index :: partition()}).

init_worker(VNodeIndex, _Args, _Props) ->
    {ok, #state{index=VNodeIndex}}.

handle_work({Bucket, FoldFun}, From, State) ->
    io:format("Handling work~n"),
    Acc = try
              FoldFun()
          catch
              {break, AccFinal} ->
                  AccFinal
          end,
    FlushFun = fun(FinalResults) ->
                       riak_core_vnode:reply(From,
                                             {final_results,
                                              {Bucket,
                                               FinalResults}})
               end,
    riak_kv_fold_buffer:flush(Acc, FlushFun),
    {noreply, State}.
    
