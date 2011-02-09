%% -------------------------------------------------------------------
%%
%% riak_delete: two-step object deletion
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

%% @doc two-step object deletion

-module(riak_kv_delete).

-export([delete/6]).

%% @spec delete(ReqId :: binary(), riak_object:bucket(), riak_object:key(),
%%             RW :: integer(), TimeoutMillisecs :: integer(), Client :: pid())
%%           -> term()
%% @doc Delete the object at Bucket/Key.  Direct return value is uninteresting,
%%      see riak_client:delete/3 for expected gen_server replies to Client.
delete(ReqId,Bucket,Key,RW0,Timeout,Client) ->           
    RealStartTime = riak_core_util:moment(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    N = proplists:get_value(n_val,BucketProps),
    case riak_kv_util:expand_rw_value(rw, RW0, BucketProps, N) of
        error ->
            Client ! {ReqId, {error, {rw_val_violation, RW0}}};
        RW ->
            {ok,C} = riak:local_client(),
            case C:get(Bucket,Key,RW,Timeout) of
                {ok, OrigObj} ->
                    RemainingTime = Timeout - (riak_core_util:moment() - RealStartTime),
                    OrigMD = hd([MD || {MD,_V} <- riak_object:get_contents(OrigObj)]),
                    NewObj = riak_object:update_metadata(OrigObj,
                                                         dict:store(<<"X-Riak-Deleted">>, "true", OrigMD)),
                    Reply = C:put(NewObj, RW, RW, RemainingTime),
                    case Reply of
                        ok -> 
                            spawn(
                              fun()-> reap(Bucket,Key,RemainingTime) end);
                        _ -> nop
                    end,
                    Client ! {ReqId, Reply};
                {error, notfound} ->
                    Client ! {ReqId, {error, notfound}};
                X ->
                    Client ! {ReqId, X}
            end                     
    end.

reap(Bucket, Key, Timeout) ->
    {ok,C} = riak:local_client(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    N = proplists:get_value(n_val,BucketProps),
    C:get(Bucket,Key,N,Timeout).
