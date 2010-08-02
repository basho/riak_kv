%% -------------------------------------------------------------------
%%
%% riak_reduce_phase: manage the mechanics of a reduce phase of a MR job
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

%% @doc manage the mechanics of a reduce phase of a MR job

-module(riak_kv_reduce_phase).

-behaviour(luke_phase).

-export([init/1, handle_input/3, handle_input_done/1, handle_event/2,
         handle_timeout/1, handle_info/2, terminate/2]).

-record(state, {qterm, rereduce, reduced=[], new_inputs=[]}).

%% @private
init([QTerm]) ->
    {_Lang, {reduce, _FunTerm, _Arg, Rereduce, _Acc}} = QTerm,
    {ok, #state{qterm=QTerm, rereduce=Rereduce}}.

handle_input(Inputs, #state{rereduce=Rereduce, reduced=Reduced0, qterm=QTerm,
                            new_inputs=New0}=State0, _Timeout) ->
    New1 = New0 ++ Inputs,
    if
        length(New1) > 20 ->
            case perform_reduce(QTerm, New1) of
                {ok, Reduced} ->
                    case Rereduce of
                        true ->
                            {no_output, State0#state{reduced=Reduced0 ++ Reduced, new_inputs=[]}, 250};
                        false ->
                            {output, Reduced, State0#state{reduced=[]}}
                    end;
                Error ->
                    {stop, Error, State0#state{reduced=[], new_inputs=[]}}
            end;
        true ->
            {no_output, State0#state{new_inputs=New1}, 250}
    end.

handle_input_done(#state{qterm=QTerm, rereduce=Rereduce, reduced=Reduced0, new_inputs=New0}=State) ->
    case Rereduce of
        true ->
            case perform_reduce(QTerm, Reduced0 ++ New0) of
                {ok, Reduced} ->
                    luke_phase:complete(),
                    {output, Reduced, State#state{reduced=Reduced}};
                Error ->
                    {stop, Error, State#state{reduced=[]}}
            end;
        false ->
            case length(New0) > 0 of
                true ->
                    case perform_reduce(QTerm, Reduced0 ++ New0) of
                        {ok, Reduced} ->
                            luke_phase:complete(),
                            {output, Reduced, State#state{reduced=Reduced}};
                        Error ->
                            {stop, Error, State#state{reduced=[]}}
                    end;
                false ->
                    luke_phase:complete(),
                    {no_output, State}
            end
    end.


handle_timeout(#state{qterm=QTerm, reduced=Reduced0, new_inputs=New0}=State) ->
    if
        length(New0) > 0 ->
            case perform_reduce(QTerm, New0) of
                {ok, Reduced} ->
                    {no_output, State#state{reduced=Reduced0 ++ Reduced, new_inputs=[]}, 250};
                Error ->
                    {stop, Error, State#state{reduced=[], new_inputs=[]}}
            end;
        true ->
            {no_output, State, 250}
    end.

handle_event(_Event, State) ->
    {no_output, State}.

handle_info(_Info, State) ->
    {no_output, State}.

terminate(_Reason, _State) ->
    ok.

perform_reduce({Lang,{reduce,FunTerm,Arg,_Rereduce, _Acc}},
               Reduced) ->
    Key = erlang:phash2({FunTerm, Arg, Reduced}),
    case luke_phase:check_cache(Key) of
        not_found ->
            try
                case {Lang, FunTerm} of
                    {erlang, {qfun,F}} ->
                        Value = F(Reduced,Arg),
                        luke_phase:cache_value(Key, Value),
                        {ok, Value};
                    {erlang, {modfun,M,F}} ->
                        Value = M:F(Reduced,Arg),
                        luke_phase:cache_value(Key, Value),
                        {ok, Value};
                    {javascript, _} ->
                        case  riak_kv_js_manager:blocking_dispatch({FunTerm,
                                                           [riak_kv_mapred_json:jsonify_not_found(R) || R <- Reduced],
                                                                    Arg}, 5) of
                            {ok, Data} when is_list(Data) ->
                                Data1 = [riak_kv_mapred_json:dejsonify_not_found(Datum) || Datum <- Data],
                                luke_phase:cache_value(Key, Data1),
                                {ok, Data1};
                            Error ->
                                throw(Error)
                        end
                end
            catch _:R ->
                    error_logger:error_msg("Failed reduce: ~p~n", [R]),
                    {error, failed_reduce}
            end;
        Value ->
            {ok, Value}
    end.
