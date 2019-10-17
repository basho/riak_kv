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

%% @doc Utilities for dealing with anonymous funs.  This is most
%% useful for upgrading from older pipes using funs to newer pipes
%% using modfun tuples.

-module(riak_pipe_fun).

-export([compat_apply/2]).

%% @doc Attempt to evaluate Fun(Inputs...). If the evaluation throws a
%% badfun error, ask the module that defines the function for an
%% alternate implementation, by calling `Module:compat_fun/1', and
%% then evaluates that instead.
compat_apply(Fun, Inputs) ->
    try
        apply(Fun, Inputs)
    catch error:{badfun,Fun} ->
            {module, Module} = erlang:fun_info(Fun, module),
            try Module:compat_fun(Fun) of
                {ok, CompatFun} ->
                    apply(CompatFun, Inputs);
                error ->
                    %% just to re-raise the original badfun
                    apply(Fun, Inputs)
            catch error:undef ->
                    %% just to re-raise the original badfun
                    apply(Fun, Inputs)
            end
    end.
