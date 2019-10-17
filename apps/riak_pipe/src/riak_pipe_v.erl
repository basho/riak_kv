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

%% @doc Helpers for validating inputs.

-module(riak_pipe_v).

-export([validate_module/2,
         validate_function/3,
         type_of/1]).

%% @doc Validate that `Module' is an atom that names a loaded or
%%      loadable module.  If a module is already loaded under that
%%      name, or {@link code:load_file/1} is able to load one, the
%%      atom `ok' is returned.  If no module is found, and `{error,
%%      Reason}' tuple is returned.  (`Label' is used in the error
%%      message).
-spec validate_module(string(), term()) -> ok | {error, iolist()}.
validate_module(Label, Module) when is_atom(Module) ->
    case code:ensure_loaded(Module) of
        {module, Module} -> ok;
        {error, Error} ->
            {error, io_lib:format(
                      "~s must be a valid module name"
                      " (failed to load ~p: ~p)",
                      [Label, Module, Error])}
    end;
validate_module(Label, Module) ->
    {error, io_lib:format("~s must be an atom, not a ~p",
                          [Label, type_of(Module)])}.

%% @doc Validate that `Fun' is a function of arity `Arity'.
%%
%%      If the function is of type `local' (anonymous functions, and
%%      functions named via `fun Name/Arity'), validation completes
%%      onces the arity is checked.
%%
%%      If the function is of type `external' (functions named via
%%      `fun Module:Function/Arity'), then it is also verified that
%%      the module is loaded or loadable (see {@link
%%      validate_module/2}) and that it exports the named function.
%%
%%      If validation completes successfully, the atom `ok' is
%%      returned.  If validation failes, an `{error, Reason}' tuple is
%%      returned.  (`Label' is used in the error message).
-spec validate_function(string(), integer(), fun() | {atom(), atom()}) ->
         ok | {error, iolist()}.
validate_function(Label, Arity, {Module, Function})
  when is_atom(Module), is_atom(Function) ->
    validate_exported_function(Label, Arity, Module, Function);
validate_function(Label, Arity, Fun) when is_function(Fun) ->
    Info = erlang:fun_info(Fun),
    case proplists:get_value(arity, Info) of
        Arity ->
            case proplists:get_value(type, Info) of
                local ->
                    %% reference was validated by compiler
                    ok;
                external ->
                    Module = proplists:get_value(module, Info),
                    Function = proplists:get_value(name, Info),
                    validate_exported_function(
                      Label, Arity, Module, Function)
            end;
        N ->
            {error, io_lib:format("~s must be of arity ~b, not ~b",
                                  [Label, Arity, N])}
    end;
validate_function(Label, Arity, Fun) ->
    {error, io_lib:format(
              "~s must be a function or {Mod, Fun} (arity ~b), not a ~p",
              [Label, Arity, type_of(Fun)])}.

%% @doc Validate an exported function.  See {@link validate_function/3}.
-spec validate_exported_function(string(), integer(), atom(), atom()) ->
         ok | {error, iolist()}.
validate_exported_function(Label, Arity, Module, Function) ->
    case validate_module("", Module) of
        ok ->
            Exports = Module:module_info(exports),
            case lists:member({Function,Arity}, Exports) of
                true -> 
                    ok;
                false ->
                    {error, io_lib:format(
                              "~s specifies ~p:~p/~b, which is not exported",
                              [Label, Module, Function, Arity])}
            end;
        {error,Error} ->
            {error, io_lib:format("invalid module named in ~s function:~n~s",
                                  [Label, Error])}
    end.

%% @doc Determine the type of a term.  For example:
%% ```
%% number = riak_pipe_v:type_of(1).
%% atom = riak_pipe_v:type_of(a).
%% pid = riak_pipe_v:type_of(self()).
%% function = riak_pipe_v:type_of(fun() -> ok end).
%% '''
-spec type_of(term()) -> pid | reference | list | tuple | atom
                       | number | binary | function.
type_of(Term) ->
    case erl_types:t_from_term(Term) of
        {c,identifier,[Type|_],_} ->
            Type; % pid,reference
        {c,Type,_,_} ->
            Type  % list,tuple,atom,number,binary,function
    end.
