%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_repl_cinfo).

-export([cluster_info_init/0, cluster_info_generator_funs/0]).

%% @spec () -> term()
%% @doc Required callback function for cluster_info: initialization.
%%
%% This function doesn't have to do anything.

cluster_info_init() ->
    ok.

%% @spec () -> list({string(), fun()})
%% @doc Required callback function for cluster_info: return list of
%%      {NameForReport, FunOfArity_1} tuples to generate ASCII/UTF-8
%%      formatted reports.

cluster_info_generator_funs() ->
    [
     {"Riak Repl status", fun status/1}
    ].

status(CPid) -> % CPid is the data collector's pid.
    cluster_info:format(CPid, "~p\n", [riak_repl_console:status(quiet)]).
