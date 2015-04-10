%% -------------------------------------------------------------------
%%
%% riak_kv_env: environmental utilities.
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

%% @doc utility functions for interacting with the environment.

-module(riak_kv_env).

-export([doc_env/0]).

-define(LINUX_PARAMS, [
                       {"vm.swappiness",                        0, gte},
                       {"net.core.wmem_default",          8388608, lte},
                       {"net.core.rmem_default",          8388608, lte},
                       {"net.core.wmem_max",              8388608, lte},
                       {"net.core.rmem_max",              8388608, lte},
                       {"net.core.netdev_max_backlog",      10000, lte},
                       {"net.core.somaxconn",               40000, lte},
                       {"net.ipv4.tcp_max_syn_backlog",     40000, lte},
                       {"net.ipv4.tcp_fin_timeout",            15, gte},
                       {"net.ipv4.tcp_keepalive_intvl",        30, gte},
                       {"net.ipv4.tcp_sack",                    1, eq},
                       {"net.ipv4.tcp_moderate_rcvbuf",         1, eq},
                       {"net.ipv4.tcp_tw_reuse",                1, eq},
                       {"net.ipv4.tcp_window_scaling",          1, eq}
                      ]).


doc_env() ->
    lager:info("Environment and OS variables:"),
    Ulimits = check_ulimits(),
    ErlLimits = check_erlang_limits(),
    OSLimits = case os:type() of
                   {unix, linux}  ->
                       check_sysctls(?LINUX_PARAMS);
                   {unix, freebsd} ->
                       [];
                   {unix, sunos} ->
                       [];
                   _ ->
                       [{warn, "Unknown OS type, no platform specific info", []}]
               end,
    lists:map(fun({F, Fmt, Args}) ->
                      lager:debug("Term: ~p", [{F, Fmt, Args}]),
                      %% fake out lager a bit here
                      F1 = case F of
                               info -> info_msg;
                               warn -> warning_msg;
                               error -> error_msg
                           end,
                      error_logger:F1("riak_kv_env: "++Fmt, Args)
              end, Ulimits ++ ErlLimits ++ OSLimits).

%% we don't really care about anything other than cores and open files
%% @private
check_ulimits() ->
    %% file ulimit
    FileLimit0 = string:strip(os:cmd("ulimit -n"), right, $\n),
    FLMsg = case FileLimit0 of
                "unlimited" ->
                    %% check the OS limit;
                    OSLimit = case os:type() of
                                  {unix, linux} ->
                                      string:strip(os:cmd("sysctl -n fs.file-max"),
                                                   right, $\n);
                                  _ -> unknown
                              end,
                    case OSLimit of
                        unknown ->
                            {warn, "Open file limit unlimited but actual limit "
                             ++ "could not be ascertained", []};
                        _ ->
                            test_file_limit(OSLimit)
                    end;
                _ ->
                    test_file_limit(FileLimit0)
            end,
    CoreLimit0 = string:strip(os:cmd("ulimit -c"), right, $\n),
    CLMsg = case CoreLimit0 of
                "unlimited" ->
                    {info, "No core size limit", []};
                _  ->
                    CoreLimit = list_to_integer(CoreLimit0),
                    case CoreLimit == 0 of
                        true ->
                            {warn, "Cores are disabled, this may "
                             ++ "hinder debugging", []};
                        false ->
                            {info, "Core size limit: ~p", [CoreLimit]}
                    end
            end,
    [FLMsg, CLMsg].

%% @private
test_file_limit(FileLimit0) ->
    FileLimit = (catch list_to_integer(FileLimit0)),
    case FileLimit of
        {'EXIT', {badarg,_}} ->
            {warn, "Open file limit was read as non-integer string: ~s",
             [FileLimit0]};

        _ ->
            case FileLimit < 4096 of
                true ->
                    {warn, "Open file limit of ~p is low, at least "
                        ++ "4096 is recommended", [FileLimit]};
                false ->
                    case FileLimit >= erlang:system_info(port_limit) of
                        true ->
                            {warn, "Erlang ports limit should be greater than open "
                                ++ "file limit ~p",[FileLimit]};
                        false -> 
                            {info, "Open file limit: ~p", [FileLimit]}
                    end
            end
    end.

%% @private
check_erlang_limits() ->
    %% processes
    PLMsg = case erlang:system_info(process_limit) of
                PL1 when PL1 < 4096 ->
                    {warn, "Erlang process limit of ~p is low, at least "
                     "4096 is recommended", [PL1]};
                PL2 ->
                    {info,"Erlang process limit: ~p", [PL2]}
            end,
    %% ports
    PortLimit = erlang:system_info(port_limit),
    PortMsg = case PortLimit < 64000 of
                  true ->
                      {warn, "Erlang ports limit of ~p is low, at least "
                       "64000 is recommended", [PortLimit]};
                  false ->
                      {info, "Erlang ports limit: ~p", [PortLimit]}
              end,

    %% ets tables
    ETSMsg = try erlang:system_info(ets_limit) of
        ETSLimit when ETSLimit < 256000 ->
            {warn,"ETS table count limit of ~p is low, at least "
                  "256000 is recommended.", [ETSLimit]};
        ETSLimit ->
            {info, "ETS table count limit: ~p",
                   [ETSLimit]}
    catch
        error:badarg ->
            {warn, "ETS table count limit cannot be determined on this "
                   "version of Erlang", []}
    end,
    %% fullsweep_after
    {fullsweep_after, GCGens} = erlang:system_info(fullsweep_after),
    GCMsg = {info, "Generations before full sweep: ~p", [GCGens]},

    %% async_threads
    TPSMsg = case erlang:system_info(thread_pool_size) of
                 TPS1 when TPS1 < 64 ->
                     {warn,"Thread pool size of ~p is low, at least 64 "
                      "suggested", [TPS1]};
                 TPS2 ->
                     {info, "Thread pool size: ~p", [TPS2]}
             end,
    %% schedulers
    Schedulers = erlang:system_info(schedulers),
    Cores = erlang:system_info(logical_processors_available),
    SMsg = case Schedulers /= Cores of
               true ->
                   {warn, "Running ~p schedulers for ~p cores, "
                    "these should match", [Schedulers, Cores]};
               false ->
                   {info, "Schedulers: ~p for ~p cores",
                    [Schedulers, Cores]}
    end,
    [PLMsg, PortMsg, ETSMsg, TPSMsg, GCMsg, SMsg].

%% @private
check_sysctls(Checklist) ->
    Sysctl = case os:cmd("which sysctl") of
      [] ->
          case os:cmd("PATH=/sbin:/bin:/usr/bin:/usr/local/bin:/usr/local/sbin " 
                      ++ "which sysctl") of
              [] -> 
                    lager:info("sysctl not found, skipping"),
                    notfound;
              P -> string:strip(P,right,$\n)
          end;
      Path -> string:strip(Path,right,$\n)
    end,
    case Sysctl of
        notfound ->
           [];
        _ -> 
            Fn = fun({Param, Val, Direction}) ->
               Output = string:strip(os:cmd(Sysctl ++" -n "++Param), right,
$\n),
               case catch list_to_integer(Output -- "\n") of
                   Actual when is_integer(Actual) ->
                       Good = case Direction of
                                  gte -> Actual =< Val;
                                  lte -> Actual >= Val;
                                  eq -> Actual == Val
                              end,
                       case Good of
                           true ->
                               {info , "sysctl ~s is ~p ~s ~p)",
                                [Param, Actual,
                                 direction_to_word(Direction),
                                 Val]};
                           false ->
                               {warn, "sysctl ~s is ~p, should be ~s~p)",
                                [Param, Actual,
                                 direction_to_word2(Direction),
                                 Val]}
                       end;
                   Other ->
                             {warn, "error retrieving ~s from sysctl:
~p",[Param,Other]}
               end
           end,
           lists:map(Fn, Checklist)
    end.
%% @private
direction_to_word(Direction) ->
    case Direction of
        gte -> "greater than or equal to";
        lte -> "lesser than or equal to";
        eq  -> "equal to"
    end.

%% @private
direction_to_word2(Direction) ->
    case Direction of
        gte -> "no more than ";
        lte -> "at least ";
        eq  -> ""
    end.
