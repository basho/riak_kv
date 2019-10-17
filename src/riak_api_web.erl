%% -------------------------------------------------------------------
%%
%% riak_api_web: setup Riak's HTTP interface
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

%% @doc Convenience functions for setting up the HTTP interface
%%      of Riak.
-module(riak_api_web).


-export([get_listeners/0,
         binding_config/2]).

get_listeners() ->
    get_listeners(http) ++ get_listeners(https).

get_listeners(Scheme) ->
    Listeners = case app_helper:try_envs([{riak_api, Scheme},
                                          {riak_core, Scheme}], []) of
                    {riak_api, Scheme, List} when is_list(List) ->
                        List;
                    {riak_core, Scheme, List} when is_list(List) ->
                        lager:warning("Setting riak_core/~s is deprecated, please use riak_api/~s", [Scheme, Scheme]),
                        List;
                    _ ->
                        []
                end,
    lists:usort([ {Scheme, Binding} || Binding <- Listeners ]).

binding_config(Scheme, Binding) ->
    {Ip, Port} = Binding,
    Name = spec_name(Scheme, Ip, Port),
    Config = spec_from_binding(Scheme, Name, Binding),

    {Name,
     {webmachine_mochiweb, start, [Config]},
     permanent, 5000, worker, [mochiweb_socket_server]}.

spec_from_binding(http, Name, {Ip, Port}) ->
    Options = 
        lists:flatten([{name, Name},
                    {ip, Ip},
                    {port, Port},
                    {nodelay, true}],
                    common_config()),
    add_recbuf(Options);
spec_from_binding(https, Name, {Ip, Port}) ->
    Options = 
        lists:flatten([{name, Name},
                    {ip, Ip},
                    {port, Port},
                    {ssl, true},
                    {ssl_opts, riak_api_ssl:options()},
                    {nodelay, true}],
                    common_config()),
    add_recbuf(Options).

add_recbuf(Options) ->
    case application:get_env(webmachine, recbuf) of
        {ok, RecBuf} ->
            [{recbuf, RecBuf}|Options];
        _ ->
            Options
    end.

spec_name(Scheme, Ip, Port) ->
    FormattedIP = if is_tuple(Ip); tuple_size(Ip) == 4 ->
                          inet_parse:ntoa(Ip);
                     is_tuple(Ip); tuple_size(Ip) == 8 ->
                          [$[, inet_parse:ntoa(Ip), $]];
                     true -> Ip
                  end,
    lists:flatten(io_lib:format("~s://~s:~p", [Scheme, FormattedIP, Port])).

common_config() ->
    [{log_dir, app_helper:get_env(riak_api, http_logdir,
                                  app_helper:get_env(riak_core, platform_log_dir, "log"))},
     {backlog, 128},
     {dispatch, [{[], riak_api_wm_urlmap, []}
                ]}].
