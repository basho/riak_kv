%% -------------------------------------------------------------------
%%
%% riak_kv_wm_stats: publishing Riak runtime stats via HTTP
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_wm_stats).

%% webmachine resource exports
-export([
         init/1,
         encodings_provided/2,
         content_types_provided/2,
         service_available/2,
         forbidden/2,
         produce_body/2,
         pretty_print/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").

-record(ctx, {}).

init(_) ->
    {ok, #ctx{}}.

%% @spec encodings_provided(webmachine:wrq(), context()) ->
%%         {[encoding()], webmachine:wrq(), context()}
%% @doc Get the list of encodings this resource provides.
%%      "identity" is provided for all methods, and "gzip" is
%%      provided for GET as well
encodings_provided(ReqData, Context) ->
    case wrq:method(ReqData) of
        'GET' ->
            {[{"identity", fun(X) -> X end},
              {"gzip", fun(X) -> zlib:gzip(X) end}], ReqData, Context};
        _ ->
            {[{"identity", fun(X) -> X end}], ReqData, Context}
    end.

%% @spec content_types_provided(webmachine:wrq(), context()) ->
%%          {[ctype()], webmachine:wrq(), context()}
%% @doc Get the list of content types this resource provides.
%%      "application/json" and "text/plain" are both provided
%%      for all requests.  "text/plain" is a "pretty-printed"
%%      version of the "application/json" content.
content_types_provided(ReqData, Context) ->
    {[{"application/json", produce_body},
      {"text/plain", pretty_print}],
     ReqData, Context}.


service_available(ReqData, Ctx) ->
    {true, ReqData, Ctx}.

forbidden(RD, Ctx) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx}.

produce_body(ReqData, Ctx) ->
    Body = mochijson2:encode({struct, get_stats()}),
    {Body, ReqData, Ctx}.

%% @spec pretty_print(webmachine:wrq(), context()) ->
%%          {string(), webmachine:wrq(), context()}
%% @doc Format the respons JSON object is a "pretty-printed" style.
pretty_print(RD1, C1=#ctx{}) ->
    {Json, RD2, C2} = produce_body(RD1, C1),
    {json_pp:print(binary_to_list(list_to_binary(Json))), RD2, C2}.

get_stats() ->
    {value, {_, Disk}, Stats} = lists:keytake(disk, 1, riak_kv_stat_bc:produce_stats()),
    DiskFlat = [{struct, [{id, list_to_binary(Id)}, {size, Size}, {used, Used}]} || {Id, Size, Used} <- Disk],
    insert_disk(Stats, {disk, DiskFlat}) ++
	convert_stats(riak_core_stat:get_stats(common) ++
			  riak_core_stat:get_stats(), riak_core_stat:prefix()).

insert_disk([{mem_allocated,_} = H|T], D) ->
    [H, D|T];
insert_disk([H|T], D) ->
    [H|insert_disk(T, D)];
insert_disk([], D) ->
    [D].

convert_stats([{[P, App, N], V}|T], P) when App==riak_core; App==common ->
    Name = case N of
	       cpu_stats -> cpu;
	       _ -> N
	   end,
    case V of
	[{value, Val}|_] ->
	    [{Name, Val}];
	[{count,C}, {one,O}] ->
	    [{atom_to_list(Name) ++ "_total", C}, {Name, O}];
	L when is_list(L) ->
	    lists:map(fun({K,Val}) when is_atom(K) ->
			      {join(Name, K), Val}
		      end, L);
	_ ->
	    []
    end ++ convert_stats(T, P);
convert_stats([], _) ->
    [].

join(A, B) ->
    binary_to_atom(<< (atom_to_binary(A, latin1))/binary, "_", (atom_to_binary(B, latin1))/binary>>, latin1).
