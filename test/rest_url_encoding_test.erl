%% Test proper URL decoding behavior over REST API.
%%
%% This test is designed to run against a running Riak node, and will be
%% skipped if the following environment variables are not set:
%% (example values corresponding to standard dev1 release follow)
%%   RIAK_TEST_HOST="127.0.0.1"
%%   RIAK_TEST_HTTP_PORT="8091"
%%   RIAK_TEST_NODE="dev1@127.0.0.1"
%%   RIAK_TEST_COOKIE="riak"
%%   RIAK_EUNIT_NODE="eunit@127.0.0.1"

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-module(rest_url_encoding_test).
-compile(export_all).

url_encoding_test_() ->
    Envs = ["RIAK_TEST_HOST", "RIAK_TEST_HTTP_PORT", "RIAK_TEST_NODE",
            "RIAK_TEST_COOKIE", "RIAK_EUNIT_NODE"],
    Vals = [os:getenv(Env) || Env <- Envs],
    case lists:member(false, Vals) of
        true ->
            ?debugFmt("Skipping rest_url_encoding_test~n", []),
            [];
        false ->
            [HostV, PortV, NodeV, CookieV, ENodeV] = Vals,
            URL = "http://" ++ HostV ++ ":" ++ PortV,
            Node = list_to_atom(NodeV),
            Cookie = list_to_atom(CookieV),
            ENode = list_to_atom(ENodeV),

            {spawn, [{setup,
                      fun() ->
                              net_kernel:start([ENode]),
                              erlang:set_cookie(node(), Cookie),
                              inets:start(),
                              inets:start(httpc, [{profile, eunit}]),
                              {URL, Node}
                      end,
                      fun(_) ->
                              inets:stop(),
                              ok
                      end,
                      fun(Args) ->
                              [?_test(compat_encoding_case(Args)),
                               ?_test(sane_encoding_case(Args))]
                      end
                     }]}
    end.

compat_encoding_case({URL, Node}) ->
    %% Ensure the cluster is running in compat mode
    ok = rpc:call(Node, application, set_env,
                  [riak_kv, http_url_encoding, compat]),

    %% Write URL encoded key
    httpc:request(put, {URL ++ "/riak/rest1/%40compat",
                        [],
                        "text/plain",
                        "Test"},
                  [], []),

    %% Retrieve key list and check that the key was not decoded
    {ok, {_, _, Body}} =
        httpc:request(get, {"http://localhost:8091/riak/rest1?keys=true",
                            []}, [], []),

    {struct, Props} = mochijson2:decode(Body),
    ?assertMatch([<<"%40compat">>], proplists:get_value(<<"keys">>, Props)),

    %% Add URL encoded link
    httpc:request(put, {URL ++ "/riak/rest_links/compat",
                        [{"Link", "</riak/rest1/%40compat>; riaktag=\"tag\""}],
                        "text/plain",
                        "Test"},
                  [], []),

    %% Retrieve link header and check that it is singly encoded, corresponding
    %% to a decoded internal link.
    {ok, {_, Headers, _}} = 
        httpc:request(get, {"http://localhost:8091/riak/rest_links/compat",
                            []}, [], []),


    Links = proplists:get_value("link", Headers),
    ?assert(string:str(Links, "</riak/rest1/%40compat>;") /= 0),

    %% Link walk should fail because key is encoded but link is decoded.
    %% Test by performing link walk and verifying link was not returned.
    {ok, {_, _, LWalk}} =
        httpc:request(get, {URL ++ "/riak/rest_links/compat/_,_,1", []},
                      [], []),
    ?assert(string:str(LWalk, "Location: /riak/rest1/%40compat") == 0),


    %% Add doubly encoded link
    httpc:request(put, {URL ++ "/riak/rest_links/compat2",
                        [{"Link", "</riak/rest1/%2540compat>; riaktag=\"tag\""}],
                        "text/plain",
                        "Test"},
                  [], []),

    %% Verify that link walk against doubly encoded link works.
    {ok, {_, _, LWalk2}} =
        httpc:request(get, {URL ++ "/riak/rest_links/compat2/_,_,1", []},
                      [], []),
    ?assert(string:str(LWalk2, "Location: /riak/rest1/%2540compat") /= 0),

    %% Test map/reduce link walking behavior.
    {ok, {_, _, Map1}} =
        httpc:request(post, {URL ++ "/mapred",
                             [],
                             "application/json",
                             "{\"inputs\": [[\"rest_links\", \"compat\"]],"
                             " \"query\": [{\"link\": {\"keep\":true}}]"
                             "}"},
                      [], []),
    ?assertEqual("[[\"rest1\",\"@compat\",\"tag\"]]", Map1),

    {ok, {_, _, Map2}} =
        httpc:request(post, {URL ++ "/mapred",
                             [],
                             "application/json",
                             "{\"inputs\": [[\"rest_links\", \"compat2\"]],"
                             " \"query\": [{\"link\": {\"keep\":true}}]"
                             "}"},
                      [], []),
    ?assertEqual("[[\"rest1\",\"%40compat\",\"tag\"]]", Map2).

sane_encoding_case({URL, Node}) ->
    %% Ensure the cluster is running in sane mode
    ok = rpc:call(Node, application, set_env,
                  [riak_kv, http_url_encoding, on]),

    %% Write URL encoded key
    httpc:request(put, {URL ++ "/riak/rest2/%40sane",
                        [],
                        "text/plain",
                        "Test"},
                  [], []),

    %% Retrieve key list and check that the key was decoded
    {ok, {_, _, Body}} =
        httpc:request(get, {"http://localhost:8091/riak/rest2?keys=true",
                            []}, [], []),

    {struct, Props} = mochijson2:decode(Body),
    ?assertMatch([<<"@sane">>], proplists:get_value(<<"keys">>, Props)),

    %% Add URL encoded link
    httpc:request(put, {URL ++ "/riak/rest_links/sane",
                        [{"Link", "</riak/rest2/%40sane>; riaktag=\"tag\""}],
                        "text/plain",
                        "Test"},
                  [], []),

    %% Retrieve link header and check that it is singly encoded, corresponding
    %% to a decoded internal link.
    {ok, {_, Headers, _}} = 
        httpc:request(get, {"http://localhost:8091/riak/rest_links/sane",
                            []}, [], []),

    Links = proplists:get_value("link", Headers),
    ?assert(string:str(Links, "</riak/rest2/%40sane>;") /= 0),

    %% Verify that link walk succeeds.
    {ok, {_, _, LWalk}} =
        httpc:request(get, {URL ++ "/riak/rest_links/sane/_,_,1", []},
                      [], []),
    ?assert(string:str(LWalk, "Location: /riak/rest2/%40sane") /= 0),

    %% Add doubly encoded link
    httpc:request(put, {URL ++ "/riak/rest_links/sane2",
                        [{"Link", "</riak/rest2/%2540sane>; riaktag=\"tag\""}],
                        "text/plain",
                        "Test"},
                  [], []),

    %% Verify that link walk against doubly encoded link fails, because key
    %% does not exist.
    {ok, {_, _, LWalk2}} =
        httpc:request(get, {URL ++ "/riak/rest_links/sane2/_,_,1", []},
                      [], []),
    ?assert(string:str(LWalk2, "Location: /riak/rest2/%2540sane") == 0),

    %% Test map/reduce link walking behavior.
    {ok, {_, _, Map1}} =
        httpc:request(post, {URL ++ "/mapred",
                             [],
                             "application/json",
                             "{\"inputs\": [[\"rest_links\", \"sane\"]],"
                             " \"query\": [{\"link\": {\"keep\":true}}]"
                             "}"},
                      [], []),
    ?assertEqual("[[\"rest2\",\"@sane\",\"tag\"]]", Map1),

    {ok, {_, _, Map2}} =
        httpc:request(post, {URL ++ "/mapred",
                             [],
                             "application/json",
                             "{\"inputs\": [[\"rest_links\", \"sane2\"]],"
                             " \"query\": [{\"link\": {\"keep\":true}}]"
                             "}"},
                      [], []),
    ?assertEqual("[[\"rest2\",\"%40sane\",\"tag\"]]", Map2).

-endif.
