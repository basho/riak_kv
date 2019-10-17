-module(riak_repl_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-compile([export_all, nowarn_export_all]).

%% basic schema test will check to make sure that all defaults from
%% the schema make it into the generated app.config
basic_schema_test() ->
    %% The defaults are defined in priv/riak_repl.schema.
    %% it is the file under test.
    Config = cuttlefish_unit:generate_templated_config(
        ["priv/riak_repl.schema"], [], context()),
    io:format("~p~n", [Config]),

    cuttlefish_unit:assert_config(Config, "riak_repl.data_root", "./repl/root"),
    cuttlefish_unit:assert_config(Config, "riak_core.cluster_mgr", {"1.2.3.4", 1234}),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssource_cluster", 5),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssource_node", 1),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssource_soft_retries", 100),
    cuttlefish_unit:assert_config(Config, "riak_repl.fssource_retry_wait", 60),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssink_node", 1),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_on_connect", true),
    cuttlefish_unit:assert_not_configured(Config, "riak_repl.fullsync_interval"),
    cuttlefish_unit:assert_config(Config, "riak_repl.rtq_max_bytes", 104857600),
    cuttlefish_unit:assert_config(Config, "riak_repl.proxy_get", disabled),
    cuttlefish_unit:assert_config(Config, "riak_repl.rt_heartbeat_interval", 15),
    cuttlefish_unit:assert_config(Config, "riak_repl.rt_heartbeat_timeout", 15),
    ok.

override_schema_test() ->
    %% Conf represents the riak.conf file that would be read in by cuttlefish.
    %% this proplists is what would be output by the conf_parse module
    Conf = [
            {["mdc", "data_root"], "/some/repl/place"},
            {["mdc", "cluster_manager"], {"4.3.2.1", 4321}},
            {["mdc", "max_fssource_cluster"], 10},
            {["mdc", "max_fssource_node"], 2},
            {["mdc", "max_fssink_node"], 4},
            {["mdc", "fullsync_on_connect"], false},
            {["mdc", "fullsync_interval", "cluster1"], "15m"},
            {["mdc", "fullsync_interval", "cluster2"], "1h"},
            {["mdc", "rtq_max_bytes"], "50MB"},
            {["mdc", "proxy_get"], on},
            {["mdc", "realtime", "heartbeat_interval"], "15m"},
            {["mdc", "realtime", "heartbeat_timeout"], "15d"},
            {["mdc", "fssource_retry_wait"], "10m"},
            {["mdc", "max_fssource_soft_retries"], 196}
           ],

    %% The defaults are defined in priv/riak_repl.schema.
    %% it is the file under test.
    Config = cuttlefish_unit:generate_templated_config(
        ["priv/riak_repl.schema"], Conf, context()),

    cuttlefish_unit:assert_config(Config, "riak_repl.data_root", "/some/repl/place"),
    cuttlefish_unit:assert_config(Config, "riak_core.cluster_mgr", {"4.3.2.1", 4321}),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssource_cluster", 10),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssource_node", 2),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssink_node", 4),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_on_connect", false),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_interval.cluster1", 15),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_interval.cluster2", 60),
    cuttlefish_unit:assert_config(Config, "riak_repl.rtq_max_bytes", 52428800),
    cuttlefish_unit:assert_config(Config, "riak_repl.proxy_get", enabled),
    cuttlefish_unit:assert_config(Config, "riak_repl.rt_heartbeat_interval", 900),
    cuttlefish_unit:assert_config(Config, "riak_repl.rt_heartbeat_timeout", 1296000),
    cuttlefish_unit:assert_config(Config, "riak_repl.fssource_retry_wait", 600),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssource_soft_retries", 196),
    ok.

%% this context() represents the substitution variables that rebar
%% will use during the build process.  riak_jmx's schema file is
%% written with some {{mustache_vars}} for substitution during
%% packaging cuttlefish doesn't have a great time parsing those, so we
%% perform the substitutions first, because that's how it would work
%% in real life.
context() ->
    [
        {repl_data_root, "./repl/root"},
        {cluster_manager_ip, "1.2.3.4"},
        {cluster_manager_port, 1234}
    ].
