-module(riak_api_schema_tests).
-include_lib("eunit/include/eunit.hrl").
-compile([export_all, nowarn_export_all]).

%% basic schema test will check to make sure that all defaults from the schema
%% make it into the generated app.config
basic_schema_test() ->
     %% The defaults are defined in priv/riak_api.schema. it is the file under test.
    Config = cuttlefish_unit:generate_templated_config("priv/riak_api.schema", [], context()),
    cuttlefish_unit:assert_config(Config, "riak_api.http", []),
    cuttlefish_unit:assert_config(Config, "riak_api.pb", []),
    cuttlefish_unit:assert_not_configured(Config, "riak_api.https"),
    cuttlefish_unit:assert_config(Config, "riak_api.pb_backlog", 128),
    cuttlefish_unit:assert_config(Config, "riak_api.disable_pb_nagle", true),
    cuttlefish_unit:assert_config(Config, "riak_api.pb_keepalive", true),
    cuttlefish_unit:assert_config(Config, "riak_api.honor_cipher_order", basho_vm(true, false)),
    cuttlefish_unit:assert_config(Config, "riak_api.tls_protocols", ['tlsv1.2']),
    cuttlefish_unit:assert_config(Config, "riak_api.check_crl", basho_vm(true, false)),
    ok.

override_schema_test() ->
    %% Conf represents the riak.conf file that would be read in by cuttlefish.
    %% this proplists is what would be output by the conf_parse module
    Conf = [
        {["listener", "http", "internal"], "127.0.0.2:8000"},
        {["listener", "http", "external"], "127.0.0.3:8000"},
        {["listener", "protobuf", "internal"], "127.0.0.8:3000"},
        {["listener", "protobuf", "external"], "127.0.0.9:3000"},
        {["listener", "https", "internal"], "127.0.0.12:443"},
        {["listener", "https", "external"], "127.0.0.13:443"},
        {["protobuf", "backlog"], 64},
        {["protobuf", "nagle"], on},
        {["protobuf", "keepalive"], off},
        {["honor_cipher_order"], off},
        {["tls_protocols", "sslv3"], on},
        {["tls_protocols", "tlsv1"], on},
        {["tls_protocols", "tlsv1", "1"], on},
        {["tls_protocols", "tlsv1", "2"], off},
        {["check_crl"], off}
    ],
    Config = cuttlefish_unit:generate_templated_config("priv/riak_api.schema", Conf, context()),


    cuttlefish_unit:assert_config(Config, "riak_api.http", [{"127.0.0.3", 8000}, {"127.0.0.2", 8000}]),
    cuttlefish_unit:assert_config(Config, "riak_api.pb", [{"127.0.0.9", 3000}, {"127.0.0.8", 3000}]),
    cuttlefish_unit:assert_config(Config, "riak_api.https", [{"127.0.0.13", 443}, {"127.0.0.12", 443}]),
    cuttlefish_unit:assert_config(Config, "riak_api.pb_backlog", 64),
    cuttlefish_unit:assert_config(Config, "riak_api.disable_pb_nagle", false),
    cuttlefish_unit:assert_config(Config, "riak_api.pb_keepalive", false),
    cuttlefish_unit:assert_config(Config, "riak_api.tls_protocols", ['tlsv1.1', tlsv1, sslv3]),
    cuttlefish_unit:assert_config(Config, "riak_api.check_crl", false),
    ok.

%% this context() represents the substitution variables that rebar
%% will use during the build process.  riak_core's schema file is
%% written with some {{mustache_vars}} for substitution during
%% packaging cuttlefish doesn't have a great time parsing those, so we
%% perform the substitutions first, because that's how it would work
%% in real life.
context() ->
    [
        {web_ip, "127.0.0.1"},
        {web_port, 8098},
        {pb_ip, "127.0.0.1"},
        {pb_port, 8087}
    ].

basho_vm(ExpectedIfBasho, ExpectedIfNot) ->
    OTPVer = erlang:system_info(otp_release),
    case string:str(OTPVer, "basho") of
        0 -> ExpectedIfNot;
        _ -> ExpectedIfBasho
    end.
