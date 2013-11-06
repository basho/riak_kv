-module(riak_kv_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

%% basic schema test will check to make sure that all defaults from the schema
%% make it into the generated app.config
basic_schema_test() ->
    %% The defaults are defined in ../priv/riak_kv.schema and multi_backend.schema. 
    %% they are the files under test. 
    Config = cuttlefish_unit:generate_templated_config(
        ["../priv/riak_kv.schema", "../priv/multi_backend.schema"], [], context()),

    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy", {on, []}),
    cuttlefish_unit:assert_config(Config, "riak_kv.storage_backend", riak_kv_bitcask_backend),
    cuttlefish_unit:assert_config(Config, "riak_kv.raw_name", "riak"),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_build_limit", {1, 3600000}),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_expire", 604800000),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_concurrency", 2),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_tick", 15000),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_data_dir", "./data/anti_entropy"),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.write_buffer_size", 4194304),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.max_open_files", 20),
    cuttlefish_unit:assert_config(Config, "riak_kv.mapred_name", "mapred"),
    cuttlefish_unit:assert_config(Config, "riak_kv.mapred_2i_pipe", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.map_js_vm_count", 8),
    cuttlefish_unit:assert_config(Config, "riak_kv.reduce_js_vm_count", 6),
    cuttlefish_unit:assert_config(Config, "riak_kv.hook_js_vm_count", 2),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_max_vm_mem", 8),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_thread_stack", 16),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_source_dir", undefined),
    cuttlefish_unit:assert_config(Config, "riak_kv.http_url_encoding", on),
    cuttlefish_unit:assert_config(Config, "riak_kv.vnode_vclocks", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.listkeys_backpressure", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.fsm_limit", 50000),
    cuttlefish_unit:assert_config(Config, "riak_kv.retry_put_coordinator_failure", on),
    cuttlefish_unit:assert_config(Config, "riak_kv.object_format", v1),
    cuttlefish_unit:assert_config(Config, "riak_kv.memory_backend.max_memory", 4096),
    cuttlefish_unit:assert_config(Config, "riak_kv.memory_backend.ttl", undefined),

    %% make sure multi backend is not on by shell_default
    cuttlefish_unit:assert_config(Config, "riak_kv.multi_backend_default", undefined),
    cuttlefish_unit:assert_config(Config, "riak_kv.multi_backend", undefined),

    cuttlefish_unit:assert_config(Config, "riak_kv.secure_referer_check", true),

    ok.

override_non_multi_backend_schema_test() ->
    %% Conf represents the riak.conf file that would be read in by cuttlefish.
    %% this proplists is what would be output by the conf_parse module
    Conf = [
        {["anti_entropy"], debug},
        {["storage_backend"], leveldb},
        {["raw_name"], "rawr"},
        {["anti_entropy", "build_limit", "number"], 2},
        {["anti_entropy", "build_limit", "per_timespan"], "1m"},
        {["anti_entropy", "expire"], "1h"},
        {["anti_entropy","concurrency"], 1},
        {["anti_entropy", "tick"], "1s"},
        {["anti_entropy", "data_dir"], "/absolute/data/anti_entropy"},
        {["anti_entropy", "write_buffer_size"], "8MB"},
        {["anti_entropy", "max_open_files"], 30},
        {["mapred_name"], "mapredeuce"},
        {["mapred_2i_pipe"], off},
        {["javascript_vm", "map_count"], 16},
        {["javascript_vm", "reduce_count"], 12},
        {["javascript_vm", "hook_count"], 4},
        {["javascript_vm", "max_vm_mem"], 16},
        {["javascript_vm", "thread_stack"], 32},
        {["javascript_vm", "source_dir"], "/tmp/js_source"},
        {["http_url_encoding"], off},
        {["vnode_vclocks"], off},
        {["listkeys_backpressure"], off},
        {["fsm_limit"], 100000},
        {["retry_put_coordinator_failure"], off},
        {["object_format"], v0},
        {["memory_backend", "max_memory"], "8GB"},
        {["memory_backend", "ttl"], "1d"},
        {["secure_referer_check"], off}
    ],

    Config = cuttlefish_unit:generate_templated_config(
        ["../priv/riak_kv.schema", "../priv/multi_backend.schema"], Conf, context()),

    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy", {on, [debug]}),
    cuttlefish_unit:assert_config(Config, "riak_kv.storage_backend", riak_kv_eleveldb_backend),
    cuttlefish_unit:assert_config(Config, "riak_kv.raw_name", "rawr"),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_build_limit", {2, 60000}),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_expire", 3600000),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_concurrency", 1),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_tick", 1000),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_data_dir", "/absolute/data/anti_entropy"),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.write_buffer_size", 8388608),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.max_open_files", 30),
    cuttlefish_unit:assert_config(Config, "riak_kv.mapred_name", "mapredeuce"),
    cuttlefish_unit:assert_config(Config, "riak_kv.mapred_2i_pipe", false),
    cuttlefish_unit:assert_config(Config, "riak_kv.map_js_vm_count", 16),
    cuttlefish_unit:assert_config(Config, "riak_kv.reduce_js_vm_count", 12),
    cuttlefish_unit:assert_config(Config, "riak_kv.hook_js_vm_count", 4),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_max_vm_mem", 16),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_thread_stack", 32),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_source_dir", "/tmp/js_source"),
    cuttlefish_unit:assert_config(Config, "riak_kv.http_url_encoding", off),
    cuttlefish_unit:assert_config(Config, "riak_kv.vnode_vclocks", false),
    cuttlefish_unit:assert_config(Config, "riak_kv.listkeys_backpressure", false),
    cuttlefish_unit:assert_config(Config, "riak_kv.fsm_limit", 100000),
    cuttlefish_unit:assert_config(Config, "riak_kv.retry_put_coordinator_failure", off),
    cuttlefish_unit:assert_config(Config, "riak_kv.object_format", v0),
    cuttlefish_unit:assert_config(Config, "riak_kv.memory_backend.max_memory", 8192),
    cuttlefish_unit:assert_config(Config, "riak_kv.memory_backend.ttl", 86400),

    %% make sure multi backend is not on by shell_default
    cuttlefish_unit:assert_config(Config, "riak_kv.multi_backend_default", undefined),
    cuttlefish_unit:assert_config(Config, "riak_kv.multi_backend", undefined),

    cuttlefish_unit:assert_config(Config, "riak_kv.secure_referer_check", false),
    ok.

multi_backend_test() ->
     Conf = [
        {["storage_backend"], multi},
        {["multi_backend", "default"], "backend_one"},
        {["multi_backend", "backend_one", "storage_backend"], "memory"},
        {["multi_backend", "backend_one", "memory_backend", "max_memory"], "8GB"},
        {["multi_backend", "backend_one", "memory_backend", "ttl"], "1d"},
        {["multi_backend", "backend_two", "storage_backend"], "memory"}
    ],

    Config = cuttlefish_unit:generate_templated_config(
        ["../priv/riak_kv.schema", "../priv/multi_backend.schema"], Conf, context()),

    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy", {on, []}),
    cuttlefish_unit:assert_config(Config, "riak_kv.storage_backend", riak_kv_multi_backend),
    cuttlefish_unit:assert_config(Config, "riak_kv.raw_name", "riak"),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_build_limit", {1, 3600000}),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_expire", 604800000),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_concurrency", 2),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_tick", 15000),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_data_dir", "./data/anti_entropy"),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.write_buffer_size", 4194304),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.max_open_files", 20),
    cuttlefish_unit:assert_config(Config, "riak_kv.mapred_name", "mapred"),
    cuttlefish_unit:assert_config(Config, "riak_kv.mapred_2i_pipe", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.map_js_vm_count", 8),
    cuttlefish_unit:assert_config(Config, "riak_kv.reduce_js_vm_count", 6),
    cuttlefish_unit:assert_config(Config, "riak_kv.hook_js_vm_count", 2),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_max_vm_mem", 8),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_thread_stack", 16),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_source_dir", undefined),
    cuttlefish_unit:assert_config(Config, "riak_kv.http_url_encoding", on),
    cuttlefish_unit:assert_config(Config, "riak_kv.vnode_vclocks", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.listkeys_backpressure", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.fsm_limit", 50000),
    cuttlefish_unit:assert_config(Config, "riak_kv.retry_put_coordinator_failure", on),
    cuttlefish_unit:assert_config(Config, "riak_kv.object_format", v1),
    cuttlefish_unit:assert_config(Config, "riak_kv.memory_backend.max_memory", 4096),
    cuttlefish_unit:assert_config(Config, "riak_kv.memory_backend.ttl", undefined),

    cuttlefish_unit:assert_config(Config, "riak_kv.secure_referer_check", true),
    %% make sure multi backend is not on by shell_default
    cuttlefish_unit:assert_config(Config, "riak_kv.multi_backend_default", <<"backend_one">>),

    ExpectedMutliConfig = [
        {<<"backend_one">>, riak_kv_memory_backend, [{ttl, 86400}, {max_memory, 8192}]},
        {<<"backend_two">>, riak_kv_memory_backend, [{max_memory, 4096}]}
    ],
    cuttlefish_unit:assert_config(Config, "riak_kv.multi_backend", ExpectedMutliConfig), 
    ok.

%% this context() represents the substitution variables that rebar will use during the build process.
%% riak_core's schema file is written with some {{mustache_vars}} for substitution during packaging
%% cuttlefish doesn't have a great time parsing those, so we perform the substitutions first, because
%% that's how it would work in real life.
context() ->
    [
        {storage_backend, "bitcask"},
        {platform_data_dir, "./data"},
        {map_js_vms,   8},
        {reduce_js_vms, 6},
        {hook_js_vms, 2}
    ].
