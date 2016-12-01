%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
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

-module(riak_kv_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

-define(DEFAULT_ENABLED_JOB_CLASSES, [
    {riak_kv, list_buckets},
    {riak_kv, list_keys},
    {riak_kv, map_reduce_js},
    {riak_kv, map_reduce},
    {riak_kv, secondary_index},
    {riak_kv, stream_list_buckets},
    {riak_kv, stream_list_keys},
    {riak_kv, stream_secondary_index}
]).

%% basic schema test will check to make sure that all defaults from the schema
%% make it into the generated app.config
basic_schema_test() ->
    %% The defaults are defined in ../priv/riak_kv.schema and multi_backend.schema.
    %% they are the files under test.
    Config = cuttlefish_unit:generate_templated_config(
        ["../priv/riak_kv.schema", "../priv/multi_backend.schema"], [], context(), predefined_schema()),

    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy", {off, []}),
    cuttlefish_unit:assert_config(Config, "riak_kv.storage_backend", riak_kv_eleveldb_backend),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_build_limit", {1, 3600000}),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_expire", 604800000),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_concurrency", 2),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_tick", 15000),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_data_dir", "./data/anti_entropy"),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.write_buffer_size", 4194304),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.max_open_files", 20),
    cuttlefish_unit:assert_config(Config, "riak_kv.aae_throttle_enabled", true),
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.aae_throttle_limits"),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.use_bloomfilter", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.map_js_vm_count", 8),
    cuttlefish_unit:assert_config(Config, "riak_kv.reduce_js_vm_count", 6),
    cuttlefish_unit:assert_config(Config, "riak_kv.hook_js_vm_count", 2),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_max_vm_mem", 8),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_thread_stack", 16),
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.js_source_dir"),
    cuttlefish_unit:assert_config(Config, "riak_kv.fsm_limit", 50000),
    cuttlefish_unit:assert_config(Config, "riak_kv.retry_put_coordinator_failure", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.object_format", v1),
    cuttlefish_unit:assert_config(Config, "riak_kv.vnode_md_cache_size", 0),
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.memory_backend.max_memory"),
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.memory_backend.ttl"),
    cuttlefish_unit:assert_config(Config, "riak_kv.handoff_rejected_max", 6),

    %% make sure multi backend is not on by shell_default
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.multi_backend_default"),
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.multi_backend"),

    cuttlefish_unit:assert_config(Config, "riak_kv.secure_referer_check", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.warn_object_size", 51200),
    cuttlefish_unit:assert_config(Config, "riak_kv.max_object_size", 512000),
    cuttlefish_unit:assert_config(Config, "riak_kv.warn_siblings", 25),
    cuttlefish_unit:assert_config(Config, "riak_kv.max_siblings", 100),

    %% Default Bucket Properties
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.pr", 0),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.r", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.w", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.pw", 0),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.dw", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.rw", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.notfound_ok", true),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.basic_quorum", false),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.allow_mult", false),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.last_write_wins", false),
    cuttlefish_unit:assert_not_configured(Config, "riak_core.default_bucket_props.precommit"),
    cuttlefish_unit:assert_not_configured(Config, "riak_core.default_bucket_props.postcommit"),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.dvv_enabled", false),
    cuttlefish_unit:assert_config(Config, "riak_dt.binary_compression", 1),
    cuttlefish_unit:assert_config(Config, "riak_kv.handoff_use_background_manager", false),
    cuttlefish_unit:assert_config(Config, "riak_kv.aae_use_background_manager", false),
    ok.

override_non_multi_backend_schema_test() ->
    %% Conf represents the riak.conf file that would be read in by cuttlefish.
    %% this proplists is what would be output by the conf_parse module
    Conf = [
        {["anti_entropy"], 'active-debug'},
        {["storage_backend"], leveldb},
        {["anti_entropy", "tree", "build_limit", "number"], 2},
        {["anti_entropy", "tree", "build_limit", "per_timespan"], "1m"},
        {["anti_entropy", "tree", "expiry"], never},
        {["anti_entropy", "concurrency_limit"], 1},
        {["anti_entropy", "trigger_interval"], "1s"},
        {["anti_entropy", "data_dir"], "/absolute/data/anti_entropy"},
        {["anti_entropy", "write_buffer_size"], "8MB"},
        {["anti_entropy", "max_open_files"], 30},
        {["anti_entropy", "throttle"], off},
        {["anti_entropy", "throttle", "tier1", "mailbox_size"], 0},
        {["anti_entropy", "throttle", "tier1", "delay"], "1d"},
        {["anti_entropy", "throttle", "tier2", "mailbox_size"], 11},
        {["anti_entropy", "throttle", "tier2", "delay"], "10d"},
        {["anti_entropy", "bloomfilter"], off},
        {["javascript", "map_pool_size"], 16},
        {["javascript", "reduce_pool_size"], 12},
        {["javascript", "hook_pool_size"], 4},
        {["javascript", "maximum_heap_size"], "16MB"},
        {["javascript", "maximum_stack_size"], "32MB"},
        {["javascript", "source_dir"], "/tmp/js_source"},
        {["max_concurrent_requests"], 100000},
        {["retry_put_coordinator_failure"], off},
        {["object", "format"], 0},
        {["metadata_cache_size"], "512KB"},
        {["memory_backend", "max_memory_per_vnode"], "8GB"},
        {["memory_backend", "ttl"], "1d"},
        {["secure_referer_check"], off},
        {["object", "size", "warning_threshold"], "10MB"},
        {["object", "size", "maximum"], "100MB"},
        {["object", "siblings", "warning_threshold"], 250},
        {["object", "siblings", "maximum"], 1000},
        {["buckets", "default",  "merge_strategy"], '2'},
        %% Default Bucket Properties
        {["buckets", "default", "pr"], quorum},
        {["buckets", "default", "r"], 2},
        {["buckets", "default", "w"], 4},
        {["buckets", "default", "pw"], all},
        {["buckets", "default", "dw"], all},
        {["buckets", "default", "rw"], 1},
        {["buckets", "default", "notfound_ok"], false},
        {["buckets", "default", "basic_quorum"], true},
        {["buckets", "default", "allow_mult"], true},
        {["buckets", "default", "last_write_wins"], true},
        {["buckets", "default", "precommit"], "module:function javascriptFunction"},
        {["buckets", "default", "postcommit"], "module2:function2"},
        {["datatypes", "compression_level"], "off"},
        {["handoff", "use_background_manager"], on},
        {["handoff", "max_rejects"], 10},
        {["anti_entropy", "use_background_manager"], on}
    ],

    Config = cuttlefish_unit:generate_templated_config(
        ["../priv/riak_kv.schema", "../priv/multi_backend.schema"], Conf, context(), predefined_schema()),

    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy", {on, [debug]}),
    cuttlefish_unit:assert_config(Config, "riak_kv.storage_backend", riak_kv_eleveldb_backend),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_build_limit", {2, 60000}),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_expire", never),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_concurrency", 1),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_tick", 1000),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_data_dir", "/absolute/data/anti_entropy"),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.write_buffer_size", 8388608),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.max_open_files", 30),
    cuttlefish_unit:assert_config(Config, "riak_kv.aae_throttle_enabled", false),
    cuttlefish_unit:assert_config(Config, "riak_kv.aae_throttle_limits", [{-1, 86400000}, {10, 864000000}]),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.use_bloomfilter", false),
    cuttlefish_unit:assert_config(Config, "riak_kv.map_js_vm_count", 16),
    cuttlefish_unit:assert_config(Config, "riak_kv.reduce_js_vm_count", 12),
    cuttlefish_unit:assert_config(Config, "riak_kv.hook_js_vm_count", 4),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_max_vm_mem", 16),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_thread_stack", 32),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_source_dir", "/tmp/js_source"),
    cuttlefish_unit:assert_config(Config, "riak_kv.fsm_limit", 100000),
    cuttlefish_unit:assert_config(Config, "riak_kv.retry_put_coordinator_failure", false),
    cuttlefish_unit:assert_config(Config, "riak_kv.object_format", v0),
    cuttlefish_unit:assert_config(Config, "riak_kv.memory_backend.max_memory", 8192),
    cuttlefish_unit:assert_config(Config, "riak_kv.memory_backend.ttl", 86400),
    cuttlefish_unit:assert_config(Config, "riak_kv.handoff_rejected_max", 10),

    %% make sure multi backend is not on by shell_default
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.multi_backend_default"),
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.multi_backend"),

    cuttlefish_unit:assert_config(Config, "riak_kv.secure_referer_check", false),
    cuttlefish_unit:assert_config(Config, "riak_kv.warn_object_size", 10485760),
    cuttlefish_unit:assert_config(Config, "riak_kv.max_object_size", 104857600),
    cuttlefish_unit:assert_config(Config, "riak_kv.warn_siblings", 250),
    cuttlefish_unit:assert_config(Config, "riak_kv.max_siblings", 1000),

    %% Default Bucket Properties
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.pr", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.r", 2),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.w", 4),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.pw", all),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.dw", all),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.rw", 1),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.notfound_ok", false),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.basic_quorum", true),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.allow_mult", true),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.last_write_wins", true),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.dvv_enabled", true),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.precommit", [
      {struct, [
                {<<"mod">>, <<"module">>},
                {<<"fun">>, <<"function">>}
               ]},
      {struct, [
                {<<"name">>, <<"javascriptFunction">>}
               ]}]),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.postcommit", [
      {struct, [
                {<<"mod">>, <<"module2">>},
                {<<"fun">>, <<"function2">>}
               ]}
    ]),
    cuttlefish_unit:assert_config(Config, "riak_dt.binary_compression", false),
    cuttlefish_unit:assert_config(Config, "riak_kv.handoff_use_background_manager", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.aae_use_background_manager", true),

    ok.

multi_backend_test() ->
     Conf = [
        {["storage_backend"], multi},
        {["multi_backend", "default"], "backend_one"},
        {["multi_backend", "backend_one", "storage_backend"], "memory"},
        {["multi_backend", "backend_one", "memory_backend", "max_memory_per_vnode"], "8GB"},
        {["multi_backend", "backend_one", "memory_backend", "ttl"], "1d"},
        {["multi_backend", "backend_two", "storage_backend"], "memory"}
    ],

    Config = cuttlefish_unit:generate_templated_config(
        ["../priv/riak_kv.schema", "../priv/multi_backend.schema"], Conf, context(), predefined_schema()),

    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy", {off, []}),
    cuttlefish_unit:assert_config(Config, "riak_kv.storage_backend", riak_kv_multi_backend),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_build_limit", {1, 3600000}),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_expire", 604800000),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_concurrency", 2),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_tick", 15000),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_data_dir", "./data/anti_entropy"),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.write_buffer_size", 4194304),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.max_open_files", 20),
    cuttlefish_unit:assert_config(Config, "riak_kv.aae_throttle_enabled", true),
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.aae_throttle_limits"),
    cuttlefish_unit:assert_config(Config, "riak_kv.anti_entropy_leveldb_opts.use_bloomfilter", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.map_js_vm_count", 8),
    cuttlefish_unit:assert_config(Config, "riak_kv.reduce_js_vm_count", 6),
    cuttlefish_unit:assert_config(Config, "riak_kv.hook_js_vm_count", 2),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_max_vm_mem", 8),
    cuttlefish_unit:assert_config(Config, "riak_kv.js_thread_stack", 16),
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.js_source_dir"),
    cuttlefish_unit:assert_config(Config, "riak_kv.fsm_limit", 50000),
    cuttlefish_unit:assert_config(Config, "riak_kv.retry_put_coordinator_failure", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.object_format", v1),
    cuttlefish_unit:assert_config(Config, "riak_kv.vnode_md_cache_size", 0),
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.memory_backend.max_memory"),
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.memory_backend.ttl"),

    cuttlefish_unit:assert_config(Config, "riak_kv.secure_referer_check", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.warn_object_size", 51200),
    cuttlefish_unit:assert_config(Config, "riak_kv.max_object_size", 512000),
    cuttlefish_unit:assert_config(Config, "riak_kv.warn_siblings", 25),
    cuttlefish_unit:assert_config(Config, "riak_kv.max_siblings", 100),

    %% Default Bucket Properties
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.pr", 0),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.r", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.w", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.pw", 0),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.dw", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.rw", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.notfound_ok", true),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.basic_quorum", false),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.allow_mult", false),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.last_write_wins", false),
    cuttlefish_unit:assert_not_configured(Config, "riak_core.default_bucket_props.precommit"),
    cuttlefish_unit:assert_not_configured(Config, "riak_core.default_bucket_props.postcommit"),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.dvv_enabled", false),
    cuttlefish_unit:assert_config(Config, "riak_dt.binary_compression", 1),

    cuttlefish_unit:assert_config(Config, "riak_kv.multi_backend_default", <<"backend_one">>),

    ExpectedMutliConfig = [
        {<<"backend_one">>, riak_kv_memory_backend, [{ttl, 86400}, {max_memory, 8192}]},
        {<<"backend_two">>, riak_kv_memory_backend, []}
    ],
    cuttlefish_unit:assert_config(Config, "riak_kv.multi_backend", ExpectedMutliConfig),
    ok.

commit_hooks_test() ->
    Conf = [
            {["buckets", "default", "precommit"], "bad:mod:fun"},
            {["buckets", "default", "postcommit"], "jsLOL"}
           ],
    Config = cuttlefish_unit:generate_templated_config(
               ["../priv/riak_kv.schema", "../priv/multi_backend.schema"], Conf, context(), predefined_schema()),
    ?assertEqual({error, apply_translations,
                  {errorlist, [
                               {error,
                                {translation_invalid_configuration,
                                 {"riak_core.default_bucket_props.postcommit",
                                  "incorrect hook format 'jsLOL'"}}},
                               {error,
                                {translation_invalid_configuration,
                                 {"riak_core.default_bucket_props.precommit",
                                  "incorrect hook format 'bad:mod:fun'"}}}
                           ]}}, Config).

datatype_compression_validator_test() ->
    Conf = [{["datatypes", "compression_level"], 10}],
    Config = cuttlefish_unit:generate_templated_config(
               ["../priv/riak_kv.schema", "../priv/multi_backend.schema"], Conf, context(), predefined_schema()),
    cuttlefish_unit:assert_error_in_phase(Config, validation),
    ok.

correct_error_handling_by_multibackend_test() ->
    Conf = [
        {["multi_backend", "default", "storage_backend"], bitcask},
        {["multi_backend", "default", "bitcask", "data_root"], "/data/default_bitcask"}
    ],

    Config = cuttlefish_unit:generate_templated_config([
        "../priv/riak_kv.schema",
        "../priv/multi_backend.schema",
        "../deps/bitcask/priv/bitcask.schema",
        "../deps/bitcask/priv/bitcask_multi.schema",
        "../deps/eleveldb/priv/eleveldb.schema",
        "../deps/eleveldb/priv/eleveldb_multi.schema",
        "../test/bad_bitcask_multi.schema"
        ],
        Conf, context(), predefined_schema()),


    ?assertMatch({error, apply_translations, {errorlist, _}}, Config),
    {error, apply_translations, {errorlist, [Error]}} = Config,

    ?assertMatch({error, {translation_invalid_configuration, _}},
                 Error),
    ok.

all_backend_multi_test() ->
    Conf = [
        {["storage_backend"], multi},
        {["multi_backend", "default"], "bitcask1"},
        {["multi_backend", "bitcask1", "storage_backend"], bitcask},
        {["multi_backend", "bitcask1", "bitcask", "data_root"], "/data/bitcask1"},
        {["multi_backend", "bitcask2", "storage_backend"], bitcask},
        {["multi_backend", "bitcask2", "bitcask", "data_root"], "/data/bitcask2"},
        {["multi_backend", "level3", "storage_backend"], leveldb},
        {["multi_backend", "level3", "leveldb", "data_root"], "/data/level3"},
        {["multi_backend", "level4", "storage_backend"], leveldb},
        {["multi_backend", "level4", "leveldb", "data_root"], "/data/level4"},
        {["multi_backend", "memory5", "storage_backend"], "memory"},
        {["multi_backend", "memory5", "memory_backend", "max_memory_per_vnode"], "8GB"},
        {["multi_backend", "memory6", "memory_backend", "ttl"], "1d"},
        {["multi_backend", "memory6", "storage_backend"], "memory"}

    ],

    Config = cuttlefish_unit:generate_templated_config([
        "../priv/riak_kv.schema",
        "../priv/multi_backend.schema",
        "../deps/bitcask/priv/bitcask.schema",
        "../deps/bitcask/priv/bitcask_multi.schema",
        "../deps/eleveldb/priv/eleveldb.schema",
        "../deps/eleveldb/priv/eleveldb_multi.schema"
        ],
        Conf, context(), predefined_schema()),


    MultiBackendConfig = proplists:get_value(multi_backend, proplists:get_value(riak_kv, Config)),

    {<<"bitcask1">>, riak_kv_bitcask_backend,  B1} = lists:keyfind(<<"bitcask1">>, 1, MultiBackendConfig),
    {<<"bitcask2">>, riak_kv_bitcask_backend,  B2} = lists:keyfind(<<"bitcask2">>, 1, MultiBackendConfig),
    {<<"level3">>,   riak_kv_eleveldb_backend, L3} = lists:keyfind(<<"level3">>,   1, MultiBackendConfig),
    {<<"level4">>,   riak_kv_eleveldb_backend, L4} = lists:keyfind(<<"level4">>,   1, MultiBackendConfig),
    {<<"memory5">>,  riak_kv_memory_backend,   M5} = lists:keyfind(<<"memory5">>,  1, MultiBackendConfig),
    {<<"memory6">>,  riak_kv_memory_backend,   M6} = lists:keyfind(<<"memory6">>,  1, MultiBackendConfig),


    %% Check B1
    cuttlefish_unit:assert_config(B1, "data_root", "/data/bitcask1"),

    %% Check B2
    cuttlefish_unit:assert_config(B2, "data_root", "/data/bitcask2"),

    %% Check L3
    cuttlefish_unit:assert_config(L3, "data_root", "/data/level3"),

    %% Check L4
    cuttlefish_unit:assert_config(L4, "data_root", "/data/level4"),

    %% Check M5
    cuttlefish_unit:assert_not_configured(M5, "ttl"),
    cuttlefish_unit:assert_config(M5, "max_memory", 8192),

    %% Check M6
    cuttlefish_unit:assert_config(M6, "ttl", 86400),
    cuttlefish_unit:assert_not_configured(M6, "max_memory"),
    ok.

job_class_enabled_test() ->
    test_job_class_enabled(riak_core_schema()).

test_job_class_enabled({true, RCSchema}) when erlang:is_list(RCSchema) ->
    Config = cuttlefish_unit:generate_templated_config(
        [RCSchema, "../priv/riak_kv.schema"], [],
        riak_core_schema_tests:context() ++ context()),

    cuttlefish_unit:assert_config(
        Config, "riak_core.job_accept_class",
        lists:sort(?DEFAULT_ENABLED_JOB_CLASSES)),
    ok;
test_job_class_enabled({error, enoent}) ->
    % If riak_core is not present, or eunit hasn't been run there, the
    % necessary schema and/or beam file won't be found. If we fail the test
    % buildbot won't pass because the riak_core .eunit files haven't been built.
    ?debugMsg("Supporting riak_core components not present,"
        " skipping job_class_enabled test").

%% this context() represents the substitution variables that rebar
%% will use during the build process.  riak_core's schema file is
%% written with some {{mustache_vars}} for substitution during
%% packaging cuttlefish doesn't have a great time parsing those, so we
%% perform the substitutions first, because that's how it would work
%% in real life.
context() ->
    [
        {storage_backend, "leveldb"},
        {map_js_vms,   8},
        {reduce_js_vms, 6},
        {hook_js_vms, 2}
    ].

%% This predefined schema covers riak_kv's dependency on
%% platform_data_dir
predefined_schema() ->
    Mapping = cuttlefish_mapping:parse({mapping,
                                        "platform_data_dir",
                                        "riak_core.platform_data_dir", [
                                            {default, "./data"},
                                            {datatype, directory}
                                       ]}),
    {[], [Mapping], []}.

%% Ensure that the riak_core_schema_tests module is loaded and return the
%% path of the riak_core.schema file.
riak_core_schema() ->
    riak_core_schema(riak_core_dir()).
riak_core_schema({RCDir, Schema}) when erlang:is_list(RCDir) ->
    case code:ensure_loaded(riak_core_schema_tests) of
        {module, _} ->
            {true, Schema};
        _ ->
            Search = filename:join([RCDir, "**", "riak_core_schema_tests.beam"]),
            % ?debugFmt("Checking ~s", [Search]),
            case filelib:wildcard(Search) of
                [Beam | _] ->
                    % ?debugFmt("Loading ~s", [Beam]),
                    case code:load_abs(filename:rootname(Beam)) of
                        {module, _} ->
                            {true, Schema};
                        Error ->
                            Error
                    end;
                [] ->
                    {error, enoent}
            end
    end;
riak_core_schema(Error) ->
    Error.

riak_core_dir() ->
    TryDeps = case os:getenv("REBAR_DEPS_DIR") of
        false ->
            ["../deps", "../.."];
        Dir ->
            [Dir, "../deps"]
    end,
    riak_core_dir(TryDeps).
riak_core_dir([Deps | TryDeps]) ->
    RCDir   = filename:join(Deps, "riak_core"),
    Schema  = filename:join([RCDir, "priv", "riak_core.schema"]),
    % ?debugFmt("Checking ~s and ~s", [RCDir, Schema]),
    case filelib:is_regular(Schema) of
        true ->
            {RCDir, Schema};
        _ ->
            riak_core_dir(TryDeps)
    end;
riak_core_dir([]) ->
    {error, enoent}.
