%% -*- tab-width: 4;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
{application, riak_kv,
 [
  {description, "Riak Key/Value Store"},
  {vsn, "0.14.0"},
  {modules, [
             lk,
             raw_link_walker,
             riak,
             riak_client,
             riak_index,
             riak_index_backend,
             riak_index_mi_backend,
             riak_kv_app,
             riak_kv_backup,
             riak_kv_backend,
             riak_kv_bitcask_backend,
             riak_kv_cache_backend,
             riak_kv_cinfo,
             riak_kv_console,
             riak_kv_delete,
             riak_kv_delete_sup,
             riak_kv_dets_backend,
             riak_kv_eleveldb_backend,
             riak_kv_ets_backend,
             riak_kv_fs_backend,
             riak_kv_gb_trees_backend,
             riak_kv_get_core,
             riak_kv_get_fsm,
             riak_kv_get_fsm_sup,
             riak_kv_index_backend,
             riak_kv_js_manager,
             riak_kv_js_sup,
             riak_kv_js_vm,
             riak_kv_keylister,
             riak_kv_keylister_sup,
             riak_kv_keys_fsm,
             riak_kv_keys_fsm_sup,
             riak_kv_legacy_vnode,
             riak_kv_lru,
             riak_kv_map_master,
             riak_kv_mapper,
             riak_kv_mapper_sup,
             riak_kv_map_phase,
             riak_kv_mapred_cache,
             riak_kv_mapred_filters,
             riak_kv_mapred_json,
             riak_kv_mapred_planner,
             riak_kv_mapred_query,
             riak_kv_mapred_term,
             riak_kv_mapreduce,
             riak_kv_multi_backend,
             riak_kv_pb_listener,
             riak_kv_pb_socket,
             riak_kv_pb_socket_sup,
             riak_kv_phase_proto,
             riak_kv_put_core,
             riak_kv_put_fsm,
             riak_kv_put_fsm_sup,
             riak_kv_reduce_phase,
             riak_kv_stat,
             riak_kv_status,
             riak_kv_sup,
             riak_kv_test_util,
             riak_kv_util,
             riak_kv_vnode,
             riak_kv_web,
             riak_kv_wm_buckets,
             riak_kv_wm_index,
             riak_kv_wm_keylist,
             riak_kv_wm_link_walker,
             riak_kv_wm_mapred,
             riak_kv_wm_object,
             riak_kv_wm_ping,
             riak_kv_wm_props,
             riak_kv_wm_stats,
             riak_kv_wm_utils,
             riak_kv_encoding_migrate,
             riak_object
            ]},
  {applications, [
                  kernel,
                  stdlib,
                  sasl,
                  crypto,
                  riak_core,
                  luke,
                  erlang_js,
                  mochiweb,
                  webmachine,
                  os_mon
                 ]},
  {registered, []},
  {mod, {riak_kv_app, []}},
  {env, [
         %% Endpoint for system stats HTTP provider
         {stats_urlpath, "stats"},

         %% Secondary code paths
         {add_paths, []},

         %% Allow Erlang MapReduce functions to be specified as
         %% strings.
         %%
         %% !!!WARNING!!!
         %% This will allow arbitrary Erlang code to be submitted
         %% through the REST and Protocol Buffers interfaces. This
         %% should only be used for development purposes.
         {allow_strfun, false}
        ]}
 ]}.
