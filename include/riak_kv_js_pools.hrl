-define(JSPOOL_HOOK, riak_kv_js_hook).
-define(JSPOOL_MAP, riak_kv_js_map).
-define(JSPOOL_REDUCE, riak_kv_js_reduce).

%% this list is used to inform the js_reload command of all JS VM
%% managers that need to reload
-define(JSPOOL_LIST, [?JSPOOL_HOOK,
                      ?JSPOOL_MAP,
                      ?JSPOOL_REDUCE]).

