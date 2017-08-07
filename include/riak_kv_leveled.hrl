%% -------------------------------------------------------------------
%%
%% riak_kv_leveled:  Can't get cuttlefish to work - so put config here
%%

-define(LEVELED_SYNCSTRATEGY, none). % can be riak_sync|none
-define(LEVELED_JOURNALSIZE, 500000000).
-define(LEVELED_LEDGERCACHE, 4000).
-define(LEVELED_JC_VALID_HOURS, [4, 5, 6]).
-define(LEVELED_JC_CHECK_INTERVAL, timer:minutes(10)).
-define(LEVELED_JC_CHECK_JITTER, 0.3).
-define(LEVELED_DATAROOT, "/data/leveled").
