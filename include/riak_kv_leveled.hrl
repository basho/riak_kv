%% -------------------------------------------------------------------
%%
%% riak_kv_leveled:  Can't get cuttlefish to work - so put config here
%%

-define(LEVELED_SYNCSTRATEGY, none). % can be riak_sync|none
-define(LEVELED_JOURNALSIZE, 500000000).
-define(LEVELED_LEDGERCACHE, 4000).
-define(LEVELED_JC_VALID_HOURS, [0, 1, 2, 3, 4, 5, 6,
                                    7, 8, 9, 10, 11, 12,
                                    13, 14, 15, 16, 17, 18,
                                    19, 20, 21, 22, 23]).
-define(LEVELED_JC_COMPACTIONS_PERDAY, 10).
-define(LEVELED_DATAROOT, "/data/leveled").
