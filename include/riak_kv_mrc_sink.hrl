%% used to communicate from riak_kv_mrc_sink to riak_kv_wm_mapred and
%% riak_kv_pb_mapred
-record(kv_mrc_sink,
        {
          ref :: reference(), % the pipe ref
          results :: [{PhaseId::integer(), Result::term()}],
          logs :: [{PhaseId::integer(), Message::term()}],
          done :: boolean()
        }).
