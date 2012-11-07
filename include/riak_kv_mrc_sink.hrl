%% used to communicate from riak_kv_mrc_sink to riak_kv_wm_mapred and
%% riak_kv_pb_mapred
-record(kv_mrc_sink,
        {
          ref :: reference(), % the pipe ref
          results :: [{PhaseId::integer(), Result::term()}],
          logs :: [{PhaseId::integer(), Message::term()}],
          done :: boolean()
        }).

%% used by riak_kv_mrc_sink:mapred_stream_sink
-record(mrc_ctx,
        {
          ref :: reference(), % the pipe ref (so we don't have to dig)
          pipe :: riak_pipe:pipe(),
          sink :: {pid(), reference()}, % sink and monitor
          sender :: {pid(), reference()}, % async sender and monitor
          timer :: {reference(), reference()}, % timeout timer and pipe ref
          keeps :: integer()
         }).
