-define(T(Details, Extra, Msg),
        riak_pipe_log:trace(Details, [?MODULE|Extra], Msg)).
-define(T_ERR(Details, Props),
        riak_pipe_log:trace(Details, [?MODULE, error], {error, Props})).
-define(L(Details, Msg),
        riak_pipe_log:log(Details, Msg)).
