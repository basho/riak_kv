-ifdef(DEBUG).
-define(DP(M), error_logger:info_msg("~p:~p "++M, [?MODULE, ?LINE])).
-define(DPV(X), error_logger:info_msg("~p:~p -- ~p =~n   ~p",
                                      [?MODULE, ?LINE, ??X, X])).

-define(DPF(M,X), error_logger:info_msg("~p:~p "++M,
                                        [?MODULE, ?LINE]++X)).
-else.
-define(DP(M), noop).
-define(DPV(X), noop).
-define(DPF(M,X), noop).
-endif.
