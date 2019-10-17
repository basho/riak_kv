-module(riak_repl2_rtq_tests).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

-define(SETUP_ENV, application:set_env(riak_repl, rtq_max_bytes, 10*1024*1024)).
-define(CLEAN_ENV, application:unset_env(riak_repl, rtq_max_bytes)).

rtq_trim_test() ->
    %% make sure the queue is 10mb
    ?SETUP_ENV,
    {ok, Pid} = riak_repl2_rtq:start_test(),
    try
        gen_server:call(Pid, {register, rtq_test}),
        %% insert over 20mb in the queue
        MyBin = crypto:strong_rand_bytes(1024*1024),
        [gen_server:cast(Pid, {push, 1, MyBin}) || _ <- lists:seq(0, 20)],

        %% we get all 10 bytes back, because in TEST mode the RTQ disregards
        %% ETS overhead
        Size = accumulate(Pid, 0, 10),
        ?assert(Size =< 10*1024*1024),
        %% the queue is now empty
        ?assert(gen_server:call(Pid, {is_empty, rtq_test}))
    after
        ?CLEAN_ENV,
        exit(Pid, kill)
    end.

ask(Pid) ->
    Self = self(),
    gen_server:call(Pid, {pull_with_ack, rtq_test,
             fun ({Seq, NumItem, Bin, _Meta}) ->
                    Self ! {rtq_entry, {NumItem, Bin}},
                    gen_server:cast(Pid, {ack, rtq_test, Seq}),
                    ok
        end}).


accumulate(_, Acc, 0) ->
    Acc;
accumulate(Pid, Acc, C) ->
    ask(Pid),
    receive
        {rtq_entry, {_N, B}} ->
            Size = byte_size(B),
            accumulate(Pid, Acc+Size, C-1)
    end.

status_test_() ->
    {setup, fun() ->
        ?SETUP_ENV,
        {ok, QPid} = riak_repl2_rtq:start_link(),
        QPid
    end,
    fun(QPid) ->
        ?CLEAN_ENV,
        riak_repl_test_util:kill_and_wait(QPid)
    end,
    fun(_QPid) -> [

        {"queue size has percentage, and is correct", fun() ->
            MyBin = crypto:strong_rand_bytes(1024 * 1024),
            [riak_repl2_rtq:push(1, MyBin) || _ <- lists:seq(1, 5)],
            Status = riak_repl2_rtq:status(),
            StatusMaxBytes = proplists:get_value(max_bytes, Status),
            StatusBytes = proplists:get_value(bytes, Status),
            StatusPercent = proplists:get_value(percent_bytes_used, Status),
            ExpectedPercent = round( (StatusBytes / StatusMaxBytes) * 100000 ) / 1000,
            ?assertEqual(ExpectedPercent, StatusPercent)
        end}

    ] end}.

summarize_test_() ->
    {setup,
     fun start_rtq/0,
     fun kill_rtq/1,
     fun(_QPid) -> [
          {"includes sequence number, object ID, and size",
           fun() ->
               Objects = push_objects(<<"BucketsOfRain">>, [<<"obj1">>, <<"obj2">>]),
               Summarized = riak_repl2_rtq:summarize(),
               Zipped = lists:zip(Objects, Summarized),
               lists:foreach(
                 fun({Obj, Summary}) ->
                     {Seq, _, _} = Summary,
                     ExpectedSummary = {Seq, riak_object:key(Obj), get_approximate_size(Obj)},
                     ?assertMatch(ExpectedSummary, Summary)
                 end,
                 Zipped)
           end
          }
         ]
     end
}.

evict_test_() ->
    {foreach,
     fun start_rtq/0,
     fun kill_rtq/1,
     [
      fun(_QPid) ->
          {"evicts object by sequence if present",
           fun() ->
               Objects = push_objects(<<"TwoPeasInABucket">>, [<<"obj1">>, <<"obj2">>]),
               [KeyToEvict, RemainingKey] = [riak_object:key(O) || O <- Objects],
               [{SeqToEvict, KeyToEvict, _}, {RemainingSeq, RemainingKey, _}] = riak_repl2_rtq:summarize(),
               ok = riak_repl2_rtq:evict(SeqToEvict),
               ?assertMatch([{RemainingSeq, RemainingKey, _}], riak_repl2_rtq:summarize()),
               ok = riak_repl2_rtq:evict(RemainingSeq + 1),
               ?assertMatch([{RemainingSeq, RemainingKey, _}], riak_repl2_rtq:summarize())
           end
          }
      end,
      fun(_QPid) ->
          {"evicts object by sequence if present and key matches",
           fun() ->
               Objects = push_objects(<<"TwoPeasInABucket">>, [<<"obj1">>, <<"obj2">>]),
               [KeyToEvict, RemainingKey] = [riak_object:key(O) || O <- Objects],
               [{SeqToEvict, KeyToEvict, _}, {RemainingSeq, RemainingKey, _}] = riak_repl2_rtq:summarize(),
               ?assertMatch({wrong_key, _, _}, riak_repl2_rtq:evict(SeqToEvict, RemainingKey)),
               ?assertMatch({not_found, _}, riak_repl2_rtq:evict(RemainingSeq + 1, RemainingKey)),
               ?assertEqual(2, length(riak_repl2_rtq:summarize())),
               ok = riak_repl2_rtq:evict(SeqToEvict, KeyToEvict),
               ?assertMatch([{RemainingSeq, RemainingKey, _}], riak_repl2_rtq:summarize())
           end
          }
      end
     ]
    }.

overload_protection_start_test_() ->
    [
        {"able to start after a crash without ets errors", fun() ->
            {ok, Rtq1} = riak_repl2_rtq:start_link(),
            unlink(Rtq1),
            exit(Rtq1, kill),
            riak_repl_test_util:wait_until_down(Rtq1),
            Got = riak_repl2_rtq:start_link(),
            ?assertMatch({ok, _Pid}, Got),
            riak_repl2_rtq:stop(),
            catch exit(whereis(riak_repl2_rtq), kill),
            ets:delete(rtq_overload_ets)
        end},

        {"start with overload and recover options", fun() ->
            Got = riak_repl2_rtq:start_link([{overload_threshold, 5000}, {overload_recover, 2500}]),
            ?assertMatch({ok, _Pid}, Got),
            riak_repl2_rtq:stop(),
            catch exit(whereis(riak_repl2_rtq), kill),
            ets:delete(rtq_overload_ets)
        end},

        {"start the rtq overload counter process", fun() ->
            Got1 = riak_repl2_rtq_overload_counter:start_link(),
            ?assertMatch({ok, _Pid}, Got1),
            {ok, Pid1} = Got1,
            unlink(Pid1),
            exit(Pid1, kill),
            riak_repl_test_util:wait_until_down(Pid1),
            Got2 = riak_repl2_rtq_overload_counter:start_link([{report_interval, 20}]),
            ?assertMatch({ok, _Pid}, Got2),
            riak_repl2_rtq_overload_counter:stop()
        end}

    ].

overload_test_() ->
    {foreach,
     fun() ->
        % if you want lager started, and you're using bash, you can put
        % ENABLE_LAGER=TRUE in front of whatever you're using to run the tests
        % (make test, rebar eunit) and it will turn on lager for you.
        case os:getenv("ENABLE_LAGER") of
            false ->
                ok;
            _ ->
                application:start(lager),
                lager:set_loglevel(lager_console_backend, debug)
        end,
        riak_repl_test_util:abstract_stats(),
        riak_repl2_rtq:start_link([{overload_threshold, 5}, {overload_recover, 1}]),
        riak_repl2_rtq_overload_counter:start_link([{report_interval, 1000}]),
        riak_repl2_rtq:register("overload_test")
    end,
    fun(_) ->
            riak_repl2_rtq_overload_counter:stop(),
            riak_repl2_rtq:stop(),
            catch exit(whereis(riak_repl2_rtq), kill),
            catch exit(whereis(riak_repl2_rtq_overload_counter), kill),
            ets:delete(rtq_overload_ets),
            riak_repl_test_util:maybe_unload_mecks([riak_repl_stats]),
            meck:unload(),
            ok
    end, [

        fun(_) -> {"rtq increments sequence number on drop", fun() ->
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            Seq1 = pull(1),
            riak_repl2_rtq:report_drops(5),
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            Seq2 = pull(1),
            ?assertEqual(Seq1 + 5 + 1, Seq2)
        end} end,

        fun(_) -> {"rtq overload reports drops", fun() ->
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            Seq1 = pull(1),
            [riak_repl2_rtq_overload_counter:drop() || _ <- lists:seq(1, 5)],
            timer:sleep(1200),
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            Seq2 = pull(1),
            ?assertEqual(Seq1 + 5 + 1, Seq2)
        end} end,

        fun(_) -> {"overload and recovery", fun() ->
            % rtq can't process anything else while it's trying to deliver,
            % so we're going to use that to clog up it's queue.
            % Msgq = 0
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            % msg queue = 0 (it's handled)
            block_rtq_pull(),
            % msg queue = 0 (it's handled)
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            % msg queue = 1 (blocked by deliver)
            block_rtq_pull(),
            % msg queue = 2 (blocked by deliver)
            [riak_repl2_rtq:push(1, term_to_binary([<<"object">>])) || _ <- lists:seq(1,5)],
            % msg queue = 7 (blocked by deliver)
            unblock_rtq_pull(),
            % msq queue = 5 (push handled, blocking deliver handled)
            % that push should have flipped the overload switch
            % meaning these will be dropped
            % these will end up dropped
            [riak_repl2_rtq:push(1, term_to_binary([<<"object">>])) || _ <- lists:seq(1,5)],
            % msq queue = 7, drops = 5
            unblock_rtq_pull(),
            timer:sleep(1200),
            % msg queue = 0, totol objects dropped = 5
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            Seq1 = pull(5),
            Seq2 = pull(1),
            ?assertEqual(Seq1 + 1 + 5, Seq2),
            Status = riak_repl2_rtq:status(),
            ?assertEqual(5, proplists:get_value(overload_drops, Status))
        end} end,

        fun(_) -> {"rtq does recover on drop report", fun() ->
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            block_rtq_pull(),
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            [riak_repl2_rtq ! goober || _ <- lists:seq(1, 10)],
            Seq1 = unblock_rtq_pull(),
            Seq2 = pull(1),
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            timer:sleep(1200),
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            Seq3 = pull(1),
            ?assertEqual(1, Seq1),
            ?assertEqual(2, Seq2),
            ?assertEqual(4, Seq3)
        end} end,

        fun(_) -> {"rtq overload sets rt_dirty to true", fun() ->
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            block_rtq_pull(),
            riak_repl2_rtq:push(1, term_to_binary([<<"object">>])),
            [riak_repl2_rtq ! goober || _ <- lists:seq(1, 10)],
            unblock_rtq_pull(),
            History = meck:history(riak_repl_stats),
            ?assertMatch([{_MeckPid, {riak_repl_stats, rt_source_errors, []}, ok}], History)
        end}
        end

    ]}.

start_rtq() ->
    ?SETUP_ENV,
    {ok, Pid} = riak_repl2_rtq:start_link(),
    gen_server:call(Pid, {register, rtq_test}),
    Pid.

kill_rtq(QPid) ->
    ?CLEAN_ENV,
    riak_repl_test_util:kill_and_wait(QPid).

object_format() -> riak_core_capability:get({riak_kv, object_format}, v0).

get_approximate_size(O) -> riak_object:approximate_size(object_format(), O).

push_objects(Bucket, Keys) -> [push_object(Bucket, O) || O <- Keys].

push_object(Bucket, Key) ->
    RandomData = crypto:strong_rand_bytes(1024 * 1024),
    Obj = riak_object:new(Bucket, Key, RandomData),
    riak_repl2_rtq:push(1, Obj),
    Obj.

pull(N) ->
    lists:foldl(fun(_Nth, _LastSeq) ->
        pull()
    end, 0, lists:seq(1, N)).

pull() ->
    Self = self(),
    riak_repl2_rtq:pull("overload_test", fun({Seq, _, _, _}) ->
        Self ! {seq, Seq},
        ok
    end),
    get_seq().

get_seq() ->
    receive {seq, S} -> S end.

block_rtq_pull() ->
    Self = self(),
    riak_repl2_rtq:pull("overload_test", fun({Seq, _, _, _}) ->
        receive
            continue ->
                ok
        end,
        Self ! {seq, Seq},
        ok
    end).

unblock_rtq_pull() ->
    riak_repl2_rtq ! continue,
    get_seq().
