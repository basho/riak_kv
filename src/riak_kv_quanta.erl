%% @doc This module serves to generate time quanta on multi - (year, month, day, hour, minute,
%% second) boundaries. The quanta are based on an origin time of Jan 1, 1970 00:00:00 (Unix Epoch).
%% The function <em>quanta/3</em> takes a time in milliseconds to bucketize, a size of the quanta, and the
%% units of said quanta. For instance, the following call would create buckets for timestamps on 15
%% minute boundaries: <em>quanta(Time, 15, m)</em>. The quanta time is returned in milliseconds since the
%% Unix epoch.

-module(riak_kv_quanta).

-export([quanta/3,
         timestamp_to_ms/1,
         ms_to_timestamp/1]).

-type time_ms() :: non_neg_integer().
%% A timestamp in millisconds representing number of millisconds from Unix epoch

-type time_unit() :: y | mo | d | h | m | s.
%%  The units of quantization available to quanta/3

-type err() :: {error, term()}.

%% @doc The Number of Days from Jan 1, 0 to Jan 1, 1970
%% We need this to compute years and months properly including leap years and variable length
%% months.
-define(DAYS_FROM_0_TO_1970, 719528).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-compile(export_all).
-endif.
-endif.
%% @clear
%% @end

%% @doc Given the time in milliseconds since the unix epoch and a time range and unit eg (15, m),
%% generate the starting timestamp of the range (quanta) in milliseconds since the epoch where the
%% time belongs. Note that Time - Quanta is less than or equal to QuantaSize * Unit (in milliseconds).
-spec quanta(time_ms(), non_neg_integer(), time_unit()) -> time_ms() | err().
quanta(Time, QuantaSize, Unit) when Unit == d; Unit == h; Unit == m; Unit == s ->
    Ms = unit_to_ms(Unit),
    Diff = Time rem (QuantaSize*Ms),
    Time - Diff;
quanta(Time, QuantaSize, mo) ->
    Timestamp = ms_to_timestamp(Time),
    Month = months_since_1970(Timestamp),
    MonthQuanta = Month - (Month rem QuantaSize),
    months_since_1970_to_ms(MonthQuanta);
quanta(Time, QuantaSize, y) ->
    Timestamp = ms_to_timestamp(Time),
    {{Year, _, _}, _} = calendar:now_to_universal_time(Timestamp),
    YearsSince1970 = Year - 1970,
    YearQuanta = Year - (YearsSince1970 rem QuantaSize),
    years_since_1970_to_ms(YearQuanta);
quanta(_, _, Unit) ->
    {error, {invalid_unit, Unit}}.

%% @doc Return the time in milliseconds since 00:00 GMT Jan 1, 1970 (Unix Epoch)
-spec timestamp_to_ms(erlang:timestamp()) -> time_ms().
timestamp_to_ms({Mega, Secs, Micro}) ->
    Mega*1000000000 + Secs*1000 + Micro div 1000.

%% @doc Return an erlang:timestamp() given the time in milliseconds since the Unix Epoch
-spec ms_to_timestamp(time_ms()) -> erlang:timestamp().
ms_to_timestamp(Time) ->
    Seconds = Time div 1000,
    MicroSeconds = (Time rem 1000) * 1000,
    {0, Seconds, MicroSeconds}.

%% @doc Return the time in milliseconds since 00:00 GMT Jan 1, 1970 (Unix Epoch)
%% This accounts for leap years. Yay!
-spec years_since_1970_to_ms(non_neg_integer()) -> time_ms().
years_since_1970_to_ms(Year) ->
    DaysSince0 = calendar:date_to_gregorian_days(Year, 1, 1),
    DaysSince1970 = DaysSince0 - ?DAYS_FROM_0_TO_1970,
    DaysSince1970 * unit_to_ms(d).

%% @doc Return the time in milliseconds since 00:00 GMT Jan 1, 1970 (Unix Epoch)
%% This accounts for variable length months. Yay!
-spec months_since_1970_to_ms(non_neg_integer()) -> time_ms().
months_since_1970_to_ms(Months) ->
    Year = 1970 + Months div 12,
    %% 0 Months since January is January
    Month = Months rem 12 + 1,
    DaysSince0 = calendar:date_to_gregorian_days(Year, Month, 1),
    DaysSince1970 = DaysSince0 - ?DAYS_FROM_0_TO_1970,
    DaysSince1970 * unit_to_ms(d).

-spec months_since_1970(erlang:timestamp()) -> non_neg_integer().
months_since_1970(Timestamp) ->
    {{Year, Month, _}, _} = calendar:now_to_universal_time(Timestamp),
    %% 12 months = 1 year and 0 months
    (Year - 1970) * 12 + Month - 1.

-spec unit_to_ms(s | m | h | d) -> time_ms().
unit_to_ms(s) ->
    1000;
unit_to_ms(m) ->
    60 * unit_to_ms(s);
unit_to_ms(h) ->
    60 * unit_to_ms(m);
unit_to_ms(d) ->
    24 * unit_to_ms(h).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

assert_year(Year0) ->
    Ms = years_since_1970_to_ms(Year0),
    {{Year, _, _}, _} = calendar:now_to_universal_time(ms_to_timestamp(Ms)),
    ?assertEqual(Year0, Year).

years_since_1970_to_ms_test() ->
    %% Plain old year
    assert_year(1979),
    %% leap year
    assert_year(2000),
    %% epoch
    assert_year(1970),
    %% older than epoch
    assert_year(1).

assert_months(Months) ->
    Ms = months_since_1970_to_ms(Months),
    ?assertEqual(Months, months_since_1970(ms_to_timestamp(Ms))).

months_since_1970_to_ms_test() ->
    assert_months(1),
    assert_months(12),
    assert_months(18),
    assert_months(24).

assert_minutes(Quanta, OkTimes) ->
    Time = timestamp_to_ms(os:timestamp()),
    QuantaMs = quanta(Time, Quanta, m),
    {_, {_, M, _}} = calendar:now_to_universal_time(ms_to_timestamp(QuantaMs)),
    ?assert(lists:member(M, OkTimes)).

quanta_minutes_test() ->
    assert_minutes(15, [0, 15, 30, 45]),
    assert_minutes(75, [0, 15, 30, 45]),
    assert_minutes(5, [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]),
    assert_minutes(6, [0, 6, 12, 18, 24, 30, 36, 42, 48, 54]).

assert_hours(Quanta, OkTimes) ->
    Time = timestamp_to_ms(os:timestamp()),
    QuantaMs = quanta(Time, Quanta, h),
    {_, {H, _, _}} = calendar:now_to_universal_time(ms_to_timestamp(QuantaMs)),
    ?assert(lists:member(H, OkTimes)).

quanta_hours_test() ->
    assert_hours(12, [0, 12]),
    assert_hours(24, [0]).

assert_days(Days) ->
    Now = os:timestamp(),
    Time = timestamp_to_ms(Now),
    QuantaMs = quanta(Time, Days, d),
    {NowDate, _} = calendar:now_to_universal_time(Now),
    {QuantaDate, _} = calendar:now_to_universal_time(ms_to_timestamp(QuantaMs)),
    NowDays = calendar:date_to_gregorian_days(NowDate),
    QuantaDays = calendar:date_to_gregorian_days(QuantaDate),
    ?assert((NowDays - QuantaDays) < Days),
    ?assert((NowDays - QuantaDays) >= 0).

quanta_days_test() ->
    assert_days(1),
    assert_days(10),
    assert_days(15),
    assert_days(28),
    assert_days(29),
    assert_days(30),
    assert_days(31).

-ifdef(EQC).
prop_quanta_bounded_test() ->
    ?assertEqual(true, eqc:quickcheck(?QC_OUT(prop_quanta_bounded()))).

prop_quanta_month_boundary_test() ->
    ?assertEqual(true, eqc:quickcheck(?QC_OUT(prop_quanta_month_boundary()))).

prop_quanta_year_boundary_test() ->
    ?assertEqual(true, eqc:quickcheck(?QC_OUT(prop_quanta_year_boundary()))).

%% Ensure that Quantas are always bounded, meaning that any time is no more than one quanta ahead of
%% the quanta start.
prop_quanta_bounded() ->
    ?FORALL({Date, Time, {Quanta, Unit}}, {date_gen(), time_gen(), quanta_gen()},
        begin
            DateTime = {Date, Time},
            SecondsFrom0To1970 = ?DAYS_FROM_0_TO_1970 * (unit_to_ms(d) div 1000),
            DateMs = (calendar:datetime_to_gregorian_seconds(DateTime) - SecondsFrom0To1970)*1000,
            QuantaMs = quanta(DateMs, Quanta, Unit),
            QuantaSize = quanta_in_ms(Quanta, Unit),
            (DateMs - QuantaMs) =< QuantaSize
        end).

%% Ensure quantas for months are always on a monthly boundary
prop_quanta_month_boundary() ->
    ?FORALL({Date, Time, {Quanta, Unit}}, {date_gen(), time_gen(), month_gen()},
        begin
            Timestamp = quanta_now_from_datetime({Date, Time}, Quanta, Unit),
            {{_, _, Day}, QuantaTime} = calendar:now_to_datetime(Timestamp),
            Day =:= 1 andalso QuantaTime =:= {0,0,0}
        end).

%% Ensure quantas for years are always on a yearly boundary
prop_quanta_year_boundary() ->
    ?FORALL({Date, Time, {Quanta, Unit}}, {date_gen(), time_gen(), year_gen()},
        begin
            Timestamp = quanta_now_from_datetime({Date, Time}, Quanta, Unit),
            {{_, Month, Day}, QuantaTime} = calendar:now_to_datetime(Timestamp),
            Month =:= 1 andalso Day =:= 1 andalso QuantaTime =:= {0,0,0}
        end).


quanta_now_from_datetime(DateTime, Quanta, Unit) ->
    SecondsFrom0To1970 = ?DAYS_FROM_0_TO_1970 * (unit_to_ms(d) div 1000),
    DateMs = (calendar:datetime_to_gregorian_seconds(DateTime) - SecondsFrom0To1970)*1000,
    QuantaMs = quanta(DateMs, Quanta, Unit),
    ms_to_timestamp(QuantaMs).


quanta_in_ms(Quanta, mo) ->
    months_since_1970_to_ms(Quanta);
quanta_in_ms(Quanta, y) ->
    %% Just use max # days in year for safety
    Quanta*366*unit_to_ms(d);
quanta_in_ms(Quanta, Unit) ->
    Quanta*unit_to_ms(Unit).

%% EQC Generators
date_gen() ->
    ?SUCHTHAT(Date, {choose(1970, 2015), choose(1, 12), choose(1, 31)}, calendar:valid_date(Date)).

time_gen() ->
    {choose(0, 23), choose(0, 59), choose(0, 59)}.

month_gen() ->
    {choose(1, 100), mo}.

year_gen() ->
    {choose(1, 40), y}.

quanta_gen() ->
    oneof([month_gen(),
           year_gen(),
           {choose(1,2000), h},
           {choose(1, 60), m}]).

-endif.

-endif.
