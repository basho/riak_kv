%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_util).
-author('Andy Gross <andy@basho.com>').
-include_lib("public_key/include/OTP-PUB-KEY.hrl").
%%-include_lib("riak_kv/include/riak_kv_vnode.hrl").
-include("riak_repl.hrl").

-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([make_peer_info/0,
         make_fake_peer_info/0,
         validate_peer_info/2,
         capability_from_vsn/1,
         get_partitions/1,
         do_repl_put/1,
         site_root_dir/1,
         ensure_site_dir/1,
         binpack_bkey/1,
         binunpack_bkey/1,
         merkle_filename/3,
         keylist_filename/3,
         non_loopback_interfaces/1,
         valid_host_ip/1,
         normalize_ip/1,
         format_socketaddrs/2,
         maybe_use_ssl/0,
         upgrade_client_to_ssl/1,
         choose_strategy/2,
         strategy_module/2,
         configure_socket/2,
         repl_helper_send/2,
         repl_helper_send_realtime/2,
         schedule_fullsync/0,
         schedule_fullsync/1,
         schedule_cluster_fullsync/1,
         schedule_cluster_fullsync/2,
         elapsed_secs/1,
         shuffle_partitions/2,
         proxy_get_active/0,
         log_dropped_realtime_obj/1,
         dropped_realtime_hook/1,
         generate_socket_tag/3,
         source_socket_stats/0,
         sink_socket_stats/0,
         get_peer_repl_nodes/0,
         get_hooks_for_modes/0,
         remove_unwanted_stats/1,
         format_ip_and_port/2,
         safe_pid_to_list/1,
         safe_get_msg_q_len/1,
         peername/2,
         sockname/2,
         deduce_wire_version_from_proto/1,
         encode_obj_msg/2,
         decode_bin_obj/1,
         make_pg_proxy_name/1,
         make_pg_name/1,
         mode_12_enabled/1,
         mode_13_enabled/1,
         maybe_get_vnode_lock/1,
         maybe_send/3
     ]).

-export([wire_version/1,
         to_wire/1,
         to_wire/2,
         to_wire/4,
         from_wire/1,
         from_wire/2,
         maybe_downconvert_binary_objs/2,
         peer_wire_format/1
        ]).

%% Defines for Wire format encode/decode
-define(MAGIC, 42). %% as opposed to 131 for Erlang term_to_binary or 51 for riak_object
-define(W1_VER, 1). %% first non-just-term-to-binary wire format
-define(W2_VER, 2). %% first non-just-term-to-binary wire format
-define(BAD_SOCKET_NUM, -1).

make_peer_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    SafeRing = riak_core_ring:downgrade(1, Ring),
    {ok, RiakVSN} = application:get_key(riak_kv, vsn),
    {ok, ReplVSN} = application:get_key(riak_repl, vsn),
    #peer_info{riak_version=RiakVSN, repl_version=ReplVSN, ring=SafeRing}.

%% Makes some plausible, but wrong, peer info. Used to get to SSL negotiation
%% without leaking sensitive information.
make_fake_peer_info() ->
    FakeRing = riak_repl_ring:ensure_config(riak_core_ring:fresh(1, node())),
    SafeRing = riak_core_ring:downgrade(1, FakeRing),
    #peer_info{riak_version="0.0.0", repl_version="0.0.0", ring=SafeRing}.

validate_peer_info(T=#peer_info{}, M=#peer_info{}) ->
    TheirPartitions = get_partitions(T#peer_info.ring),
    OurPartitions = get_partitions(M#peer_info.ring),
    TheirPartitions =:= OurPartitions.

%% Build a default capability from the version information in #peerinfo{}
capability_from_vsn(#peer_info{repl_version = ReplVsnStr}) ->
    ReplVsn = parse_vsn(ReplVsnStr),
    case ReplVsn >= {0, 14, 0} of
        true ->
            [bounded_queue];
        false ->
            []
    end.

get_partitions(Ring) ->
    lists:sort([P || {P, _} <-
            riak_core_ring:all_owners(riak_core_ring:upgrade(Ring))]).

do_repl_put({RemoteTypeHash, Object}) ->
    Bucket = riak_object:bucket(Object),
    Type = riak_object:type(Object),
    case riak_core_bucket:get_bucket(Bucket) of
        {error, no_type} ->
            lager:warning("Type ~p not defined on sink, not doing put", [Type]),
            ok;
        _Props ->
            BucketPropsMatch =
                riak_repl_bucket_type_util:bucket_props_match(Type,
                                                              RemoteTypeHash),
            do_repl_put(Object, Bucket, BucketPropsMatch)
    end;
do_repl_put(Object) ->
    Bucket = riak_object:bucket(Object),
    do_repl_put(Object, Bucket, true).

do_repl_put(_Object, _B, false) ->
    %% Remote and local bucket properties differ so ignore this object
    lager:warning("Remote and local bucket properties differ for type ~p",
                  [riak_object:type(_Object)]),
    ok;
do_repl_put(Object, B, true) ->
    K = riak_object:key(Object),
    case repl_helper_recv(Object) of
        ok ->
            ReqId = erlang:phash2({self(), os:timestamp()}),
            Opts = [asis, disable_hooks, {update_last_modified, false}],

            {ok, PutPid} = riak_kv_put_fsm:start_link(ReqId, Object, all, all,
                    ?REPL_FSM_TIMEOUT,
                    self(), Opts),

            MRef = erlang:monitor(process, PutPid),

            %% block waiting for response
            wait_for_response(ReqId, "put"),

            %% wait for put FSM to exit
            receive
                {'DOWN', MRef, _, _, _} ->
                    ok
            after 60000 ->
                    lager:warning("put fsm did not exit on schedule"),
                    ok
            end,

            case riak_kv_util:is_x_deleted(Object) of
                true ->
                    lager:debug("Incoming deleted obj ~p/~p", [B, K]),
                    _ = reap(ReqId, B, K),
                    %% block waiting for response
                    wait_for_response(ReqId, "reap");
                false ->
                    lager:debug("Incoming obj ~p/~p", [B, K])
            end;
        cancel ->
            lager:debug("Skipping repl received object ~p/~p", [B, K])
    end.

reap(ReqId, B, K) ->
    riak_kv_get_fsm:start(ReqId, B, K, 1, ?REPL_FSM_TIMEOUT, self()).

wait_for_response(ReqId, Verb) ->
    receive
        {ReqId, {error, Reason}} ->
            lager:debug("Failed to ~s replicated object: ~p", [Verb, Reason]);
        {ReqId, _} ->
            ok
    after 60000 ->
            lager:warning("Timed out after 1 minute doing ~s on replicated object",
            [Verb]),
            ok
    end.

repl_helper_recv(Object) ->
    case application:get_env(riak_core, repl_helper) of
        undefined -> ok;
        {ok, Mods} ->
            repl_helper_recv(Mods, Object)
    end.

repl_helper_recv([], _O) ->
    ok;
repl_helper_recv([{App, Mod}|T], Object) ->
    try Mod:recv(Object) of
        ok ->
            repl_helper_recv(T, Object);
        cancel ->
            cancel;
        Other ->
            lager:error("Unexpected result running repl recv helper "
                "~p from application ~p : ~p",
                [Mod, App, Other]),
            repl_helper_recv(T, Object)
    catch
        What:Why ->
            lager:error("Crash while running repl recv helper "
                "~p from application ~p : ~p:~p",
                [Mod, App, What, Why]),
            repl_helper_recv(T, Object)
    end.

maybe_send(Object, C, Proto) ->
    maybe_send(riak_object:bucket(Object), Object, C, Proto).

maybe_send({_T, _B}, Object, C, {Major, _Minor}) when Major >=3 ->
    repl_helper_send(Object, C);
maybe_send({_T, _B}, _Object, _C, Proto) ->
    lager:debug("Negotiated protocol version:~p does not support typed buckets, not sending", [Proto]),
    cancel;
maybe_send(_B, Object, C, _Proto) ->
    repl_helper_send(Object, C).

repl_helper_send(Object, C) ->
    B = riak_object:bucket(Object),
    case fullsync_enabled_for_bucket(B, C) of
        true ->
            case application:get_env(riak_core, repl_helper) of
                undefined -> [];
                {ok, Mods} ->
                    repl_helper_send(Mods, Object, C, [])
            end;
        false ->
            lager:debug("Repl disabled for bucket ~p", [B]),
            cancel
    end.

fullsync_enabled_for_bucket(Bucket, _C) ->
    case riak_core_bucket:get_bucket(Bucket) of
        {error, _Reason} ->
            %% Bucket type does not exist so do not enable fullsync
            false;
        Props ->
            not is_consistent_bucket(Props) andalso is_fullsync_enabled(Props)
    end.

is_consistent_bucket(Props) ->
    case lists:keyfind(consistent, 1, Props) of
        {consistent, true} ->
            true;
        _ ->
            false
    end.

is_fullsync_enabled(Props) ->
    %% Default to enabling fullsync for all buckets and only disable
    %% if explicitly indicated.
    case lists:keyfind(repl, 1, Props) of
        {repl, Val} when Val =:= false; Val =:= realtime ->
            false;
        _ ->
            true
    end.

repl_helper_send([], _O, _C, Acc) ->
    Acc;
repl_helper_send([{App, Mod}|T], Object, C, Acc) ->
    try Mod:send(Object, C) of
        Objects when is_list(Objects) ->
            repl_helper_send(T, Object, C, Objects ++ Acc);
        ok ->
            repl_helper_send(T, Object, C, Acc);
        cancel ->
            cancel;
         Other ->
            lager:error("Unexpected result running repl send helper "
                "~p from application ~p : ~p",
                [Mod, App, Other]),
            repl_helper_send(T, Object, C, Acc)
    catch
        What:Why ->
            lager:error("Crash while running repl send helper "
                "~p from application ~p : ~p:~p",
                [Mod, App, What, Why]),
            repl_helper_send(T, Object, C, Acc)
    end.

repl_helper_send_realtime(Object, C) ->
    case application:get_env(riak_core, repl_helper) of
        undefined -> [];
        {ok, Mods} ->
            repl_helper_send_realtime(Mods, Object, C, [])
    end.

repl_helper_send_realtime([], _O, _C, Acc) ->
    Acc;
repl_helper_send_realtime([{App, Mod}|T], Object, C, Acc) ->
    try Mod:send_realtime(Object, C) of
        Objects when is_list(Objects) ->
            repl_helper_send_realtime(T, Object, C, Objects ++ Acc);
        ok ->
            repl_helper_send_realtime(T, Object, C, Acc);
        cancel ->
            cancel;
         Other ->
            lager:error("Unexpected result running repl realtime send helper "
                "~p from application ~p : ~p",
                [Mod, App, Other]),
            repl_helper_send_realtime(T, Object, C, Acc)
    catch
        What:Why ->
            lager:error("Crash while running repl realtime send helper "
                "~p from application ~p : ~p:~p",
                [Mod, App, What, Why]),
            repl_helper_send_realtime(T, Object, C, Acc)
    end.


site_root_dir(Site) ->
    {ok, DataRootDir} = application:get_env(riak_repl, data_root),
    SitesRootDir = filename:join([DataRootDir, "sites"]),
    filename:join([SitesRootDir, Site]).

ensure_site_dir(Site) ->
    ok = filelib:ensure_dir(
           filename:join([riak_repl_util:site_root_dir(Site), ".empty"])).

binpack_bkey({{Type, B}, K}) ->
    ST = size(Type),
    SB = size(B),
    SK = size(K),
    <<ST:32/integer, Type/binary, SB:32/integer, B/binary, SK:32/integer, K/binary>>;
binpack_bkey({B, K}) ->
    SB = size(B),
    SK = size(K),
    <<SB:32/integer, B/binary, SK:32/integer, K/binary>>.

binunpack_bkey(<<ST:32/integer, Type:ST/binary, SB:32/integer, B:SB/binary, SK:32/integer, K:SK/binary>>) ->
    {{Type, B}, K};
binunpack_bkey(<<SB:32/integer,B:SB/binary,SK:32/integer,K:SK/binary>>) ->
    {B,K}.

merkle_filename(WorkDir, Partition, Type) ->
    Ext = case Type of
        ours ->
            ".merkle";
        theirs ->
            ".theirs"
    end,
    filename:join(WorkDir,integer_to_list(Partition)++Ext).

keylist_filename(WorkDir, Partition, Type) ->
    Ext = case Type of
        ours ->
            ".ours.sterm";
        theirs ->
            ".theirs.sterm"
    end,
    filename:join(WorkDir,integer_to_list(Partition)++Ext).

%% @doc IFs is in the form returned by inet:getifaddrs()
%%      Returns interfaces with the "up" flag, but without the
%%      "loopback" flag
non_loopback_interfaces(IFs) ->
    lists:filter(
        fun({_Name, Attrs}) ->
            Flags = proplists:get_value(flags, Attrs),
            lists:member(up, Flags) andalso not lists:member(loopback, Flags)
        end, IFs).

%% Returns true if the IP address given is a valid host IP address
valid_host_ip(IP) ->
    {ok, IFs} = inet:getifaddrs(),
    {ok, NormIP} = normalize_ip(IP),
    lists:foldl(
        fun({_IF, Attrs}, Match) ->
                case lists:member({addr, NormIP}, Attrs) of
                    true ->
                        true;
                    _ ->
                        Match
                end
        end, false, IFs).

%% Convert IP address the tuple form
normalize_ip(IP) when is_list(IP) ->
    inet_parse:address(IP);
normalize_ip(IP) when is_tuple(IP) ->
    {ok, IP}.

format_socketaddrs(Socket, Transport) ->
    {ok, {LocalIP, LocalPort}} = Transport:sockname(Socket),
    {ok, {RemoteIP, RemotePort}} = Transport:peername(Socket),
    lists:flatten(io_lib:format("~s:~p-~s:~p", [inet_parse:ntoa(LocalIP),
                                                LocalPort,
                                                inet_parse:ntoa(RemoteIP),
                                                RemotePort])).

maybe_use_ssl() ->
    SSLOpts = [
        {certfile, app_helper:get_env(riak_repl, certfile, undefined)},
        {keyfile, app_helper:get_env(riak_repl, keyfile, undefined)},
        {cacerts, load_certs(app_helper:get_env(riak_repl, cacertdir, undefined))},
        {depth, app_helper:get_env(riak_repl, ssl_depth, 1)},
        {verify_fun, {fun verify_ssl/3,
                get_my_common_name(app_helper:get_env(riak_repl, certfile,
                        undefined))}},
        {verify, verify_peer},
        {server_name_indication, disable},
        {fail_if_no_peer_cert, true},
        {secure_renegotiate, true} %% both sides are erlang, so we can force this
    ],
    Enabled = app_helper:get_env(riak_repl, ssl_enabled, false) == true,
    case validate_ssl_config(Enabled, SSLOpts) of
        true ->
            SSLOpts;
        {error, Reason} ->
            lager:error("Error, invalid SSL configuration: ~s", [Reason]),
            false;
        false ->
            %% not all the SSL options are configured, use TCP
            false
    end.

validate_ssl_config(false, _) ->
    %% ssl is disabled
    false;
validate_ssl_config(true, []) ->
    %% all options validated
    true;
validate_ssl_config(true, [{certfile, CertFile}|Rest]) ->
    case filelib:is_regular(CertFile) of
        true ->
            validate_ssl_config(true, Rest);
        false ->
            {error, lists:flatten(io_lib:format("Certificate ~p is not a file",
                                                [CertFile]))}
    end;
validate_ssl_config(true, [{keyfile, KeyFile}|Rest]) ->
    case filelib:is_regular(KeyFile) of
        true ->
            validate_ssl_config(true, Rest);
        false ->
            {error, lists:flatten(io_lib:format("Key ~p is not a file",
                                                [KeyFile]))}
    end;
validate_ssl_config(true, [{cacerts, CACerts}|Rest]) ->
    case CACerts of
        undefined ->
            {error, lists:flatten(
                    io_lib:format("CA cert dir ~p is invalid",
                                  [app_helper:get_env(riak_repl, cacertdir,
                                                      undefined)]))};
        [] ->
            {error, lists:flatten(
                    io_lib:format("Unable to load any CA certificates from ~p",
                                  [app_helper:get_env(riak_repl, cacertdir,
                                                      undefined)]))};
        Certs when is_list(Certs) ->
            validate_ssl_config(true, Rest)
    end;
validate_ssl_config(true, [_|Rest]) ->
    validate_ssl_config(true, Rest).

upgrade_client_to_ssl(Socket) ->
    case maybe_use_ssl() of
        false ->
            {error, no_ssl_config};
        Config ->
            ssl:connect(Socket, Config)
    end.

load_certs(undefined) ->
    undefined;
load_certs(CertDir) ->
    case file:list_dir(CertDir) of
        {ok, Certs} ->
            load_certs(lists:map(fun(Cert) -> filename:join(CertDir, Cert)
                    end, Certs), []);
        {error, _} ->
            undefined
    end.

load_certs([], Acc) ->
    lager:debug("Successfully loaded ~p CA certificates", [length(Acc)]),
    Acc;
load_certs([Cert|Certs], Acc) ->
    case filelib:is_dir(Cert) of
        true ->
            load_certs(Certs, Acc);
        _ ->
            lager:debug("Loading certificate ~p", [Cert]),
            load_certs(Certs, load_cert(Cert) ++ Acc)
    end.

load_cert(Cert) ->
    {ok, Bin} = file:read_file(Cert),
    case filename:extension(Cert) of
        ".der" ->
            %% no decoding necessary
            [Bin];
        _ ->
            %% assume PEM otherwise
            Contents = public_key:pem_decode(Bin),
            [DER || {Type, DER, Cipher} <- Contents, Type == 'Certificate', Cipher == 'not_encrypted']
    end.

%% Custom SSL verification function for checking common names against the
%% whitelist.
verify_ssl(_, {bad_cert, _} = Reason, _) ->
    {fail, Reason};
verify_ssl(_, {extension, _}, UserState) ->
    {unknown, UserState};
verify_ssl(_, valid, UserState) ->
    %% this is the check for the CA cert
    {valid, UserState};
verify_ssl(_, valid_peer, undefined) ->
    lager:error("Unable to determine local certificate's common name"),
    {fail, bad_local_common_name};
verify_ssl(Cert, valid_peer, MyCommonName) ->

    CommonName = get_common_name(Cert),

    case string:to_lower(CommonName) == string:to_lower(MyCommonName) of
        true ->
            lager:error("Peer certificate's common name matches local "
                "certificate's common name"),
            {fail, duplicate_common_name};
        _ ->
            case validate_common_name(CommonName,
                    app_helper:get_env(riak_repl, peer_common_name_acl, "*")) of
                {true, Filter} ->
                    lager:info("SSL connection from ~s granted by ACL ~s",
                        [CommonName, Filter]),
                    {valid, MyCommonName};
                false ->
                    lager:error("SSL connection from ~s denied, no matching ACL",
                        [CommonName]),
                    {fail, no_acl}
            end
    end.

%% read in the configured 'certfile' and extract the common name from it
get_my_common_name(undefined) ->
    undefined;
get_my_common_name(CertFile) ->
    case catch(load_cert(CertFile)) of
        [CertBin|_] ->
            OTPCert = public_key:pkix_decode_cert(CertBin, otp),
            get_common_name(OTPCert);
        _ ->
            undefined
    end.

%% get the common name attribute out of an OTPCertificate record
get_common_name(OTPCert) ->
    %% You'd think there'd be an easier way than this giant mess, but I
    %% couldn't find one.
    {rdnSequence, Subject} = OTPCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject,
    [Att] = [Attribute#'AttributeTypeAndValue'.value || [Attribute] <- Subject,
        Attribute#'AttributeTypeAndValue'.type == ?'id-at-commonName'],
    case Att of
        {printableString, Str} -> Str;
        {utf8String, Bin} -> binary_to_list(Bin)
    end.

%% Validate common name matches one of the configured filters. Filters can
%% have at most one '*' wildcard in the leftmost component of the hostname.
validate_common_name(_, []) ->
    false;
validate_common_name(_, "*") ->
    {true, "*"};
validate_common_name(CN, [Filter|Filters]) ->
    T1 = string:tokens(string:to_lower(CN), "."),
    T2 = string:tokens(string:to_lower(Filter), "."),
    case length(T1) == length(T2) of
        false ->
            validate_common_name(CN, Filters);
        _ ->
            case hd(T2) of
                "*" ->
                    case tl(T1) == tl(T2) of
                        true ->
                            {true, Filter};
                        _ ->
                            validate_common_name(CN, Filters)
                    end;
                _ ->
                    case T1 == T2 of
                        true ->
                            {true, Filter};
                        _ ->
                            validate_common_name(CN, Filters)
                    end
            end
    end.

%% Choose the common strategy closest to the start of both side's list of
%% preferences. We can't use a straight list comprehension here because for
%% the inputs [a, b, c] and [c, b, a] we want the output to be 'b'. If a LC
%% like `[CS || CS <- ClientStrats, SS <- ServerStrats, CS == SS]' was used,
%% you'd get 'a' or 'c', depending on which list was first.
%%
%% Instead, we assign a 'weight' to each strategy, which is calculated as two
%% to the power of the index in the list, so you get something like this:
%% `[{a,2}, {b,4}, {c,8}]' and `[{c,2}, {b,4}, {a, 8}]'. When we run them
%% through the list comprehension we get: `[{a,10},{b,8},{c,10}]'. The entry
%% with the *lowest* weight is the one that is the common element closest to
%% the beginning in both lists, and thus the one we want. A power of two is
%% used so that there is always one clear winner, simple index weighting will
%% often give the same score to several entries.
choose_strategy(ServerStrats, ClientStrats) ->
    %% find the first common strategy in both lists
    CalcPref = fun(E, Acc) ->
            Index = length(Acc) + 1,
            Preference = math:pow(2, Index),
            [{E, Preference}|Acc]
    end,
    LocalPref = lists:foldl(CalcPref, [], ServerStrats),
    RemotePref = lists:foldl(CalcPref, [], ClientStrats),
    %% sum the common strategy preferences together
    TotalPref = [{S, Pref1+Pref2} || {S, Pref1} <- LocalPref, {RS, Pref2} <- RemotePref, S == RS],
    case TotalPref of
        [] ->
            %% no common strategies, force legacy
            ?LEGACY_STRATEGY;
        _ ->
            %% sort and return the first one
            element(1, hd(lists:keysort(2, TotalPref)))
    end.

strategy_module(Strategy, server) ->
    list_to_atom(lists:flatten(["riak_repl_", atom_to_list(Strategy),
                "_server"]));
strategy_module(Strategy, client) ->
    list_to_atom(lists:flatten(["riak_repl_", atom_to_list(Strategy),
                "_client"])).

%% set some common socket options, based on appenv
configure_socket(Transport, Socket) ->
    RB = case app_helper:get_env(riak_repl, recbuf) of
        RecBuf when is_integer(RecBuf), RecBuf > 0 ->
            [{recbuf, RecBuf}];
        _ ->
            []
    end,
    SB = case app_helper:get_env(riak_repl, sndbuf) of
        SndBuf when is_integer(SndBuf), SndBuf > 0 ->
            [{sndbuf, SndBuf}];
        _ ->
            []
    end,

    SockOpts = RB ++ SB,
    case SockOpts of
        [] ->
            ok;
        _ ->
            Transport:setopts(Socket, SockOpts)
    end.

%% send a start_fullsync to the calling process when it is time for fullsync
schedule_fullsync() ->
    schedule_fullsync(self()).

%% Skip lists + tuples for 1.2 repl, it's only a BNW feature
schedule_fullsync(Pid) ->
    case application:get_env(riak_repl, fullsync_interval) of
        {ok, disabled} ->
            ok;
        {ok, [{_,_} | _]} ->
            ok;
        {ok, Tuple} when is_tuple(Tuple) ->
            ok;
        {ok, FullsyncIvalMins} ->
            FullsyncIval = timer:minutes(FullsyncIvalMins),
            erlang:send_after(FullsyncIval, Pid, start_fullsync),
            ok
    end.

start_fullsync_timer(Pid, FullsyncIvalMins, Cluster) ->
    FullsyncIval = timer:minutes(FullsyncIvalMins),
    lager:info("Fullsync for ~p scheduled in ~p minutes",
               [Cluster, FullsyncIvalMins]),
    spawn(fun() ->
                timer:sleep(FullsyncIval),
                gen_server:cast(Pid, start_fullsync)
        end).

%% send a start_fullsync to the calling process for a given cluster
%% when it is time for fullsync
schedule_cluster_fullsync(Cluster) ->
    schedule_cluster_fullsync(Cluster, self()).

schedule_cluster_fullsync(Cluster, Pid) ->
    case application:get_env(riak_repl, fullsync_interval) of
        {ok, disabled} ->
            ok;
        {ok, [{_,_} | _] = List} ->
            case proplists:lookup(Cluster, List) of
                none -> ok;
                {_, FullsyncIvalMins} ->
                    start_fullsync_timer(Pid, FullsyncIvalMins, Cluster),
                    ok
            end;
        {ok, {Cluster, FullsyncIvalMins}} ->
            start_fullsync_timer(Pid, FullsyncIvalMins, Cluster),
            ok;
        {ok, FullsyncIvalMins} when not is_tuple(FullsyncIvalMins) ->
            %% this will affect ALL clusters that have fullsync enabled
            start_fullsync_timer(Pid, FullsyncIvalMins, Cluster),
            ok;
        _ ->
            ok
    end.

%% Work out the elapsed time in seconds, rounded to centiseconds.
elapsed_secs(Then) ->
    CentiSecs = timer:now_diff(os:timestamp(), Then) div 10000,
    CentiSecs / 100.0.

shuffle_partitions(Partitions, Seed) ->
    lager:info("Shuffling partition list using seed ~p", [Seed]),
    _ = rand:seed(exrop, Seed),
    [Partition || {Partition, _} <-
        lists:keysort(2, [{Key, rand:uniform()} || Key <- Partitions])].

%% Parse the version into major, minor, micro digits, ignoring any release
%% candidate suffix
parse_vsn(Str) ->
    Toks = string:tokens(Str, "."),
    Vsns = [begin
                {I,_R} = string:to_integer(T),
                I
            end || T <- Toks],
    list_to_tuple(Vsns).

%% doc Check app.config to see if repl proxy_get is enabled
%% Defaults to false.
proxy_get_active() ->
    case application:get_env(riak_repl, proxy_get) of
        {ok, enabled} -> true;
        _ -> false
    end.


log_dropped_realtime_obj(Obj) ->
    DroppedKey = riak_object:key(Obj),
    DroppedBucket = riak_object:bucket(Obj),
    lager:info("REPL dropped object: ~p ~p",
               [ DroppedBucket, DroppedKey]).

dropped_realtime_hook(Obj) ->
    Hook = app_helper:get_env(riak_repl, dropped_hook),
    case Hook of
        {Mod, Fun} ->
                Mod:Fun(Obj);
        _ -> pass
    end.

%% generate a unique ID for a socket to log stats against
generate_socket_tag(Prefix, Transport, Socket) ->
    {ok, {{O1, O2, O3, O4}, PeerPort}} = Transport:peername(Socket),
    {ok, {_Address, Portnum}} = Transport:sockname(Socket),
    lists:flatten(io_lib:format("~s_~p -> ~p.~p.~p.~p:~p",[
                Prefix,
                Portnum,
                O1, O2, O3, O4,
                PeerPort])).

remove_unwanted_stats([]) ->
  [];
remove_unwanted_stats(Stats) ->
    UnwantedProps = [sndbuf, recbuf, buffer, active,
                     type, send_max, send_avg, snd_cnt],
    lists:foldl(fun(K, Acc) -> proplists:delete(K, Acc) end, Stats, UnwantedProps).

source_socket_stats() ->
    AllStats = riak_core_tcp_mon:status(),
    [ remove_unwanted_stats(SocketStats) ||
        SocketStats <- AllStats,
        proplists:is_defined(tag, SocketStats),
        {repl_rt, source, _} <- [proplists:get_value(tag, SocketStats)] ].

sink_socket_stats() ->
    %% It doesn't seem like it's possible to pass in "source" below as a
    %% param
    AllStats = riak_core_tcp_mon:status(),
    [ remove_unwanted_stats(SocketStats) ||
        SocketStats <- AllStats,
        proplists:is_defined(tag, SocketStats),
        {repl_rt, sink, _} <- [proplists:get_value(tag, SocketStats)] ].

%% get other riak_apps across the cluster
get_peer_repl_nodes() ->
     [Node || Node <- riak_core_node_watcher:nodes(riak_repl),
            Node =/= node()].

%% get bucket hooks for current repl mode
%% This allows V1.2 and BNW to coexist.
get_hooks_for_modes() ->
    Modes = riak_repl_console:get_modes(),
    [ proplists:get_value(K,?REPL_MODES)
     || K <- Modes, proplists:is_defined(K,?REPL_MODES)].

mode_12_enabled(ReplModes) ->
    lists:member(mode_repl12, ReplModes).
mode_13_enabled(ReplModes) ->
    lists:member(mode_repl13, ReplModes).

format_ip_and_port(Ip, Port) when is_list(Ip) ->
    lists:flatten(io_lib:format("~s:~p",[Ip,Port]));
format_ip_and_port(Ip, Port) when is_tuple(Ip) ->
    lists:flatten(io_lib:format("~s:~p",[inet_parse:ntoa(Ip),
                                         Port])).

safe_pid_to_list(Pid) when is_pid(Pid) ->
    erlang:pid_to_list(Pid);
safe_pid_to_list(NotAPid) ->
    NotAPid.

safe_get_msg_q_len(Pid) when is_pid(Pid) ->
    {message_queue_len, Len} = erlang:process_info(Pid, message_queue_len),
    Len;
safe_get_msg_q_len(Other) ->
    Other.

peername(Socket, Transport) ->
    case Transport:peername(Socket) of
        {ok, {Ip, Port}} ->
            format_ip_and_port(Ip, Port);
        {error, Reason} ->
            %% just return a string so JSON doesn't blow up
            lists:flatten(io_lib:format("error:~p", [Reason]))
    end.

sockname(Socket, Transport) ->
    case Transport:sockname(Socket) of
        {ok, {Ip, Port}} ->
            format_ip_and_port(Ip, Port);
        {error, Reason} ->
            %% just return a string so JSON doesn't blow up
            lists:flatten(io_lib:format("error:~p", [Reason]))
    end.

make_pg_proxy_name(Remote) ->
    list_to_atom("pg_proxy_" ++ Remote).

make_pg_name(Remote) ->
    list_to_atom("pg_requester_" ++ Remote).

% everything from version 1.5 and up should use the new binary objects
deduce_wire_version_from_proto({_Proto, {CommonMajor, _CMinor}, {CommonMajor, _HMinor}}) when CommonMajor > 2 ->
    w2;
deduce_wire_version_from_proto({_Proto, {CommonMajor, _CMinor}, {CommonMajor, _HMinor}}) when CommonMajor > 1 ->
    w1;
deduce_wire_version_from_proto({_Proto, {_CommonMajor, CMinor}, {_CommonMajor, HMinor}}) when CMinor >= 5 andalso HMinor >= 5 ->
    w1;
% legacy wire binary packing.
deduce_wire_version_from_proto({_Proto, _Client, _Host}) ->
    w0.

%% Typically, Cmd :: fs_diff_obj | diff_obj
encode_obj_msg(V, {Cmd, RObj}) when is_binary(RObj) ->
    encode_obj_msg(V, {Cmd, RObj}, undefined);
encode_obj_msg(V, {Cmd, RObj}) ->
    encode_obj_msg(V, {Cmd, RObj}, riak_object:type(RObj)).

encode_obj_msg(V, {Cmd, RObj}, undefined) ->
    term_to_binary({Cmd, encode_obj(V, RObj)});
encode_obj_msg(V, {Cmd, RObj}, T) ->
    BTHash = case riak_repl_bucket_type_util:property_hash(T) of
                 undefined ->
                     0;
                 Hash ->
                     Hash
             end,
    term_to_binary({Cmd, {BTHash, encode_obj(V, RObj)}}).

%% A wrapper around to_wire which leaves the object unencoded when using the w0 wire protocol.
encode_obj(w0, RObj) ->
    RObj;
encode_obj(W, RObj) ->
    to_wire(W, RObj).

decode_bin_obj({BTHash, BinObj}) -> {BTHash, from_wire(BinObj)};
decode_bin_obj(BinObj) -> from_wire(BinObj).

%% @doc Create binary wire formatted replication blob for riak 2.0+, complete with
%%      possible type, bucket and key for reconstruction on the other end. BinObj should be
%%      in the new format as obtained from riak_object:to_binary(v1, RObj).
new_w2({T, B}, K, BinObj) when is_binary(B), is_binary(T), is_binary(K), is_binary(BinObj) ->
    KLen = byte_size(K),
    BLen = byte_size(B),
    TLen = byte_size(T),
    <<?MAGIC:8/integer, ?W2_VER:8/integer,
      TLen:32/integer, T:TLen/binary,
      BLen:32/integer, B:BLen/binary,
      KLen:32/integer, K:KLen/binary, BinObj/binary>>.

%% @doc Create a new binary wire formatted replication blob, complete with
%%      bucket and key for reconstruction on the other end. BinObj should be
%%      in the new format as obtained from riak_object:to_binary(v1, RObj).
new_w1(B, K, BinObj) when is_binary(B), is_binary(K), is_binary(BinObj) ->
   KLen = byte_size(K),
   BLen = byte_size(B),
   <<?MAGIC:8/integer, ?W1_VER:8/integer,
      BLen:32/integer, B:BLen/binary,
      KLen:32/integer, K:KLen/binary, BinObj/binary>>.

%% @doc Return the wire format version of the given wire blob
wire_version(<<131, _Rest/binary>>) ->
    w0;
wire_version(<<?MAGIC:8/integer, ?W1_VER:8/integer, _Rest/binary>>) ->
    w1;
wire_version(<<?MAGIC:8/integer, ?W2_VER:8/integer, _Rest/binary>>) ->
    w2;
wire_version(<<?MAGIC:8/integer, N:8/integer, _Rest/binary>>) ->
    list_to_atom(lists:flatten(io_lib:format("w~p", [N])));
wire_version(_Other) ->
    plain.

%% @doc Convert a plain or binary riak object to repl wire format.
%%      Bucket and Key will only be added if the new riak_object
%%      binary format is supplied (because it doesn't contain them).
to_wire(w0, Objects) when is_list(Objects) ->
    term_to_binary(Objects);
to_wire(w0, Object) ->
    term_to_binary(Object);
to_wire(w1, Objects) when is_list(Objects) ->
    BObjs = [to_wire(w1,O) || O <- Objects],
    term_to_binary(BObjs);
to_wire(w1, Object) when not is_binary(Object) ->
    B = riak_object:bucket(Object),
    K = riak_object:key(Object),
    to_wire(w1, B, K, Object);
to_wire(w2, Objects) when is_list(Objects) ->
    BObjs = [to_wire(w2,O) || O <- Objects],
    term_to_binary(BObjs);
to_wire(w2, Object) when not is_binary(Object) ->
    B = riak_object:bucket(Object),
    K = riak_object:key(Object),
    to_wire(w2, B, K, Object).

%% When the wire format is known and objects are packed in a list of binaries
from_wire(w0, BinObjList) ->
      binary_to_term(BinObjList);
from_wire(w1, BinObjList) ->
    BinObjs = binary_to_term(BinObjList),
    [from_wire(BObj) || BObj <- BinObjs];
from_wire(w2, BinObjList) ->
    BinObjs = binary_to_term(BinObjList),
    [from_wire(BObj) || BObj <- BinObjs].

%% @doc Convert from wire format to non-binary riak_object form
from_wire(<<131, _Rest/binary>>=BinObjTerm) ->
    binary_to_term(BinObjTerm);
%% @doc Convert from wire version w2, which has bucket type information
from_wire(<<?MAGIC:8/integer, ?W2_VER:8/integer,
            TLen:32/integer, T:TLen/binary,
            BLen:32/integer, B:BLen/binary,
            KLen:32/integer, K:KLen/binary, BinObj/binary>>) ->
    case T of
        <<>> ->
            riak_object:from_binary(B, K, BinObj);
        _ ->
            riak_object:from_binary({T, B}, K, BinObj)
    end;
from_wire(<<?MAGIC:8/integer, ?W1_VER:8/integer,
            BLen:32/integer, B:BLen/binary,
            KLen:32/integer, K:KLen/binary, BinObj/binary>>) ->
    riak_object:from_binary(B, K, BinObj);
from_wire(X) when is_binary(X) ->
    lager:error("An unknown replicaion wire format has been detected: ~p", [X]),
    {error, unknown_wire_format};
from_wire(RObj) ->
    RObj.

to_wire(w0, _B, _K, <<131,_/binary>>=Bin) ->
    Bin;
to_wire(w0, _B, _K, RObj) when not is_binary(RObj) ->
    to_wire(w0, RObj);
to_wire(w1, B, K, <<131,_/binary>>=Bin) ->
    %% no need to wrap a full old object. just use w0 format
    to_wire(w0, B, K, Bin);
to_wire(w1, B, K, <<_/binary>>=Bin) ->
    new_w1(B, K, Bin);
to_wire(w1, B, K, RObj) ->
    new_w1(B, K, riak_object:to_binary(v1, RObj));
to_wire(w2, {T,B}, K, RObj) ->
    new_w2({T,B}, K, riak_object:to_binary(v1, RObj));
to_wire(w2, B, K, RObj) ->
    new_w2({<<>>, B}, K, riak_object:to_binary(v1, RObj));
to_wire(_W, _B, _K, _RObj) ->
    {error, unsupported_wire_version}.

to_wire(Obj) ->
    to_wire(w0, unused, unused, Obj).


%% @doc BinObjs are in new riak binary object format. If the remote sink
%%      is storing older non-binary objects, then we need to downconvert
%%      the objects before sending. V is the format expected by the sink.
maybe_downconvert_binary_objs(BinObjs, SinkVer) ->
    case SinkVer of
        w1 ->
            %% great! nothing to do.
            BinObjs;
        w0 ->
            %% old sink. downconvert
            %% use a fully qualified call to make mecking of from_wire possible
            Objs = ?MODULE:from_wire(w1, BinObjs),
            to_wire(w0, Objs)
    end.

%% return the wire format supported by the peer node: w0 | w1
peer_wire_format(Peer) ->
    case riak_core_util:safe_rpc(Peer, riak_core_capability, get, [{riak_kv, object_format}]) of
        {unknown_capability,{riak_kv,object_format}} ->
            w0;
        v1 ->
            w1;
        _Other ->
            %% failed RPC call? Assume lowest format
            w0
    end.

%% @private
%% @doc Unless skipping the background manager, try to acquire the per-vnode lock.
%%      Sets our task meta-data in the lock as 'repl_fullsync', which is useful for
%%      seeing what's holding the lock via @link riak_core_background_mgr:ps/0.
-spec maybe_get_vnode_lock(SrcPartition::integer()) -> ok | {error, Reason::term()}.
maybe_get_vnode_lock(SrcPartition) ->
    case riak_core_bg_manager:use_bg_mgr(riak_repl, fullsync_use_background_manager) of
        true  ->
            Lock = {vnode_lock, SrcPartition},
            case riak_core_bg_manager:get_lock(Lock, self(), [{task, repl_fullsync}]) of
                {ok, _Ref} ->
                    ok;
                max_concurrency ->
                    lager:debug("max_concurrency for lock ~p: info ~p locks ~p",
                                [SrcPartition,
                                 riak_core_bg_manager:lock_info(Lock),
                                 riak_core_bg_manager:all_locks(Lock)]),
                    Reason = {max_concurrency, Lock},
                    {error, Reason}
            end;
        false ->
            ok
    end.

%% Some eunit tests
-ifdef(TEST).

do_wire_old_format_test() ->
    %% old wire format is just term_to_binary of the riak object,
    %% so encoded should be the same sa the binary object.
    Bucket = <<"0b:foo">>,
    Key = <<"key">>,
    RObj = riak_object:new(Bucket, Key, <<"val">>),
    BObj = term_to_binary(RObj),
    %% cover to_wire(... binary-Obj)
    EncodedBinObj = to_wire(w0, Bucket, Key, BObj),
    ?assert(EncodedBinObj == BObj),
    %% cover to_wire(... non-binary-Obj)
    EncodedObj = to_wire(w0, Bucket, Key, RObj),
    ?assert(EncodedObj == BObj),
    DecodedBinObj = from_wire(EncodedBinObj),
    ?assert(DecodedBinObj == RObj),
    DecodedObj = from_wire(EncodedObj),
    ?assert(DecodedObj == RObj).

do_wire_new_format_binary_test() ->
    Bucket = <<"0b:foo">>,
    Key = <<"key">>,
    RObj = riak_object:new(Bucket, Key, <<"val">>),
    %% encode new binary form of riak object
    BObj = riak_object:to_binary(v1, RObj),
    Encoded = to_wire(w1, Bucket, Key, BObj),
    Decoded = from_wire(Encoded),
    ?assert(Decoded == RObj).

do_wire_new_format_test() ->
    Bucket = <<"0b:foo">>,
    Key = <<"key">>,
    RObj = riak_object:new(Bucket, Key, <<"val">>),
    %% encode record version of riak object
    Encoded = to_wire(w1, Bucket, Key, RObj),
    Decoded = from_wire(Encoded),
    ?assert(Decoded == RObj).

do_wire_new_format_old_object_test() ->
    Bucket = <<"0b:foo">>,
    Key = <<"key">>,
    RObj = riak_object:new(Bucket, Key, <<"val">>),
    %% encode old binary form of riak object
    BObj = riak_object:to_binary(v0, RObj),
    ?assert(BObj == term_to_binary(RObj)),
    Encoded = to_wire(w1, Bucket, Key, BObj),
    Decoded = from_wire(Encoded),
    ?assert(Decoded == RObj).

do_wire_unknown_format_test() ->
    Bucket = <<"0b:foo">>,
    Key = <<"key">>,
    RObj = riak_object:new(Bucket, Key, <<"val">>),
    Encoded = to_wire(w9, Bucket, Key, term_to_binary(RObj)),
    ?assertEqual({error, unsupported_wire_version}, Encoded),
    Decoded = from_wire(<<1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>),
    ?assertEqual({error, unknown_wire_format}, Decoded).

do_wire_list_w0_test() ->
    Bucket = <<"0b:foo">>,
    Key = <<"key">>,
    RObj = riak_object:new(Bucket, Key, <<"val">>),
    Objs = [RObj, RObj, RObj],
    Encoded = to_wire(w0, Objs),
    Decoded = from_wire(w0, Encoded),
    ?assert(Decoded == Objs).

do_wire_list_w1_test() ->
    Bucket = <<"0b:foo">>,
    Key = <<"key">>,
    RObj = riak_object:new(Bucket, Key, <<"val">>),
    Objs = [RObj, RObj, RObj],
    Encoded = to_wire(w1, Objs),
    Decoded = from_wire(w1, Encoded),
    ?assert(Decoded == Objs).

do_non_loopback_interfaces_test() ->
    Addrs = [{"lo",
                [{flags,[up,loopback,running]},
                {hwaddr,[0,0,0,0,0,0]},
                {addr,{127,0,0,1}},
                {netmask,{255,0,0,0}},
                {addr,{0,0,0,0,0,0,0,1}},
                {netmask,{65535,65535,65535,65535,65535,65535,65535,65535}}]},
            {"lo0",
                [{flags,[up,loopback,running]},
                {hwaddr,[0,0,0,0,0,0]},
                {addr,{127,0,0,1}},
                {netmask,{255,0,0,0}},
                {addr,{0,0,0,0,0,0,0,1}},
                {netmask,{65535,65535,65535,65535,65535,65535,65535,65535}}]},
            {"eth0",
                [{flags,[up,broadcast,running,multicast]},
                {addr, {10, 0, 0, 99}},
                {netmask, {255, 0, 0, 0}}]}],
    Res = non_loopback_interfaces(Addrs),
    ?assertEqual(false, proplists:is_defined("lo", Res)),
    ?assertEqual(false, proplists:is_defined("lo0", Res)),
    ?assertEqual(true, proplists:is_defined("eth0", Res)).

do_wire_list_w1_bucket_type_test() ->
    Type = <<"type">>,
    Bucket = <<"0b:foo">>,
    Key = <<"key">>,
    RObj = riak_object:new({Type, Bucket}, Key, <<"val">>),
    Objs = [RObj],
    Encoded = to_wire(w2, Objs),
    Decoded = from_wire(w2, Encoded),
    ?assertEqual(Decoded, Objs).

-endif.
