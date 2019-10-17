%% -------------------------------------------------------------------
%%
%% riak_api_ssl: configuration for SSL/TLS connections over PB and HTTP
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Configuration and validation routines for SSL/TLS connections
%% to clients.
-module(riak_api_ssl).

-export([options/0]).
-include_lib("public_key/include/public_key.hrl").

%% @doc Returns a list of common options for SSL/TLS connections.
-spec options() -> [ssl:ssl_option()].
options() ->
    CoreSSL = app_helper:get_env(riak_core, ssl),
    CACertFile = proplists:get_value(cacertfile, CoreSSL),
    CertFile = proplists:get_value(certfile, CoreSSL),
    KeyFile = proplists:get_value(keyfile, CoreSSL),
    Versions = app_helper:get_env(riak_api, tls_protocols, ['tlsv1.2']),
    HonorCipherOrder = app_helper:get_env(riak_api, honor_cipher_order, false),
    CheckCRL = app_helper:get_env(riak_api, check_crl, false),

    {Ciphers, _} = riak_core_ssl_util:parse_ciphers(riak_core_security:get_ciphers()),
    CACerts = riak_core_ssl_util:load_certs(CACertFile),

    [{certfile, CertFile},
     {keyfile, KeyFile},
     {cacerts, CACerts},
     {ciphers, Ciphers},
     {versions, Versions},
     %% force peer validation, even though
     %% we don't care if the peer doesn't
     %% send a certificate
     {verify, verify_peer},
     {server_name_indication, disable},
     {reuse_sessions, false} %% required!
    ] ++
    %% conditionally include the honor cipher order, don't pass it if it
    %% disabled because it will crash any
    %% OTP installs that lack the patch to
    %% implement honor_cipher_order
    [{honor_cipher_order, true} || HonorCipherOrder ] ++
    %% if we're validating CRLs, define a
    %% verify_fun for them.
    [{verify_fun, {fun validate_function/3, {CACerts, []}}} || CheckCRL ].



%% @doc Validator function for SSL negotiation.
%%
validate_function(Cert, valid_peer, State) ->
    lager:debug("validing peer ~p with ~p intermediate certs",
                [riak_core_ssl_util:get_common_name(Cert),
                 length(element(2, State))]),
    %% peer certificate validated, now check the CRL
    Res = (catch check_crl(Cert, State)),
    lager:debug("CRL validate result for ~p: ~p",
                [riak_core_ssl_util:get_common_name(Cert), Res]),
    {Res, State};
validate_function(Cert, valid, {TrustedCAs, IntermediateCerts}=State) ->
    case public_key:pkix_is_self_signed(Cert) of
        true ->
            %% this is a root cert, no CRL
            {valid, {TrustedCAs, [Cert|IntermediateCerts]}};
        false ->
            %% check is valid CA certificate, add to the list of
            %% intermediates
            Res = (catch check_crl(Cert, State)),
            lager:debug("CRL intermediate CA validate result for ~p: ~p",
                        [riak_core_ssl_util:get_common_name(Cert), Res]),
            {Res, {TrustedCAs, [Cert|IntermediateCerts]}}
    end;
validate_function(_Cert, {bad_cert, _} = Reason, _UserState) ->
    {fail, Reason};
validate_function(_Cert, {extension, _}, UserState) ->
    {unknown, UserState}.

%% @doc Given a certificate, find CRL distribution points for the given
%%      certificate, fetch, and attempt to validate each CRL through
%%      issuer_function/4.
%%
check_crl(Cert, State) ->
    %% pull the CRL distribution point(s) out of the certificate, if any
    case pubkey_cert:select_extension(?'id-ce-cRLDistributionPoints',
                                      pubkey_cert:extensions_list(Cert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.extensions)) of
        undefined ->
            lager:debug("no CRL distribution points for ~p",
                         [riak_core_ssl_util:get_common_name(Cert)]),
            %% fail; we can't validate if there's no CRL
            no_crl;
        CRLExtension ->
            CRLDistPoints = CRLExtension#'Extension'.extnValue,
            DPointsAndCRLs = lists:foldl(fun(Point, Acc) ->
                            %% try to read the CRL over http or from a
                            %% local file
                            case fetch_point(Point) of
                                not_available ->
                                    Acc;
                                Res ->
                                    [{Point, Res} | Acc]
                            end
                    end, [], CRLDistPoints),
            public_key:pkix_crls_validate(Cert,
                                          DPointsAndCRLs,
                                          [{issuer_fun,
                                            {fun issuer_function/4, State}}])
    end.

%% @doc Given a list of distribution points for CRLs, certificates and
%%      both trusted and intermediary certificates, attempt to build and
%%      authority chain back via build_chain to verify that it is valid.
%%
issuer_function(_DP, CRL, _Issuer, {TrustedCAs, IntermediateCerts}) ->
    %% XXX the 'Issuer' we get passed here is the AuthorityKeyIdentifier,
    %% which we are not currently smart enough to understand
    %% Read the CA certs out of the file
    Certs = [public_key:pkix_decode_cert(DER, otp) || DER <- TrustedCAs],
    %% get the real issuer out of the CRL
    Issuer = public_key:pkix_normalize_name(
            pubkey_cert_records:transform(
                CRL#'CertificateList'.tbsCertList#'TBSCertList'.issuer, decode)),
    %% assume certificates are ordered from root to tip
    case find_issuer(Issuer, IntermediateCerts ++ Certs) of
        undefined ->
            lager:debug("unable to find certificate matching CRL issuer ~p",
                        [Issuer]),
            error;
        IssuerCert ->
            case build_chain({public_key:pkix_encode('OTPCertificate',
                                                     IssuerCert,
                                                     otp),
                              IssuerCert}, IntermediateCerts, Certs, []) of
                undefined ->
                    error;
                {OTPCert, Path} ->
                    {ok, OTPCert, Path}
            end
    end.

%% @doc Attempt to build authority chain back using intermediary
%%      certificates, falling back on trusted certificates if the
%%      intermediary chain of certificates does not fully extend to the
%%      root.
%%
%%      Returns: {RootCA :: #OTPCertificate{}, Chain :: [der_encoded()]}
%%
build_chain({DER, Cert}, IntCerts, TrustedCerts, Acc) ->
    %% check if this cert is self-signed, if it is, we've reached the
    %% root of the chain
    Issuer = public_key:pkix_normalize_name(
            Cert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.issuer),
    Subject = public_key:pkix_normalize_name(
            Cert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject),
    case Issuer == Subject of
        true ->
            case find_issuer(Issuer, TrustedCerts) of
                undefined ->
                    undefined;
                TrustedCert ->
                    %% return the cert from the trusted list, to prevent
                    %% issuer spoofing
                    {TrustedCert,
                     [public_key:pkix_encode(
                                'OTPCertificate', TrustedCert, otp)|Acc]}
            end;
        false ->
            Match = lists:foldl(
                      fun(C, undefined) ->
                              S = public_key:pkix_normalize_name(C#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject),
                              %% compare the subject to the current issuer
                              case Issuer == S of
                                  true ->
                                      %% we've found our man
                                      {public_key:pkix_encode('OTPCertificate', C, otp), C};
                                  false ->
                                      undefined
                              end;
                         (_E, A) ->
                              %% already matched
                              A
                      end, undefined, IntCerts),
            case Match of
                undefined when IntCerts /= TrustedCerts ->
                    %% continue the chain by using the trusted CAs
                    lager:debug("Ran out of intermediate certs, switching to trusted certs~n"),
                    build_chain({DER, Cert}, TrustedCerts, TrustedCerts, Acc);
                undefined ->
                    lager:debug("Can't construct chain of trust beyond ~p",
                                [riak_core_ssl_util:get_common_name(Cert)]),
                    %% can't find the current cert's issuer
                    undefined;
                Match ->
                    build_chain(Match, IntCerts, TrustedCerts, [DER|Acc])
            end
    end.

%% @doc Given a certificate and a list of trusted or intermediary
%%      certificates, attempt to find a match in the list or bail with
%%      undefined.
find_issuer(Issuer, Certs) ->
    lists:foldl(
      fun(OTPCert, undefined) ->
              %% check if this certificate matches the issuer
              Normal = public_key:pkix_normalize_name(
                        OTPCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject),
              case Normal == Issuer of
                  true ->
                      OTPCert;
                  false ->
                      undefined
              end;
         (_E, Acc) ->
              %% already found a match
              Acc
      end, undefined, Certs).

%% @doc Find distribution points for a given CRL and then attempt to
%%      fetch the CRL from the first available.
fetch_point(#'DistributionPoint'{distributionPoint={fullName, Names}}) ->
    Decoded = [{NameType,
                pubkey_cert_records:transform(Name, decode)}
               || {NameType, Name} <- Names],
    fetch(Decoded).

%% @doc Given a list of locations to retrieve a CRL from, attempt to
%%      retrieve either from a file or http resource and bail as soon as
%%      it can be found.
%%
%%      Currently, only hand a armored PEM or DER encoded file, with
%%      defaulting to DER.
%%
fetch([]) ->
    not_available;
fetch([{uniformResourceIdentifier, "file://"++_File}|Rest]) ->
    lager:debug("fetching CRLs from file URIs is not supported"),
    fetch(Rest);
fetch([{uniformResourceIdentifier, "http"++_=URL}|Rest]) ->
    lager:debug("getting CRL from ~p~n", [URL]),
    _ = inets:start(),
    case httpc:request(get, {URL, []}, [], [{body_format, binary}]) of
        {ok, {_Status, _Headers, Body}} ->
            case Body of
                <<"-----BEGIN", _/binary>> ->
                    [{'CertificateList',
                      DER, _}=CertList] = public_key:pem_decode(Body),
                    {DER, public_key:pem_entry_decode(CertList)};
                _ ->
                    %% assume DER encoded
                    CertList = public_key:pem_entry_decode(
                            {'CertificateList', Body, not_encrypted}),
                    {Body, CertList}
            end;
        {error, _Reason} ->
            lager:debug("failed to get CRL ~p~n", [_Reason]),
            fetch(Rest)
    end;
fetch([Loc|Rest]) ->
    %% unsupported CRL location
    lager:debug("unable to fetch CRL from unsupported location ~p",
                [Loc]),
    fetch(Rest).
