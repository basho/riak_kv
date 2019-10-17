%% @doc Some security helper functions for Riak API endpoints
-module(riak_api_web_security).

-export([is_authorized/1]).

%% @doc Check if the user is authorized
-spec is_authorized(any()) -> {true, any()} | false | insecure.
is_authorized(ReqData) ->
    case riak_core_security:is_enabled() of
        true ->
            Scheme = wrq:scheme(ReqData),
            case Scheme == https of
                true ->
                    case wrq:get_req_header("Authorization", ReqData) of
                        "Basic " ++ Base64 ->
                            UserPass = base64:decode_to_string(Base64),
                            [User, Pass] = [list_to_binary(X) || X <-
                                                                 string:tokens(UserPass, ":")],
                            {ok, Peer} = inet_parse:address(wrq:peer(ReqData)),
                            case riak_core_security:authenticate(User, Pass,
                                    [{ip, Peer}])
                                of
                                {ok, Sec} ->
                                    {true, Sec};
                                {error, _} ->
                                    false
                            end;
                        _ ->
                            false
                    end;
                false ->
                    %% security is enabled, but they're connecting over HTTP.
                    %% which means if they authed, the credentials would be in
                    %% plaintext
                    insecure
            end;
        false ->
            {true, undefined} %% no security context
    end.
