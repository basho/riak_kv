-module(riak_api_test_util).
-compile([export_all, nowarn_export_all]).

wait_for_application_shutdown(App) ->
    case lists:keymember(App, 1, application:which_applications()) of
        true ->
            timer:sleep(250),
            wait_for_application_shutdown(App);
        false ->
            ok
    end.

%% The following three functions build a list of dependent
%% applications. They will not handle circular or mutually-dependent
%% applications.
dep_apps(App) ->
    application:load(App),
    {ok, Apps} = application:get_key(App, applications),
    Apps.

all_deps(App, Deps) ->
    [[ all_deps(Dep, [App|Deps]) || Dep <- dep_apps(App),
                                    not lists:member(Dep, Deps)], App].

resolve_deps(App) ->
    DepList = all_deps(App, []),
    {AppOrder, _} = lists:foldl(fun(A,{List,Set}) ->
                                        case sets:is_element(A, Set) of
                                            true ->
                                                {List, Set};
                                            false ->
                                                {List ++ [A], sets:add_element(A, Set)}
                                        end
                                end,
                                {[], sets:new()},
                                lists:flatten(DepList)),
    AppOrder.

is_otp_base_app(kernel) -> true;
is_otp_base_app(stdlib) -> true;
is_otp_base_app(_) -> false.
