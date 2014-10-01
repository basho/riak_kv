%% Escript that will update the line numbers for links to functions
%% in the markdown documentation. 
%%
%% It expects function references like the following in the text:
%%  - [module:function/arity][]
%%  - [name for reference][module:function/arity]
%%  - [module:function/arity](path/to/source.erl#L34)
%%
%% And writes a markdown reference section at the end of the file.
%% These references are not visible when markdown is rendered.
%%
%% [mod:fun1/arity]: ../src/mod_file.erl#L34
%% [mod:fun2/arity]: ../src/mod_file.erl#L56
%% ...
%%
%% To update the docs, run this after compiling the library:
%%
%% $ cd riak_ensemble/doc
%% $ escript update_line_numbers.erl ../ebin *.md

-module(update_line_numbers).
-mode(compile).
-export([main/1]).

main([Ebin | MdFiles]) ->
    [update_file(Ebin, MdFile) || MdFile <- MdFiles].

update_file(Ebin, MdFile) ->
    % Parse function references in file
    {ok, Text} = file:read_file(MdFile),
    WantedFuns = lists:usort(parse_funs(Text)),
    FunSet = sets:from_list(WantedFuns),
    WantedMods = lists:usort([Mod || {Mod, _, _} <- WantedFuns]),
    % Get line numbers from beam files into a dict
    FoundLines = lists:usort(lists:flatten([get_line_nums(beam_filename(Ebin, Mod), Mod, FunSet) || Mod <- WantedMods])),
    FoundSet = sets:from_list([MFA || {MFA, _Line} <- FoundLines]),
    MissingSet = sets:subtract(FunSet, FoundSet),
    sets:size(MissingSet) > 0 andalso report_missing(MissingSet),
    LineList = [fun_line_text(E) || E <- FoundLines],
    LineMap = dict:from_list(LineList),
    % Insert line number info into target files
    update_file(MdFile, Text, LineMap, LineList),
    io:format("Updated function line numbers in ~p\n", [MdFile]).

report_missing(MissingSet) ->
    MissingList = lists:sort(sets:to_list(MissingSet)),
    io:format(standard_error, "[WARNING] Could not find the following functions:\n", []),
    [io:format(standard_error, "\t~p:~p/~p\n", [M, F, A]) ||
     {M, F, A} <- MissingList].

parse_funs(Text) ->
    {ok, Re} = re:compile("\\[(\\w+):(\\w+)/(\\d+)\\]"),
    case re:run(Text, Re, [{capture, [1,2,3], binary}, global]) of
        nomatch ->
            [];
        {match, Matches} ->
            [{binary_to_atom(Mod, utf8),
              binary_to_atom(Fun, utf8),
              binary_to_integer(Arity)}
             || [Mod, Fun, Arity] <- lists:usort(Matches)]
    end.

beam_filename(Ebin, Mod) ->
    filename:join(Ebin, atom_to_list(Mod) ++ ".beam").

update_file(Filename, Text, LineMap, LineList) ->
    NewText = update_lines(Text, LineMap, LineList),
    file:write_file(Filename, NewText).

% Extracts all function line numbers from abstract code chunk in beam file.
get_line_nums(BeamFile, Mod, FunSet) ->
    {ok, {_, [{abstract_code, {_, Items}}]}} = beam_lib:chunks(BeamFile, [abstract_code]),
    [{{Mod, Fun, Arity}, Line} || {function, Line, Fun, Arity, _} <- Items,
     sets:is_element({Mod, Fun, Arity}, FunSet)].

fun_line_text({{Mod, Fun, Arity}, Line}) ->
    {mfa_bin(Mod, Fun, Arity), line_url(Mod, Line)}.

mfa_bin(M, F, A) ->
    BM = atom_to_binary(M, utf8),
    BF = atom_to_binary(F, utf8),
    BA = integer_to_binary(A),
    <<"[", BM/binary, ":", BF/binary,"/", BA/binary, "]">>.

line_url(M, L) ->
    list_to_binary("../src/" ++ atom_to_list(M) ++ ".erl#L" ++ integer_to_list(L)).

update_lines(Text, LineMap, LineList) ->
    Tokens = re:split(Text, "(\\[\\w+:\\w+/\\d+\\])\\s*(:.*$\n|\\([^)]*\\))", [multiline]),
    Lines1 = replace_line_nums(Tokens, LineMap, []),
    RefLines = lists:flatten([ [Fun, <<": ">>, Line, <<"\n">>] || {Fun, Line} <- LineList]),
    Lines1 ++ RefLines.

replace_line_nums([], _, Acc) ->
    lists:reverse(Acc);
replace_line_nums([_, <<":", _/binary>> | Rest], LineMap, Acc) ->
    replace_line_nums(Rest, LineMap, Acc);
replace_line_nums([MaybeFun, Bin = <<"(", _/binary>> | Rest], LineMap, Acc) ->
    case dict:find(MaybeFun, LineMap) of
        {ok, _Line} ->
            NewText = list_to_binary([MaybeFun, "[]"]),
            replace_line_nums(Rest, LineMap, [NewText|Acc]);
        _ ->
            replace_line_nums(Rest, LineMap, [Bin, MaybeFun | Acc])
    end;
replace_line_nums([Bin|Rest], LineMap, Acc) ->
    replace_line_nums(Rest, LineMap, [Bin|Acc]).

