-module(yz_index).
-include("yokozuna.hrl").
-compile(export_all).

-define(YZ_DEFAULT_DIR, filename:join(["data", "yz"])).

%% @doc This module contains functionaity for using and administrating
%%      indexes.  In this case an index is an instance of a Solr Core.

%%%===================================================================
%%% API
%%%===================================================================

%% TODO: Allow data dir to be changed
-spec create(string()) -> ok.
create(Name) ->
    IndexDir = index_dir(Name),
    ConfDir = filename:join([IndexDir, "conf"]),
    ConfFiles = filelib:wildcard(filename:join([?YZ_PRIV, "conf", "*"])),
    DataDir = filename:join([IndexDir, "data"]),
    JavaLibDir = java_lib_dir(),

    make_dirs([ConfDir, DataDir, JavaLibDir]),
    copy_files(ConfFiles, ConfDir),

    CoreProps = [
                 {name, Name},
                 {index_dir, IndexDir},
                 {cfg_file, ?YZ_CORE_CFG_FILE},
                 {java_lib_dir, JavaLibDir},
                 {schema_file, ?YZ_SCHEMA_FILE}
                ],
    ok = yz_solr:core(create, CoreProps).

%%%===================================================================
%%% Private
%%%===================================================================

copy_files([], _) ->
    ok;
copy_files([File|Rest], Dir) ->
    Basename = filename:basename(File),
    {ok, _} = file:copy(File, filename:join([Dir, Basename])),
    copy_files(Rest, Dir).

index_dir(Name) ->
    YZDir = app_helper:get_env(?YZ_APP_NAME, yz_dir, ?YZ_DEFAULT_DIR),
    filename:absname(filename:join([YZDir, Name])).

java_lib_dir() ->
    ?YZ_PRIV ++ "/java_lib".

make_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            ok;
        false ->
            ok = filelib:ensure_dir(Dir),
            ok = file:make_dir(Dir)
    end.

make_dirs([]) ->
    ok;
make_dirs([Dir|Rest]) ->
    ok = make_dir(Dir),
    make_dirs(Rest).

schema_file() ->
    ?YZ_PRIV ++ "/" ++ ?YZ_SCHEMA_FILE.

%% Old stuff relate to exception handling ideas I was playing with

%% create2(Name, _Schema) ->
%%     CfgDir = cfg_dir(Name),

%%     try
%%         ok = make_dir(CfgDir)
%%         %% ok = copy_config(CfgDir),
%%         %% ok = copy_schema(Schema, CfgDir)
%%     catch _:Reason ->
%%             %% If file calls don't throw then reason will be badmatch,
%%             %% if they throw something like {"cannot make dir",
%%             %% Reason} then it's more clear.
%%             ?ERROR("Problem creating index ~p ~p~n", [Name, Reason]),
%%             %% TODO: this will return `ok` I should actually be
%%             %% throwing here too and building up the exception
%%             %% chain...the key is determining the boundary of when to
%%             %% stop...perhaps the function at stop of stack for a
%%             %% process?
%%     end.

%% create_dir(Name) ->
%%     CfgDir = cfg_dir(Name),
%%     case filelib:ensure_dir(CfgDir) of
%%         ok ->
%%             case file:make_dir(CfgDir) of
%%                 ok ->
%%                     {ok, CfgDir};
%%                 Err ->
%%                     Err
%%             end;
%%         Err ->
%%             Err
%%     end.

%% make_dir(Dir) ->
%%     try
%%         ok = filelib:ensure_dir(Dir),
%%         ok = file:make_dir(Dir)
%%     catch error:Reason ->
%%             throw({error_creating_dir, Dir, Reason})
%%     end.
