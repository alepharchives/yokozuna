-module(yz_exchange_fsm).
-behaviour(gen_fsm).
-include("yokozuna.hrl").
-compile(export_all).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-record(state, {index :: p(),
                index_n :: {p(),n()},
                yz_tree :: tree(),
                kv_tree :: tree(),
                built :: integer(),
                from :: any()}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Initialize the exchange FSM to exchange between Yokozuna and
%%      KV for the `Preflist' replicas on `Index'.
-spec start(p(), {p(),n()}, tree(), tree(), pid()) ->
                   {ok, pid()} | {error, any()}.
start(Index, Preflist, YZTree, KVTree, Manager) ->
    gen_fsm:start(?MODULE, [Index, Preflist, YZTree, KVTree, Manager], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% step1: have Index and Preflist for exchange - pass to fsm
%%
%% step2: try to aquie lock from yz mgr and kv mgr
%%
%% step3: get pid of yz tree from mgr and kv tree from my
%%
%% step4: aquire lock from yz tree and kv tree
%%
%% step5: start exchange of Preflist between yz and kv tree
init([Index, Preflist, YZTree, KVTree, Manager]) ->
    monitor(process, Manager),
    monitor(process, YZTree),
    monitor(process, KVTree),

    S = #state{index=Index,
               index_n=Preflist,
               yz_tree=YZTree,
               kv_tree=KVTree,
               built=0},
    gen_fsm:send_event(self(), start_exchange),
    lager:debug("Starting exchange between KV and Yokozuna: ~p", [Index]),
    {ok, prepare_exchange, S}.

prepare_exchange(start_exchange, S) ->
    YZTree = S#state.yz_tree,
    KVTree = S#state.kv_tree,

    case yz_entropy_mgr:get_lock(exchange) of
        ok ->
            case yz_index_hashtree:get_lock(YZTree, ?MODULE) of
                ok ->
                    case riak_kv_entropy_manager:get_lock(?MODULE) of
                        ok ->
                            case riak_kv_index_hashtree:get_lock(KVTree,
                                                                 ?MODULE) of
                                ok ->
                                    update_trees(start_exchange, S);
                                _ ->
                                    send_exchange_status(already_locked, S),
                                    {stop, normal, S}
                            end;
                        Error ->
                            send_exchange_status(Error, S),
                            {stop, normal, S}
                    end;
                _ ->
                    send_exchange_status(already_locked, S),
                    {stop, normal, S}
            end;
        Error ->
            send_exchange_status(Error, S),
            {stop, normal, S}
    end;

prepare_exchange(timeout, S) ->
    do_timeout(S).

update_trees(start_exchange, S=#state{yz_tree=YZTree,
                                      kv_tree=KVTree,
                                      index=Index,
                                      index_n=IndexN}) ->

    update_request(yz_index_hashtree, YZTree, Index, IndexN),
    update_request(riak_kv_index_hashtree, KVTree, Index, IndexN),
    {next_state, update_trees, S};

update_trees({not_responsible, Index, IndexN}, S) ->
    lager:debug("Index ~p does not cover preflist ~p", [Index, IndexN]),
    send_exchange_status({not_responsible, Index, IndexN}, S),
    {stop, normal, S};

update_trees({tree_built, _, _}, S) ->
    Built = S#state.built + 1,
    case Built of
        2 ->
            lager:debug("Moving to key exchange"),
            {next_state, key_exchange, S, 0};
        _ ->
            {next_state, update_trees, S#state{built=Built}}
    end.

key_exchange(timeout, S=#state{index=Index,
                               yz_tree=YZTree,
                               kv_tree=KVTree,
                               index_n=IndexN}) ->
    lager:debug("Starting key exchange for partition ~p preflist ~p",
                [Index, IndexN]),

    Remote = fun(get_bucket, {L, B}) ->
                     exchange_bucket_kv(KVTree, IndexN, L, B);
                (key_hashes, Segment) ->
                     exchange_segment_kv(KVTree, IndexN, Segment)
             end,

    {ok, RC} = riak:local_client(),
    AccFun = fun(KeyDiff, Acc) ->
                     lists:foldl(fun(Diff, Acc2) ->
                                         read_repair_keydiff(RC, Diff),
                                         case Acc2 of
                                             [] ->
                                                 [1];
                                             [Count] ->
                                                 [Count+1]
                                         end
                                 end, Acc, KeyDiff)
             end,

    case riak_kv_index_hashtree:compare(IndexN, Remote, AccFun, YZTree) of
        [] ->
            ok;
        [Count] ->
            lager:info("Repaired ~b keys during active anti-entropy exchange "
                       "of ~p", [Count, IndexN])
    end,
    {stop, normal, S}.

read_repair_keydiff(RC, {_, KeyBin}) ->
    BKey = {Bucket, Key} = binary_to_term(KeyBin),
    lager:debug("Anti-entropy forced read repair and re-index: ~p/~p", [Bucket, Key]),
    {ok, Obj} = RC:get(Bucket, Key),
    Ring = yz_misc:get_ring(transformed),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    Idx = riak_core_util:chash_key(BKey),
    N = proplists:get_value(n_val,BucketProps),
    Preflist = lists:sublist(riak_core_ring:preflist(Idx, Ring), N),
    lists:foreach(fun({Partition, Node}) ->
                          FakeState = fake_kv_vnode_state(Partition),
                          rpc:call(Node, yz_kv, index, [Obj, anti_entropy, FakeState])
                  end, Preflist),
    ok.

fake_kv_vnode_state(Partition) ->
    {state,Partition,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake,fake}.

exchange_bucket_kv(Tree, IndexN, Level, Bucket) ->
    riak_kv_index_hashtree:exchange_bucket(IndexN, Level, Bucket, Tree).

exchange_segment_kv(Tree, IndexN, Segment) ->
    riak_kv_index_hashtree:exchange_segment(IndexN, Segment, Tree).

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info({'DOWN', _, _, _, _}, _StateName, State) ->
    %% Either the entropy manager, local hashtree, or remote hashtree has
    %% exited. Stop exchange.
    {stop, normal, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

update_request(Module, Tree, Index, IndexN) ->
    as_event(fun() ->
                     case Module:update(IndexN, Tree) of
                         ok ->
                             {tree_built, Index, IndexN};
                         not_responsible ->
                             {not_responsible, Index, IndexN}
                     end
             end).

as_event(F) ->
    Self = self(),
    spawn_link(fun() ->
                       Result = F(),
                       gen_fsm:send_event(Self, Result)
               end),
    ok.

do_timeout(S=#state{index=Index, index_n=Preflist}) ->
    lager:info("Timeout during exchange of partition ~p for preflist ~p ",
               [Index, Preflist]),
    send_exchange_status({timeout, Index, Preflist}, S),
    {stop, normal, S}.

send_exchange_status(Status, #state{index=Index,
                                    index_n=IndexN}) ->
    yz_entropy_mgr:exchange_status(Index, IndexN, Status).
