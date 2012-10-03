%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(yz_doc).
-compile(export_all).
-include("yokozuna.hrl").

%% @doc Functionality for working with Yokozuna documents.

%%%===================================================================
%%% API
%%%===================================================================

add_to_doc({doc, Fields}, Field) ->
    {doc, [Field|Fields]}.

-spec doc_id(riak_object:riak_object(), binary()) -> binary().
doc_id(O, Partition) ->
    <<(riak_object:key(O))/binary,"_",Partition/binary>>.

%% @doc Given an object generate the doc to be indexed by Solr.
-spec make_doc(riak_object:riak_object(), binary(), binary()) -> doc().
make_doc(O, FPN, Partition) ->
    ExtractedFields = extract_fields(O),
    Fields = [{id, doc_id(O, Partition)},
              {?YZ_ED_FIELD, gen_ed(O, Partition)},
              {?YZ_FPN_FIELD, FPN},
              {?YZ_NODE_FIELD, ?ATOM_TO_BIN(node())},
              {?YZ_PN_FIELD, Partition},
              {?YZ_RK_FIELD, riak_key(O)}],
    {doc, lists:append([ExtractedFields, Fields])}.

-spec extract_fields(obj()) ->  fields() | {error, any()}.
extract_fields(O) ->
    case yz_kv:is_tombstone(O) of
        false ->
            CT = yz_kv:get_obj_ct(O),
            Value = hd(riak_object:get_values(O)),
            ExtractorDef = yz_extractor:get_def(CT, [check_default]),
            case yz_extractor:run(Value, ExtractorDef) of
                {error, Reason} ->
                    ?ERROR("failed to index with reason ~s~nValue: ~s", [Reason, Value]),
                    {error, Reason};
                Fields ->
                    Fields
            end;
        true ->
            []
    end.

%%%===================================================================
%%% Private
%%%===================================================================

%% TODO: Just pass metadata in?
%%
%% TODO: I don't like having X-Riak-Last-Modified in here.  Add
%%       function to riak_object.
doc_ts(O) ->
    MD = riak_object:get_metadata(O),
    dict:fetch(<<"X-Riak-Last-Modified">>, MD).

doc_vclock(O) ->
    riak_object:vclock(O).

gen_ts() ->
    {{Year, Month, Day},
     {Hour, Min, Sec}} = calendar:now_to_universal_time(erlang:now()),
    list_to_binary(io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0B",
                                 [Year,Month,Day,Hour,Min,Sec])).

%% NOTE: All of this data needs to be in one field to efficiently
%%       iterate.  Otherwise the doc would have to be fetched for each
%%       entry.
gen_ed(O, Partition) ->
    TS = gen_ts(),
    RiakBucket = riak_bucket(O),
    RiakKey = riak_key(O),
    Hash = hash_object(O),
    <<TS/binary," ",Partition/binary," ",RiakBucket/binary," ",RiakKey/binary," ",Hash/binary>>.

%% TODO: do this in KV vnode and pass to hook
hash_object(O) ->
    Vclock = riak_object:vclock(O),
    O2 = riak_object:set_vclock(O, lists:sort(Vclock)),
    Hash = erlang:phash2(term_to_binary(O2)),
    term_to_binary(Hash).

riak_bucket(O) ->
    riak_object:bucket(O).

riak_key(O) ->
    riak_object:key(O).

value(O) ->
    riak_object:get_value(O).
