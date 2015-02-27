%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(dike_cache).

-behaviour(paxos_server).

%%% dike initialization
-export([table_holder/4]).

%%% API dike internal group
-export([handle_call/3, handle_cast/2, init/1, init/2, export_state/1]).

-export([insert/4, lookup/2, dirty_lookup/2, delete/2, select/2, add_mili_seconds/2, clear/1]).
%%% bahaviour callbacks
-export([start_link/2]).

-include("dike_ring.hrl").

-define(PREFIX, "$dike_cache").
-define(DEFAULT_LIMIT, 100).
-define(NOW, erlang:now()).
%-define(NOW, os:timestamp()).


%% ===================================================================
%%% API for Dike Initialization
%% ===================================================================
start_link(Name, GroupCount) ->
    Table = table_name(Name),
    TTLTable = ttl_table_name(Name),
    {ok, TableHolder} = proc_lib:start_link(?MODULE, table_holder, [Name, GroupCount, Table, TTLTable]),
    register(Name, TableHolder),
    try init_dike(Name, GroupCount, TableHolder, Table, TTLTable) of
        _ ->
            {ok, TableHolder}
    catch
        Error:Reason ->
            lager:error("failed to init dike for ~s ~b catched: ~p", [Name, GroupCount, {Error, Reason, erlang:get_stacktrace()}]),
            exit(TableHolder, failed),
            {error, failed}
    end.

init_dike(Name, GroupCount, TableHolder, Table, TTLTable) ->
    [begin
         dike_master:add_group(VNode, ?MODULE),
         ok = dike_dispatcher:request(VNode, {init, TableHolder, Table, TTLTable, GroupCount})
     end || VNode <- generate_paxos_groups(Name, GroupCount)].

table_holder(Name, GroupCount, Table, TTLTable) ->
    Table = ets:new(Table, [ordered_set, public, named_table, {keypos, 1}, {read_concurrency, true}]),
    TTLTable = ets:new(TTLTable, [ordered_set, public, named_table, {keypos, 1}]),
    proc_lib:spawn_link(fun() -> checker(Name, GroupCount, 500, TTLTable) end),
    proc_lib:init_ack({ok, self()}),
    timer:sleep(infinity).

lookup(Ring, Key) ->
    case request(Ring, Key, {lookup, Key}) of
        [] ->
            undefined;
        [{Key, Value, CreationTime, Till}] ->
            {Key, Value, CreationTime, Till}
    end.

dirty_lookup(Name, Key) ->
    ets:lookup(table_name(Name), Key).

delete(Name, Key) ->
    request(Name, Key, {delete, Key}).

check_timed_out_key(Name, Key) ->
    Now = ?NOW,
    Result = request(Name, Key, {check_timed_out_key, Key, Now}),
    lager:debug("timed out key ~p is ~p", [Key, Result]),
    Result.

%-type option()  :: {direction, asc | dsc} | {limit, non_neg_integer()} | { offset, non_neg_integer()} | {key, any()} | {order_by, key | ttl}.
select(#ring{name = Name}, Options) ->
    KeyTable = table_name(Name),
    TTLTable = ttl_table_name(Name),
    FoldFunction = case proplists:get_value(direction, Options, asc) of
        asc -> foldl;
        dsc -> foldr
    end,
    Offset    = proplists:get_value(offset, Options, 0),
    Limit     = proplists:get_value(limit, Options, ?DEFAULT_LIMIT),
    %KeyAfter  = proplists:get_value(key, Options),
    Table = case proplists:get_value(order_by, Options, key) of
        key -> KeyTable;
        ttl -> TTLTable
    end,
    {Count, ResultList} = ets:FoldFunction(fun(_, {C, Acc}) when (C < Offset) orelse (C >= Limit + Offset) ->
                            {C + 1, Acc};
                        ({{_, Key}, _}, {C, Acc}) ->
                            case ets:lookup(KeyTable, Key) of
                                [V] -> {C + 1, [V | Acc]};
                                [] -> {C, Acc}
                            end;
                        (V, {C, Acc}) ->
                            {C + 1, [V | Acc]}
                     end, {0, []}, Table),
    {Count, lists:reverse(ResultList)}.

insert(Ring, Key, Value, TTL) ->
    Now = ?NOW,
    Till = add_mili_seconds(Now, TTL),
    request(Ring, Key, {insert, Key, Value, Now, Till}).

clear(#ring{name = Name, groupcount = GroupCount}) ->
    [begin
        ok = dike_dispatcher:request(VNode, clear)
     end || VNode <- generate_paxos_groups(Name, GroupCount)].

%% ===================================================================
%%% API Dike Internal Group
%% ===================================================================
request(_Ring = #ring{name = Name, groupcount = GroupCount}, Key, Request) ->
    Id = hash(Key, GroupCount),
    dike_dispatcher:request(group_name(Name, Id), Request).


%% ===================================================================
%%% Bahaviour Callbacks
%% ===================================================================
-record(state, {entries = [], table_name, ttl_table_name, group_id, group_count}).
init(Options) ->
    GroupName = proplists:get_value(paxos_group, Options),
    [TableNameBin, Group] = binary:split(atom_to_binary(GroupName, utf8), <<"-vnode-">>),
    {ok, #state{table_name = binary_to_atom(TableNameBin, utf8), group_id = binary_to_integer(Group)}}.

init(#state{table_name = TableName, entries = Entries} = State, _Options) ->
    ets:insert(TableName, Entries),
    {ok, State#state{entries = []}}.

export_state(#state{table_name = TableName, group_id = Id, group_count = Count} = State) ->
    Entries = accumulate_row(Id, Count, TableName),
    State#state{entries = Entries}.

handle_call({init, TableHolder, _, TTLTableName, GroupCount}, From, State) ->
    (node() == node(TableHolder)) andalso link(TableHolder),
    reply(From, ok, State#state{group_count = GroupCount, ttl_table_name = TTLTableName});

handle_call(clear, From, State = #state{table_name = TableName}) ->
    ets:delete_all_objects(TableName),
    reply(From, ok, State);

handle_call({lookup, Key}, From, State = #state{table_name = TableName}) ->
    Result = ets:lookup(TableName, Key),
    {reply, fun() -> paxos_server:reply(From, Result) end, State};

handle_call({delete, Key}, From, State = #state{table_name = TableName, ttl_table_name = TTLTableName}) ->
    case ets:lookup(TableName, Key) of
        [{Key, _, _, Till}] ->
            ets:delete(TableName, Key),
            ets:delete(TTLTableName, {Till, Key});
        [] ->
            ok
    end,
    reply(From, ok, State);

handle_call({check_timed_out_key, Key, Now}, From, State = #state{table_name = TableName, ttl_table_name = TTLTableName}) ->
    Result = case ets:lookup(TableName, Key) of
        [{Key, _, _, Till}] ->
            case timer:now_diff(Till, Now) < 0 of
                true ->
                    ets:delete(TableName, Key),
                    ets:delete(TTLTableName, {Till, Key}),
                    deleted;
                false ->
                    not_timed_out
            end;
        [] ->
            not_found
    end,
    reply(From, Result, State);

handle_call({insert, Key, Value, Now, Till}, From, State = #state{table_name = TableName, ttl_table_name = TTLTableName}) ->
    ets:insert(TableName, {Key, Value, Now, Till}),
    TimeEntry = case Till == infinity of
        true -> <<>>;
        false -> Till
    end,
    ets:insert(TTLTableName, {{TimeEntry, Key}, tstamp}),
    reply(From, ok, State).

handle_cast(_, State) ->
    {noreply, State}.


%% ===================================================================
%%% Internal functions
%% ===================================================================
batch_read(Now, TTLTable, ETSAction, Acc) ->
    case ETSAction() of
        '$end_of_table' -> Acc;
        {<<>>, _} -> Acc;
        {TimeEntry, Key} = Value ->
            case timer:now_diff(TimeEntry, Now) < 0 of
                true -> batch_read(Now, TTLTable, fun() -> ets:next(TTLTable, Value) end, [Key | Acc]);
                false -> Acc
            end
    end.

checker(Name, GroupCount, Time, TTLTable) ->
    TimeoutKeys = lists:reverse(batch_read(?NOW, TTLTable, fun() -> ets:first(TTLTable) end, [])),
    [check_timed_out_key(#ring{name = Name, groupcount = GroupCount}, Key) || Key <- TimeoutKeys],
    timer:sleep(Time),
    checker(Name, GroupCount, Time, TTLTable).

accumulate_row(Id, Count, TableName) ->
    ets:foldl(fun(V, Acc) ->
                      case hash(element(1, V), Count) of
                          Id -> [V | Acc];
                          _ -> Acc
                      end
              end, [], TableName).

hash(Key, Count) ->
    (erlang:phash2(Key) rem Count) + 1.

generate_paxos_groups(Name, GroupCount) ->
    [group_name(Name, I)|| I <- lists:seq(1, GroupCount)].

group_name(Name, GroupIndex) ->
    list_to_atom(table_name_string(Name) ++ "-vnode-" ++ integer_to_list(GroupIndex)).

ttl_table_name(Name) ->
    list_to_atom(table_name_string(Name) ++ "_ttl").

table_name(Name) ->
    list_to_atom(table_name_string(Name)).

table_name_string(Name) when is_atom(Name) ->
    table_name_string(atom_to_list(Name));
table_name_string(Name) when is_list(Name) ->
    ?PREFIX ++ "_" ++ Name.

add_mili_seconds(_, infinity) ->
    infinity;
add_mili_seconds({MegaS, S, MicroS}, MS) ->
    AddMegaS = MS div 1000000000,
    TailMS = MS - (AddMegaS * 1000000000),
    {MegaS + AddMegaS, S + (TailMS div 1000), MicroS + (TailMS rem 1000) * 1000}.

reply(From, Result, State) ->
    {reply, fun() -> paxos_server:reply(From, Result) end, State}.
