%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \_, infinity}_/ |___/\___/_/ .___/_/_/ /_/\_, infinity} /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(dike_cache_api_SUITE).
-compile([{parse_transform, lager_transform}]).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-define(LOG_LEVEL, debug).
-define(LOG_FILE, "/log/console.log").

%% ===================================================================
%% Common Test API
%% ===================================================================
all_tests() ->
    [basic].

groups() ->
    [{ets_cache, [],  all_tests()}].

all() ->
    [{group, ets_cache}].

init_per_group(ets_cache, Config) ->
    [{module, dike_cache} | Config].

init_per_suite(Config) ->
    Nodes = dike_test:nodes_dike_init("dike_cache", 5),
    [{nodes, Nodes} | Config].

end_per_suite(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    dike_test:stop_nodes(Nodes -- [node()]).

end_per_group(_, Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Module = proplists:get_value(module, Config),
    [{ok, _} = rpc:call(Node, application, ensure_all_started, [dike_cache]) || Node <- Nodes],
    rpc:multicall(Nodes, dike_cache_api, new, [Module, cache, 16]),
    Config.

end_per_testcase(_TestCase, Config) ->
    [begin
         rpc:call(SlaveNode, application, stop, [dike]),
         rpc:call(SlaveNode, application, stop, [dike_cache])
     end || SlaveNode <- proplists:get_value(nodes, Config)],
    ok.

%% ===================================================================
%% Test Functions
%% ===================================================================
basic(_Config) ->
    ok = dike_cache_api:insert(cache, 1, 10, infinity),
    ok = dike_cache_api:insert(cache, 2, 20, infinity),
    ok = dike_cache_api:insert(cache, 3, 30, infinity),
    {_, 10, infinity} = dike_cache_api:lookup(cache, 1),
    {_, 20, infinity} = dike_cache_api:lookup(cache, 2),
    {_, 30, infinity} = dike_cache_api:lookup(cache, 3),
    {_, DataASC} = dike_cache_api:select(cache, [{direction, asc}]),
    {_, DataDSC} = dike_cache_api:select(cache, [{direction, dsc}]),
    LA = lists:foldr(fun({X,_,_,_}, Sum) -> [X|Sum] end, [], DataASC),
    LD = lists:foldr(fun({X,_,_,_}, Sum) -> [X|Sum] end, [], DataDSC),
    true = (lists:sort(LA) == LA),
    true = (lists:reverse(lists:sort(LD)) == LD),
    {3, [{3, 30, _, infinity}]} = dike_cache_api:select(cache, [{direction, asc}, {offset, 2}]),
    {3, [{1, 10, _, infinity}]} = dike_cache_api:select(cache, [{direction, dsc}, {offset, 2}]),
    {3,[]} = dike_cache_api:select(cache, [{direction, dsc}, {offset, 1}, {limit, 0}]),
    {3,[{2, 20, _, infinity}]} = dike_cache_api:select(cache, [{direction, dsc}, {offset, 1}, {limit, 1}]),
    {3,[{3, 30, _, infinity}]} = dike_cache_api:select(cache, [{direction, asc}, {offset, 2}, {limit, 1}]),
    dike_cache_api:clear(cache),
    {0, []} = dike_cache_api:select(cache).
