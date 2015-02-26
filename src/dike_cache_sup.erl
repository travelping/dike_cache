%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(dike_cache_sup).
-export([start_link/0, lookup_cache/1, start_child/3, stop_child/1]).
-export([init/1]).

-behaviour(supervisor).
-define(CHILD(Name, Mod), ?CHILD(Name, Mod, [])).
-define(CHILD(Name, Mod, Args), {Name, {Mod, start_link, Args}, permanent, 1000, worker, [Mod]}).

%% ===================================================================
%% Application callbacks
%% ===================================================================
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Module, Id, Count) ->
    Spec = cache_spec(Module, Id, Count),
    ets:insert(?MODULE, {Id, Module, Count}),
    supervisor:start_child(?MODULE, Spec).

stop_child(Name) ->
    ets:delete(?MODULE, Name),
    ok = supervisor:terminate_child(?MODULE, Name),
    ok = supervisor:delete_child(?MODULE, Name).

init([]) ->
    ets:new(?MODULE, [public, named_table, {read_concurrency, true}]),
    {ok, {{one_for_one, 50, 100}, []}}.

%% ===================================================================
%% Internal Functions
%% ===================================================================
cache_spec(Module, Id, Count) ->
    ?CHILD(Id, Module, [Id, Count]).

lookup_cache(Name) ->
    [{Name, Module, Count}] = ets:lookup(?MODULE, Name),
    {Module, Count}.