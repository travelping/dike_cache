%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

%% @doc This module provides a behaviour abstraction of the distributed cache interface for various dike cache implementations.
-module(dike_cache_api).
-export([new/3, insert/4, delete/2, lookup/2, dirty_lookup/2, select/2, select/1, clear/1]).

-include("dike_ring.hrl").

-type key()          :: any().
%%       This is the key of the key-value store.

-type value()        :: any().
%%       This is the value of the key-value store.

-type cache()        :: any().
%%       The cache identifies the cache dike database.

-type offset()       :: non_neg_integer().
%%       The offset is used to ignore a number of elements of the database identified by the ring in forward direction.

-type limit()        :: non_neg_integer().
%%       Used to identify the number of elements (key-value pairs with timeout) to be retrieved from the database.

-type time_until()   :: erlang:timestamp() | infinity.
-type time_to_live() :: non_neg_integer() | infinity.
%%       This identifies the time the cache entry is valid in ms. If the cache entry shall be available for ever,
%%       the time_to_live is set to infinity.

-type direction()    :: asc | dsc.
%%       The search direction: asc for ascending and dsc for descending.

-type option()  :: {direction, direction()} | {limit, limit()} | { offset, offset()} | {key, key()}.
%%       'direction' defines the search 'direction'. The default 'direction' is ascending.
%%       'limit' defines the maximum number of key-value pairs to be retrieved. The default 'limit' is ?DefaultLimit.
%%       'offset' defines the number of key-value pairs to be skipped in the defined 'direction'. The default 'offset' is 0.
%%       For direction backward in combination with a key, the offset is limited to 0 and 1.
%%       'key' defines from which key shall be taken as the first element used by document internal document ordering.
%%       When 'key' is used in conjuction with offset, 0 means the key itself and the offset 1 means the next key after
%%       the given key, if that key exists..
%%       Without the option 'key', access is taken by 'offset' from the beginning for 'direction' ascending and from the end for
%%       'direction' descending.

%%% Note: as the  @doc directive below crashes edoc, it has be replaced by "@@doc"

-callback select(Cache::cache()) -> [{key(), value(), time_to_live()}].
%% @@doc Retrieve a list of all elements.

-callback select(Cache::cache(), OptionList::[Option::option()]) -> [{key(), value(), time_to_live()}].
%% @@doc Retrieve a list of elements according to the pattern defined by options.

-callback start_link(Name::atom(), GroupCount::pos_integer()) -> cache().
%% @@doc Creates an new dike_database.
%% @todo Define return type.

-callback clear(Cache::cache()) -> [ok].
%% @@doc Clears the cache for the given ring.
%% @todo Define return type.

-callback delete(Cache::cache(), Key::key()) -> ok.
%% @@doc Deletes the given key from the given ring.
%% @todo Define return type.

-callback lookup(Cache::cache(), Key::key()) -> undefined | {Key::key(), Value::value(), TimeUntil::time_to_live()}.
%% @@doc Looks up the cache for the given key.
%% @todo Define return type.

-callback insert(Cache::cache(), Key::key(), Value::value(), TimeToLive::time_until()) -> ok.
%% @@doc Adds the given key with the TTL.
%% @todo Define return type.

-spec new(Module::module(), Name::atom(), GroupCount::pos_integer()) -> Cache::cache().
new(Module, Name, GroupCount) ->
    {ok, _Child} = dike_cache_sup:start_child(Module, Name, GroupCount),
    Name.

insert(Cache, Key, Value, TTL) ->
    {Module, Ring} = ring(Cache),
    Module:insert(Ring, Key, Value, TTL).

delete(Cache, Key) ->
    {Module, Ring} = ring(Cache),
    Module:delete(Ring, Key).

lookup(Cache, Key) ->
    {Module, Ring} = ring(Cache),
    Module:lookup(Ring, Key).

dirty_lookup(Cache, Key) ->
    {Module, _Ring} = ring(Cache),
    Module:dirty_lookup(Cache, Key).

select(Cache) ->
    select(Cache, []).

select(Cache, Options) ->
    {Module, Ring} = ring(Cache),
    Module:select(Ring, Options).

clear(Cache) ->
    {Module, Ring} = ring(Cache),
    Module:clear(Ring).

ring(Name) ->
    {Module, GroupCount} = dike_cache_sup:lookup_cache(Name),
    {Module, #ring{name = Name, groupcount = GroupCount}}.
