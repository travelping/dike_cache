
# dike_cache

Distributed consistent cache based on dike and paxos algorythm for distribution. Cache can be seen as simple consistent distributed in memory key-value store with TTL.

## Installation

    rebar get-deps && rebar compile

## Unit-tests

For running common tests, you need [tetrapak](https://github.com/travelping/tetrapak)

    tetrapak test

## Using dike_cache

Configure dike for local start:

    > dike_test:local_init(), application:ensure_all_started(dike_cache).

Consult [dike](https://github.com/travelping/dike) README on how to start or configure a cluster of dike nodes.

    > dike_cache_api:new(dike_cache, test, GroupCount = 64).
    test
    > dike_cache_api:insert(test, "key", "value", 1500), timer:sleep(2000), dike_cache_api:lookup(test, "key").

Group count specifies, how many dike groups should be used. The requests distribution is happend between groups through a simple consistent hashing algorythm. As dike_cache is implemented with 1 ets public table. The Group Count defines, how many parallel processes can working with the table, which allows better thoughtput.

Selects, which allows sorting and pagination on cache, are eventually consistent(as they readed dirty from a table), and there exists eventually consistent lookup, which is mostly usefull for cache use cases:

    > dike_cache:dirty_lookup(test, "key").

dirty_lookup is only one direct lookup in ets table, mostly 10 times quicker as consistent lookup.
