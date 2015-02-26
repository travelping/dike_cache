%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(dike_cache_app).

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/1]).


%% ===================================================================
%% Application callbacks
%% ===================================================================
start() ->
    application:ensure_all_started(dike_cache).
    
start(_StartType, _StartArgs) ->
    dike_cache_sup:start_link().

stop(_State) ->
    ok.