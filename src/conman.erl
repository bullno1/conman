-module(conman).
-behaviour(poolboy_worker).
-behaviour(gen_server).
-export([transaction/2, transaction/3, child_spec/6]).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2, format_status/2]).

-callback init(Args::any()) -> State :: any().
-callback connect(State) -> {ok | {error, any()}, State} when State::any().
-callback disconnect(State :: any()) -> ok.
-callback transaction(Fun, Args, State) -> {ok, Result, State} | {error, Reason, State} when
	Fun :: fun((Connection :: any()) -> Result),
	Args :: list(),
	State :: any(),
	Result :: any(),
	Reason :: any().

-record(state, {
	module,
	conn_state,
	backoff,
	connected = false
}).

child_spec(PoolId, Module, ConnOpts, PoolOpts, BackoffMin, BackoffMax) ->
	WorkerOpts = [
		{module, Module},
		{conn_opts, ConnOpts},
		{backoff_min, BackoffMin},
		{backoff_max, BackoffMax}
	],
	poolboy:child_spec(PoolId, [{worker_module, conman} | PoolOpts], WorkerOpts).

-spec transaction(Pool, fun((Connection) -> Result)) -> {ok, Result} | {error, Reason} when
	Pool :: term(),
	Connection :: term(),
	Result :: term(),
	Reason :: term().
transaction(Pool, Fun) -> transaction(Pool, Fun, []).

-spec transaction(Pool, fun((Connection) -> Result), list()) -> {ok, Result} | {error, Reason} when
	Pool :: term(),
	Connection :: term(),
	Result :: term(),
	Reason :: term().
transaction(Pool, Fun, Args) ->
	poolboy:transaction(Pool,
		fun(Worker) ->
			gen_server:call(Worker, {transaction, Fun, Args})
		end).

% poolboy_worker
start_link(Args) -> gen_server:start_link(?MODULE, Args, []).

% gen_server

init(Args) ->
	{module, Module} = lists:keyfind(module, 1, Args),
	{conn_opts, ConnOpts} = lists:keyfind(conn_opts, 1, Args),
	ConnState = Module:init(ConnOpts),
	{backoff_min, BackoffMin} = lists:keyfind(backoff_min, 1, Args),
	{backoff_max, BackoffMax} = lists:keyfind(backoff_max, 1, Args),
	Backoff = backoff:init(BackoffMin, BackoffMax, self(), try_connect),
	State = #state{
		module = Module,
		conn_state = ConnState,
		backoff = Backoff
	},
	process_flag(trap_exit, true),
	erlang:start_timer(0, self(), try_connect),
	{ok, State}.

terminate(_Reason, #state{module=Module, conn_state=ConnState}) ->
	Module:disconnect(ConnState).

handle_call({transaction, Fun, Args}, _From, #state{connected = true,
                                                    module = Mod,
                                                    conn_state = ConnState,
                                                    backoff = Backoff} = State) ->
	case Mod:transaction(Fun, Args, ConnState) of
		{ok, Result, NewConnState} ->
			{reply, {ok, Result}, State#state{conn_state = NewConnState}};
		{error, disconnected, NewConnState} ->
			error_logger:error_report([disconnected, {module, Mod}]),
			{_, Backoff2} = backoff:fail(Backoff),
			backoff:fire(Backoff2),
			{reply, {error, disconnected}, State#state{conn_state = NewConnState, backoff = Backoff2, connected = false}};
		{error, Err, NewConnState} ->
			{reply, {error, Err}, State#state{conn_state = NewConnState}}
	end;

handle_call({transaction, _Fun}, _From, #state{connected = false} = State) ->
	{reply, {error, disconnected}, State}.

handle_cast(_Req, State) -> {stop, unexpected, State}.

handle_info({timeout, _, try_connect}, #state{module=Mod, conn_state=ConnState, backoff=Backoff} = State) ->
	case Mod:connect(ConnState) of
		{ok, ConnState2} ->
			{_, Backoff2} = backoff:succeed(Backoff),
			{noreply, State#state{conn_state = ConnState2, backoff = Backoff2, connected = true}};
		{{error, Err}, ConnState2} ->
			error_logger:error_report([{connection_failed, Err}, {module, Mod}]),
			{_, Backoff2} = backoff:fail(Backoff),
			backoff:fire(Backoff2),
			{noreply, State#state{conn_state = ConnState2, backoff = Backoff2}}
	end.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

format_status(_Opt, [_PDict, State]) ->
	FormattedState = lists:zip(record_info(fields, state), tl(tuple_to_list(State))),
	[{data, [{"State", FormattedState}]}].
