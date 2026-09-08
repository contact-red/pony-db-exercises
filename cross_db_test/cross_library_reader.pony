use pg = "postgres"
use "pony_check"
use lori = "lori"
use "constrained_types"

class val _CrossOdbcResult
  let nv: NormalizedValue
  new val create(nv': NormalizedValue) => nv = nv'

actor CrossLibraryReader is (pg.SessionStatusNotify & pg.ResultReceiver)
  """
  Runs the pg half of a cross-library literal comparison. Receives the ODBC
  result computed synchronously by the property, fires the same SELECT via pg
  SimpleQuery, and compares both results against each other and against expected.
  """
  let _env: Env
  var _session: (pg.Session | None) = None
  var _authenticated: Bool = false
  var _connection_failed: Bool = false
  var _ph: (PropertyHelper | None) = None
  var _scenario: (TestScenario | None) = None
  var _odbc_result: (_CrossOdbcResult | None) = None
  var _pending: (_CrossPending | None) = None
  var _remaining: USize

  new create(env: Env, num_samples: USize) =>
    _env = env
    _remaining = num_samples
    match lori.MakeConnectionTimeout(5_000)
    | let ct: lori.ConnectionTimeout =>
      let server = pg.ServerConnectInfo(
        lori.TCPConnectAuth(_env.root), "postgres", "5432"
        where auth_requirement' = pg.AllowAnyAuth,
        connection_timeout' = ct)
      let db = pg.DatabaseConnectInfo("postgres", "postgres", "postgres")
      _session = pg.Session(server, db, this)
    | let _: ValidationFailure => None
    end

  be read(scenario: TestScenario, odbc_result: _CrossOdbcResult,
    ph: PropertyHelper)
  =>
    if _connection_failed then
      _ph = ph
      _scenario = scenario
      _odbc_result = odbc_result
      _fail("pg connection previously failed")
      return
    end
    if not _authenticated then
      _pending = _CrossPending(scenario, odbc_result, ph)
      return
    end
    _ph = ph
    _scenario = scenario
    _odbc_result = odbc_result
    try
      let session = _session as pg.Session
      session.execute(pg.SimpleQuery(scenario.select_sql()), this)
    end

  be pg_session_authenticated(session: pg.Session) =>
    _authenticated = true
    match _pending
    | let p: _CrossPending =>
      _pending = None
      read(p.scenario, p.odbc_result, p.ph)
    end

  be pg_session_connection_failed(session: pg.Session,
    reason: pg.ConnectionFailureReason)
  =>
    _connection_failed = true
    let reason_str = _ConnReason(reason)
    _env.out.print(
      "pg session (CrossLibraryReader) connection FAILED: " + reason_str)
    match _pending
    | let p: _CrossPending =>
      _pending = None
      _ph = p.ph
      _scenario = p.scenario
      _odbc_result = p.odbc_result
    end
    _fail("pg connection failed: " + reason_str)

  be pg_query_result(session: pg.Session, result: pg.Result) =>
    match result
    | let rs: pg.ResultSet =>
      try
        let scenario = _scenario as TestScenario
        let wrapped = _odbc_result as _CrossOdbcResult
        let odbc_result = wrapped.nv
        let row = rs.rows()(0)?
        let field = row.fields(0)?
        let pg_result = scenario.col_type.normalize_pg(field.value)
        if not NormalizedEq(pg_result, odbc_result) then
          _fail(scenario.string()
            + ": libraries disagree."
            + " odbc=" + NormalizedValueString(odbc_result)
            + " pg=" + NormalizedValueString(pg_result))
        elseif not NormalizedEq(pg_result, scenario.expected) then
          _fail(scenario.string()
            + ": both agree but differ from expected."
            + " got=" + NormalizedValueString(pg_result)
            + " expected=" + NormalizedValueString(scenario.expected))
        else
          _complete()
        end
      else
        _fail("pg read: no rows/fields")
      end
    else
      _fail("pg read: expected ResultSet")
    end

  be pg_query_failed(session: pg.Session, query: pg.Query,
    failure: (pg.ErrorResponseMessage | pg.ClientQueryError))
  =>
    let scenario_str = try (_scenario as TestScenario).string() else "?" end
    _fail("pg query failed for " + scenario_str)

  fun ref _fail(msg: String val) =>
    match _ph
    | let ph: PropertyHelper =>
      ph.fail(msg)
      ph.complete_action("done")
      _ph = None
    end
    _tick_remaining()

  fun ref _complete() =>
    match _ph
    | let ph: PropertyHelper =>
      ph.complete_action("done")
      _ph = None
    end
    _tick_remaining()

  fun ref _tick_remaining() =>
    if _remaining > 0 then _remaining = _remaining - 1 end
    if _remaining == 0 then
      match _session
      | let s: pg.Session => s.close()
      end
    end

class val _CrossPending
  let scenario: TestScenario
  let odbc_result: _CrossOdbcResult
  let ph: PropertyHelper

  new val create(scenario': TestScenario, odbc_result': _CrossOdbcResult,
    ph': PropertyHelper)
  =>
    scenario = scenario'
    odbc_result = odbc_result'
    ph = ph'
