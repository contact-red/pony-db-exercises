use pg = "postgres"
use "pony_check"
use lori = "lori"
use "constrained_types"

actor PgReader is (pg.SessionStatusNotify & pg.ResultReceiver)
  """
  Handles stateless pg-only reads (S2: literal→SimpleQuery, S4: param→PreparedQuery).
  Holds a persistent pg Session across samples. Closes the Session after
  all expected samples have completed to avoid connection exhaustion.
  """
  let _env: Env
  var _session: (pg.Session | None) = None
  var _authenticated: Bool = false
  var _connection_failed: Bool = false
  var _ph: (PropertyHelper | None) = None
  var _scenario: (TestScenario | None) = None
  var _pending: ((_PgPending | None)) = None
  var _remaining: USize

  new create(env: Env, num_samples: USize) =>
    _env = env
    _remaining = num_samples
    match lori.MakeConnectionTimeout(5_000)
    | let ct: lori.ConnectionTimeout =>
      let server = pg.ServerConnectInfo(
        lori.TCPConnectAuth(_env.root), "127.0.0.1", "5432"
        where auth_requirement' = pg.AllowAnyAuth,
        connection_timeout' = ct)
      let db = pg.DatabaseConnectInfo("postgres", "postgres", "postgres")
      _session = pg.Session(server, db, this)
    | let _: ValidationFailure => None
    end

  be read_literal(scenario: TestScenario, ph: PropertyHelper) =>
    """
    S2: SELECT literal::type via SimpleQuery.
    """
    if _connection_failed then
      _ph = ph
      _scenario = scenario
      _fail("pg connection previously failed")
      return
    end
    if not _authenticated then
      _pending = _PgPending(scenario, ph, false)
      return
    end
    _ph = ph
    _scenario = scenario
    try
      let session = _session as pg.Session
      session.execute(pg.SimpleQuery(scenario.select_sql()), this)
    end

  be read_param(scenario: TestScenario, ph: PropertyHelper) =>
    """
    S4: SELECT $1::type via PreparedQuery with typed params.
    """
    if _connection_failed then
      _ph = ph
      _scenario = scenario
      _fail("pg connection previously failed")
      return
    end
    if not _authenticated then
      _pending = _PgPending(scenario, ph, true)
      return
    end
    _ph = ph
    _scenario = scenario
    try
      let session = _session as pg.Session
      let sql: String val = "SELECT $1::" + scenario.col_type.pg_type_name()
      let params = _build_params(scenario)
      session.execute(pg.PreparedQuery(sql, params), this)
    end

  fun _build_params(scenario: TestScenario): Array[pg.FieldDataTypes] val =>
    """
    Build typed params from the scenario's expected value.
    """
    match scenario.expected
    | NvNull =>
      recover val [as pg.FieldDataTypes: None] end
    | let v: NvBool =>
      recover val [as pg.FieldDataTypes: v.value] end
    | let v: NvInt =>
      // Use the appropriate integer width based on the column type
      match scenario.col_type
      | ColTinyint =>
        recover val [as pg.FieldDataTypes: v.value.i16()] end
      | ColSmallint =>
        recover val [as pg.FieldDataTypes: v.value.i16()] end
      | ColInteger =>
        recover val [as pg.FieldDataTypes: v.value.i32()] end
      else
        recover val [as pg.FieldDataTypes: v.value] end
      end
    | let v: NvFloat =>
      match v.precision
      | FloatPrecisionF32 =>
        recover val [as pg.FieldDataTypes: v.value.f32()] end
      | FloatPrecisionF64 =>
        recover val [as pg.FieldDataTypes: v.value] end
      end
    | let v: NvText =>
      recover val [as pg.FieldDataTypes: v.value] end
    | let v: NvDate =>
      let days = _date_to_pg_days(v.year, v.month, v.day)
      recover val [as pg.FieldDataTypes: pg.PgDate(days)] end
    | let v: NvTime =>
      let us: I64 = (v.hour.i64() * 3_600_000_000)
        + (v.minute.i64() * 60_000_000)
        + (v.second.i64() * 1_000_000)
      match pg.MakePgTimeMicroseconds(us)
      | let ptm: pg.PgTimeMicroseconds =>
        recover val [as pg.FieldDataTypes: pg.PgTime(ptm)] end
      else
        recover val [as pg.FieldDataTypes: None] end
      end
    | let v: NvTimestamp =>
      let days = _date_to_pg_days(v.year, v.month, v.day)
      let time_us: I64 = (v.hour.i64() * 3_600_000_000)
        + (v.minute.i64() * 60_000_000)
        + (v.second.i64() * 1_000_000)
        + v.microsecond.i64()
      let total_us: I64 = (days.i64() * 86_400_000_000) + time_us
      recover val [as pg.FieldDataTypes: pg.PgTimestamp(total_us)] end
    end

  fun _date_to_pg_days(year: I32, month: I32, day: I32): I32 =>
    """
    Convert (year, month, day) to days since 2000-01-01 (PostgreSQL epoch).
    Uses the inverse of the Julian Day algorithm.
    """
    // Convert to Julian Day Number
    let a = (14 - month) / 12
    let y = (year.i64() + 4800) - a.i64()
    let m = (month.i64() + (12 * a.i64())) - 3
    let jdn = ((((((day.i64() + (((153 * m) + 2) / 5)) + (365 * y)) + (y / 4)) - (y / 100)) + (y / 400)) - 32045)
    // PostgreSQL epoch JDN = 2451545
    (jdn - 2451545).i32()

  // ---- pg callbacks ----

  be pg_session_authenticated(session: pg.Session) =>
    _authenticated = true
    match _pending
    | let p: _PgPending =>
      _pending = None
      if p.is_param then
        read_param(p.scenario, p.ph)
      else
        read_literal(p.scenario, p.ph)
      end
    end

  be pg_session_connection_failed(session: pg.Session,
    reason: pg.ConnectionFailureReason)
  =>
    _connection_failed = true
    match _pending
    | let p: _PgPending =>
      _pending = None
      _ph = p.ph
      _scenario = p.scenario
    end
    _fail("pg connection failed")

  be pg_query_result(session: pg.Session, result: pg.Result) =>
    match result
    | let rs: pg.ResultSet =>
      try
        let scenario = _scenario as TestScenario
        let row = rs.rows()(0)?
        let field = row.fields(0)?
        let pg_result = scenario.col_type.normalize_pg(field.value)
        if not NormalizedEq(pg_result, scenario.expected) then
          _fail(scenario.string()
            + ": pg disagrees with expected."
            + " pg=" + NormalizedValueString(pg_result)
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

class val _PgPending
  let scenario: TestScenario
  let ph: PropertyHelper
  let is_param: Bool

  new val create(scenario': TestScenario, ph': PropertyHelper,
    is_param': Bool)
  =>
    scenario = scenario'
    ph = ph'
    is_param = is_param'
