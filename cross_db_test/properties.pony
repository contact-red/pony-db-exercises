use "pony_test"
use "pony_check"
use "odbc"
use pg = "postgres"

// ===========================================================================
// S1: SELECT literal → ODBC read (synchronous)
// S3: SELECT ?::type → ODBC param roundtrip (synchronous)
// ===========================================================================

primitive OdbcLiteral
primitive OdbcParam
type OdbcTestMode is (OdbcLiteral | OdbcParam)

class iso OdbcOnlyProperty is Property1[TestScenario]
  let _col_type: ColType
  let _mode: OdbcTestMode
  let _num_samples: USize
  var _conn: (Connection | None) = None

  new iso create(
    col_type: ColType,
    mode: OdbcTestMode,
    num_samples: USize)
  =>
    _col_type = col_type
    _mode = mode
    _num_samples = num_samples

  fun name(): String =>
    let mode_str = match _mode
    | OdbcLiteral => "literal->odbc"
    | OdbcParam => "odbc-param->odbc"
    end
    mode_str + "/" + _col_type.test_name()

  fun params(): PropertyParams =>
    PropertyParams(where num_samples' = _num_samples)

  fun gen(): Generator[TestScenario] =>
    TestScenarioGenerator(_col_type)

  fun ref property(scenario: TestScenario, ph: PropertyHelper) ? =>
    let conn = _ensure_conn()?
    let result = match _mode
    | OdbcLiteral => _query_literal(conn, scenario)?
    | OdbcParam => _query_param(conn, scenario)?
    end
    if not NormalizedEq(result, scenario.expected) then
      ph.fail(name() + " " + scenario.string()
        + ": odbc=" + NormalizedValueString(result)
        + " expected=" + NormalizedValueString(scenario.expected))
    end

  fun ref _ensure_conn(): Connection ? =>
    match _conn
    | let c: Connection => c
    else
      match Odbc.connect(Dsn("DSN=psqlred"))
      | let c: Connection =>
        _conn = c
        c
      | let _: ConnectError => error
      end
    end

  fun ref _query_literal(conn: Connection, scenario: TestScenario)
    : NormalizedValue ?
  =>
    match conn.query(scenario.select_sql())
    | let cursor: Cursor =>
      let result = match cursor.fetch()
      | let row: Row =>
        scenario.col_type.normalize_odbc(row, ColIndex(1))?
      | EndOfRows => error
      | let _: FetchError => error
      end
      cursor.close()
      result
    | let _: ExecError => error
    end

  fun ref _query_param(conn: Connection, scenario: TestScenario)
    : NormalizedValue ?
  =>
    let sql: String val = "SELECT ?::" + scenario.col_type.pg_type_name()
    match conn.prepare(sql)
    | let stmt: Statement =>
      let sv = _bind_value(stmt, scenario)
      match sv
      | let _: BindError =>
        stmt.close()
        error
      end
      match stmt.execute()
      | let _: Executed =>
        let result = match stmt.fetch()
        | let row: Row =>
          scenario.col_type.normalize_odbc(row, ColIndex(1))?
        | EndOfRows =>
          stmt.close()
          error
        | let _: FetchError =>
          stmt.close()
          error
        end
        stmt.close_cursor()
        stmt.close()
        result
      | let _: ExecError =>
        stmt.close()
        error
      end
    | let _: PrepareError => error
    end

  fun _bind_value(stmt: Statement, scenario: TestScenario): (Bound | BindError) =>
    match scenario.expected
    | NvNull => stmt.bind(ParamIndex(1), SqlNull)
    | let v: NvBool => stmt.bind(ParamIndex(1), SqlBool(v.value))
    | let v: NvInt =>
      match scenario.col_type
      | ColTinyint => stmt.bind(ParamIndex(1), SqlTinyInt(v.value.i8()))
      | ColSmallint => stmt.bind(ParamIndex(1), SqlSmallInt(v.value.i16()))
      | ColInteger => stmt.bind(ParamIndex(1), SqlInteger(v.value.i32()))
      else
        stmt.bind(ParamIndex(1), SqlBigInt(v.value))
      end
    | let v: NvFloat => stmt.bind(ParamIndex(1), SqlFloat(v.value))
    | let v: NvText => stmt.bind(ParamIndex(1), SqlText(v.value))
    | let v: NvDate =>
      stmt.bind(ParamIndex(1),
        SqlDate(v.year.i16(), v.month.i32().u16(), v.day.i32().u16()))
    | let v: NvTime =>
      stmt.bind(ParamIndex(1), SqlTime(v.hour, v.minute, v.second))
    | let v: NvTimestamp =>
      stmt.bind(ParamIndex(1),
        SqlTimestamp(
          v.year.i16(), v.month.i32().u16(), v.day.i32().u16(),
          v.hour, v.minute, v.second,
          v.microsecond * 1000)) // microseconds → nanoseconds
    end

// ===========================================================================
// S2: SELECT literal → pg SimpleQuery (async)
// S4: SELECT $1::type → pg PreparedQuery (async)
// ===========================================================================

primitive PgLiteral
primitive PgParam
type PgTestMode is (PgLiteral | PgParam)

class iso PgOnlyProperty is Property1[TestScenario]
  let _col_type: ColType
  let _mode: PgTestMode
  let _num_samples: USize
  var _reader: (PgReader | None) = None

  new iso create(
    col_type: ColType,
    mode: PgTestMode,
    num_samples: USize)
  =>
    _col_type = col_type
    _mode = mode
    _num_samples = num_samples

  fun name(): String =>
    let mode_str = match _mode
    | PgLiteral => "literal->pg-simple"
    | PgParam => "pg-param->pg-prepared"
    end
    mode_str + "/" + _col_type.test_name()

  fun params(): PropertyParams =>
    PropertyParams(where
      num_samples' = _num_samples,
      async' = true,
      timeout' = 120_000_000_000)

  fun gen(): Generator[TestScenario] =>
    TestScenarioGenerator(_col_type)

  fun ref property(scenario: TestScenario, ph: PropertyHelper) =>
    ph.expect_action("done")
    let reader = match _reader
    | let r: PgReader => r
    else
      let r = PgReader(ph.env, _num_samples)
      _reader = r
      r
    end
    match _mode
    | PgLiteral => reader.read_literal(scenario, ph)
    | PgParam => reader.read_param(scenario, ph)
    end

// ===========================================================================
// Stateful: write once → read via all three methods
// ===========================================================================

class iso StatefulProperty is Property1[TestScenario]
  let _col_type: ColType
  let _write_method: StatefulWriteMethod
  let _tx_mode: TxMode
  let _num_samples: USize
  var _coordinator: (StatefulCoordinator | None) = None

  new iso create(
    col_type: ColType,
    write_method: StatefulWriteMethod,
    tx_mode: TxMode,
    num_samples: USize)
  =>
    _col_type = col_type
    _write_method = write_method
    _tx_mode = tx_mode
    _num_samples = num_samples

  fun name(): String =>
    let wm = match _write_method
    | OdbcWrite => "odbc-write"
    | PgBinaryWrite => "pg-bin-write"
    | PgTextWrite => "pg-txt-write"
    end
    let tm = match _tx_mode
    | Autocommit => "auto"
    | ExplicitCommit => "commit"
    | RollbackVerify => "rollback"
    end
    wm + "/" + tm + "/" + _col_type.test_name()

  fun params(): PropertyParams =>
    PropertyParams(where
      num_samples' = _num_samples,
      async' = true,
      timeout' = 120_000_000_000)

  fun gen(): Generator[TestScenario] =>
    TestScenarioGenerator(_col_type)

  fun ref property(scenario: TestScenario, ph: PropertyHelper) =>
    ph.expect_action("done")
    let coord = match _coordinator
    | let c: StatefulCoordinator => c
    else
      let c = StatefulCoordinator(ph.env, _col_type, _num_samples)
      _coordinator = c
      c
    end
    coord.run_sample(scenario, _write_method, _tx_mode, ph)
