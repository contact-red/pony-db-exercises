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
    // 10-minute timeout — large-payload registrations (ColLargeText) can
    // spend several seconds per sample, and the PonyCheck default is too
    // tight for 5 multi-MB samples back-to-back. Short types finish well
    // inside the ceiling so this costs nothing in practice.
    PropertyParams(where
      num_samples' = _num_samples,
      timeout' = 600_000_000_000)

  fun gen(): Generator[TestScenario] =>
    TestScenarioGenerator(_col_type)

  fun ref property(scenario: TestScenario, ph: PropertyHelper) ? =>
    let conn = _ensure_conn(ph)?
    let result = match _mode
    | OdbcLiteral => _query_literal(conn, scenario)?
    | OdbcParam => _query_param(conn, scenario)?
    end
    if not NormalizedEq(result, scenario.expected) then
      ph.fail(name() + " " + scenario.string()
        + ": odbc=" + NormalizedValueString(result)
        + " expected=" + NormalizedValueString(scenario.expected))
    end

  fun ref _ensure_conn(ph: PropertyHelper): Connection ? =>
    match _conn
    | let c: Connection => c
    else
      match Odbc.connect(Dsn("DSN=psqlred"))
      | let c: Connection =>
        _conn = c
        c
      | let e: ConnectError =>
        ph.fail("ODBC connect failed (DSN=psqlred): " + e.string())
        error
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

// ===========================================================================
// Pg-only stateful: pg writes, pg reads, no ODBC.
// ===========================================================================

class iso PgStatefulProperty is Property1[TestScenario]
  let _col_type: ColType
  let _write_method: PgStatefulWriteMethod
  let _tx_mode: TxMode
  let _num_samples: USize
  var _coordinator: (PgStatefulCoordinator | None) = None

  new iso create(
    col_type: ColType,
    write_method: PgStatefulWriteMethod,
    tx_mode: TxMode,
    num_samples: USize)
  =>
    _col_type = col_type
    _write_method = write_method
    _tx_mode = tx_mode
    _num_samples = num_samples

  fun name(): String =>
    let wm = match _write_method
    | PgBinaryWrite => "pg-bin-write"
    | PgTextWrite => "pg-txt-write"
    end
    let tm = match _tx_mode
    | Autocommit => "auto"
    | ExplicitCommit => "commit"
    | RollbackVerify => "rollback"
    end
    "pg-only/" + wm + "/" + tm + "/" + _col_type.test_name()

  fun params(): PropertyParams =>
    // 10-minute timeout — this property is expected to carry multi-MB
    // payloads, so 5 samples can legitimately take several minutes.
    PropertyParams(where
      num_samples' = _num_samples,
      async' = true,
      timeout' = 600_000_000_000)

  fun gen(): Generator[TestScenario] =>
    TestScenarioGenerator(_col_type)

  fun ref property(scenario: TestScenario, ph: PropertyHelper) =>
    ph.expect_action("done")
    let coord = match _coordinator
    | let c: PgStatefulCoordinator => c
    else
      let c = PgStatefulCoordinator(ph.env, _col_type, _num_samples)
      _coordinator = c
      c
    end
    coord.run_sample(scenario, _write_method, _tx_mode, ph)

// ===========================================================================
// Odbc-only stateful: odbc writes, odbc reads, no pg.
// Synchronous — no coordinator actor needed.
// ===========================================================================

class iso OdbcStatefulProperty is Property1[TestScenario]
  let _col_type: ColType
  let _write_method: OdbcStatefulWriteMethod
  let _tx_mode: TxMode
  let _num_samples: USize
  var _conn: (Connection | None) = None
  var _table_created: Bool = false
  var _remaining: USize

  new iso create(
    col_type: ColType,
    write_method: OdbcStatefulWriteMethod,
    tx_mode: TxMode,
    num_samples: USize)
  =>
    _col_type = col_type
    _write_method = write_method
    _tx_mode = tx_mode
    _num_samples = num_samples
    _remaining = num_samples

  fun name(): String =>
    let wm = match _write_method
    | OdbcWrite => "odbc-lit-write"
    | OdbcParamWrite => "odbc-param-write"
    end
    let tm = match _tx_mode
    | Autocommit => "auto"
    | ExplicitCommit => "commit"
    | RollbackVerify => "rollback"
    end
    "odbc-only/" + wm + "/" + tm + "/" + _col_type.test_name()

  fun params(): PropertyParams =>
    // 10-minute timeout — this property carries multi-MB payloads; 5 samples
    // of 1–4 MiB each can legitimately take a minute or two.
    PropertyParams(where
      num_samples' = _num_samples,
      timeout' = 600_000_000_000)

  fun gen(): Generator[TestScenario] =>
    TestScenarioGenerator(_col_type)

  fun ref property(scenario: TestScenario, ph: PropertyHelper) =>
    try
      let conn = _ensure_conn(ph)?
      let table = OdbcOnlyTableName(_write_method, _col_type)
      if not _table_created then
        _create_table(conn, table)
        _table_created = true
      end
      _run_sample(conn, table, scenario, ph)?
    end
    if _remaining > 0 then _remaining = _remaining - 1 end
    if (_remaining == 0) and _table_created then
      try
        let conn = _conn as Connection
        let table = OdbcOnlyTableName(_write_method, _col_type)
        match conn.exec("DROP TABLE IF EXISTS " + table)
        | let _: ExecError => None
        end
      end
    end

  fun ref _ensure_conn(ph: PropertyHelper): Connection ? =>
    match _conn
    | let c: Connection => c
    else
      match Odbc.connect(Dsn("DSN=psqlred"))
      | let c: Connection =>
        _conn = c
        c
      | let e: ConnectError =>
        ph.fail("ODBC connect failed (DSN=psqlred): " + e.string())
        error
      end
    end

  fun ref _create_table(conn: Connection, table: String val) =>
    match conn.exec("DROP TABLE IF EXISTS " + table)
    | let _: ExecError => None
    end
    match conn.exec(
      "CREATE UNLOGGED TABLE " + table
      + " (id BIGSERIAL PRIMARY KEY, val " + _col_type.pg_type_name() + ")")
    | let _: ExecError => None
    end

  fun ref _run_sample(
    conn: Connection,
    table: String val,
    scenario: TestScenario,
    ph: PropertyHelper) ?
  =>
    match _tx_mode
    | ExplicitCommit | RollbackVerify =>
      match conn.begin()
      | let _: TxBeginError =>
        ph.fail("ODBC begin failed")
        error
      end
    end

    let id = match _write_method
    | OdbcWrite => _insert_literal(conn, table, scenario, ph)?
    | OdbcParamWrite => _insert_param(conn, table, scenario, ph)?
    end

    match _tx_mode
    | ExplicitCommit =>
      match conn.commit()
      | let _: TxCommitError =>
        ph.fail("ODBC commit failed")
        error
      end
    | RollbackVerify =>
      match conn.rollback()
      | let _: TxRollbackError =>
        ph.fail("ODBC rollback failed")
        error
      end
      _verify_rollback(conn, table, id, ph)?
      return
    end

    let result = _select_by_id(conn, table, id, ph)?
    if not NormalizedEq(result, scenario.expected) then
      ph.fail(name() + " " + scenario.string()
        + ": odbc-read=" + NormalizedValueString(result)
        + " expected=" + NormalizedValueString(scenario.expected))
    end

  fun _insert_literal(
    conn: Connection,
    table: String val,
    scenario: TestScenario,
    ph: PropertyHelper): I64 ?
  =>
    match conn.query(scenario.insert_sql(table))
    | let cursor: Cursor =>
      let id = _fetch_id(cursor, ph)?
      cursor.close()
      id
    | let e: ExecError =>
      ph.fail("ODBC INSERT (literal) failed: " + e.string())
      error
    end

  fun _insert_param(
    conn: Connection,
    table: String val,
    scenario: TestScenario,
    ph: PropertyHelper): I64 ?
  =>
    let sql: String val =
      "INSERT INTO " + table + " (val) VALUES (?) RETURNING id"
    match conn.prepare(sql)
    | let stmt: Statement =>
      match _bind_write_value(stmt, scenario)
      | let _: BindError =>
        stmt.close()
        ph.fail("ODBC INSERT (param) bind failed")
        error
      end
      match stmt.execute()
      | let _: Executed =>
        let id = match stmt.fetch()
        | let row: Row =>
          try
            match row.int(ColIndex(1))?
            | let v: I64 => v
            else
              stmt.close()
              ph.fail("INSERT RETURNING: NULL id")
              error
            end
          else
            stmt.close()
            ph.fail("INSERT RETURNING: column access failed")
            error
          end
        else
          stmt.close()
          ph.fail("INSERT RETURNING: no rows / fetch error")
          error
        end
        stmt.close_cursor()
        stmt.close()
        id
      | let e: ExecError =>
        stmt.close()
        ph.fail("ODBC INSERT (param) execute failed: " + e.string())
        error
      end
    | let _: PrepareError =>
      ph.fail("ODBC INSERT (param) prepare failed")
      error
    end

  fun _bind_write_value(stmt: Statement, scenario: TestScenario)
    : (Bound | BindError)
  =>
    // Mirrors OdbcOnlyProperty._bind_value; kept separate since the write
    // path doesn't share state with the stateless select path.
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
          v.microsecond * 1000))
    end

  fun _fetch_id(cursor: Cursor, ph: PropertyHelper): I64 ? =>
    match cursor.fetch()
    | let row: Row =>
      match row.int(ColIndex(1))?
      | let v: I64 => v
      else
        ph.fail("INSERT RETURNING: NULL id")
        error
      end
    else
      ph.fail("INSERT RETURNING: no rows / fetch error")
      error
    end

  fun _select_by_id(
    conn: Connection,
    table: String val,
    id: I64,
    ph: PropertyHelper): NormalizedValue ?
  =>
    let sql: String val =
      "SELECT val FROM " + table + " WHERE id = " + id.string()
    match conn.query(sql)
    | let cursor: Cursor =>
      let result = match cursor.fetch()
      | let row: Row =>
        _col_type.normalize_odbc(row, ColIndex(1))?
      | EndOfRows =>
        cursor.close()
        ph.fail("ODBC read: no rows for id " + id.string())
        error
      | let e: FetchError =>
        cursor.close()
        ph.fail("ODBC read fetch: " + e.string())
        error
      end
      cursor.close()
      result
    | let e: ExecError =>
      ph.fail("ODBC read query: " + e.string())
      error
    end

  fun _verify_rollback(
    conn: Connection,
    table: String val,
    id: I64,
    ph: PropertyHelper) ?
  =>
    let sql: String val =
      "SELECT val FROM " + table + " WHERE id = " + id.string()
    match conn.query(sql)
    | let cursor: Cursor =>
      match cursor.fetch()
      | let _: Row =>
        cursor.close()
        ph.fail("Rollback failed: row still present after ROLLBACK")
        error
      | EndOfRows => cursor.close()
      | let e: FetchError =>
        cursor.close()
        ph.fail("Rollback verify fetch: " + e.string())
        error
      end
    | let e: ExecError =>
      ph.fail("Rollback verify query: " + e.string())
      error
    end
