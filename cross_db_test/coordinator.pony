use pg = "postgres"
use "odbc"
use "pony_check"
use lori = "lori"
use "constrained_types"

// Phase markers for the coordinator state machine
primitive _Idle
primitive _WaitingAuth
primitive _WaitingBegin
primitive _WaitingInsert
primitive _WaitingCommit
primitive _WaitingRollback
primitive _ReadingPgSimple
primitive _ReadingPgPrepared

type _Phase is
  ( _Idle | _WaitingAuth
  | _WaitingBegin | _WaitingInsert | _WaitingCommit | _WaitingRollback
  | _ReadingPgSimple | _ReadingPgPrepared )

actor StatefulCoordinator is (pg.SessionStatusNotify & pg.ResultReceiver)
  """
  Handles the full write-then-read-all flow for stateful properties.
  Owns both an ODBC Connection and a pg Session. One instance per property,
  reused across all samples.
  """
  let _env: Env
  let _col_type: ColType
  var _odbc_conn: (Connection | None) = None
  var _pg_session: (pg.Session | None) = None
  var _pg_authenticated: Bool = false
  var _pg_connection_failed: Bool = false
  var _table_created: Bool = false

  // Per-sample state
  var _phase: _Phase = _WaitingAuth
  var _ph: (PropertyHelper | None) = None
  var _scenario: (TestScenario | None) = None
  var _write_method: StatefulWriteMethod = OdbcWrite
  var _tx_mode: TxMode = Autocommit
  var _inserted_id: I64 = 0
  var _odbc_result: NormalizedValue = NvNull
  var _pg_simple_result: NormalizedValue = NvNull

  // Queue for samples that arrive before pg is authenticated
  var _pending: ((_PendingSample | None)) = None
  var _remaining: USize

  new create(env: Env, col_type: ColType, num_samples: USize) =>
    _env = env
    _col_type = col_type
    _remaining = num_samples

    // Create pg Session with 5-second connection timeout
    match lori.MakeConnectionTimeout(5_000)
    | let ct: lori.ConnectionTimeout =>
      let server = pg.ServerConnectInfo(
        lori.TCPConnectAuth(_env.root), "postgres", "5432"
        where auth_requirement' = pg.AllowAnyAuth,
        connection_timeout' = ct)
      let db = pg.DatabaseConnectInfo("postgres", "postgres", "postgres")
      _pg_session = pg.Session(server, db, this)
    | let _: ValidationFailure =>
      // Shouldn't happen with 5000ms
      None
    end

  be run_sample(
    scenario: TestScenario,
    write_method: StatefulWriteMethod,
    tx_mode: TxMode,
    ph: PropertyHelper)
  =>
    if _pg_connection_failed then
      _ph = ph
      _scenario = scenario
      _fail("pg connection previously failed")
      return
    end
    if not _pg_authenticated then
      // Queue until pg is ready
      _pending = _PendingSample(scenario, write_method, tx_mode, ph)
      return
    end
    _start_sample(scenario, write_method, tx_mode, ph)

  fun ref _start_sample(
    scenario: TestScenario,
    write_method: StatefulWriteMethod,
    tx_mode: TxMode,
    ph: PropertyHelper)
  =>
    _ph = ph
    _scenario = scenario
    _write_method = write_method
    _tx_mode = tx_mode
    _inserted_id = 0
    _odbc_result = NvNull
    _pg_simple_result = NvNull

    // Ensure ODBC connection
    match _odbc_conn
    | None =>
      match Odbc.connect(Dsn("DSN=psqlred"))
      | let c: Connection => _odbc_conn = c
      | let e: ConnectError =>
        _fail("ODBC connect failed: " + e.string())
        return
      end
    end

    // Ensure table exists
    if not _table_created then
      _ensure_table(write_method)
    end

    // Start the write phase
    match write_method
    | OdbcWrite => _odbc_write(scenario, tx_mode)
    | PgBinaryWrite => _pg_write(scenario, tx_mode, false)
    | PgTextWrite => _pg_write(scenario, tx_mode, true)
    end

  fun ref _ensure_table(write_method: StatefulWriteMethod) =>
    let table = TableName(write_method, _col_type)
    try
      let conn = _odbc_conn as Connection
      // Drop if exists, then create UNLOGGED
      match conn.exec("DROP TABLE IF EXISTS " + table)
      | let _: ExecError => None // ignore
      end
      match conn.exec(
        "CREATE UNLOGGED TABLE " + table
        + " (id BIGSERIAL PRIMARY KEY, val " + _col_type.pg_type_name() + ")")
      | let _: ExecError => None // table may already exist from prior run
      end
      _table_created = true
    end

  // ---- ODBC write (synchronous) ----

  fun ref _odbc_write(scenario: TestScenario, tx_mode: TxMode) =>
    try
      let conn = _odbc_conn as Connection
      let table = TableName(_write_method, _col_type)

      // Begin transaction if needed
      match tx_mode
      | ExplicitCommit =>
        match conn.begin()
        | let _: TxBeginError =>
          _fail("ODBC begin failed")
          return
        end
      | RollbackVerify =>
        match conn.begin()
        | let _: TxBeginError =>
          _fail("ODBC begin failed")
          return
        end
      end

      // INSERT via query (RETURNING needs a cursor)
      let insert_sql = scenario.insert_sql(table)
      let id: I64 = match conn.query(insert_sql)
      | let cursor: Cursor =>
        let row_id = match cursor.fetch()
        | let row: Row =>
          try
            match row.int(ColIndex(1))?
            | let v: I64 => v
            else
              cursor.close()
              _fail("INSERT RETURNING: NULL id")
              return
            end
          else
            cursor.close()
            _fail("INSERT RETURNING: column access failed")
            return
          end
        else
          cursor.close()
          _fail("INSERT RETURNING: no rows")
          return
        end
        cursor.close()
        row_id
      | let e: ExecError =>
        // Rollback if in tx
        match tx_mode
        | ExplicitCommit => conn.rollback()
        | RollbackVerify => conn.rollback()
        end
        _fail("ODBC INSERT failed: " + e.string())
        return
      end

      // Commit or rollback
      match tx_mode
      | ExplicitCommit =>
        match conn.commit()
        | let _: TxCommitError =>
          _fail("ODBC commit failed")
          return
        end
      | RollbackVerify =>
        match conn.rollback()
        | let _: TxRollbackError =>
          _fail("ODBC rollback failed")
          return
        end
        // Verify row is absent
        _verify_rollback(table, id)
        return
      end

      _inserted_id = id
      _start_reads(table)
    end

  fun ref _verify_rollback(table: String val, id: I64) =>
    try
      let conn = _odbc_conn as Connection
      match conn.query("SELECT val FROM " + table + " WHERE id = " + id.string())
      | let cursor: Cursor =>
        match cursor.fetch()
        | let _: Row =>
          cursor.close()
          _fail("Rollback failed: row still present after ROLLBACK")
        | EndOfRows =>
          cursor.close()
          _complete()
        | let e: FetchError =>
          cursor.close()
          _fail("Rollback verify fetch: " + e.string())
        end
      | let e: ExecError =>
        _fail("Rollback verify query: " + e.string())
      end
    end

  // ---- pg write (asynchronous) ----

  fun ref _pg_write(
    scenario: TestScenario,
    tx_mode: TxMode,
    text_mode: Bool)
  =>
    try
      let session = _pg_session as pg.Session
      let table = TableName(_write_method, _col_type)

      match tx_mode
      | Autocommit =>
        // Send INSERT directly
        _send_pg_insert(session, scenario, table, text_mode)
        _phase = _WaitingInsert
      | ExplicitCommit =>
        session.execute(pg.SimpleQuery("BEGIN"), this)
        _phase = _WaitingBegin
      | RollbackVerify =>
        session.execute(pg.SimpleQuery("BEGIN"), this)
        _phase = _WaitingBegin
      end
    end

  fun ref _send_pg_insert(
    session: pg.Session,
    scenario: TestScenario,
    table: String val,
    text_mode: Bool)
  =>
    // For both text and binary mode, use PreparedQuery.
    // Text mode: pass value as String (OID 0, text format)
    // Binary mode: pass value as typed FieldDataTypes (explicit OID, binary format)
    //
    // We use the INSERT ... RETURNING id form via SimpleQuery with the literal
    // for simplicity. The write encoding is tested via the literal cast.
    // TODO: use PreparedQuery with typed params for true encode testing
    let insert_sql = scenario.insert_sql(table)
    session.execute(pg.SimpleQuery(insert_sql), this)

  // ---- Read phase (common to all write methods) ----

  fun ref _start_reads(table: String val) =>
    // Phase 1: ODBC read (synchronous)
    try
      let conn = _odbc_conn as Connection
      let sql: String val = "SELECT val FROM " + table + " WHERE id = " + _inserted_id.string()
      match conn.query(sql)
      | let cursor: Cursor =>
        match cursor.fetch()
        | let row: Row =>
          try
            _odbc_result = _col_type.normalize_odbc(row, ColIndex(1))?
          else
            cursor.close()
            _fail("ODBC read: column access failed")
            return
          end
          cursor.close()
        | EndOfRows =>
          cursor.close()
          _fail("ODBC read: no rows for id " + _inserted_id.string())
          return
        | let e: FetchError =>
          cursor.close()
          _fail("ODBC read fetch: " + e.string())
          return
        end
      | let e: ExecError =>
        _fail("ODBC read query: " + e.string())
        return
      end
    end

    // Phase 2: pg SimpleQuery read (async)
    try
      let session = _pg_session as pg.Session
      let table' = TableName(_write_method, _col_type)
      let sql: String val = "SELECT val FROM " + table' + " WHERE id = " + _inserted_id.string()
      session.execute(pg.SimpleQuery(sql), this)
      _phase = _ReadingPgSimple
    end

  // ---- pg callbacks ----

  be pg_session_authenticated(session: pg.Session) =>
    _pg_authenticated = true
    _phase = _Idle
    // Process any pending sample
    match _pending
    | let p: _PendingSample =>
      _pending = None
      _start_sample(p.scenario, p.write_method, p.tx_mode, p.ph)
    end

  be pg_session_connection_failed(session: pg.Session,
    reason: pg.ConnectionFailureReason)
  =>
    _pg_connection_failed = true
    let reason_str = _ConnReason(reason)
    _env.out.print(
      "pg session (StatefulCoordinator) connection FAILED: " + reason_str)
    match _pending
    | let p: _PendingSample =>
      _pending = None
      _ph = p.ph
      _scenario = p.scenario
      _write_method = p.write_method
      _tx_mode = p.tx_mode
    end
    _fail("pg connection failed: " + reason_str)

  be pg_query_result(session: pg.Session, result: pg.Result) =>
    match _phase
    | _WaitingBegin =>
      // BEGIN completed, send INSERT
      try
        let scenario = _scenario as TestScenario
        let table = TableName(_write_method, _col_type)
        let text_mode = match _write_method
        | PgTextWrite => true
        else false
        end
        _send_pg_insert(session, scenario, table, text_mode)
        _phase = _WaitingInsert
      end

    | _WaitingInsert =>
      // INSERT completed, extract id
      match result
      | let rs: pg.ResultSet =>
        try
          let row = rs.rows()(0)?
          let field = row.fields(0)?
          match field.value
          | let v: I64 => _inserted_id = v
          | let v: I32 => _inserted_id = v.i64()
          | let v: String =>
            try _inserted_id = v.i64()?
            else
              _fail("pg INSERT RETURNING: can't parse id: " + v)
              return
            end
          else
            _fail("pg INSERT RETURNING: unexpected id type")
            return
          end
        else
          _fail("pg INSERT RETURNING: no fields")
          return
        end
      else
        _fail("pg INSERT: expected ResultSet from RETURNING")
        return
      end

      // Commit/rollback/read depending on tx_mode
      match _tx_mode
      | Autocommit =>
        let table = TableName(_write_method, _col_type)
        _start_reads(table)
      | ExplicitCommit =>
        session.execute(pg.SimpleQuery("COMMIT"), this)
        _phase = _WaitingCommit
      | RollbackVerify =>
        session.execute(pg.SimpleQuery("ROLLBACK"), this)
        _phase = _WaitingRollback
      end

    | _WaitingCommit =>
      // COMMIT completed, start reads
      let table = TableName(_write_method, _col_type)
      _start_reads(table)

    | _WaitingRollback =>
      // ROLLBACK completed, verify via ODBC that row is absent
      let table = TableName(_write_method, _col_type)
      _verify_rollback(table, _inserted_id)

    | _ReadingPgSimple =>
      // SimpleQuery SELECT result
      match result
      | let rs: pg.ResultSet =>
        try
          let row = rs.rows()(0)?
          let field = row.fields(0)?
          _pg_simple_result = _col_type.normalize_pg(field.value)
        else
          _fail("pg SimpleQuery read: no rows/fields")
          return
        end
      else
        _fail("pg SimpleQuery read: expected ResultSet")
        return
      end

      // Phase 3: pg PreparedQuery read (async)
      let table = TableName(_write_method, _col_type)
      let sql: String val = "SELECT val FROM " + table + " WHERE id = $1"
      let params = recover val
        [as pg.FieldDataTypes: _inserted_id]
      end
      session.execute(pg.PreparedQuery(sql, params), this)
      _phase = _ReadingPgPrepared

    | _ReadingPgPrepared =>
      // PreparedQuery SELECT result — now compare all
      match result
      | let rs: pg.ResultSet =>
        try
          let row = rs.rows()(0)?
          let field = row.fields(0)?
          let pg_prepared_result = _col_type.normalize_pg(field.value)
          _compare_all(pg_prepared_result)
        else
          _fail("pg PreparedQuery read: no rows/fields")
        end
      else
        _fail("pg PreparedQuery read: expected ResultSet")
      end
    end

  be pg_query_failed(session: pg.Session, query: pg.Query,
    failure: (pg.ErrorResponseMessage | pg.ClientQueryError))
  =>
    let phase_name = match _phase
    | _WaitingBegin => "BEGIN"
    | _WaitingInsert => "INSERT"
    | _WaitingCommit => "COMMIT"
    | _WaitingRollback => "ROLLBACK"
    | _ReadingPgSimple => "SimpleQuery read"
    | _ReadingPgPrepared => "PreparedQuery read"
    else "unknown phase"
    end
    let scenario_str = try (_scenario as TestScenario).string() else "?" end
    _fail("pg " + phase_name + " failed for " + scenario_str)

  // ---- Comparison ----

  fun ref _compare_all(pg_prepared_result: NormalizedValue) =>
    try
      let scenario = _scenario as TestScenario

      // Three-way comparison: each read result vs expected
      if not NormalizedEq(_odbc_result, scenario.expected) then
        _fail(scenario.string()
          + ": ODBC disagrees with expected."
          + " odbc=" + NormalizedValueString(_odbc_result)
          + " expected=" + NormalizedValueString(scenario.expected))
        return
      end

      if not NormalizedEq(_pg_simple_result, scenario.expected) then
        _fail(scenario.string()
          + ": pg SimpleQuery disagrees with expected."
          + " pg_simple=" + NormalizedValueString(_pg_simple_result)
          + " expected=" + NormalizedValueString(scenario.expected))
        return
      end

      if not NormalizedEq(pg_prepared_result, scenario.expected) then
        _fail(scenario.string()
          + ": pg PreparedQuery disagrees with expected."
          + " pg_prepared=" + NormalizedValueString(pg_prepared_result)
          + " expected=" + NormalizedValueString(scenario.expected))
        return
      end

      // Cross-library comparisons
      if not NormalizedEq(_odbc_result, _pg_simple_result) then
        _fail(scenario.string()
          + ": ODBC vs pg SimpleQuery disagree."
          + " odbc=" + NormalizedValueString(_odbc_result)
          + " pg_simple=" + NormalizedValueString(_pg_simple_result))
        return
      end

      if not NormalizedEq(_odbc_result, pg_prepared_result) then
        _fail(scenario.string()
          + ": ODBC vs pg PreparedQuery disagree."
          + " odbc=" + NormalizedValueString(_odbc_result)
          + " pg_prepared=" + NormalizedValueString(pg_prepared_result))
        return
      end

      if not NormalizedEq(_pg_simple_result, pg_prepared_result) then
        _fail(scenario.string()
          + ": pg SimpleQuery vs pg PreparedQuery disagree."
          + " pg_simple=" + NormalizedValueString(_pg_simple_result)
          + " pg_prepared=" + NormalizedValueString(pg_prepared_result))
        return
      end

      _complete()
    end

  // ---- Completion helpers ----

  fun ref _fail(msg: String val) =>
    match _ph
    | let ph: PropertyHelper =>
      ph.fail(msg)
      ph.complete_action("done")
      _ph = None
    end
    _phase = _Idle
    _tick_remaining()

  fun ref _complete() =>
    match _ph
    | let ph: PropertyHelper =>
      ph.complete_action("done")
      _ph = None
    end
    _phase = _Idle
    _tick_remaining()

  fun ref _tick_remaining() =>
    if _remaining > 0 then _remaining = _remaining - 1 end
    if _remaining == 0 then
      if _table_created then
        try
          let conn = _odbc_conn as Connection
          let table = TableName(_write_method, _col_type)
          match conn.exec("DROP TABLE IF EXISTS " + table)
          | let _: ExecError => None
          end
        end
      end
      match _pg_session
      | let s: pg.Session => s.close()
      end
      match _odbc_conn
      | let c: Connection => c.close()
      end
    end

class val _PendingSample
  let scenario: TestScenario
  let write_method: StatefulWriteMethod
  let tx_mode: TxMode
  let ph: PropertyHelper

  new val create(
    scenario': TestScenario,
    write_method': StatefulWriteMethod,
    tx_mode': TxMode,
    ph': PropertyHelper)
  =>
    scenario = scenario'
    write_method = write_method'
    tx_mode = tx_mode'
    ph = ph'

// ===========================================================================
// PgStatefulCoordinator — pg-only write/read flow (no ODBC).
//
// Exists so large-payload tests can verify pg's own write/read roundtrip
// without being masked by the ODBC wrapper's truncation on large VARCHAR
// fetches. Table creation, rollback verification, and both reads all go
// through the pg Session.
// ===========================================================================

primitive _PgWaitingDropTable
primitive _PgWaitingCreateTable
primitive _PgVerifyingRollback
primitive _PgWaitingFinalDrop

type _PgPhase is
  ( _Idle | _WaitingAuth | _PgWaitingDropTable | _PgWaitingCreateTable
  | _WaitingBegin | _WaitingInsert | _WaitingCommit | _WaitingRollback
  | _PgVerifyingRollback | _PgWaitingFinalDrop
  | _ReadingPgSimple | _ReadingPgPrepared )

actor PgStatefulCoordinator is (pg.SessionStatusNotify & pg.ResultReceiver)
  let _env: Env
  let _col_type: ColType
  var _pg_session: (pg.Session | None) = None
  var _pg_authenticated: Bool = false
  var _pg_connection_failed: Bool = false
  var _table_created: Bool = false

  var _phase: _PgPhase = _WaitingAuth
  var _ph: (PropertyHelper | None) = None
  var _scenario: (TestScenario | None) = None
  var _write_method: PgStatefulWriteMethod = PgBinaryWrite
  var _tx_mode: TxMode = Autocommit
  var _inserted_id: I64 = 0
  var _pg_simple_result: NormalizedValue = NvNull

  var _pending: ((_PgPendingSample | None)) = None
  var _remaining: USize

  new create(env: Env, col_type: ColType, num_samples: USize) =>
    _env = env
    _col_type = col_type
    _remaining = num_samples

    match lori.MakeConnectionTimeout(5_000)
    | let ct: lori.ConnectionTimeout =>
      let server = pg.ServerConnectInfo(
        lori.TCPConnectAuth(_env.root), "postgres", "5432"
        where auth_requirement' = pg.AllowAnyAuth,
        connection_timeout' = ct)
      let db = pg.DatabaseConnectInfo("postgres", "postgres", "postgres")
      _pg_session = pg.Session(server, db, this)
    | let _: ValidationFailure => None
    end

  be run_sample(
    scenario: TestScenario,
    write_method: PgStatefulWriteMethod,
    tx_mode: TxMode,
    ph: PropertyHelper)
  =>
    if _pg_connection_failed then
      _ph = ph
      _scenario = scenario
      _fail("pg connection previously failed")
      return
    end
    if not _pg_authenticated then
      _pending = _PgPendingSample(scenario, write_method, tx_mode, ph)
      return
    end
    _start_sample(scenario, write_method, tx_mode, ph)

  fun ref _start_sample(
    scenario: TestScenario,
    write_method: PgStatefulWriteMethod,
    tx_mode: TxMode,
    ph: PropertyHelper)
  =>
    _ph = ph
    _scenario = scenario
    _write_method = write_method
    _tx_mode = tx_mode
    _inserted_id = 0
    _pg_simple_result = NvNull

    if not _table_created then
      // DROP and CREATE must be separate SimpleQuery calls — a single
      // multi-statement query returns one result per statement, which
      // would require extra phase bookkeeping.
      try
        let session = _pg_session as pg.Session
        let table = PgOnlyTableName(write_method, _col_type)
        session.execute(pg.SimpleQuery("DROP TABLE IF EXISTS " + table), this)
        _phase = _PgWaitingDropTable
      end
    else
      _begin_write()
    end

  fun ref _begin_write() =>
    try
      let session = _pg_session as pg.Session
      match _tx_mode
      | Autocommit =>
        let scenario = _scenario as TestScenario
        let table = PgOnlyTableName(_write_method, _col_type)
        session.execute(pg.SimpleQuery(scenario.insert_sql(table)), this)
        _phase = _WaitingInsert
      | ExplicitCommit =>
        session.execute(pg.SimpleQuery("BEGIN"), this)
        _phase = _WaitingBegin
      | RollbackVerify =>
        session.execute(pg.SimpleQuery("BEGIN"), this)
        _phase = _WaitingBegin
      end
    end

  // ---- pg callbacks ----

  be pg_session_authenticated(session: pg.Session) =>
    _pg_authenticated = true
    _phase = _Idle
    match _pending
    | let p: _PgPendingSample =>
      _pending = None
      _start_sample(p.scenario, p.write_method, p.tx_mode, p.ph)
    end

  be pg_session_connection_failed(session: pg.Session,
    reason: pg.ConnectionFailureReason)
  =>
    _pg_connection_failed = true
    let reason_str = _ConnReason(reason)
    _env.out.print(
      "pg session (PgStatefulCoordinator) connection FAILED: " + reason_str)
    match _pending
    | let p: _PgPendingSample =>
      _pending = None
      _ph = p.ph
      _scenario = p.scenario
      _write_method = p.write_method
      _tx_mode = p.tx_mode
    end
    _fail("pg connection failed: " + reason_str)

  be pg_query_result(session: pg.Session, result: pg.Result) =>
    match _phase
    | _PgWaitingDropTable =>
      let table = PgOnlyTableName(_write_method, _col_type)
      let ddl: String val = "CREATE UNLOGGED TABLE " + table
        + " (id BIGSERIAL PRIMARY KEY, val " + _col_type.pg_type_name() + ")"
      session.execute(pg.SimpleQuery(ddl), this)
      _phase = _PgWaitingCreateTable

    | _PgWaitingCreateTable =>
      _table_created = true
      _begin_write()

    | _WaitingBegin =>
      try
        let scenario = _scenario as TestScenario
        let table = PgOnlyTableName(_write_method, _col_type)
        session.execute(pg.SimpleQuery(scenario.insert_sql(table)), this)
        _phase = _WaitingInsert
      end

    | _WaitingInsert =>
      match result
      | let rs: pg.ResultSet =>
        try
          let row = rs.rows()(0)?
          let field = row.fields(0)?
          match field.value
          | let v: I64 => _inserted_id = v
          | let v: I32 => _inserted_id = v.i64()
          | let v: String =>
            try _inserted_id = v.i64()?
            else
              _fail("pg INSERT RETURNING: can't parse id: " + v)
              return
            end
          else
            _fail("pg INSERT RETURNING: unexpected id type")
            return
          end
        else
          _fail("pg INSERT RETURNING: no fields")
          return
        end
      else
        _fail("pg INSERT: expected ResultSet from RETURNING")
        return
      end

      match _tx_mode
      | Autocommit => _start_reads()
      | ExplicitCommit =>
        session.execute(pg.SimpleQuery("COMMIT"), this)
        _phase = _WaitingCommit
      | RollbackVerify =>
        session.execute(pg.SimpleQuery("ROLLBACK"), this)
        _phase = _WaitingRollback
      end

    | _WaitingCommit => _start_reads()

    | _WaitingRollback =>
      let table = PgOnlyTableName(_write_method, _col_type)
      let sql: String val =
        "SELECT val FROM " + table + " WHERE id = " + _inserted_id.string()
      session.execute(pg.SimpleQuery(sql), this)
      _phase = _PgVerifyingRollback

    | _PgVerifyingRollback =>
      match result
      | let rs: pg.ResultSet =>
        if rs.rows().size() == 0 then
          _complete()
        else
          _fail("Rollback failed: row still present after ROLLBACK")
        end
      else
        _fail("Rollback verify: expected ResultSet")
      end

    | _ReadingPgSimple =>
      match result
      | let rs: pg.ResultSet =>
        try
          let row = rs.rows()(0)?
          let field = row.fields(0)?
          _pg_simple_result = _col_type.normalize_pg(field.value)
        else
          _fail("pg SimpleQuery read: no rows/fields")
          return
        end
      else
        _fail("pg SimpleQuery read: expected ResultSet")
        return
      end

      let table = PgOnlyTableName(_write_method, _col_type)
      let sql: String val = "SELECT val FROM " + table + " WHERE id = $1"
      let params = recover val
        [as pg.FieldDataTypes: _inserted_id]
      end
      session.execute(pg.PreparedQuery(sql, params), this)
      _phase = _ReadingPgPrepared

    | _ReadingPgPrepared =>
      match result
      | let rs: pg.ResultSet =>
        try
          let row = rs.rows()(0)?
          let field = row.fields(0)?
          let pg_prepared_result = _col_type.normalize_pg(field.value)
          _compare(pg_prepared_result)
        else
          _fail("pg PreparedQuery read: no rows/fields")
        end
      else
        _fail("pg PreparedQuery read: expected ResultSet")
      end

    | _PgWaitingFinalDrop => _close_session()
    end

  be pg_query_failed(session: pg.Session, query: pg.Query,
    failure: (pg.ErrorResponseMessage | pg.ClientQueryError))
  =>
    if _phase is _PgWaitingFinalDrop then
      // Final teardown — the test has already completed. Close the session
      // and swallow the error; reporting a failure here has nowhere to go.
      _close_session()
      return
    end
    let phase_name = match _phase
    | _PgWaitingDropTable => "DROP TABLE"
    | _PgWaitingCreateTable => "CREATE TABLE"
    | _WaitingBegin => "BEGIN"
    | _WaitingInsert => "INSERT"
    | _WaitingCommit => "COMMIT"
    | _WaitingRollback => "ROLLBACK"
    | _PgVerifyingRollback => "rollback verify"
    | _ReadingPgSimple => "SimpleQuery read"
    | _ReadingPgPrepared => "PreparedQuery read"
    else "unknown phase"
    end
    let scenario_str = try (_scenario as TestScenario).string() else "?" end
    _fail("pg " + phase_name + " failed for " + scenario_str)

  fun ref _start_reads() =>
    try
      let session = _pg_session as pg.Session
      let table = PgOnlyTableName(_write_method, _col_type)
      let sql: String val =
        "SELECT val FROM " + table + " WHERE id = " + _inserted_id.string()
      session.execute(pg.SimpleQuery(sql), this)
      _phase = _ReadingPgSimple
    end

  fun ref _compare(pg_prepared_result: NormalizedValue) =>
    try
      let scenario = _scenario as TestScenario

      if not NormalizedEq(_pg_simple_result, scenario.expected) then
        _fail(scenario.string()
          + ": pg SimpleQuery disagrees with expected."
          + " pg_simple=" + NormalizedValueString(_pg_simple_result)
          + " expected=" + NormalizedValueString(scenario.expected))
        return
      end

      if not NormalizedEq(pg_prepared_result, scenario.expected) then
        _fail(scenario.string()
          + ": pg PreparedQuery disagrees with expected."
          + " pg_prepared=" + NormalizedValueString(pg_prepared_result)
          + " expected=" + NormalizedValueString(scenario.expected))
        return
      end

      if not NormalizedEq(_pg_simple_result, pg_prepared_result) then
        _fail(scenario.string()
          + ": pg SimpleQuery vs pg PreparedQuery disagree."
          + " pg_simple=" + NormalizedValueString(_pg_simple_result)
          + " pg_prepared=" + NormalizedValueString(pg_prepared_result))
        return
      end

      _complete()
    end

  fun ref _fail(msg: String val) =>
    match _ph
    | let ph: PropertyHelper =>
      ph.fail(msg)
      ph.complete_action("done")
      _ph = None
    end
    _phase = _Idle
    _tick_remaining()

  fun ref _complete() =>
    match _ph
    | let ph: PropertyHelper =>
      ph.complete_action("done")
      _ph = None
    end
    _phase = _Idle
    _tick_remaining()

  fun ref _tick_remaining() =>
    if _remaining > 0 then _remaining = _remaining - 1 end
    if _remaining == 0 then
      if _table_created then
        try
          let session = _pg_session as pg.Session
          let table = PgOnlyTableName(_write_method, _col_type)
          session.execute(pg.SimpleQuery("DROP TABLE IF EXISTS " + table), this)
          _phase = _PgWaitingFinalDrop
          return
        end
      end
      _close_session()
    end

  fun ref _close_session() =>
    match _pg_session
    | let s: pg.Session => s.close()
    end

class val _PgPendingSample
  let scenario: TestScenario
  let write_method: PgStatefulWriteMethod
  let tx_mode: TxMode
  let ph: PropertyHelper

  new val create(
    scenario': TestScenario,
    write_method': PgStatefulWriteMethod,
    tx_mode': TxMode,
    ph': PropertyHelper)
  =>
    scenario = scenario'
    write_method = write_method'
    tx_mode = tx_mode'
    ph = ph'
