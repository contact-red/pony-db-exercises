use "pony_test"
use "pony_check"
use "odbc"
use "lib:odbc"
use pg = "postgres"
use lori = "lori"
use "constrained_types"

use @getenv[Pointer[U8] ref](name: Pointer[U8] tag)

primitive _ConnReason
  """
  Human-readable string for a pg ConnectionFailureReason. Used so CI output
  says *why* a pg connection failed rather than just "pg connection failed".
  """
  fun apply(reason: pg.ConnectionFailureReason): String =>
    match reason
    | pg.ConnectionFailedDNS => "DNS resolution failed"
    | pg.ConnectionFailedTCP => "TCP connection failed (server unreachable)"
    | pg.ConnectionFailedTimeout => "connection timed out"
    | pg.ConnectionFailedTimerError => "connect timer subscription failed"
    | pg.ConnectionClosedByServer =>
      "server closed connection before ready"
    | pg.SSLServerRefused => "server refused SSL"
    | pg.TLSHandshakeFailed => "TLS handshake failed"
    | pg.TLSAuthFailed => "TLS certificate verification failed"
    | pg.UnsupportedAuthenticationMethod =>
      "server requested an unsupported authentication method"
    | pg.AuthenticationMethodRejected =>
      "server auth method rejected by AuthRequirement policy"
    | pg.ServerVerificationFailed => "SCRAM server verification failed"
    | pg.ProtocolViolation => "protocol violation"
    | let e: pg.InvalidPassword =>
      "invalid password (28P01): " + e.response().message
    | let e: pg.InvalidAuthorizationSpecification =>
      "invalid authorization spec (28000): " + e.response().message
    | let e: pg.TooManyConnections =>
      "too many connections (53300): " + e.response().message
    | let e: pg.InvalidDatabaseName =>
      "invalid database name (3D000): " + e.response().message
    | let e: pg.ServerRejected =>
      "server rejected startup (" + e.response().code + "): "
        + e.response().message
    end

actor _PgStartupProbe is pg.SessionStatusNotify
  """
  One-shot connectivity check for the pg driver. Prints a single line to
  stdout reporting either success or the specific ConnectionFailureReason.
  Runs independently of PonyTest so CI logs show up-front whether the pg
  driver can reach the database.
  """
  let _env: Env
  var _session: (pg.Session | None) = None
  var _reported: Bool = false

  new create(env: Env) =>
    _env = env
    match lori.MakeConnectionTimeout(5_000)
    | let ct: lori.ConnectionTimeout =>
      let server = pg.ServerConnectInfo(
        lori.TCPConnectAuth(_env.root), "postgres", "5432"
        where auth_requirement' = pg.AllowAnyAuth,
        connection_timeout' = ct)
      let db = pg.DatabaseConnectInfo("postgres", "postgres", "postgres")
      _session = pg.Session(server, db, this)
    | let _: ValidationFailure =>
      _env.out.print("pg startup probe: invalid connection timeout")
    end

  be pg_session_authenticated(session: pg.Session) =>
    if not _reported then
      _env.out.print("pg connection OK (postgres:5432)")
      _reported = true
    end
    session.close()

  be pg_session_connection_failed(session: pg.Session,
    reason: pg.ConnectionFailureReason)
  =>
    if not _reported then
      _env.out.print("pg connection FAILED: " + _ConnReason(reason))
      _reported = true
    end

primitive _EnvSamples
  """
  Read PONYCHECK_SAMPLES from the environment. Defaults to 100.
  Uses C FFI because PonyTest's tests() is `fun tag` and cannot access Env.

  Set before running:
    PONYCHECK_SAMPLES=1000 ./build/cross_db_test --sequential
  """
  fun tag apply(): USize =>
    let ptr = @getenv("PONYCHECK_SAMPLES".cstring())
    if ptr.is_null() then return 100 end
    try
      String.copy_cstring(ptr).usize()?
    else
      100
    end

actor Main is TestList
  new create(env: Env) =>
    // Startup connectivity probes — print up-front in CI logs so it's
    // unambiguous which driver (if either) can't reach the database.
    match Odbc.connect(Dsn("DSN=psqlred"))
    | let conn: Connection =>
      env.out.print("ODBC connection OK (DSN=psqlred)")
      conn.close()
    | let e: ConnectError =>
      env.out.print("ODBC connection FAILED: " + e.string())
    end

    _PgStartupProbe(env)

    PonyTest(env, this)

  new make() => None

  fun tag tests(test: PonyTest) =>
    let n: USize = _EnvSamples()

    let col_types: Array[ColType] val = [
      ColBoolean; ColTinyint; ColSmallint; ColInteger; ColBigint
      ColReal; ColDouble; ColText
      ColDate; ColTime; ColTimestamp
    ]

    for ct in col_types.values() do
      // --- Stateless properties (4 per type) ---

      // S1: literal → ODBC decode
      test(Property1UnitTest[TestScenario](
        OdbcOnlyProperty(ct, OdbcLiteral, n)))

      // S2: literal → pg SimpleQuery decode
      test(Property1UnitTest[TestScenario](
        PgOnlyProperty(ct, PgLiteral, n)))

      // S3: ODBC param → ODBC roundtrip
      test(Property1UnitTest[TestScenario](
        OdbcOnlyProperty(ct, OdbcParam, n)))

      // S4: pg param → pg PreparedQuery roundtrip
      test(Property1UnitTest[TestScenario](
        PgOnlyProperty(ct, PgParam, n)))

      // --- Stateful properties (9 per type) ---

      let write_methods: Array[StatefulWriteMethod] val =
        [OdbcWrite; PgBinaryWrite; PgTextWrite]

      let data_tx_modes: Array[DataTxMode] val =
        [Autocommit; ExplicitCommit]

      for wm in write_methods.values() do
        // Data properties: write once → read all three ways
        for tm in data_tx_modes.values() do
          test(Property1UnitTest[TestScenario](
            StatefulProperty(ct, wm, tm, n)))
        end

        // Rollback verification
        test(Property1UnitTest[TestScenario](
          StatefulProperty(ct, wm, RollbackVerify, n)))
      end
    end

    // --- Normalizer unit tests ---
    test(_TestNormalizeDateString)
    test(_TestNormalizeTimeString)
    test(_TestNormalizeTimestampString)
    test(_TestNormalizedEqFloat)
