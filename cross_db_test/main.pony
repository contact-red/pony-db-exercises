use "pony_test"
use "pony_check"
use "odbc"
use "lib:odbc"

use @getenv[Pointer[U8] ref](name: Pointer[U8] tag)

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
    // Log ODBC driver version at startup
    match Odbc.connect(Dsn("DSN=psqlred"))
    | let conn: Connection =>
      env.out.print("ODBC connection OK — driver version logging TBD")
      conn.close()
    | let e: ConnectError =>
      env.out.print("ODBC connection failed: " + e.string())
    end

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
