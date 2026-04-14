use "pony_test"
use pg = "postgres"

class iso _TestNormalizeDateString is UnitTest
  fun name(): String => "normalize/date-string-parsing"

  fun apply(h: TestHelper) =>
    // Standard dates
    _check(h, "2024-03-15", NvDate(2024, 3, 15))
    _check(h, "2000-01-01", NvDate(2000, 1, 1))
    _check(h, "0001-01-01", NvDate(1, 1, 1))
    _check(h, "9999-12-28", NvDate(9999, 12, 28))

  fun _check(h: TestHelper, input: String, expected: NvDate) =>
    let result = ColDate._parse_pg_date_string(input.clone())
    match result
    | let d: NvDate =>
      h.assert_eq[I32](expected.year, d.year, "year for " + input)
      h.assert_eq[I32](expected.month, d.month, "month for " + input)
      h.assert_eq[I32](expected.day, d.day, "day for " + input)
    else
      h.fail("Failed to parse date: " + input)
    end

class iso _TestNormalizeTimeString is UnitTest
  fun name(): String => "normalize/time-string-parsing"

  fun apply(h: TestHelper) =>
    _check(h, "00:00:00", NvTime(0, 0, 0))
    _check(h, "12:30:45", NvTime(12, 30, 45))
    _check(h, "23:59:59", NvTime(23, 59, 59))
    // With fractional seconds (should be ignored)
    _check(h, "12:30:45.123456", NvTime(12, 30, 45))

  fun _check(h: TestHelper, input: String, expected: NvTime) =>
    let result = ColTime._parse_pg_time_string(input.clone())
    match result
    | let t: NvTime =>
      h.assert_eq[U16](expected.hour, t.hour, "hour for " + input)
      h.assert_eq[U16](expected.minute, t.minute, "minute for " + input)
      h.assert_eq[U16](expected.second, t.second, "second for " + input)
    else
      h.fail("Failed to parse time: " + input)
    end

class iso _TestNormalizeTimestampString is UnitTest
  fun name(): String => "normalize/timestamp-string-parsing"

  fun apply(h: TestHelper) =>
    _check(h, "2024-03-15 12:30:45", NvTimestamp(2024, 3, 15, 12, 30, 45, 0))
    _check(h, "2024-03-15 12:30:45.123456",
      NvTimestamp(2024, 3, 15, 12, 30, 45, 123456))
    _check(h, "2000-01-01 00:00:00", NvTimestamp(2000, 1, 1, 0, 0, 0, 0))
    _check(h, "2024-03-15 12:30:45.1",
      NvTimestamp(2024, 3, 15, 12, 30, 45, 100000))

  fun _check(h: TestHelper, input: String, expected: NvTimestamp) =>
    let result = ColTimestamp._parse_pg_timestamp_string(input.clone())
    match result
    | let ts: NvTimestamp =>
      h.assert_eq[I32](expected.year, ts.year, "year for " + input)
      h.assert_eq[I32](expected.month, ts.month, "month for " + input)
      h.assert_eq[I32](expected.day, ts.day, "day for " + input)
      h.assert_eq[U16](expected.hour, ts.hour, "hour for " + input)
      h.assert_eq[U16](expected.minute, ts.minute, "minute for " + input)
      h.assert_eq[U16](expected.second, ts.second, "second for " + input)
      h.assert_eq[U32](expected.microsecond, ts.microsecond,
        "microsecond for " + input)
    else
      h.fail("Failed to parse timestamp: " + input)
    end

class iso _TestNormalizedEqFloat is UnitTest
  fun name(): String => "normalize/float-equality"

  fun apply(h: TestHelper) =>
    // Exact zeros should be equal
    h.assert_true(
      NormalizedEq(NvFloat(0.0, FloatPrecisionF32), NvFloat(0.0, FloatPrecisionF32)),
      "0.0 == 0.0 (F32)")

    // F32 precision: values within relative epsilon should be equal
    h.assert_true(
      NormalizedEq(NvFloat(1.0, FloatPrecisionF32), NvFloat(1.0000001, FloatPrecisionF32)),
      "1.0 ~= 1.0000001 (F32)")

    // Values far apart should not be equal
    h.assert_false(
      NormalizedEq(NvFloat(1.0, FloatPrecisionF32), NvFloat(2.0, FloatPrecisionF32)),
      "1.0 != 2.0 (F32)")

    // F64 precision: tighter epsilon
    h.assert_true(
      NormalizedEq(NvFloat(1.0, FloatPrecisionF64), NvFloat(1.0, FloatPrecisionF64)),
      "1.0 == 1.0 (F64)")

    // Type mismatch should fail
    h.assert_false(
      NormalizedEq(NvInt(1), NvFloat(1.0, FloatPrecisionF64)),
      "NvInt != NvFloat")

    // Null equality
    h.assert_true(
      NormalizedEq(NvNull, NvNull),
      "NULL == NULL")
