use "pony_check"
use pg = "postgres"
use "odbc"
use "collections"

interface val ColType
  """
  A SQL column type. Knows its PostgreSQL DDL name, how to generate test
  scenarios, and how to normalize results from each library.
  """
  fun pg_type_name(): String val
  fun test_name(): String val => pg_type_name()
  fun gen_scenario(rnd: Randomness): TestScenario
  fun normalize_odbc(row: Row, col: ColIndex): NormalizedValue ?
  fun normalize_pg(fd: pg.FieldData): NormalizedValue

// ---------------------------------------------------------------------------
// Boolean
// ---------------------------------------------------------------------------

primitive ColBoolean is ColType
  fun pg_type_name(): String val => "boolean"

  fun gen_scenario(rnd: Randomness): TestScenario =>
    if rnd.u8(0, 19) == 0 then
      TestScenario(this, "NULL", NvNull)
    else
      let v = rnd.bool()
      let lit = if v then "TRUE" else "FALSE" end
      TestScenario(this, lit, NvBool(v))
    end

  fun normalize_odbc(row: Row, col: ColIndex): NormalizedValue ? =>
    match row.bool(col)?
    | let v: Bool => NvBool(v)
    | SqlNull => NvNull
    end

  fun normalize_pg(fd: pg.FieldData): NormalizedValue =>
    match fd
    | let v: Bool => NvBool(v)
    | let v: String =>
      // SimpleQuery text format: "t"/"f"
      NvBool(v == "t")
    | None => NvNull
    else NvNull
    end

// ---------------------------------------------------------------------------
// Tinyint (I8) — PostgreSQL has no tinyint; uses smallint as the column type.
// Generator constrains values to I8 range. psqlODBC reports SQL_SMALLINT,
// so normalize_odbc accepts both SqlTinyInt and SqlSmallInt.
// ---------------------------------------------------------------------------

primitive ColTinyint is ColType
  fun pg_type_name(): String val => "smallint"
  fun test_name(): String val => "tinyint"

  fun gen_scenario(rnd: Randomness): TestScenario =>
    if rnd.u8(0, 19) == 0 then
      TestScenario(this, "NULL", NvNull)
    else
      let v = rnd.i8()
      TestScenario(this, v.string(), NvInt(v.i64()))
    end

  fun normalize_odbc(row: Row, col: ColIndex): NormalizedValue ? =>
    match row.column(col)?
    | SqlNull => NvNull
    | let v: SqlTinyInt => NvInt(v.value.i64())
    | let v: SqlSmallInt => NvInt(v.value.i64())
    else NvNull
    end

  fun normalize_pg(fd: pg.FieldData): NormalizedValue =>
    match fd
    | let v: I16 => NvInt(v.i64())
    | None => NvNull
    else NvNull
    end

// ---------------------------------------------------------------------------
// Smallint (I16)
// ---------------------------------------------------------------------------

primitive ColSmallint is ColType
  fun pg_type_name(): String val => "smallint"

  fun gen_scenario(rnd: Randomness): TestScenario =>
    if rnd.u8(0, 19) == 0 then
      TestScenario(this, "NULL", NvNull)
    else
      let v = rnd.i16()
      TestScenario(this, v.string(), NvInt(v.i64()))
    end

  fun normalize_odbc(row: Row, col: ColIndex): NormalizedValue ? =>
    match row.column(col)?
    | SqlNull => NvNull
    | let v: SqlSmallInt => NvInt(v.value.i64())
    else NvNull
    end

  fun normalize_pg(fd: pg.FieldData): NormalizedValue =>
    match fd
    | let v: I16 => NvInt(v.i64())
    | None => NvNull
    else NvNull
    end

// ---------------------------------------------------------------------------
// Integer (I32)
// ---------------------------------------------------------------------------

primitive ColInteger is ColType
  fun pg_type_name(): String val => "integer"

  fun gen_scenario(rnd: Randomness): TestScenario =>
    if rnd.u8(0, 19) == 0 then
      TestScenario(this, "NULL", NvNull)
    else
      let v = rnd.i32()
      TestScenario(this, v.string(), NvInt(v.i64()))
    end

  fun normalize_odbc(row: Row, col: ColIndex): NormalizedValue ? =>
    match row.column(col)?
    | SqlNull => NvNull
    | let v: SqlInteger => NvInt(v.value.i64())
    else NvNull
    end

  fun normalize_pg(fd: pg.FieldData): NormalizedValue =>
    match fd
    | let v: I32 => NvInt(v.i64())
    | None => NvNull
    else NvNull
    end

// ---------------------------------------------------------------------------
// Bigint (I64)
// ---------------------------------------------------------------------------

primitive ColBigint is ColType
  fun pg_type_name(): String val => "bigint"

  fun gen_scenario(rnd: Randomness): TestScenario =>
    if rnd.u8(0, 19) == 0 then
      TestScenario(this, "NULL", NvNull)
    else
      let v = rnd.i64()
      TestScenario(this, v.string(), NvInt(v))
    end

  fun normalize_odbc(row: Row, col: ColIndex): NormalizedValue ? =>
    match row.column(col)?
    | SqlNull => NvNull
    | let v: SqlBigInt => NvInt(v.value)
    else NvNull
    end

  fun normalize_pg(fd: pg.FieldData): NormalizedValue =>
    match fd
    | let v: I64 => NvInt(v)
    | None => NvNull
    else NvNull
    end

// ---------------------------------------------------------------------------
// Real (F32)
// ---------------------------------------------------------------------------

primitive ColReal is ColType
  fun pg_type_name(): String val => "real"

  fun gen_scenario(rnd: Randomness): TestScenario =>
    if rnd.u8(0, 19) == 0 then
      TestScenario(this, "NULL", NvNull)
    else
      // Generate F64, truncate to F32 range to avoid overflow.
      // Compute expected from the parsed literal, not the original value,
      // because F32.string() may lose precision. The database will parse the
      // literal, so expected must match what the database sees.
      let v = rnd.f64(-1e6, 1e6).f32()
      let lit: String val = v.string()
      let expected_v = try lit.f32()? else F32(0) end
      TestScenario(this, lit, NvFloat(expected_v.f64(), FloatPrecisionF32))
    end

  fun normalize_odbc(row: Row, col: ColIndex): NormalizedValue ? =>
    // ODBC reads REAL as F64 via SQL_C_DOUBLE.
    // Truncate to F32 before normalizing so both sides compare at F32 precision.
    match row.column(col)?
    | SqlNull => NvNull
    | let v: SqlFloat => NvFloat(v.value.f32().f64(), FloatPrecisionF32)
    else NvNull
    end

  fun normalize_pg(fd: pg.FieldData): NormalizedValue =>
    match fd
    | let v: F32 => NvFloat(v.f64(), FloatPrecisionF32)
    | let v: String =>
      // SimpleQuery text format: parse to F32 then promote
      try NvFloat(v.f32()?.f64(), FloatPrecisionF32)
      else NvNull
      end
    | None => NvNull
    else NvNull
    end

// ---------------------------------------------------------------------------
// Double precision (F64)
// ---------------------------------------------------------------------------

primitive ColDouble is ColType
  fun pg_type_name(): String val => "double precision"

  fun gen_scenario(rnd: Randomness): TestScenario =>
    if rnd.u8(0, 19) == 0 then
      TestScenario(this, "NULL", NvNull)
    else
      // Compute expected from the parsed literal for the same reason as ColReal.
      let v = rnd.f64(-1e15, 1e15)
      let lit: String val = v.string()
      let expected_v = try lit.f64()? else F64(0) end
      TestScenario(this, lit, NvFloat(expected_v, FloatPrecisionF64))
    end

  fun normalize_odbc(row: Row, col: ColIndex): NormalizedValue ? =>
    match row.column(col)?
    | SqlNull => NvNull
    | let v: SqlFloat => NvFloat(v.value, FloatPrecisionF64)
    else NvNull
    end

  fun normalize_pg(fd: pg.FieldData): NormalizedValue =>
    match fd
    | let v: F64 => NvFloat(v, FloatPrecisionF64)
    | let v: String =>
      try NvFloat(v.f64()?, FloatPrecisionF64)
      else NvNull
      end
    | None => NvNull
    else NvNull
    end

// ---------------------------------------------------------------------------
// Text
// ---------------------------------------------------------------------------

primitive ColText is ColType
  fun pg_type_name(): String val => "text"

  fun gen_scenario(rnd: Randomness): TestScenario =>
    if rnd.u8(0, 19) == 0 then
      TestScenario(this, "NULL", NvNull)
    else
      let len = rnd.usize(0, 100)
      let raw = recover val
        let buf = String(len)
        var i: USize = 0
        while i < len do
          buf.push(rnd.u8(0x20, 0x7E)) // printable ASCII
          i = i + 1
        end
        buf
      end
      // Escape single quotes for SQL literal
      let escaped = recover val raw.clone().>replace("'", "''") end
      TestScenario(this, "'" + escaped + "'", NvText(raw))
    end

  fun normalize_odbc(row: Row, col: ColIndex): NormalizedValue ? =>
    match row.column(col)?
    | SqlNull => NvNull
    | let v: SqlText => NvText(v.value)
    else NvNull
    end

  fun normalize_pg(fd: pg.FieldData): NormalizedValue =>
    match fd
    | let v: String => NvText(v)
    | None => NvNull
    else NvNull
    end

// ---------------------------------------------------------------------------
// Date
// ---------------------------------------------------------------------------

primitive ColDate is ColType
  fun pg_type_name(): String val => "date"

  fun gen_scenario(rnd: Randomness): TestScenario =>
    if rnd.u8(0, 19) == 0 then
      TestScenario(this, "NULL", NvNull)
    else
      let y = rnd.i32(1, 9999)
      let m = rnd.i32(1, 12)
      let d = rnd.i32(1, 28) // avoid month-end edge cases
      let lit: String val = "'" + _pad4(y) + "-" + _pad2(m) + "-" + _pad2(d) + "'"
      TestScenario(this, lit, NvDate(y, m, d))
    end

  fun normalize_odbc(row: Row, col: ColIndex): NormalizedValue ? =>
    match row.column(col)?
    | SqlNull => NvNull
    | let v: SqlDate => NvDate(v.year.i32(), v.month.i32(), v.day.i32())
    else NvNull
    end

  fun normalize_pg(fd: pg.FieldData): NormalizedValue =>
    match fd
    | let v: pg.PgDate =>
      // Parse PgDate.string() which returns "YYYY-MM-DD"
      _parse_pg_date_string(v.string())
    | None => NvNull
    else NvNull
    end

  fun _parse_pg_date_string(s: String iso): NormalizedValue =>
    // Format: "YYYY-MM-DD" (zero-padded)
    let sv: String val = consume s
    try
      let parts = sv.split("-")
      let y = parts(0)?.i32()?
      let m = parts(1)?.i32()?
      let d = parts(2)?.i32()?
      NvDate(y, m, d)
    else
      NvNull
    end

  fun _pad2(v: I32): String val =>
    if v < 10 then "0" + v.string() else v.string() end

  fun _pad4(v: I32): String val =>
    if v < 10 then "000" + v.string()
    elseif v < 100 then "00" + v.string()
    elseif v < 1000 then "0" + v.string()
    else v.string()
    end

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

primitive ColTime is ColType
  fun pg_type_name(): String val => "time"

  fun gen_scenario(rnd: Randomness): TestScenario =>
    if rnd.u8(0, 19) == 0 then
      TestScenario(this, "NULL", NvNull)
    else
      let h = rnd.u16(0, 23)
      let m = rnd.u16(0, 59)
      let s = rnd.u16(0, 59)
      let lit: String val = "'" + _pad2(h) + ":" + _pad2(m) + ":" + _pad2(s) + "'"
      TestScenario(this, lit, NvTime(h, m, s))
    end

  fun normalize_odbc(row: Row, col: ColIndex): NormalizedValue ? =>
    match row.column(col)?
    | SqlNull => NvNull
    | let v: SqlTime => NvTime(v.hour, v.minute, v.second)
    else NvNull
    end

  fun normalize_pg(fd: pg.FieldData): NormalizedValue =>
    match fd
    | let v: pg.PgTime =>
      // Parse PgTime.string() which returns "HH:MM:SS"
      _parse_pg_time_string(v.string())
    | None => NvNull
    else NvNull
    end

  fun _parse_pg_time_string(raw: String iso): NormalizedValue =>
    let sv: String val = consume raw
    try
      let parts = sv.split(":")
      let h = parts(0)?.u16()?
      let m = parts(1)?.u16()?
      // seconds might have fractional part, take only integer
      let sec_str = parts(2)?
      let sec_parts = sec_str.split(".")
      let sec = sec_parts(0)?.u16()?
      NvTime(h, m, sec)
    else
      NvNull
    end

  fun _pad2(v: U16): String val =>
    if v < 10 then "0" + v.string() else v.string() end

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

primitive ColTimestamp is ColType
  fun pg_type_name(): String val => "timestamp"

  fun gen_scenario(rnd: Randomness): TestScenario =>
    if rnd.u8(0, 19) == 0 then
      TestScenario(this, "NULL", NvNull)
    else
      let y = rnd.i32(1, 9999)
      let mo = rnd.i32(1, 12)
      let d = rnd.i32(1, 28)
      let h = rnd.u16(0, 23)
      let mi = rnd.u16(0, 59)
      let s = rnd.u16(0, 59)
      let us = rnd.u32(0, 999999)
      let lit: String val = "'" + _pad4(y) + "-" + _pad2i(mo) + "-" + _pad2i(d)
        + " " + _pad2u(h) + ":" + _pad2u(mi) + ":" + _pad2u(s)
        + _frac_str(us) + "'"
      TestScenario(this, lit, NvTimestamp(y, mo, d, h, mi, s, us))
    end

  fun normalize_odbc(row: Row, col: ColIndex): NormalizedValue ? =>
    match row.column(col)?
    | SqlNull => NvNull
    | let v: SqlTimestamp =>
      // SqlTimestamp.fraction is nanoseconds; convert to microseconds
      let us = v.fraction / 1000
      NvTimestamp(
        v.year.i32(), v.month.i32(), v.day.i32(),
        v.hour, v.minute, v.second, us)
    else NvNull
    end

  fun normalize_pg(fd: pg.FieldData): NormalizedValue =>
    match fd
    | let v: pg.PgTimestamp =>
      // Parse PgTimestamp.string() which returns "YYYY-MM-DD HH:MM:SS[.ffffff]"
      _parse_pg_timestamp_string(v.string())
    | None => NvNull
    else NvNull
    end

  fun _parse_pg_timestamp_string(raw: String iso): NormalizedValue =>
    let sv: String val = consume raw
    try
      // Split "YYYY-MM-DD HH:MM:SS.ffffff" into date and time parts
      let dt_parts = sv.split(" ")
      let date_str = dt_parts(0)?
      let time_str = dt_parts(1)?

      let date_parts = date_str.split("-")
      let y = date_parts(0)?.i32()?
      let mo = date_parts(1)?.i32()?
      let d = date_parts(2)?.i32()?

      let time_parts = time_str.split(":")
      let h = time_parts(0)?.u16()?
      let mi = time_parts(1)?.u16()?

      // Seconds may have fractional part
      let sec_str = time_parts(2)?
      let sec_parts = sec_str.split(".")
      let sec = sec_parts(0)?.u16()?
      let us: U32 = try
        let frac_str = sec_parts(1)?
        // Pad to 6 digits
        let padded = recover val
          let buf = String(6)
          buf.append(frac_str)
          while buf.size() < 6 do buf.push('0') end
          buf
        end
        padded.u32()?
      else
        0
      end

      NvTimestamp(y, mo, d, h, mi, sec, us)
    else
      NvNull
    end

  fun _pad4(v: I32): String val =>
    if v < 10 then "000" + v.string()
    elseif v < 100 then "00" + v.string()
    elseif v < 1000 then "0" + v.string()
    else v.string()
    end

  fun _pad2i(v: I32): String val =>
    if v < 10 then "0" + v.string() else v.string() end

  fun _pad2u(v: U16): String val =>
    if v < 10 then "0" + v.string() else v.string() end

  fun _frac_str(us: U32): String val =>
    """
    Format microseconds as a fractional seconds suffix: ".NNNNNN"
    Left-padded to 6 digits, trailing zeros trimmed.
    Example: 83512 → ".083512", 100000 → ".1", 0 → ""
    """
    if us == 0 then
      ""
    else
      // Build the 6-digit zero-padded representation
      let us_str: String val = us.string()
      let full = recover val
        let buf = String(7)
        buf.push('.')
        // Left-pad with zeros to exactly 6 digits
        var pad: USize = 6 - us_str.size()
        while pad > 0 do
          buf.push('0')
          pad = pad - 1
        end
        buf.append(us_str)
        buf
      end
      // Trim trailing zeros
      var end_idx = full.size()
      try
        while (end_idx > 0) and (full(end_idx - 1)? == '0') do
          end_idx = end_idx - 1
        end
      end
      full.trim(0, end_idx)
    end
