primitive FloatPrecisionF32
primitive FloatPrecisionF64
type FloatPrecision is (FloatPrecisionF32 | FloatPrecisionF64)

type NormalizedValue is
  ( NvNull
  | NvBool
  | NvInt
  | NvFloat
  | NvText
  | NvDate
  | NvTime
  | NvTimestamp )

primitive NvNull is Stringable
  fun string(): String iso^ => "NULL".clone()

class val NvBool is (Equatable[NvBool] & Stringable)
  let value: Bool
  new val create(v: Bool) => value = v
  fun eq(that: box->NvBool): Bool => value == that.value
  fun string(): String iso^ => value.string()

class val NvInt is (Equatable[NvInt] & Stringable)
  let value: I64
  new val create(v: I64) => value = v
  fun eq(that: box->NvInt): Bool => value == that.value
  fun string(): String iso^ => value.string()

class val NvFloat is (Stringable)
  let value: F64
  let precision: FloatPrecision
  new val create(v: F64, precision': FloatPrecision) =>
    value = v
    precision = precision'
  fun string(): String iso^ => value.string()

class val NvText is (Equatable[NvText] & Stringable)
  let value: String val
  new val create(v: String val) => value = v
  fun eq(that: box->NvText): Bool => value == that.value
  fun string(): String iso^ => value.clone()

class val NvDate is (Equatable[NvDate] & Stringable)
  let year: I32
  let month: I32
  let day: I32
  new val create(y: I32, m: I32, d: I32) =>
    year = y; month = m; day = d
  fun eq(that: box->NvDate): Bool =>
    (year == that.year) and (month == that.month) and (day == that.day)
  fun string(): String iso^ =>
    (year.string() + "-" + month.string() + "-" + day.string()).clone()

class val NvTime is (Equatable[NvTime] & Stringable)
  let hour: U16
  let minute: U16
  let second: U16
  new val create(h: U16, m: U16, s: U16) =>
    hour = h; minute = m; second = s
  fun eq(that: box->NvTime): Bool =>
    (hour == that.hour) and (minute == that.minute) and (second == that.second)
  fun string(): String iso^ =>
    (hour.string() + ":" + minute.string() + ":" + second.string()).clone()

class val NvTimestamp is (Equatable[NvTimestamp] & Stringable)
  let year: I32
  let month: I32
  let day: I32
  let hour: U16
  let minute: U16
  let second: U16
  let microsecond: U32
  new val create(
    y: I32, mo: I32, d: I32,
    h: U16, mi: U16, s: U16, us: U32 = 0)
  =>
    year = y; month = mo; day = d
    hour = h; minute = mi; second = s
    microsecond = us
  fun eq(that: box->NvTimestamp): Bool =>
    (year == that.year) and (month == that.month) and (day == that.day) and
    (hour == that.hour) and (minute == that.minute) and
    (second == that.second) and (microsecond == that.microsecond)
  fun string(): String iso^ =>
    let base = year.string() + "-" + month.string() + "-" + day.string()
      + " " + hour.string() + ":" + minute.string() + ":" + second.string()
    if microsecond > 0 then
      (base + "." + microsecond.string()).clone()
    else
      base.clone()
    end

primitive NormalizedEq
  """
  Compare two NormalizedValues with type-appropriate logic.
  Float comparison uses combined absolute + relative epsilon.
  """
  fun apply(a: NormalizedValue, b: NormalizedValue): Bool =>
    match (a, b)
    | (NvNull, NvNull) => true
    | (let x: NvBool, let y: NvBool) => x.value == y.value
    | (let x: NvInt, let y: NvInt) => x.value == y.value
    | (let x: NvFloat, let y: NvFloat) => _float_eq(x, y)
    | (let x: NvText, let y: NvText) => x.value == y.value
    | (let x: NvDate, let y: NvDate) => x == y
    | (let x: NvTime, let y: NvTime) => x == y
    | (let x: NvTimestamp, let y: NvTimestamp) => x == y
    else
      false
    end

  fun _float_eq(x: NvFloat, y: NvFloat): Bool =>
    let diff = (x.value - y.value).abs()
    let scale = x.value.abs().max(y.value.abs())
    let eps: F64 = match x.precision
    | FloatPrecisionF32 => (scale * 1.2e-7).max(1.2e-7)
    | FloatPrecisionF64 => (scale * 2.3e-16).max(2.3e-16)
    end
    diff <= eps

primitive NormalizedValueString
  """
  Convert a NormalizedValue to a debug string.
  """
  fun apply(nv: NormalizedValue): String val =>
    match nv
    | NvNull => "NULL"
    | let v: NvBool => v.string()
    | let v: NvInt => v.string()
    | let v: NvFloat => v.string()
    | let v: NvText => "'" + v.value + "'"
    | let v: NvDate => v.string()
    | let v: NvTime => v.string()
    | let v: NvTimestamp => v.string()
    end
