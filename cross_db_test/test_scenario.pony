use "pony_check"

class val TestScenario is Stringable
  """
  A single test case: a SQL type, a literal value to cast, and the
  expected normalized result.
  """
  let col_type: ColType
  let sql_literal: String val
  let expected: NormalizedValue

  new val create(
    col_type': ColType,
    sql_literal': String val,
    expected': NormalizedValue)
  =>
    col_type = col_type'
    sql_literal = sql_literal'
    expected = expected'

  fun select_sql(): String val =>
    "SELECT " + sql_literal + "::" + col_type.pg_type_name()

  fun insert_sql(table: String val): String val =>
    "INSERT INTO " + table + " (val) VALUES (" + sql_literal
      + "::" + col_type.pg_type_name() + ") RETURNING id"

  fun select_by_id_sql(table: String val, id: I64): String val =>
    "SELECT val FROM " + table + " WHERE id = " + id.string()

  fun string(): String iso^ =>
    (sql_literal + "::" + col_type.pg_type_name()).clone()

primitive TestScenarioGenerator
  fun apply(col_type: ColType): Generator[TestScenario] =>
    let ct: ColType = col_type
    Generator[TestScenario](
      object is GenObj[TestScenario]
        fun generate(rnd: Randomness): TestScenario^ =>
          ct.gen_scenario(rnd)
      end)
