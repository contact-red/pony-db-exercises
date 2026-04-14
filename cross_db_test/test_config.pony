// Write methods
primitive LiteralSelect
  fun name(): String val => "literal"
primitive OdbcParamSelect
  fun name(): String val => "odbc-param"
primitive PgParamSelect
  fun name(): String val => "pg-param"
primitive OdbcWrite
  fun name(): String val => "odbc-write"
primitive PgBinaryWrite
  fun name(): String val => "pg-bin-write"
primitive PgTextWrite
  fun name(): String val => "pg-txt-write"

type WriteMethod is
  ( LiteralSelect | OdbcParamSelect | PgParamSelect
  | OdbcWrite | PgBinaryWrite | PgTextWrite )

type StatelessWriteMethod is (LiteralSelect | OdbcParamSelect | PgParamSelect)
type StatefulWriteMethod is (OdbcWrite | PgBinaryWrite | PgTextWrite)

// Transaction modes
primitive Autocommit
  fun name(): String val => "autocommit"
primitive ExplicitCommit
  fun name(): String val => "commit"
primitive RollbackVerify
  fun name(): String val => "rollback"

type TxMode is (Autocommit | ExplicitCommit | RollbackVerify)
type DataTxMode is (Autocommit | ExplicitCommit)

// Helpers for table naming
primitive TableName
  fun apply(write: StatefulWriteMethod, col_type: ColType): String val =>
    let prefix = match write
    | OdbcWrite => "pbt_odbc_"
    | PgBinaryWrite => "pbt_pgbin_"
    | PgTextWrite => "pbt_pgtxt_"
    end
    prefix + col_type.pg_type_name().clone().>replace(" ", "_")
