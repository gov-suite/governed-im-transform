import { governedImRDS as gimRDS } from "../deps.ts";
import * as diaImpl from "../dialect.ts";
import * as naming from "../naming.ts";

export class SQLiteDialect extends diaImpl.ANSI {
  public static readonly dialectName = gimRDS.SQLiteEngineName;
  public static readonly dialectNameAliases = [
    SQLiteDialect.dialectName,
  ];

  constructor(
    ns: gimRDS.NamingStrategy | gimRDS.NamingStrategyConstructor =
      naming.UppercaseObjectNames,
  ) {
    super(SQLiteDialect.dialectName, ns);
  }

  autoIdentityNativeColumnDDL(
    defn: diaImpl.ColumnSqlDdlGenInput,
  ): diaImpl.ColumnSqlDDL {
    return {
      columnDDL:
        `${defn.columnName} ${defn.sqlTypes.nonRefDDL} PRIMARY KEY AUTOINCREMENT`,
    };
  }
}
