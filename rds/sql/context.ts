import {
  artfPersist as ap,
  contextMgr as cm,
  governedImRDS as gimRDS,
} from "./deps.ts";
import type * as sqlTr from "./transform.ts";

export interface RdbmsModelSqlTransformerContext
  extends gimRDS.RdbmsModelContext {
  isRdbmsModelSqlTransformerContext: true;
  options: sqlTr.RdbmsModelTransformerOptions;
  transformations: { [artfName: string]: ap.PersistenceResult | undefined };
}

export function isRdbmsModelSqlTransformerContext(
  ctx: cm.Context,
): ctx is RdbmsModelSqlTransformerContext {
  return "isRdbmsModelSqlTransformerContext" in ctx;
}

export const rdbmsCtxFactory = new (class {
  public rdbmsModelSqlTransformerContext(
    options: sqlTr.RdbmsModelTransformerOptions,
  ): RdbmsModelSqlTransformerContext {
    return {
      isContext: true,
      isSpecificationContext: true,
      isRdbmsEngineContext: true,
      isRdbmsModelContext: true,
      isRdbmsModelSqlTransformerContext: true,
      spec: options.spec,
      rdbmsModel: options.rdbmsModel,
      dialect: options.dialect,
      options: options,
      execEnvs: cm.ctxFactory.envTODO,
      transformations: {},
      isPostgreSQL: gimRDS.rdbmsCtxFactory.isPostgreSqlEngine(options.dialect),
      isSQLite: gimRDS.rdbmsCtxFactory.isSQLiteEngine(options.dialect),
    };
  }

  public rdbmsModelContext(
    options: sqlTr.RdbmsModelTransformerOptions,
  ): gimRDS.RdbmsModelContext {
    return {
      ...this.rdbmsModelSqlTransformerContext(options),
      isRdbmsModelContext: true,
      isRdbmsEngineContext: true,
      rdbmsModel: options.rdbmsModel,
    };
  }

  public rdbmsSqlDdlContext(
    options: sqlTr.RdbmsModelTransformerOptions,
  ): gimRDS.RdbmsSqlDdlContext {
    return {
      ...this.rdbmsModelContext(options),
      isRdbmsSqlDdlContext: true,
    };
  }

  public rdbmsTableSqlDdlContext(
    options: sqlTr.RdbmsModelTransformerOptions,
    table: gimRDS.Table,
  ): gimRDS.RdbmsTableSqlDdlContext {
    const ddlCtx = this.rdbmsSqlDdlContext(options);
    return {
      ...ddlCtx,
      isRdbmsTableSqlDdlContext: true,
      table: table,
      tableName: table.name(ddlCtx),
    };
  }

  public rdbmsSqlValueContext(
    rsCtx: gimRDS.RdbmsModelContext,
  ): gimRDS.RdbmsSqlValueContext {
    return {
      ...rsCtx,
      isRdbmsSqlValueContext: true,
    };
  }

  public rdbmsContentTableSqlDmlContext(
    options: sqlTr.RdbmsModelTransformerOptions,
    table: gimRDS.Table,
  ): gimRDS.RdbmsContentTableSqlDmlContext {
    return {
      ...this.rdbmsSqlDdlContext(options),
      isRdbmsContentTableSqlDmlContext: true,
      table: table,
    };
  }

  public rdbmsContentInsertContext(
    options: sqlTr.RdbmsModelTransformerOptions,
    table: gimRDS.Table,
  ): gimRDS.RdbmsContentInsertContext {
    return {
      ...this.rdbmsContentTableSqlDmlContext(options, table),
      isRdbmsContentInsertContext: true,
    };
  }

  public rdbmsSqlDmlUpdateContext(
    options: sqlTr.RdbmsModelTransformerOptions,
    rdbmsModel: gimRDS.RdbmsModelStruct,
    table: gimRDS.Table,
  ): gimRDS.RdbmsContentUpdateContext {
    return {
      ...this.rdbmsContentTableSqlDmlContext(options, table),
      isRdbmsContentUpdateContext: true,
    };
  }

  public rdbmsContentDeleteContext(
    options: sqlTr.RdbmsModelTransformerOptions,
    rdbmsModel: gimRDS.RdbmsModelStruct,
    table: gimRDS.Table,
  ): gimRDS.RdbmsContentDeleteContext {
    return {
      ...this.rdbmsContentTableSqlDmlContext(options, table),
      isRdbmsContentDeleteContext: true,
    };
  }
})();
