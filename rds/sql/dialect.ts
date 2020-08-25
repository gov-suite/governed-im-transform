import * as sqlTrCtx from "./context.ts";
import {
  artfPersist as ap,
  governedIM as gimc,
  governedImRDS as gimRDS,
  polyglotArtfNature,
  textWhitespace as tw,
  valueMgr as vm,
} from "./deps.ts";
import { defaultAttrRdbmsEngineMapper } from "./dialect/ansi-sql-types.ts";

export interface ColumnSqlDDL {
  readonly columnDDL: string;
  readonly tableDDL?: string;
}

export interface TableSqlDDL {
  readonly tableDDL: string;
}

export interface ContentSql {
  readonly sql: string[];
  readonly persistDestination?: ap.PersistenceDestinationSupplier;
}

export interface ColumnSqlDdlGenInput {
  readonly ctx: gimRDS.RdbmsTableSqlDdlContext;
  readonly column: gimRDS.PersistentColumn;
  readonly columnName: string;
  readonly primaryKey: string;
  readonly notNull: string;
  readonly sqlTypes: gimRDS.ContextualSqlTypes;
}

export type ColumnValueSqlDML = string;
export type InsertRowSqlDML = string;

export interface PersistableArtifact {
  readonly destAs: ap.PersistenceDestAs;
  readonly artifact: ap.MutableTextArtifact;
}

export interface PersistableArtifacts {
  readonly beforeStructs: PersistableArtifact[];
  readonly main: PersistableArtifact;
  readonly structs: PersistableArtifact[];
  readonly afterStructs: PersistableArtifact[];
  readonly views: PersistableArtifact[];
  readonly types: PersistableArtifact[];
  readonly storedProcedures: PersistableArtifact[];
  readonly storedFunctions: PersistableArtifact[];
  readonly content: PersistableArtifact[];
  readonly afterContent: PersistableArtifact[];
}

export abstract class ANSI implements gimRDS.Dialect {
  readonly isDialect = true;
  readonly attrSqlTypesMapper = defaultAttrRdbmsEngineMapper;
  readonly namingStrategy: gimRDS.NamingStrategy;

  constructor(
    readonly name: gimRDS.DialectName,
    strategy: gimRDS.NamingStrategy | gimRDS.NamingStrategyConstructor,
  ) {
    this.namingStrategy = gimRDS.isNamingStrategy(strategy)
      ? strategy
      : new strategy();
  }

  sqlType(
    ctx: gimRDS.RdbmsModelContext,
    forSrc: gimRDS.AttrMapperSource,
    ifNotFound?: gimRDS.AttrSqlTypesConstructor,
  ): gimRDS.AttrSqlType | undefined {
    return this.attrSqlTypesMapper.map(ctx, forSrc, ifNotFound);
  }

  storeValueSQL(
    rsCtx: gimRDS.RdbmsSqlValueContext,
    av: gimc.AttributeValue,
    tc?: gimRDS.TableColumn,
  ): string {
    if (tc && tc.column.storeValueSQL) {
      return tc.column.storeValueSQL(rsCtx, av);
    }

    const value = av.attrValue;
    switch (typeof value) {
      case "string":
        return `'${value}'`;
      case "object":
        if ("identifier" in value) return `'${value.identifier()}'`;
        if (gimc.isEnumerationValue(value)) return `${value.id}`;
        return value;
      case "function":
        // if it's a function, emit it as-is (assume it's literal SQL)
        // must match rdbms/dialect.ts PrepareValueSQL function interface
        return value(rsCtx, tc, av);
      default:
        return value;
    }
  }

  insertRowDML(
    rsCtx: gimRDS.RdbmsModelContext,
    row: gimc.EntityAttrValues<any>,
  ): InsertRowSqlDML {
    const table = rsCtx.rdbmsModel.table(row.entity);
    if (!table) {
      return `-- Table ${row.entity.name.inflect()} not found, unable to generate insertRowDML`;
    }

    const valueCtx = gimRDS.rdbmsCtxFactory.rdbmsSqlValueContext(rsCtx);
    const names: string[] = [];
    const values: string[] = [];

    const colsHandled: Set<gimRDS.Column> = new Set();
    for (const av of row.attrValues) {
      const columnName = this.namingStrategy.tableColumnName({
        entity: table.entity,
        attr: av.attr,
      });
      names.push(columnName);
      const column = table.columnsByName.get(columnName);
      if (!column) {
        values.push(
          `/* Column ${columnName} not found in table ${table.name(rsCtx)} */`,
        );
        continue;
      }
      if (av.isValid) {
        values.push(
          this.storeValueSQL(valueCtx, av, { table: table, column: column }),
        );
      } else {
        values.push(`/* ${av.error} */`);
      }
      colsHandled.add(column);
    }

    for (const c of table.columns) {
      if (colsHandled.has(c)) continue;
      if (c.forAttr.valueSupplier) {
        const vs = c.forAttr.valueSupplier(valueCtx);
        const av = vs(valueCtx, c.forAttr);
        const columnName = this.namingStrategy.tableColumnName({
          entity: table.entity,
          attr: av.attr,
        });
        names.push(columnName);
        values.push(
          this.storeValueSQL(valueCtx, av, { table: table, column: c }),
        );
      }
    }

    return `INSERT INTO ${table.name(rsCtx)} (${
      names.join(
        ", ",
      )
    }) VALUES (${values.join(", ")});`;
  }

  abstract autoIdentityNativeColumnDDL(
    defn: ColumnSqlDdlGenInput,
  ): ColumnSqlDDL;

  identityColumnDDL(defn: ColumnSqlDdlGenInput): ColumnSqlDDL {
    if (gimc.isAutoIdentityNative(defn.column.forAttr)) {
      if (defn.column.forAttr.isIdentity) {
        return this.autoIdentityNativeColumnDDL(defn);
      }
    }
    return this.genericColumnDDL(defn);
  }

  referenceColumnDDL(defn: ColumnSqlDdlGenInput): ColumnSqlDDL {
    let defaultValue = "";
    if (defn.column.forAttr.valueSupplier) {
      const valueCtx = gimRDS.rdbmsCtxFactory.rdbmsSqlValueContext(
        defn.ctx,
      );
      const vs = defn.column.forAttr.valueSupplier(valueCtx);
      const av = vs(valueCtx, defn.column.forAttr);
      if (av && av.attrValue) {
        defaultValue = ` DEFAULT ${
          this.storeValueSQL(
            valueCtx,
            {
              attr: defn.column.forAttr,
              attrValue: av.attrValue,
              isValid: true,
            },
            { table: defn.ctx.table, column: defn.column },
          )
        }`;
      }
    }
    return {
      columnDDL: `${defn.columnName} ${
        vm.resolveTextValue(defn.ctx, defn.sqlTypes.fKRefDDL)
      }${defn.primaryKey}${defn.notNull}${defaultValue}`,
      tableDDL: `FOREIGN KEY (${defn.columnName}) REFERENCES ${
        defn.column.references?.table.name(defn.ctx)
      }(${defn.column.references?.column.name(defn.ctx)})`,
    };
  }

  dateColumnDDL(defn: ColumnSqlDdlGenInput): ColumnSqlDDL {
    let defaultValue = gimc.isDefaultToNow(defn.column.forAttr)
      ? " DEFAULT CURRENT_DATE"
      : "";
    return {
      columnDDL: `${defn.columnName} ${
        vm.resolveTextValue(defn.ctx, defn.sqlTypes.nonRefDDL)
      }${defn.primaryKey}${defn.notNull}${defaultValue}`,
    };
  }

  timeColumnDDL(defn: ColumnSqlDdlGenInput): ColumnSqlDDL {
    let defaultValue = gimc.isDefaultToNow(defn.column.forAttr)
      ? " DEFAULT CURRENT_TIME"
      : "";
    return {
      columnDDL: `${defn.columnName} ${
        vm.resolveTextValue(defn.ctx, defn.sqlTypes.nonRefDDL)
      }${defn.primaryKey}${defn.notNull}${defaultValue}`,
    };
  }

  dateTimeColumnDDL(defn: ColumnSqlDdlGenInput): ColumnSqlDDL {
    let defaultValue = gimc.isDefaultToNow(defn.column.forAttr)
      ? " DEFAULT CURRENT_TIMESTAMP"
      : "";
    return {
      columnDDL: `${defn.columnName} ${
        vm.resolveTextValue(defn.ctx, defn.sqlTypes.nonRefDDL)
      }${defn.primaryKey}${defn.notNull}${defaultValue}`,
    };
  }

  
  booleanColumnDDL(defn: ColumnSqlDdlGenInput): ColumnSqlDDL {
    let defaultValue = gimc.isDefaultBooleanValue(defn.column.forAttr)
      ? " DEFAULT FALSE"
      : "";
    return {
      columnDDL: `${defn.columnName} ${
        vm.resolveTextValue(defn.ctx, defn.sqlTypes.nonRefDDL)
      }${defn.primaryKey}${defn.notNull}${defaultValue}`,
    };
  }


  genericColumnDDL(defn: ColumnSqlDdlGenInput): ColumnSqlDDL {
    return {
      columnDDL: `${defn.columnName} ${
        vm.resolveTextValue(defn.ctx, defn.sqlTypes.nonRefDDL)
      }${defn.primaryKey}${defn.notNull}`,
    };
  }

  columnDDL(defn: ColumnSqlDdlGenInput): ColumnSqlDDL {
    if (gimc.isIdentity(defn.column.forAttr)) {
      return this.identityColumnDDL(defn);
    }
    if (defn.column.references) return this.referenceColumnDDL(defn);
    if (gimc.isDateTime(defn.column.forAttr)) {
      return this.dateTimeColumnDDL(defn);
    }
    if (gimc.isTime(defn.column.forAttr)) return this.timeColumnDDL(defn);
    if (gimc.isDate(defn.column.forAttr)) return this.dateColumnDDL(defn);
    if (gimc.isBoolean(defn.column.forAttr)) return this.booleanColumnDDL(defn);
    return this.genericColumnDDL(defn);
  }

  tableDDL(ctx: gimRDS.RdbmsTableSqlDdlContext): TableSqlDDL {
    const columnsDDL: string[] = [];
    const foreignKeysDDL: string[] = [];
    for (const column of ctx.table.columns) {
      const ddl = this.columnDDL({
        ctx: ctx,
        column: column,
        columnName: column.name(ctx),
        primaryKey: column.primaryKey ? " PRIMARY KEY" : "",
        notNull: column.nullable ? "" : " NOT NULL",
        sqlTypes: column.sqlTypes(ctx),
      });
      columnsDDL.push(`    ${ddl.columnDDL}`);
      if (ddl.tableDDL) {
        foreignKeysDDL.push(`    ${ddl.tableDDL}`);
      }
    }
    const tableDDL: string =
      `CREATE TABLE IF NOT EXISTS ${
        ctx.dialect.namingStrategy.objectDefnNames().tableDefnName(
          ctx,
          ctx.table.entity,
        )
      } (\n` +
      [...columnsDDL, ...foreignKeysDDL].join(",\n") +
      "\n);";
    return {
      tableDDL: tableDDL,
    };
  }

  public addEntityAttrsContentDDL(
    rsCtxt: gimRDS.RdbmsSqlDdlContext,
    eav: gimc.EntityAttrValues<gimc.Entity>,
    result: string[],
    sep?: () => boolean,
  ): void {
    const table = rsCtxt.rdbmsModel.table(eav.entity);
    if (table) {
      if (sep && sep()) result.push("");
      result.push(
        this.insertRowDML(rsCtxt, eav),
      );
    } else {
      console.error(
        `[RDBMS_0901] Unable to create entity content row, table for '${eav.entity.name.inflect()}' was not found in RDBMS Model.`,
      );
    }
  }

  public contentSql(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
  ): ContentSql[] {
    const rsCtxt = sqlTrCtx.rdbmsCtxFactory.rdbmsSqlDdlContext(ctx.options);
    const contentSql: ContentSql[] = [];
    let previousTable: gimRDS.Table | undefined = undefined;
    rsCtxt.rdbmsModel.spec.target?.consumeContent(
      (
        imc: gimc.InformationModelContent,
        eeec: gimc.ExecEnvsEntityContent<gimc.Entity>,
      ): void => {
        var result: string[] = [];
        var first = true;
        eeec.supplyContent((eav: gimc.EntityAttrValues<gimc.Entity>): void => {
          if (gimRDS.isRdbmsRowValues(eav)) {
            if (!eav.isRowCompatibleWithEngine(ctx)) return;
          }
          this.addEntityAttrsContentDDL(rsCtxt, eav, result, (): boolean => {
            const table = rsCtxt.rdbmsModel.table(eav.entity);

            // we group all inserts for a single table and then add new line between different tables
            const addSeparator = !first && table
              ? (previousTable ? previousTable != table : false)
              : false;
            previousTable = table;
            first = false;
            return addSeparator;
          });
        });
        if (result.length > 0) {
          contentSql.push({
            sql: result,
            persistDestination: ap.isPersistenceDestinationSupplier(eeec)
              ? eeec
              : undefined,
          });
        }
      },
    );
    return contentSql;
  }

  public modelDDL(ctx: sqlTrCtx.RdbmsModelSqlTransformerContext): string {
    let tablesDDL: string[] = [];
    for (const table of ctx.options.rdbmsModel.tables) {
      const tableDdlCtx = sqlTrCtx.rdbmsCtxFactory.rdbmsTableSqlDdlContext(
        ctx.options,
        table,
      );
      const ddl = this.tableDDL(tableDdlCtx);
      tablesDDL = tablesDDL.concat(ddl.tableDDL);
    }
    return tablesDDL.join("\n\n");
  }

  viewDDL(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    view: gimRDS.View<any>,
    sqlSupplier: gimRDS.SqlViewQuerySupplier<any>,
  ): gimRDS.ViewBodySqlQuery | gimRDS.CreateViewStatement {
    const query = sqlSupplier.sqlViewQuery(ctx, view);
    if (gimRDS.isViewBodySqlQuery(query)) {
      const objDefnName = ctx.dialect.namingStrategy.objectDefnNames()
        .viewDefnName(ctx, view.entity);
      return {
        ...query,
        sql: tw.unindentWhitespace(`
        DROP VIEW IF EXISTS ${objDefnName};
        CREATE OR REPLACE VIEW ${objDefnName}(
          ${
          tw.wordWrap(
            view.columns.map((c) => c.name(ctx)).join(", "),
            "\n          ",
          )
        }) AS ${query.sql};
        `),
      };
    } else {
      return { ...query, sql: query.sql };
    }
  }

  buildPersistableArtifactsBeforeStructs(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    pa: PersistableArtifacts,
    ph: ap.PersistenceHandler,
    ns: gimRDS.ArtifactPersistenceNamingStrategy,
  ): void {
  }

  buildPersistableArtifactsAfterStructsBeforeContent(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    pa: PersistableArtifacts,
    ph: ap.PersistenceHandler,
    ns: gimRDS.ArtifactPersistenceNamingStrategy,
  ): void {
    for (const view of ctx.options.rdbmsModel.views) {
      if (gimRDS.isSqlViewQuerySupplier(view.entity)) {
        const viewSql = this.viewDDL(ctx, view, view.entity);
        const viewArtifact = ph.createMutableTextArtifact(ctx, {
          nature: polyglotArtfNature.sqlArtifact,
        });
        viewArtifact.appendText(ctx, viewSql.sql);
        pa.views.push(
          {
            destAs: {
              persistAsName: ns.viewArtifactName(ctx, viewSql, viewArtifact),
              persistOptions: { appendIfExists: true, appendDelim: "\n" },
            },
            artifact: viewArtifact,
          },
        );
      }
    }
  }

  buildPersistableContentArtifacts(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    pa: PersistableArtifacts,
    ph: ap.PersistenceHandler,
    ns: gimRDS.ArtifactPersistenceNamingStrategy,
  ): void {
    const content = this.contentSql(ctx);
    if (content.length > 0) {
      for (const cSql of content) {
        const contentSql = cSql.sql.join("\n");
        const contentArtifact = ph.createMutableTextArtifact(ctx, {
          nature: polyglotArtfNature.sqlArtifact,
        });
        contentArtifact.appendText(ctx, contentSql);
        const destAs: ap.PersistenceDestAs = cSql.persistDestination
          ? cSql.persistDestination.persistDestAs
          : {
            persistAsName: ns.modelPrimaryArtifactName(ctx, contentArtifact),
            persistOptions: { appendIfExists: true, appendDelim: "\n" },
          };
        pa.content.push(
          {
            destAs: destAs,
            artifact: contentArtifact,
          },
        );
      }
    }
  }

  buildPersistableArtifactsAfterContent(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    pa: PersistableArtifacts,
    ph: ap.PersistenceHandler,
    ns: gimRDS.ArtifactPersistenceNamingStrategy,
  ): void {
  }

  persistArtifacts(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    ph: ap.PersistenceHandler,
    artifacts: PersistableArtifact[],
  ): void {
    for (const a of artifacts) {
      ph.persistTextArtifact(
        ctx,
        vm.resolveTextValue(ctx, a.destAs.persistAsName),
        a.artifact,
        a.destAs.persistOptions,
      );
    }
  }

  persistModel(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
  ): ap.TextArtifact {
    const ph = ctx.options.persist;
    const ns = ctx.options.rdbmsModel.artifactsNamingStrategy;
    const mainArtifact = ph.createMutableTextArtifact(ctx, {
      nature: polyglotArtfNature.sqlArtifact,
    });
    const pa: PersistableArtifacts = {
      beforeStructs: [],
      structs: [],
      main: {
        artifact: mainArtifact,
        destAs: {
          persistAsName: ns.modelPrimaryArtifactName(ctx, mainArtifact),
        },
      },
      afterStructs: [],
      types: [],
      views: [],
      storedFunctions: [],
      storedProcedures: [],
      content: [],
      afterContent: [],
    };

    this.buildPersistableArtifactsBeforeStructs(ctx, pa, ph, ns);
    mainArtifact.appendText(ctx, this.modelDDL(ctx));
    this.buildPersistableArtifactsAfterStructsBeforeContent(ctx, pa, ph, ns);
    this.buildPersistableContentArtifacts(ctx, pa, ph, ns);
    this.buildPersistableArtifactsAfterContent(ctx, pa, ph, ns);

    this.persistArtifacts(ctx, ph, pa.beforeStructs);
    ctx.transformations.modelPrimaryArtifactResult = ph.persistTextArtifact(
      ctx,
      ns.modelPrimaryArtifactName(ctx, mainArtifact),
      mainArtifact,
    );
    this.persistArtifacts(ctx, ph, pa.structs);
    this.persistArtifacts(ctx, ph, pa.afterStructs);
    this.persistArtifacts(ctx, ph, pa.types);
    this.persistArtifacts(ctx, ph, pa.views);
    this.persistArtifacts(
      ctx,
      ph,
      pa.content.filter((x) => x.destAs.persistAsName == "content-all"),
    );
    this.persistArtifacts(ctx, ph, pa.storedFunctions);
    this.persistArtifacts(ctx, ph, pa.storedProcedures);
    this.persistArtifacts(
      ctx,
      ph,
      pa.content.filter((x) => x.destAs.persistAsName != "content-all"),
    );
    this.persistArtifacts(ctx, ph, pa.afterContent);
    return mainArtifact;
  }
}
