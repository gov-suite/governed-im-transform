import * as sqlTrCtx from "../context.ts";
import {
  artfPersist as ap,
  contextMgr as cm,
  governedIM as gimc,
  governedImRDS as gimRDS,
  governedImRDSModels as models,
  governedImTransform as gimTr,
  polyglotArtfNature,
  textInflect as infl,
  textWhitespace as tw,
  valueMgr as vm,
} from "../deps.ts";
import * as diaImpl from "../dialect.ts";
import * as naming from "../naming.ts";
import * as sqlTr from "../transform.ts";

export interface PostgreSqlEngineExtension {
  readonly isPostgreSqlEngineExtension: true;
  emitExtnInitSQL(ctx: cm.Context, mta: ap.MutableTextArtifact): void;
}

export function defaultPostgreSqlEmitExtnInitSQL(
  extnName: vm.TextValue,
): (ctx: cm.Context, mta: ap.MutableTextArtifact) => void {
  return (ctx: cm.Context, mta: ap.MutableTextArtifact): void => {
    mta.appendText(ctx, `CREATE EXTENSION IF NOT EXISTS ${extnName};\n`);
  };
}

export interface PostgreSqlEncryptionExtension
  extends PostgreSqlEngineExtension {}

export interface PostgreSqlStoredRoutineLintExtension
  extends PostgreSqlEngineExtension {}

export abstract class AbstractPostgreSqlDialect extends diaImpl.ANSI {
  public static readonly dialectName = gimRDS.PostreSqlEngineName;
  public static readonly dialectNameAliases = [
    AbstractPostgreSqlDialect.dialectName,
    infl.snakeCaseValue("Postgres"),
  ];

  constructor(
    name: gimRDS.DialectName,
    ns: gimRDS.NamingStrategy | gimRDS.NamingStrategyConstructor,
  ) {
    super(name, ns);
  }

  autoIdentityNativeColumnDDL(
    defn: diaImpl.ColumnSqlDdlGenInput,
  ): diaImpl.ColumnSqlDDL {
    return {
      columnDDL: `${defn.columnName} BIGSERIAL PRIMARY KEY`,
    };
  }

  callStoredProcedureStatement(
    rsCtx: gimRDS.RdbmsModelContext,
    eav: gimc.EntityAttrValues<any>,
  ): string {
    const valueCtx = gimRDS.rdbmsCtxFactory.rdbmsSqlValueContext(rsCtx);
    const values: string[] = [];

    for (const av of eav.attrValues) {
      if (av.isValid) {
        values.push(this.storeValueSQL(valueCtx, av));
      } else {
        values.push(`/* ${av.error} */`);
      }
    }

    const routineName = this.namingStrategy.storedProcedureName(eav.entity);
    return `CALL ${routineName}(${values.join(", ")});`;
  }

  public addEntityAttrsContentDDL(
    rsCtxt: gimRDS.RdbmsSqlDdlContext,
    eav: gimc.EntityAttrValues<gimc.Entity>,
    result: string[],
    sep?: () => boolean,
  ): void {
    if (
      gimRDS.isStoredRoutineEntity(eav.entity) &&
      gimRDS.isStoredProcedureCodeSupplier(eav.entity)
    ) {
      result.push("");
      result.push(this.callStoredProcedureStatement(rsCtxt, eav));
    } else {
      super.addEntityAttrsContentDDL(rsCtxt, eav, result, sep);
    }
  }

  typeDefnDDL(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    typeDefn: gimRDS.TypeDefn<any>,
    sqlSupplier: gimRDS.TypeDefnSqlSupplier<any>,
  ): gimRDS.SqlStatement {
    if (sqlSupplier.typeDefnSqlStatement) {
      return sqlSupplier.typeDefnSqlStatement(ctx, typeDefn);
    }
    const objDefnName = ctx.dialect.namingStrategy
      .objectDefnNames()
      .typeDefnName(ctx, typeDefn.entity);
    return {
      isSqlStatement: true,
      sql: "\n" +
        tw.unindentWhitespace(`
        DROP TYPE IF EXISTS ${objDefnName};
        CREATE TYPE ${objDefnName} AS (${
          typeDefn.columns
            .map((c) => `${c.name(ctx)} ${c.sqlTypes(ctx).typeDefn}`)
            .join(", ")
        });`),
    };
  }

  argSignatureDDL(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    routine: gimRDS.StoredFunction<any>,
    arg: gimRDS.StoredRoutineArg,
  ): string {
    let mutability = undefined;
    switch (arg.argMutability) {
      case gimRDS.StoredRoutineArgMutability.InOnly:
        mutability = "IN";
        break;

      case gimRDS.StoredRoutineArgMutability.OutOnly:
        mutability = "OUT";
        break;

      case gimRDS.StoredRoutineArgMutability.InOut:
        mutability = "INOUT";
        break;
    }

    return `${mutability} ${arg.argName} ${arg.argType}`;
  }

  storedFunctionDDL(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    routine: gimRDS.StoredFunction<any>,
    codeSupplier: gimRDS.StoredFunctionCodeSupplier<any>,
  ): gimRDS.StoredRoutineCode {
    const fn = codeSupplier.storedFunctionCode(ctx, routine);
    if (gimRDS.isStoredRoutineBodyCode(fn)) {
      const objDefnName = ctx.dialect.namingStrategy
        .objectDefnNames()
        .storedFuncDefnName(ctx, routine.entity);
      const argNamesVals = routine.args
        ? `(${
          routine
            .args!.map((a) => this.argSignatureDDL(ctx, routine, a))
            .join(",")
        })`
        : "";
      const argTypes = routine.args
        ? `(${routine.args.map((a) => a.argType).join(",")})`
        : "";
      let storedFuncReturns = undefined;
      if (!routine.columns || routine.columns.length == 0) {
        storedFuncReturns = "RETURNS VOID";
      } else if (routine.columns && routine.columns.length == 1) {
        storedFuncReturns = "RETURNS " +
          routine.columns[0].sqlTypes(ctx).storedFuncOut;
      } else if (routine.columns && routine.columns.length > 1) {
        storedFuncReturns = `RETURNS TABLE (${
          routine
            .columns!.map(
              (c) => `${c.name(ctx)} ${c.sqlTypes(ctx).storedFuncOut}`,
            )
            .join(", ")
        })`;
      }
      const plPgSqlCheck = this.plPgCheckFunctionDDL(ctx, routine, undefined);

      return {
        ...fn,
        isStoredRoutineCode: true,
        sourceCode: tw.unindentWhitespace(
          `
        DROP FUNCTION IF EXISTS ${objDefnName}${argTypes};
        CREATE OR REPLACE FUNCTION ${objDefnName}${argNamesVals}
        ${storedFuncReturns}
        LANGUAGE 'plpgsql'
        IMMUTABLE 
        AS $BODY$
        ${fn.bodyCode}
        $BODY$;
        -- ALTER FUNCTION ${objDefnName}${argTypes} OWNER TO (TODO: OWNER);
        ${plPgSqlCheck};
        `,
        ),
      };
    } else {
      return { ...fn, sourceCode: fn.sourceCode };
    }
  }

  storedProcedureWrapperFunctionDDL(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    routine: gimRDS.StoredProcedure<any>,
    spfw: gimRDS.StoredProcedureFunctionWrapper,
  ): gimRDS.StoredRoutineCode {
    const objWfDefnName = ctx.dialect.namingStrategy
      .objectDefnNames()
      .storedProcWrapperFuncDefnName(ctx, routine.entity, spfw);
    const objProcDefName = ctx.dialect.namingStrategy
      .objectDefnNames()
      .storedProcDefnName(ctx, routine.entity);
    const argNamesVals = routine.args
      ? `(${
        routine
          .args!.map((a) => this.argSignatureDDL(ctx, routine, a))
          .join(",")
      })`
      : "";
    const argTypes = routine.args
      ? `(${routine.args.map((a) => a.argType).join(",")})`
      : "";
    const argValues = routine.args
      ? `(${routine.args.map((a) => a.argName).join(",")})`
      : "";
    const wrapperCode = spfw.wrapperRoutineCode ||
      `CALL  ${objProcDefName}${argValues} ;`;

    let funcReturns = undefined;
    if (!routine.columns || routine.columns.length == 0) {
      funcReturns = "RETURNS VOID";
    } else if (routine.columns && routine.columns.length == 1) {
      funcReturns = "RETURNS " +
        routine.columns[0].sqlTypes(ctx).storedFuncOut;
    } else if (routine.columns && routine.columns.length > 1) {
      funcReturns = `RETURNS TABLE (${
        routine
          .columns!.map(
            (c) => `${c.name(ctx)} ${c.sqlTypes(ctx).storedFuncOut}`,
          )
          .join(", ")
      })`;
    }
    const plPgSqlCheck = this.plPgCheckFunctionDDL(ctx, routine, spfw);

    return {
      isStoredRoutineCode: true,
      sourceCode: `
      DROP FUNCTION IF EXISTS ${objWfDefnName}${argTypes};
      CREATE OR REPLACE FUNCTION ${objWfDefnName}${argNamesVals}
      ${funcReturns}
      LANGUAGE 'plpgsql'
      COST 100 VOLATILE
      AS $BODY$
        BEGIN
          ${wrapperCode}
        END;
      $BODY$;
      -- ALTER FUNCTION ${objWfDefnName}${argTypes} OWNER TO (TODO: OWNER);
      ${plPgSqlCheck};
      `,
    };
  }

  storedProcedureDDL(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    routine: gimRDS.StoredProcedure<any>,
    codeSupplier: gimRDS.StoredProcedureCodeSupplier<any>,
  ): gimRDS.StoredRoutineCode {
    const sp = codeSupplier.storedProcedureCode(ctx, routine);
    if (gimRDS.isStoredRoutineBodyCode(sp)) {
      const objDefnName = ctx.dialect.namingStrategy
        .objectDefnNames()
        .storedProcDefnName(ctx, routine.entity);
      const argNamesVals = routine.args
        ? `(${
          routine
            .args!.map((a) => this.argSignatureDDL(ctx, routine, a))
            .join(", ")
        })`
        : "";
      const argTypes = routine.args
        ? `(${routine.args.map((a) => a.argType).join(", ")})`
        : "";
      const wrapperFuncCode = codeSupplier.storedProcedureFunctionWrapper
        ? this.storedProcedureWrapperFunctionDDL(
          ctx,
          routine,
          codeSupplier.storedProcedureFunctionWrapper(),
        ).sourceCode
        : "";
      const plPgSqlCheck = this.plPgCheckFunctionDDL(ctx, routine, undefined);

      return {
        ...sp,
        isStoredRoutineCode: true,
        sourceCode: tw.unindentWhitespace(`
      DROP PROCEDURE IF EXISTS ${objDefnName}${argTypes};
      CREATE OR REPLACE PROCEDURE ${objDefnName}${argNamesVals}
      LANGUAGE 'plpgsql'
      AS $BODY$
        ${sp.bodyCode}
        $BODY$;
        -- ALTER PROCEDURE ${objDefnName}${argTypes} OWNER TO (TODO: OWNER);
      ${plPgSqlCheck};
      ${wrapperFuncCode}`),
      };
    } else {
      return { ...sp, sourceCode: sp.sourceCode };
    }
  }

  buildPersistableArtifactsAfterStructsBeforeContent(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    pa: diaImpl.PersistableArtifacts,
    ph: ap.PersistenceHandler,
    ns: gimRDS.ArtifactPersistenceNamingStrategy,
  ): void {
    super.buildPersistableArtifactsAfterStructsBeforeContent(ctx, pa, ph, ns);

    for (const typeDefn of ctx.options.rdbmsModel.typeDefns) {
      if (gimRDS.isTypeDefnSqlSupplier(typeDefn.entity)) {
        const stmt = this.typeDefnDDL(ctx, typeDefn, typeDefn.entity);
        const tdArtifact = ph.createMutableTextArtifact(ctx, {
          nature: polyglotArtfNature.sqlArtifact,
        });
        tdArtifact.appendText(ctx, vm.resolveTextValue(ctx, stmt.sql) + "\n");
        pa.types.push({
          destAs: {
            persistAsName: ns.typeDefnArtifactName(ctx, stmt, tdArtifact),
            persistOptions: { appendIfExists: true, appendDelim: "\n" },
          },
          artifact: tdArtifact,
        });
      }
    }

    for (const fn of ctx.options.rdbmsModel.functions) {
      if (gimRDS.isStoredFunctionCodeSupplier(fn.entity)) {
        const code = this.storedFunctionDDL(ctx, fn, fn.entity);
        const fnArtifact = ph.createMutableTextArtifact(ctx, {
          nature: polyglotArtfNature.sqlArtifact,
        });
        fnArtifact.appendText(ctx, code.sourceCode);
        pa.storedFunctions.push({
          destAs: {
            persistAsName: ns.storedFunctionArtifactName(ctx, code, fnArtifact),
            persistOptions: { appendIfExists: true, appendDelim: "\n" },
          },
          artifact: fnArtifact,
        });
      }
    }

    for (const sp of ctx.options.rdbmsModel.procedures) {
      if (gimRDS.isStoredProcedureCodeSupplier(sp.entity)) {
        const code = this.storedProcedureDDL(ctx, sp, sp.entity);
        const spArtifact = ph.createMutableTextArtifact(ctx, {
          nature: polyglotArtfNature.sqlArtifact,
        });
        spArtifact.appendText(ctx, code.sourceCode);
        pa.storedProcedures.push({
          destAs: {
            persistAsName: ns.storedProcedureArtifactName(
              ctx,
              code,
              spArtifact,
            ),
            persistOptions: { appendIfExists: true, appendDelim: "\n" },
          },
          artifact: spArtifact,
        });
      }
    }
  }

  plPgCheckFunctionDDL(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    routine: gimRDS.StoredProcedure<any>,
    spfw: gimRDS.StoredProcedureFunctionWrapper | undefined,
  ): string {
    var plPgSqlQuery: string;
    const entPlPgSql = ctx.rdbmsModel.imStructure.entities.find((e) =>
      e instanceof models.PlPgSqlCheckResult
    );
    let objDefnName = undefined;
    if (spfw != undefined) {
      objDefnName = ctx.dialect.namingStrategy
        .objectDefnNames()
        .storedProcWrapperFuncDefnName(ctx, routine.entity, spfw);
    } else {
      objDefnName = ctx.dialect.namingStrategy
        .objectDefnNames()
        .storedFuncDefnName(ctx, routine.entity);
    }
    const entityName = entPlPgSql?.name.singular.inflect();
    const plPgCheckFunctionName = models.PLPGSQL_CHECK_FUNCTION_NAME;
    const plPgCheckFunctionColumns = models.PLPGSQL_CHECK_FUNCTION_COLUMNS;
    if (entityName != undefined) {
      plPgSqlQuery = `
       INSERT INTO ${entityName}
      (${
        entPlPgSql?.attrs.slice(1, 12).map((x) =>
          x.name.relationalColumnName.inflect()
        ).join(", ")
      })
      SELECT ${plPgCheckFunctionColumns} FROM ${plPgCheckFunctionName}('${objDefnName}')`;
    } else {
      plPgSqlQuery =
        ` SELECT ${plPgCheckFunctionColumns} FROM ${plPgCheckFunctionName}('${objDefnName}')`;
    }
    return plPgSqlQuery;
  }
}

export class PostgreSqlDialect extends AbstractPostgreSqlDialect {
  public static readonly dialectName = AbstractPostgreSqlDialect.dialectName;
  public static readonly dialectNameAliases =
    AbstractPostgreSqlDialect.dialectNameAliases;

  constructor(ns?: gimRDS.NamingStrategy | gimRDS.NamingStrategyConstructor) {
    super(PostgreSqlDialect.dialectName, ns ? ns : naming.LowercaseObjectNames);
  }
}

export class PostgreSqlCommonPkColNamedIdDialect
  extends AbstractPostgreSqlDialect {
  public static readonly dialectName = infl.snakeCaseValue(
    AbstractPostgreSqlDialect.dialectName.inflect() + ":CommonPkColNamedID",
  );
  public static readonly dialectNameAliases = [
    PostgreSqlCommonPkColNamedIdDialect.dialectName,
    infl.snakeCaseValue(
      AbstractPostgreSqlDialect.dialectNameAliases[1].inflect() +
        ":CommonPkColNamedID",
    ),
  ];

  constructor() {
    super(
      PostgreSqlDialect.dialectName,
      new naming.CommonPkColNamedID(new naming.LowercaseObjectNames()),
    );
  }
}

export class PostgreSqlTransformerWithDriverScript
  implements gimTr.InfoModelTransformer {
  readonly isInfoModelTransformer = true;

  constructor(readonly options: sqlTr.RdbmsModelTransformerOptions) {}

  public transform(ctx: sqlTrCtx.RdbmsModelSqlTransformerContext): void {
    const ph = ctx.options.persist;
    this.options.dialect.persistModel(ctx);
    const driverArtifact = ph.createMutableTextArtifact(ctx, {
      nature: polyglotArtfNature.postgreSqlArtifact,
    });
    if (ph.results.length > 0) {
      for (const pr of ph.results) {
        driverArtifact.appendText(
          ctx,
          `\\include ${pr.finalArtifactNamePhysical}\n`,
        );
      }
    }
    ph.persistTextArtifact(ctx, "driver", driverArtifact, {
      logicalNamingStrategy: ap.natureNamingStrategy(),
      physicalNamingStrategy: ap.natureNamingStrategy(),
    });
  }
}
