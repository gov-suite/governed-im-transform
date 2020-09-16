import type * as sqlTrCtx from "./context.ts";
import {
  artfPersist as ap,
  governedIM as gimc,
  governedImRDS as gimRDS,
  textInflect as infl,
  valueMgr as vm,
} from "./deps.ts";

export class DefaultRdbmsModelPersistenceNamingStrategy
  implements gimRDS.ArtifactPersistenceNamingStrategy {
  constructor(readonly rdbmsModel: gimRDS.RdbmsModelStruct) {
  }

  modelPrimaryArtifactName(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    artifact: ap.TextArtifact,
  ): string {
    return vm.resolveTextValue(
      ctx,
      ctx.options.rdbmsModel.imStructure.namespace.identifier,
    );
  }

  viewArtifactName(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    view: gimRDS.ViewBodySqlQuery | gimRDS.CreateViewStatement,
    artifact: ap.TextArtifact,
  ): string {
    return view.persistAsName
      ? view.persistAsName
      : this.modelPrimaryArtifactName(ctx, artifact);
  }

  storedProcedureArtifactName(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    routine: gimRDS.StoredRoutineCode,
    artifact: ap.TextArtifact,
  ): string {
    return routine.persistAsName
      ? routine.persistAsName
      : this.modelPrimaryArtifactName(ctx, artifact);
  }

  storedFunctionArtifactName(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    routine: gimRDS.StoredRoutineCode,
    artifact: ap.TextArtifact,
  ): string {
    return routine.persistAsName
      ? routine.persistAsName
      : this.modelPrimaryArtifactName(ctx, artifact);
  }

  typeDefnArtifactName(
    ctx: sqlTrCtx.RdbmsModelSqlTransformerContext,
    typeDefn: gimRDS.SqlStatement,
    artifact: ap.TextArtifact,
  ): string {
    return typeDefn.persistAsName
      ? typeDefn.persistAsName
      : this.modelPrimaryArtifactName(ctx, artifact);
  }
}

export class DefaultObjectDefnNamingStrategy
  implements gimRDS.ObjectDefnNamingStrategy {
  protected readonly tables: gimRDS.ObjectDefnNameType;
  protected readonly views: gimRDS.ObjectDefnNameType;
  protected readonly storedProcedures: gimRDS.ObjectDefnNameType;
  protected readonly storedFunctions: gimRDS.ObjectDefnNameType;
  protected readonly typeDefns: gimRDS.ObjectDefnNameType;

  constructor(
    readonly ns: gimRDS.NamingStrategy,
    defaultType: gimRDS.ObjectDefnNameType,
  ) {
    this.tables = defaultType;
    this.views = defaultType;
    this.storedProcedures = defaultType;
    this.storedFunctions = defaultType;
    this.typeDefns = defaultType;
  }

  tableDefnName(ctx: gimRDS.RdbmsEngineContext, entity: gimc.Entity): string {
    switch (this.tables) {
      case gimRDS.ObjectDefnNameType.ObjectNameOnly:
        return this.ns.tableName(entity);
      case gimRDS.ObjectDefnNameType.NamespaceQualifiedObjectName:
        return vm.resolveTextValue(ctx, this.ns.schemaName(entity)) + "." +
          this.ns.tableName(entity);
    }
  }

  viewDefnName(ctx: gimRDS.RdbmsEngineContext, entity: gimc.Entity): string {
    switch (this.views) {
      case gimRDS.ObjectDefnNameType.ObjectNameOnly:
        return this.ns.viewName(entity);
      case gimRDS.ObjectDefnNameType.NamespaceQualifiedObjectName:
        return vm.resolveTextValue(ctx, this.ns.schemaName(entity)) + "." +
          this.ns.viewName(entity);
    }
  }

  storedProcDefnName(
    ctx: gimRDS.RdbmsEngineContext,
    entity: gimc.Entity,
  ): string {
    switch (this.storedProcedures) {
      case gimRDS.ObjectDefnNameType.ObjectNameOnly:
        return this.ns.storedProcedureName(entity);
      case gimRDS.ObjectDefnNameType.NamespaceQualifiedObjectName:
        return vm.resolveTextValue(ctx, this.ns.schemaName(entity)) + "." +
          this.ns.storedProcedureName(entity);
    }
  }

  storedProcWrapperFuncDefnName(
    ctx: gimRDS.RdbmsEngineContext,
    entity: gimc.Entity,
    spfw: gimRDS.StoredProcedureFunctionWrapper,
  ): string {
    switch (this.storedFunctions) {
      case gimRDS.ObjectDefnNameType.ObjectNameOnly:
        return spfw.wrapperStoredFunctionName;
      case gimRDS.ObjectDefnNameType.NamespaceQualifiedObjectName:
        return vm.resolveTextValue(ctx, this.ns.schemaName(entity)) + "." +
          spfw.wrapperStoredFunctionName;
    }
  }

  storedFuncDefnName(
    ctx: gimRDS.RdbmsEngineContext,
    entity: gimc.Entity,
  ): string {
    switch (this.storedFunctions) {
      case gimRDS.ObjectDefnNameType.ObjectNameOnly:
        return this.ns.storedFunctionName(entity);
      case gimRDS.ObjectDefnNameType.NamespaceQualifiedObjectName:
        return vm.resolveTextValue(ctx, this.ns.schemaName(entity)) + "." +
          this.ns.storedFunctionName(entity);
    }
  }

  typeDefnName(ctx: gimRDS.RdbmsEngineContext, entity: gimc.Entity): string {
    switch (this.typeDefns) {
      case gimRDS.ObjectDefnNameType.ObjectNameOnly:
        return this.ns.typeDefnName(entity);
      case gimRDS.ObjectDefnNameType.NamespaceQualifiedObjectName:
        return vm.resolveTextValue(ctx, this.ns.schemaName(entity)) + "." +
          this.ns.typeDefnName(entity);
    }
  }
}

export function guessName(
  ns: gimRDS.NamingStrategy,
  params: gimRDS.GuessNameParams,
): string | undefined {
  if (gimc.isEntity(params.obj)) {
    return ns.tableName(params.obj);
  } else if (gimc.isAttribute(params.obj)) {
    if (params.includeEntityNameWithAttrName) {
      return ns.qualifiedColumnName(
        { entity: params.obj.parent, attr: params.obj },
      );
    }
    return ns.tableColumnName(
      { entity: params.obj.parent, attr: params.obj },
    );
  }
  return undefined;
}

export class UppercaseObjectNames implements gimRDS.NamingStrategy {
  readonly isNamingStrategy = true;
  readonly strategyName = infl.snakeCaseValue("Uppercase_All");
  readonly strategyDescr =
    "All column names, tables, and other identifiers are in uppercase";
  protected readonly odns: gimRDS.ObjectDefnNamingStrategy;

  constructor(
    defaultType: gimRDS.ObjectDefnNameType =
      gimRDS.ObjectDefnNameType.ObjectNameOnly,
  ) {
    this.odns = new DefaultObjectDefnNamingStrategy(this, defaultType);
  }
  guessName(params: gimRDS.GuessNameParams): string | undefined {
    return guessName(this, params);
  }
  schemaName(entity: gimc.Entity): gimRDS.SchemaName {
    return entity.namespace.identifier;
  }
  tableName(entity: gimc.Entity): gimRDS.TableName {
    return entity.name.singular.inflect().toLocaleUpperCase();
  }
  viewName(entity: gimc.Entity): gimRDS.ViewName {
    return this.tableName(entity);
  }
  typeDefnName(entity: gimc.Entity): gimRDS.ViewName {
    return this.viewName(entity);
  }
  storedFunctionName(entity: gimc.Entity): gimRDS.StoredRoutineName {
    return this.tableName(entity);
  }
  storedProcedureName(entity: gimc.Entity): gimRDS.StoredRoutineName {
    return this.tableName(entity);
  }
  tableColumnName(ref: gimc.Reference<gimc.Entity>): gimRDS.ColumnName {
    return ref.attr.name.relationalColumnName.inflect().toLocaleUpperCase();
  }
  columnName(name: string): gimRDS.ColumnName {
    return name.toLocaleUpperCase();
  }
  qualifiedColumnName(
    ref: gimc.Reference<gimc.Entity>,
  ): gimRDS.QualifiedColumnName {
    return this.tableName(ref.entity) + "." + this.tableColumnName(ref);
  }
  objectDefnNames(): gimRDS.ObjectDefnNamingStrategy {
    return this.odns;
  }
  artifactNames(
    rdbmsModel: gimRDS.RdbmsModelStruct,
  ): gimRDS.ArtifactPersistenceNamingStrategy {
    return new DefaultRdbmsModelPersistenceNamingStrategy(rdbmsModel);
  }
}

export class LowercaseObjectNames implements gimRDS.NamingStrategy {
  readonly isNamingStrategy = true;
  readonly strategyName = infl.snakeCaseValue("Lowercase_All");
  readonly strategyDescr =
    "All column names, tables, and other identifiers are in uppercase";
  protected readonly odns: gimRDS.ObjectDefnNamingStrategy;

  constructor(
    defaultType: gimRDS.ObjectDefnNameType =
      gimRDS.ObjectDefnNameType.ObjectNameOnly,
  ) {
    this.odns = new DefaultObjectDefnNamingStrategy(this, defaultType);
  }

  guessName(params: gimRDS.GuessNameParams): string | undefined {
    return guessName(this, params);
  }
  schemaName(entity: gimc.Entity): gimRDS.SchemaName {
    return entity.namespace.identifier;
  }
  tableName(entity: gimc.Entity): gimRDS.TableName {
    return entity.name.singular.inflect().toLocaleLowerCase();
  }
  viewName(entity: gimc.Entity): gimRDS.ViewName {
    return this.tableName(entity);
  }
  typeDefnName(entity: gimc.Entity): gimRDS.ViewName {
    return this.viewName(entity);
  }
  storedFunctionName(entity: gimc.Entity): gimRDS.StoredRoutineName {
    return this.tableName(entity);
  }
  storedProcedureName(entity: gimc.Entity): gimRDS.StoredRoutineName {
    return this.tableName(entity);
  }
  tableColumnName(ref: gimc.Reference<gimc.Entity>): gimRDS.ColumnName {
    return ref.attr.name.relationalColumnName.inflect().toLocaleLowerCase();
  }
  columnName(name: string): gimRDS.ColumnName {
    return name.toLocaleLowerCase();
  }
  qualifiedColumnName(
    ref: gimc.Reference<gimc.Entity>,
  ): gimRDS.QualifiedColumnName {
    return this.tableName(ref.entity) + "." + this.tableColumnName(ref);
  }
  objectDefnNames(): gimRDS.ObjectDefnNamingStrategy {
    return this.odns;
  }
  artifactNames(
    rdbmsModel: gimRDS.RdbmsModelStruct,
  ): gimRDS.ArtifactPersistenceNamingStrategy {
    return new DefaultRdbmsModelPersistenceNamingStrategy(rdbmsModel);
  }
}

export class CommonPkColNamedID implements gimRDS.NamingStrategy {
  readonly isNamingStrategy = true;
  readonly strategyName: infl.InflectableValue;
  readonly strategyDescr: string;

  constructor(readonly baseStrategy: gimRDS.NamingStrategy) {
    const baseStrategyName = baseStrategy.strategyName.inflect();
    this.strategyName = infl.snakeCaseValue(
      baseStrategyName + "_Common_PK_Col_Named_ID",
    );
    this.strategyDescr =
      `"Wraps ${baseStrategyName} naming strategy but forces all primary key columns to be called 'id'"`;
  }

  guessName(params: gimRDS.GuessNameParams): string | undefined {
    return guessName(this, params);
  }
  schemaName(entity: gimc.Entity): gimRDS.SchemaName {
    return entity.namespace.identifier;
  }
  tableName(entity: gimc.Entity): gimRDS.TableName {
    return this.baseStrategy.tableName(entity);
  }
  viewName(entity: gimc.Entity): gimRDS.ViewName {
    return this.tableName(entity);
  }
  typeDefnName(entity: gimc.Entity): gimRDS.ViewName {
    return this.viewName(entity);
  }
  storedFunctionName(entity: gimc.Entity): gimRDS.StoredRoutineName {
    return this.tableName(entity);
  }
  storedProcedureName(entity: gimc.Entity): gimRDS.StoredRoutineName {
    return this.tableName(entity);
  }
  tableColumnName(ref: gimc.Reference<gimc.Entity>): gimRDS.ColumnName {
    if (
      gimc.isIdentityManager(ref.entity) && ref.entity.isIdentityAttr(ref.attr)
    ) {
      return this.columnName("id");
    }
    if (gimc.isDerivedFromIdentity(ref.attr)) {
      // if attr is derived from an identity attribute and the name
      // has not been changed, then replace the column name with our
      // standard
      if (gimc.isDerivedNameSameAsSource(ref.attr)) {
        return this.columnName("id");
      }
    }
    return this.baseStrategy.tableColumnName(ref);
  }
  columnName(name: string): gimRDS.ColumnName {
    return this.baseStrategy.columnName(name);
  }
  qualifiedColumnName(
    ref: gimc.Reference<gimc.Entity>,
  ): gimRDS.QualifiedColumnName {
    return this.tableName(ref.entity) + "." + this.tableColumnName(ref);
  }
  objectDefnNames(): gimRDS.ObjectDefnNamingStrategy {
    return this.baseStrategy.objectDefnNames();
  }
  artifactNames(
    rdbmsModel: gimRDS.RdbmsModelStruct,
  ): gimRDS.ArtifactPersistenceNamingStrategy {
    return this.baseStrategy.artifactNames(rdbmsModel);
  }
}

export class LowercaseCommonPkColNamedID extends CommonPkColNamedID {
  constructor() {
    super(new LowercaseObjectNames());
  }
}
