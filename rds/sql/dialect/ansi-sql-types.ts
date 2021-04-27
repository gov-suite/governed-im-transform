import { governedIM as gimc, governedImRDS as gimRDS } from "../deps.ts";

export function sqlTypes(
  baseSqlType: gimRDS.SqlType,
): gimRDS.ContextualSqlTypes {
  return {
    fKRefDDL: baseSqlType,
    nonRefDDL: baseSqlType,
    storedProcIn: baseSqlType,
    storedProcOut: baseSqlType,
    storedFuncIn: baseSqlType,
    storedFuncOut: baseSqlType,
    typeDefn: baseSqlType,
  };
}

export class IntegerSqlType implements gimRDS.AttrSqlType {
  static readonly baseSqlType = `INTEGER`;
  readonly attr: gimc.Integer;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
  ) {
    if (gimc.isAttribute(forSrc)) {
      this.attr = forSrc as gimc.Integer;
    } else {
      this.attr = forSrc.attr as gimc.Integer;
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    return new gimRDS.IntegerColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.IntegerTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return sqlTypes(IntegerSqlType.baseSqlType);
  }
}

export class NumericIdentitySqlType implements gimRDS.AttrSqlType {
  readonly attr: gimc.NumericIdentity;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
  ) {
    if (gimc.isAttribute(forSrc)) {
      this.attr = forSrc as gimc.NumericIdentity;
    } else {
      this.attr = forSrc.attr as gimc.NumericIdentity;
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    return new gimRDS.NumericIdentityColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.NumericIdentityTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return sqlTypes(IntegerSqlType.baseSqlType);
  }
}

export class BooleanSqlType implements gimRDS.AttrSqlType {
  static readonly baseSqlType = `BOOLEAN`;
  readonly attr: gimc.Boolean;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
  ) {
    if (gimc.isAttribute(forSrc)) {
      this.attr = forSrc as gimc.Boolean;
    } else {
      this.attr = forSrc.attr as gimc.Boolean;
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    return new gimRDS.BooleanColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.BooleanTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return sqlTypes(BooleanSqlType.baseSqlType);
  }
}

export class AutoIdentityNativeSqlType implements gimRDS.AttrSqlType {
  readonly attr: gimc.AutoIdentityNative;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
  ) {
    if (gimc.isAttribute(forSrc)) {
      this.attr = forSrc as gimc.AutoIdentityNative;
    } else {
      this.attr = forSrc.attr as gimc.AutoIdentityNative;
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    return new gimRDS.AutoIdentityNativeColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.AutoIdentityNativeTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return sqlTypes(UuidSqlType.baseSqlType);
  }
}

export class TextSqlType implements gimRDS.AttrSqlType {
  readonly attr: gimc.Text;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
  ) {
    if (gimc.isAttribute(forSrc)) {
      this.attr = forSrc as gimc.Text;
    } else {
      this.attr = forSrc.attr as gimc.Text;
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    return new gimRDS.TextColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.TextTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return {
      ...sqlTypes("TEXT"),
      nonRefDDL: `VARCHAR(${this.attr.maxLength})`,
      fKRefDDL: `VARCHAR(${this.attr.maxLength})`,
    };
  }
}

export class EncryptedTextSqlType implements gimRDS.AttrSqlType {
  readonly attr: gimc.Text;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
  ) {
    if (gimc.isAttribute(forSrc)) {
      this.attr = forSrc as gimc.Text;
    } else {
      this.attr = forSrc.attr as gimc.Text;
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    // TODO push this into dialect, not here (this is supposed to be generic?)
    return new gimRDS.PostgreSqlEncryptedTextColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.PostgreSqlEncryptedTextTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return {
      ...sqlTypes("TEXT"),
      nonRefDDL: `VARCHAR(${this.attr.maxLength})`,
      fKRefDDL: `VARCHAR(${this.attr.maxLength})`,
    };
  }
}

export class UuidSqlType implements gimRDS.AttrSqlType {
  static readonly baseSqlType = `UUID`;
  readonly attr: gimc.UuidText;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
  ) {
    if (gimc.isAttribute(forSrc)) {
      this.attr = forSrc as gimc.UuidText;
    } else {
      this.attr = forSrc.attr as gimc.UuidText;
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    // TODO push this into dialect, not here (this is supposed to be generic?)
    return new gimRDS.UuidColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.UuidTransientColumn(ctx, te, this);
  }
  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return sqlTypes(UuidSqlType.baseSqlType);
  }
}

export class TextIdentitySqlType implements gimRDS.AttrSqlType {
  readonly attr: gimc.TextIdentity;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
  ) {
    if (gimc.isAttribute(forSrc)) {
      this.attr = forSrc as gimc.TextIdentity;
    } else {
      this.attr = forSrc.attr as gimc.TextIdentity;
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    return new gimRDS.TextIdentityColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.TextIdentityTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return {
      ...sqlTypes("TEXT"),
      nonRefDDL: `VARCHAR(${this.attr.maxLength})`,
      fKRefDDL: `VARCHAR(${this.attr.maxLength})`,
    };
  }
}

export class DateSqlType implements gimRDS.AttrSqlType {
  readonly attr: gimc.Date;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
  ) {
    if (gimc.isAttribute(forSrc)) {
      this.attr = forSrc as gimc.Date;
    } else {
      this.attr = forSrc.attr as gimc.Date;
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    return new gimRDS.DateColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.DateTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return sqlTypes("DATE");
  }
}

export class DateTimeSqlType implements gimRDS.AttrSqlType {
  readonly attr: gimc.DateTime;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
  ) {
    if (gimc.isAttribute(forSrc)) {
      this.attr = forSrc as gimc.DateTime;
    } else {
      this.attr = forSrc.attr as gimc.DateTime;
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    return new gimRDS.DateTimeColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.DateTimeTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return sqlTypes("TIMESTAMPTZ");
  }
}

export class SelfReferenceSqlType implements gimRDS.AttrSqlType {
  // deno-lint-ignore no-explicit-any
  readonly attr: gimc.SelfReference<any>;
  readonly refSqlTypes?: gimRDS.ContextualSqlTypes;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
    eh?: gimRDS.AttrSqlTypesErrorHandler,
  ) {
    if (gimc.isAttribute(forSrc)) {
      // deno-lint-ignore no-explicit-any
      this.attr = forSrc as gimc.SelfReference<any>;
    } else {
      // deno-lint-ignore no-explicit-any
      this.attr = forSrc.attr as gimc.SelfReference<any>;
    }
    if (gimc.isIdentityManager(this.attr.parent)) {
      const refSqlType = ctx.dialect.sqlType(ctx, this.attr.parent.identity);
      if (refSqlType) {
        this.refSqlTypes = refSqlType.sqlTypes(ctx);
      } else if (eh) {
        eh(
          this.attr,
          `[SQT002] PK ref not found for ${this.attr.parent.name.inflect()}.${this.attr.name.relationalColumnName.inflect()})`,
        );
      }
    } else if (eh) {
      eh(
        this.attr,
        `[SQT003] ${this.attr.parent.name.inflect()}.${this.attr.name.relationalColumnName.inflect()}) has no primary key to reference`,
      );
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    return new gimRDS.SelfReferenceColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.SelfReferenceTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return this.refSqlTypes ? this.refSqlTypes : sqlTypes("ERROR[SQT000-1]");
  }
}

export class RelationshipSqlType implements gimRDS.AttrSqlType {
  // deno-lint-ignore no-explicit-any
  readonly attr: gimc.Relationship<any>;
  readonly refSqlTypes?: gimRDS.ContextualSqlTypes;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
    eh?: gimRDS.AttrSqlTypesErrorHandler,
  ) {
    if (gimc.isAttribute(forSrc)) {
      // deno-lint-ignore no-explicit-any
      this.attr = forSrc as gimc.Relationship<any>;
    } else {
      // deno-lint-ignore no-explicit-any
      this.attr = forSrc.attr as gimc.Relationship<any>;
    }
    const ref = this.attr.reference;
    const refSqlType = ctx.dialect.sqlType(ctx, ref.attr);
    if (refSqlType) {
      this.refSqlTypes = refSqlType.sqlTypes(ctx);
    } else {
      if (eh) {
        eh(
          this.attr,
          `[SQT001] ref not found for ${this.attr.parent.name.inflect()}.${this.attr.name.relationalColumnName.inflect()})`,
        );
      }
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    return new gimRDS.RelationshipColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.RelationshipTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return this.refSqlTypes ? this.refSqlTypes : sqlTypes("ERROR[SQT000]");
  }
}

export class JsonSqlType implements gimRDS.AttrSqlType {
  readonly attr: gimc.Json;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
  ) {
    if (gimc.isAttribute(forSrc)) {
      this.attr = forSrc as gimc.Json;
    } else {
      this.attr = forSrc.attr as gimc.Json;
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    return new gimRDS.JsonColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.JsonTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return sqlTypes("JSON");
  }
}

export class JsonbSqlType implements gimRDS.AttrSqlType {
  readonly attr: gimc.Jsonb;

  constructor(
    ctx: gimRDS.RdbmsEngineContext,
    readonly forSrc: gimRDS.AttrMapperSource,
  ) {
    if (gimc.isAttribute(forSrc)) {
      this.attr = forSrc as gimc.Jsonb;
    } else {
      this.attr = forSrc.attr as gimc.Jsonb;
    }
  }

  get forAttr(): gimc.Attribute {
    return this.attr;
  }

  persistentColumn(
    ctx: gimRDS.RdbmsEngineContext,
    table: gimRDS.Table,
  ): gimRDS.PersistentColumn {
    return new gimRDS.JsonbColumn(ctx, table, this);
  }

  transientColumn(
    ctx: gimRDS.RdbmsEngineContext,
    te: gimc.TransientEntity,
  ): gimRDS.TransientColumn {
    return new gimRDS.JsonbTransientColumn(ctx, te, this);
  }

  sqlTypes(ctx: gimRDS.RdbmsEngineContext): gimRDS.ContextualSqlTypes {
    return sqlTypes("JSONB");
  }
}

export class AttrAnsiRdbmsEngineSqlTypesMapper
  implements gimRDS.AttrSqlTypesMapper {
  static readonly ansiAttrSqlTypes: gimRDS.AttrSqlTypesRegistration[] = [
    {
      registryKeys: [gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Integer"],
      constructor: IntegerSqlType,
    },
    {
      registryKeys: [
        gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.AutoIdentityNative",
      ],
      constructor: AutoIdentityNativeSqlType,
    },
    {
      registryKeys: [
        gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.NumericIdentity",
      ],
      constructor: NumericIdentitySqlType,
    },
    {
      registryKeys: [gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.TextIdentity"],
      constructor: TextIdentitySqlType,
    },
    {
      registryKeys: [gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Text"],
      constructor: TextSqlType,
    },
    {
      registryKeys: [gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.EncryptedText"],
      constructor: EncryptedTextSqlType,
    },
    {
      registryKeys: [gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.UuidText"],
      constructor: UuidSqlType,
    },
    {
      registryKeys: [gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Date"],
      constructor: DateSqlType,
    },
    {
      registryKeys: [gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.DateTime"],
      constructor: DateTimeSqlType,
    },
    {
      registryKeys: [
        gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.SelfReference",
      ],
      constructor: SelfReferenceSqlType,
    },
    {
      registryKeys: [
        gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Relationship",
      ],
      constructor: RelationshipSqlType,
    },
    {
      registryKeys: [gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Boolean"],
      constructor: BooleanSqlType,
    },
    {
      registryKeys: [gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Json"],
      constructor: JsonSqlType,
    },
    {
      registryKeys: [gimc.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Jsonb"],
      constructor: JsonbSqlType,
    },
  ];
  readonly ansiAttrMap: gimRDS.AttrSqlType[] = [];

  constructor() {
  }

  map(
    ctx: gimRDS.RdbmsEngineContext,
    forSrc: gimRDS.AttrMapperSource,
    ifNotFound?: gimRDS.AttrSqlTypesConstructor,
  ): gimRDS.AttrSqlType | undefined {
    const attr = gimc.isAttribute(forSrc) ? forSrc : forSrc.attr;
    for (const aast of AttrAnsiRdbmsEngineSqlTypesMapper.ansiAttrSqlTypes) {
      for (const aastRegKey of aast.registryKeys) {
        for (const attrRegKey of attr.registryKeys(ctx)) {
          if (
            aastRegKey.toLocaleUpperCase() == attrRegKey.toLocaleUpperCase()
          ) {
            return new aast.constructor(ctx, attr);
          }
        }
      }
    }

    return ifNotFound ? new ifNotFound(ctx, attr) : undefined;
  }
}

export const defaultAttrRdbmsEngineMapper =
  new AttrAnsiRdbmsEngineSqlTypesMapper();
