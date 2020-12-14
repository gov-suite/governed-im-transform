import { assert } from "https://deno.land/std@0.81.0/testing/asserts.ts";
import {
  governedIM as gim,
  governedImRDS as gimRDS,
  governedImTransform as gimTr,
  polyglotArtfNature,
  textInflect as infl,
} from "./deps.ts";
import * as trSQL from "../sql/mod.ts";

export interface Field {
  readonly column: gimRDS.PersistentColumn;
  readonly name: string;
  readonly tsType: string;
  readonly decorators?: string[];
  readonly nullable: boolean;
}

export interface FieldCreatorErrorHandler {
  onFieldCreationError(
    totr: TypeOrmTransformer,
    tc: gimRDS.TableColumn,
    msg: string,
    ...args: any[]
  ): void;
}

const defaultFieldCreatorErrorHandler: FieldCreatorErrorHandler = {
  onFieldCreationError(
    totr: TypeOrmTransformer,
    tc: gimRDS.TableColumn,
    msg: string,
    ...args: any[]
  ): void {
    console.error(msg);
  },
};

export type FieldCreatorResult = Field | null;

export interface FieldCreator {
  createField(
    parent: FieldCreator,
    totr: TypeOrmTransformer,
    tc: gimRDS.TableColumn,
    eh?: FieldCreatorErrorHandler,
  ): FieldCreatorResult;
}

export interface FieldCreatorForAttrKeys {
  registryKeys: gim.AttributeRegistryKeys;
  constructor: FieldCreator;
}

export class TypeOrmAttributeColumnMatcher implements FieldCreator {
  readonly chained: FieldCreator | undefined;
  readonly creators: Map<gim.AttributeRegistryKey, FieldCreator> = new Map();

  constructor(defns: FieldCreatorForAttrKeys[], chained?: FieldCreator) {
    this.chained = chained;
    for (const ccd of defns) {
      for (const rk of ccd.registryKeys) {
        if (this.creators.get(rk)) {
          console.error(
            `Ignoring redefinition of column creator for attr registration key: ${rk}`,
          );
        } else {
          this.creators.set(rk, ccd.constructor);
        }
      }
    }
  }

  createField(
    parent: FieldCreator,
    totr: TypeOrmTransformer,
    tc: gimRDS.TableColumn,
    eh?: FieldCreatorErrorHandler,
  ): FieldCreatorResult {
    if (this.chained) {
      const ccr = this.chained.createField(parent, totr, tc, eh);
      if (ccr != null) return ccr;
    }

    const rKeys = tc.column.forAttr.registryKeys(
      gimRDS.rdbmsCtxFactory.TODO(totr.options.rdbmsModel.spec),
    );
    for (const rk of rKeys) {
      const cc = this.creators.get(rk);
      if (cc) {
        return cc.createField(parent, totr, tc, eh);
      }
    }

    if (eh) {
      eh.onFieldCreationError(
        totr,
        tc,
        `${
          tc.column.qualifiedName(totr.context)
        } tc.column.forAttr did not match any valid typescript field type registered as ${rKeys}.`,
      );
    }
    return null;
  }
}

export const defaultAttrFieldCreators: FieldCreatorForAttrKeys[] = [
  {
    registryKeys: [
      gim.DEFAULT_REGISTRY_KEY_MODULE + ".attr.AutoIdentityNative",
    ],
    constructor: {
      createField(
        parent: FieldCreator,
        totr: TypeOrmTransformer,
        tc: gimRDS.TableColumn,
        errorHandler?: FieldCreatorErrorHandler,
      ): FieldCreatorResult {
        const attr = tc.column.forAttr as gim.AutoIdentityNative;
        const isRequired = attr.isRequired(
          gimRDS.rdbmsCtxFactory.TODO(totr.options.rdbmsModel.spec),
        );
        return {
          column: tc.column,
          name: "id", // if this isn't called "id" some code generation problems occur
          tsType: "number",
          decorators: [
            `@ApiProperty({ required: ${isRequired} })`,
            `@PrimaryGeneratedColumn({name: '${
              tc.column.name(totr.context)
            }'})`,
          ],
          nullable: !isRequired,
        };
      },
    },
  },
  {
    registryKeys: [gim.DEFAULT_REGISTRY_KEY_MODULE + ".attr.NumericIdentity"],
    constructor: {
      createField(
        parent: FieldCreator,
        totr: TypeOrmTransformer,
        tc: gimRDS.TableColumn,
        errorHandler?: FieldCreatorErrorHandler,
      ): FieldCreatorResult {
        const attr = tc.column.forAttr as gim.NumericIdentity;
        const isRequired = attr.isRequired(
          gimRDS.rdbmsCtxFactory.TODO(totr.options.rdbmsModel.spec),
        );
        return {
          column: tc.column,
          name: "id", // if this isn't called "id" some code generation problems occur
          tsType: "number",
          decorators: [
            `@ApiProperty({ required: ${isRequired} })`,
            `@PrimaryColumn({name: '${tc.column.name(totr.context)}'})`,
          ],
          nullable: !isRequired,
        };
      },
    },
  },
  {
    registryKeys: [
      gim.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Text",
      // TODO gim.DEFAULT_REGISTRY_KEY_MODULE + ".attr.EncryptedText",
    ],
    constructor: {
      createField(
        parent: FieldCreator,
        tr: TypeOrmTransformer,
        tc: gimRDS.TableColumn,
        errorHandler?: FieldCreatorErrorHandler,
      ): FieldCreatorResult {
        const attr = tc.column.forAttr as gim.Text;
        const isRequired = attr.isRequired(
          gimRDS.rdbmsCtxFactory.TODO(tr.options.rdbmsModel.spec),
        );
        return {
          column: tc.column,
          name: infl.toCamelCase(attr.name.objectFieldName),
          tsType: "string",
          decorators: [
            `@ApiProperty({ required: ${isRequired} })`,
            `@Column({name: '${tc.column.name(tr.context)}'})`,
          ],
          nullable: !isRequired,
        };
      },
    },
  },
  {
    registryKeys: [gim.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Integer"],
    constructor: {
      createField(
        parent: FieldCreator,
        tr: TypeOrmTransformer,
        tc: gimRDS.TableColumn,
        errorHandler?: FieldCreatorErrorHandler,
      ): FieldCreatorResult {
        const attr = tc.column.forAttr as gim.Integer;
        const isRequired = attr.isRequired(
          gimRDS.rdbmsCtxFactory.TODO(tr.options.rdbmsModel.spec),
        );
        return {
          column: tc.column,
          name: infl.toCamelCase(attr.name.objectFieldName),
          tsType: "number",
          decorators: [
            `@ApiProperty({ required: ${isRequired} })`,
            `@Column({name: '${tc.column.name(tr.context)}'})`,
          ],
          nullable: !isRequired,
        };
      },
    },
  },
  {
    registryKeys: [
      gim.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Boolean",
      // TODO gim.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Boolean",
    ],
    constructor: {
      createField(
        parent: FieldCreator,
        tr: TypeOrmTransformer,
        tc: gimRDS.TableColumn,
        errorHandler?: FieldCreatorErrorHandler,
      ): FieldCreatorResult {
        const attr = tc.column.forAttr as gim.Boolean;
        const isRequired = attr.isRequired(
          gimRDS.rdbmsCtxFactory.TODO(tr.options.rdbmsModel.spec),
        );
        return {
          column: tc.column,
          name: infl.toCamelCase(attr.name.objectFieldName),
          tsType: "boolean",
          decorators: [
            `@ApiProperty({ required: ${isRequired} })`,
            `@Column({name: '${tc.column.name(tr.context)}'})`,
          ],
          nullable: !isRequired,
        };
      },
    },
  },
  {
    registryKeys: [gim.DEFAULT_REGISTRY_KEY_MODULE + ".attr.DateTime"],
    constructor: {
      createField(
        parent: FieldCreator,
        tr: TypeOrmTransformer,
        tc: gimRDS.TableColumn,
        errorHandler?: FieldCreatorErrorHandler,
      ): FieldCreatorResult {
        const attr = tc.column.forAttr as gim.DateTime;
        const isRequired = attr.isRequired(
          gimRDS.rdbmsCtxFactory.TODO(tr.options.rdbmsModel.spec),
        );
        return {
          column: tc.column,
          name: infl.toCamelCase(attr.name.objectFieldName),
          tsType: "Date",
          decorators: [
            `@ApiProperty({ required: ${isRequired} })`,
            `@Column({name: '${tc.column.name(tr.context)}'})`,
          ],
          nullable: !isRequired,
        };
      },
    },
  },
  {
    registryKeys: [gim.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Json"],
    constructor: {
      createField(
        parent: FieldCreator,
        tr: TypeOrmTransformer,
        tc: gimRDS.TableColumn,
        errorHandler?: FieldCreatorErrorHandler,
      ): FieldCreatorResult {
        const attr = tc.column.forAttr as gim.Json;
        const isRequired = attr.isRequired(
          gimRDS.rdbmsCtxFactory.TODO(tr.options.rdbmsModel.spec),
        );
        return {
          column: tc.column,
          name: infl.toCamelCase(attr.name.objectFieldName),
          tsType: "string",
          decorators: [
            `@ApiProperty({ required: ${isRequired} })`,
            `@Column({name: '${tc.column.name(tr.context)}'})`,
          ],
          nullable: !isRequired,
        };
      },
    },
  },
  {
    registryKeys: [gim.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Jsonb"],
    constructor: {
      createField(
        parent: FieldCreator,
        tr: TypeOrmTransformer,
        tc: gimRDS.TableColumn,
        errorHandler?: FieldCreatorErrorHandler,
      ): FieldCreatorResult {
        const attr = tc.column.forAttr as gim.Jsonb;
        const isRequired = attr.isRequired(
          gimRDS.rdbmsCtxFactory.TODO(tr.options.rdbmsModel.spec),
        );
        return {
          column: tc.column,
          name: infl.toCamelCase(attr.name.objectFieldName),
          tsType: "string",
          decorators: [
            `@ApiProperty({ required: ${isRequired} })`,
            `@Column({name: '${tc.column.name(tr.context)}'})`,
          ],
          nullable: !isRequired,
        };
      },
    },
  },
  {
    registryKeys: [
      gim.DEFAULT_REGISTRY_KEY_MODULE + ".attr.BelongsToRelationship",
    ],
    constructor: {
      createField(
        parent: FieldCreator,
        tr: TypeOrmTransformer,
        tc: gimRDS.TableColumn,
        errorHandler?: FieldCreatorErrorHandler,
      ): FieldCreatorResult {
        const attr = tc.column.forAttr as gim.BelongsTo<gim.Entity>;
        const isRequired = attr.isRequired(
          gimRDS.rdbmsCtxFactory.TODO(tr.options.rdbmsModel.spec),
        );
        assert(tc.column.references);
        const refColumn = parent.createField(
          parent,
          tr,
          tc.column.references,
          errorHandler,
        );
        if (refColumn) {
          const refEntityIdentifier =
            refColumn.column.forAttr.parent?.name.singular ??
              infl.snakeCaseValue("PARENT_MISSING");
          return {
            column: tc.column,
            name: infl.toCamelCase(attr.name.objectFieldName),
            tsType: infl.toPascalCase(refEntityIdentifier),
            decorators: attr.isBackReferenced && attr.backRefName
              ? [
                `@ApiProperty({ required: ${isRequired} })`,
                `@ManyToOne(type => ${
                  infl.toPascalCase(
                    refEntityIdentifier,
                  )
                }, ${
                  infl.toCamelCase(
                    refEntityIdentifier,
                  )
                } => ${
                  infl.toCamelCase(
                    refEntityIdentifier,
                  )
                }.${infl.toCamelCase(attr.backRefName.plural)})`,
                `@JoinColumn()`,
              ]
              : [
                `@OneToOne(type => ${
                  infl.toPascalCase(
                    refEntityIdentifier,
                  )
                })`,
                `@JoinColumn({name: '${tc.column.name(tr.context)}'})`,
              ],
            nullable: !isRequired,
          };
        }
        if (errorHandler) {
          errorHandler.onFieldCreationError(
            tr,
            tc,
            `${
              tc.column.qualifiedName(tr.context)
            } tc.column.forAttr is a Relationship but attr.reference ${attr.reference.entity.name.singular.inflect()}.${attr.reference.attr.name.relationalColumnName.inflect()} could not be created`,
          );
        }
        return null;
      },
    },
  },
  {
    registryKeys: [gim.DEFAULT_REGISTRY_KEY_MODULE + ".attr.Relationship"],
    constructor: {
      createField(
        parent: FieldCreator,
        tr: TypeOrmTransformer,
        tc: gimRDS.TableColumn,
        errorHandler?: FieldCreatorErrorHandler,
      ): FieldCreatorResult {
        const attr = tc.column.forAttr as gim.Relationship<gim.Entity>;
        const isRequired = attr.isRequired(
          gimRDS.rdbmsCtxFactory.TODO(tr.options.rdbmsModel.spec),
        );
        assert(tc.column.references);
        const refColumn = parent.createField(
          parent,
          tr,
          tc.column.references,
          errorHandler,
        );
        if (refColumn) {
          const tsType = infl.toPascalCase(
            refColumn.column.forAttr.parent?.name.singular ??
              infl.snakeCaseValue("PARENT_MISSING"),
          );
          return {
            column: tc.column,
            name: infl.toCamelCase(attr.name.objectFieldName),
            tsType: tsType,
            decorators: [
              `@ApiProperty({ required: ${isRequired} })`,
              `@OneToOne(type => ${tsType})`,
              `@JoinColumn({name: '${tc.column.name(tr.context)}'})`,
            ],
            nullable: !isRequired,
          };
        }
        if (errorHandler) {
          errorHandler.onFieldCreationError(
            tr,
            tc,
            `${
              tc.column.qualifiedName(tr.context)
            } ea.attr is a Relationship but attr.reference ${attr.reference.entity.name.singular.inflect()}.${attr.reference.attr.name.relationalColumnName.inflect()} could not be created`,
          );
        }
        return null;
      },
    },
  },
];

export const defaultFieldCreator = new TypeOrmAttributeColumnMatcher(
  defaultAttrFieldCreators,
);

type EntityClassNamespace = string;

export interface EntityClass {
  readonly table: gimRDS.Table;
  readonly nameSpace: EntityClassNamespace;
  readonly className: string;
  readonly extendsEntity?: gimRDS.TableColumn;
  readonly fields: Field[];
}

export class TypeOrmTransformer implements gimTr.InfoModelTransformer {
  readonly isInfoModelTransformer = true;
  readonly fc: FieldCreator = defaultFieldCreator;
  readonly entityClasses: EntityClass[] = [];
  readonly context: trSQL.RdbmsModelSqlTransformerContext;

  constructor(readonly options: trSQL.RdbmsModelTransformerOptions) {
    this.context = trSQL.rdbmsCtxFactory.rdbmsModelSqlTransformerContext(
      this.options,
    );
    for (const table of this.options.rdbmsModel.tables) {
      const entity = table.entity;
      const typeName = infl.toPascalCase(entity.name.singular);
      let fields: Field[] = [];
      let extendsEntity: gimRDS.TableColumn | undefined = undefined;
      for (const col of table.columns) {
        if (gim.isExtendsRelationship(col.forAttr)) {
          extendsEntity = col.references;
          continue;
        }
        if (gim.isSelfReference(col.forAttr)) continue; // TODO: need to handle this
        const field = this.fc.createField(
          this.fc,
          this,
          { table: table, column: col },
          defaultFieldCreatorErrorHandler,
        );
        if (field == null) continue;
        fields.push(field);
      }
      this.entityClasses.push({
        nameSpace: table.entity.registryKeys(
          gimRDS.rdbmsCtxFactory.TODO(options.rdbmsModel.spec),
        )[0],
        table: table,
        className: typeName,
        fields: fields,
        extendsEntity: extendsEntity,
      });
    }
  }

  transform(context: trSQL.RdbmsModelSqlTransformerContext): void {
    // TODO: transform(context) duplicates this.context and need to fix in Field gens
    const tsCode: string[] = [
      "import { Injectable } from '@nestjs/common';",
      "import { TypeOrmCrudService } from '@nestjsx/crud-typeorm';",
      "import { InjectRepository } from '@nestjs/typeorm';",
      "import { Repository } from 'typeorm';",
      "import { Module } from '@nestjs/common';",
      "import { TypeOrmModule } from '@nestjs/typeorm';",
      "import { Controller } from '@nestjs/common';",
      "import { Crud, CrudController } from '@nestjsx/crud';",
      "import { Entity, Column, PrimaryGeneratedColumn, PrimaryColumn, OneToOne, OneToMany, ManyToOne, JoinColumn } from 'typeorm';",
      "import { ApiProperty } from '@nestjs/swagger';\n",
      "export const allModules = [];\n",
    ];
    for (const cls of this.entityClasses) {
      const fieldsCode: string[] = [];
      for (const field of cls.fields) {
        if (field.decorators) {
          fieldsCode.push("");
          for (const decorator of field.decorators) {
            fieldsCode.push(`  ${decorator}`);
          }
        }
        fieldsCode.push(
          `  ${field.name}${field.nullable ? "?" : ""}: ${field.tsType};`,
        );
      }
      const backRefs: string[] = [];
      if (cls.table.entity.backRefs) {
        for (const backRef of cls.table.entity.backRefs) {
          const typeIdentifier = backRef.rel.fromAttr.parent?.name.singular ??
            infl.snakeCaseValue("PARENT_MISSING");
          const type = infl.toPascalCase(typeIdentifier);
          backRefs.push(
            `  @OneToMany(type => ${type}, ${
              infl.toCamelCase(
                typeIdentifier,
              )
            } => ${infl.toCamelCase(typeIdentifier)}.${
              infl.toCamelCase(
                backRef.rel.fromAttr.name.objectFieldName,
              )
            })`,
            `  ${infl.toCamelCase(backRef.name.plural)} : ${type}[]`,
          );
        }
      }
      let extendsClass: string[] = [];
      if (cls.extendsEntity && cls.extendsEntity.table.primaryKey) {
        const extendsPkColName = cls.extendsEntity.table.primaryKey.name(
          this.context,
        );
        const extendsPropertyName = infl.toCamelCase(
          cls.extendsEntity.table.entity.name.singular,
        );
        const extendsClassName = infl.toPascalCase(
          cls.extendsEntity.table.entity.name.singular,
        );
        extendsClass = [
          "",
          `  @PrimaryColumn({name: '${extendsPkColName}'})`,
          `  @ApiProperty({ required: true })`,
          `  id: number;\n`,
          `  @OneToOne(type => ${extendsClassName})`,
          `  @JoinColumn({name: '${extendsPkColName}'})`,
          `  ${extendsPropertyName}: ${extendsClassName};`,
        ];
      }
      tsCode.push(
        `@Entity({name: '${cls.table.name(this.context)}'})`,
        `export class ${cls.className} {`,
        ...extendsClass,
        ...fieldsCode,
        ...backRefs,
        "}\n",
        "@Injectable()",
        `export class ${cls.className}Service extends TypeOrmCrudService<${cls.className}> {`,
        `    constructor(@InjectRepository(${cls.className}) repo: Repository<${cls.className}>) {`,
        "        super(repo);",
        "    }",
        "}\n",
        `@Crud({ model: { type: ${cls.className} }})`,
        `@Controller('${infl.toKebabCase(cls.table.entity.name.singular)}')`,
        `export class ${cls.className}Controller implements CrudController<${cls.className}> {`,
        `    constructor(public service: ${cls.className}Service) { }`,
        "}\n",
        `@Module({`,
        `  controllers: [${cls.className}Controller],`,
        `  providers: [${cls.className}Service],`,
        `  imports: [`,
        `    TypeOrmModule.forFeature([${cls.className}]),`,
        `  ],`,
        `  exports: [${cls.className}Service],`,
        `})`,
        `export class ${cls.className}Module {}`,
        `allModules.push(${cls.className}Module)\n`,
      );
    }
    const ph = this.options.persist;
    const artifact = ph.createMutableTextArtifact(this.context, {
      nature: polyglotArtfNature.typeScriptArtifact,
    });
    artifact.appendText(this.context, tsCode.join("\n"));
    const pr = ph.persistTextArtifact(
      this.context,
      this.options.rdbmsModel.imStructure.namespace.identifier,
      artifact,
    );
    context.transformations.modelPrimaryArtifactResult = pr;
  }
}
