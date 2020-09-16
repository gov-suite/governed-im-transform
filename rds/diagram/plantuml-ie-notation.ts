import type * as trSQL from "../sql/mod.ts";
import {
  governedIM as gim,
  governedImRDS as gimRDS,
  governedImTransform as gimTr,
  polyglotArtfNature,
  textInflect as infl,
} from "./deps.ts";

export type IncludeColumnPredicate = (
  ctx: gimRDS.RdbmsEngineContext,
  tc: gimRDS.TableColumn,
) => boolean;
export type IncludeEntityPredicate = (
  ctx: gimRDS.RdbmsEngineContext,
  table: gimRDS.Table,
) => boolean;
export type IncludeRelationshipPredicate = (
  ctx: gimRDS.RdbmsEngineContext,
  includeColumn: IncludeColumnPredicate,
  includeEntity: IncludeEntityPredicate,
  rel: gimRDS.TableColumnRelationship,
) => boolean;

function includeAllColumns(
  ctx: gimRDS.RdbmsEngineContext,
  tc: gimRDS.TableColumn,
): boolean {
  return true;
}

function includeNonHousekeepingColumns(
  ctx: gimRDS.RdbmsEngineContext,
  tc: gimRDS.TableColumn,
): boolean {
  const columnName = tc.column.name(ctx);
  return columnName != "created_at" && columnName != "updated_on" &&
    columnName != "record_status_id";
}

function includeAllEntities(
  ctx: gimRDS.RdbmsEngineContext,
  table: gimRDS.Table,
): boolean {
  return true;
}

function includeNonEnumEntities(
  ctx: gimRDS.RdbmsEngineContext,
  table: gimRDS.Table,
): boolean {
  return !gim.isEnumeration(table.entity);
}

function includeAllRelationships(
  ctx: gimRDS.RdbmsEngineContext,
  includeColumn: IncludeColumnPredicate,
  includeEntity: IncludeEntityPredicate,
  rel: gimRDS.TableColumnRelationship,
): boolean {
  if (!includeEntity(ctx, rel.source.table)) return false;
  if (!includeColumn(ctx, rel.source)) return false;
  return true;
}

function includeNonEnumRelationships(
  ctx: gimRDS.RdbmsEngineContext,
  includeColumn: IncludeColumnPredicate,
  includeEntity: IncludeEntityPredicate,
  rel: gimRDS.TableColumnRelationship,
): boolean {
  if (!includeEntity(ctx, rel.source.table)) return false;
  if (!includeColumn(ctx, rel.source)) return false;
  return !gim.isEnumeration(rel.references.table.entity);
}

export class PlantUmlInfoEngrModelTransformer
  implements gimTr.InfoModelTransformer {
  readonly isInfoModelTransformer = true;
  readonly includeColumn: IncludeColumnPredicate =
    includeNonHousekeepingColumns;
  readonly includeEntity: IncludeEntityPredicate = includeNonEnumEntities;
  readonly includeRelationship: IncludeRelationshipPredicate =
    includeNonEnumRelationships;
  readonly reCtx: gimRDS.RdbmsEngineContext;

  constructor(readonly options: trSQL.RdbmsModelTransformerOptions) {
    this.reCtx = gimRDS.rdbmsCtxFactory.rdbmsEngineContext(
      options.spec,
      options.dialect,
    );
  }

  protected column(tc: gimRDS.TableColumn): string {
    let required = tc.column.nullable ? "" : "*";
    const name = tc.column.primaryKey
      ? `**${tc.column.name(this.reCtx)}**`
      : tc.column.name(this.reCtx);
    let descr = tc.column.references
      ? (gim.isEnumeration(tc.column.references.table.entity)
        ? ` <<ENUM(${tc.column.references.table.name(this.reCtx)})>> `
        : ` <<FK(${tc.column.references.table.name(this.reCtx)})>>`)
      : "";
    if ("isSelfReference" in tc.column.forAttr) descr = " <<SELF>>";
    let sqlType = tc.column.references
      ? tc.column.references.column.sqlTypes(this.reCtx).fKRefDDL
      : tc.column.sqlTypes(this.reCtx).nonRefDDL;
    return `    ${required} ${name}: ${sqlType}${descr}`;
  }

  protected backRef(
    table: gimRDS.Table,
    backRef: gim.InboundRelationshipBackRef<gim.Entity>,
  ): string {
    const name = infl.toCamelCase(backRef.name.plural);
    const type = backRef.rel.fromAttr.parent
      ? infl.toPascalCase(backRef.rel.fromAttr.parent.name)
      : "SHOULD_NEVER_HAPPEN!";
    return `    ${name}: ${type}[]`;
  }

  protected table(table: gimRDS.Table): string[] {
    let columns: string[] = [];
    const pk = table.primaryKey;
    if (pk) {
      columns.push(this.column({ table: table, column: pk }));
      columns.push("    --");
    }
    for (const column of table.columns) {
      if (column.primaryKey) continue;
      const tc = { table: table, column: column };
      if (!this.includeColumn(this.reCtx, tc)) continue;
      columns.push(this.column(tc));
    }

    let backRefs: string[] = [];
    if (table.entity.backRefs) {
      for (const backRef of table.entity.backRefs) {
        backRefs.push(this.backRef(table, backRef));
      }
    }
    if (backRefs.length > 0) {
      backRefs.unshift("    --");
    }

    return [
      `  entity "${gim.isEnumeration(table.entity) ? "Enum " : ""}${
        table.name(this.reCtx)
      }" as ${table.name(this.reCtx)} {`,
      ...columns,
      ...backRefs,
      `  }`,
    ];
  }

  protected tables(): string[] {
    let result: string[] = [];
    for (const table of this.options.rdbmsModel.tables) {
      if (!this.includeEntity(this.reCtx, table)) {
        continue;
      }

      result = result.concat(this.table(table));
    }
    return result;
  }

  protected relationships(): string[] {
    let result: string[] = [];
    console.dir(this.options.rdbmsModel.relationships);
    for (const rel of this.options.rdbmsModel.relationships) {
      if (
        !this.includeRelationship(
          this.reCtx,
          this.includeColumn,
          this.includeEntity,
          rel,
        )
      ) {
        console.log("01", rel);
        continue;
      }
      const refIsEnum = !gim.isEnumeration(rel.references.table.entity);
      console.log("02", refIsEnum);

      const src = rel.source;
      const ref = rel.references;
      // Relationship types see: https://plantuml.com/es/ie-diagram
      // Zero or One	|o--
      // Exactly One	||--
      // Zero or Many	}o--
      // One or Many	}|--
      const relIndicator = refIsEnum ? "|o..o|" : "|o..o{";
      result.push(
        `  ${ref.table.name(this.reCtx)} ${relIndicator} ${
          src.table.name(this.reCtx)
        }`,
      );
    }
    console.log("03", result.length);
    if (result.length > 0) result.unshift("");
    console.dir(result);
    return result;
  }

  public transform(context: trSQL.RdbmsModelSqlTransformerContext): void {
    const ph = this.options.persist;
    const artifact = ph.createMutableTextArtifact(context, {
      nature: polyglotArtfNature.plantUmlArtifact,
    });
    const content = [
      `@startuml ${
        this.options.rdbmsModel.spec.target?.structure.namespace.qualifiedName
      }`,
      "  hide circle",
      "  skinparam linetype ortho",
      "  skinparam roundcorner 20",
      "  skinparam class {",
      "    BackgroundColor White",
      "    ArrowColor Silver",
      "    BorderColor Silver",
      "    FontColor Black",
      "    FontSize 12",
      "  }\n",
      ...this.tables(),
      ...this.relationships(),
      "@enduml",
    ].join("\n");
    artifact.appendText(context, content);
    const pr = ph.persistTextArtifact(
      context,
      this.options.rdbmsModel.imStructure.namespace.identifier,
      artifact,
    );
    context.transformations.modelPrimaryArtifactResult = pr;
  }
}
