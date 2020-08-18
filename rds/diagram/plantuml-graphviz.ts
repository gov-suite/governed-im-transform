import * as trSQL from "../sql/mod.ts";
import {
  governedIM as gim,
  governedImRDS as gimRDS,
  governedImTransform as gimTr,
  polyglotArtfNature,
  textInflect as infl,
} from "./deps.ts";

export type CreateNodePredicate = (table: gimRDS.Table) => boolean;
export type CreateEdgePredicate = (
  createNodePredicate: CreateNodePredicate,
  rel: gimRDS.TableColumnRelationship,
) => boolean;

function createAllNodes(table: gimRDS.Table): boolean {
  return true;
}

function createNonEnumDomainNodes(table: gimRDS.Table): boolean {
  return !("isEnumeration" in table.entity || "isDomain" in table.entity);
}

function createAllEdges(
  createNodePredicate: CreateNodePredicate,
  rel: gimRDS.TableColumnRelationship,
): boolean {
  if (!createNodePredicate(rel.source.table)) return false;
  return true;
}

function createNonEnumDomainEdges(
  createNodePredicate: CreateNodePredicate,
  rel: gimRDS.TableColumnRelationship,
): boolean {
  if (!createNodePredicate(rel.source.table)) return false;
  return !(
    "isEnumeration" in rel.references.table.entity ||
    "isDomain" in rel.references.table.entity
  );
}

export class PlantUmlGraphvizTransformer implements gimTr.InfoModelTransformer {
  readonly isInfoModelTransformer = true;
  readonly reCtx: gimRDS.RdbmsEngineContext;
  readonly createNodePredicate: CreateNodePredicate = createNonEnumDomainNodes;
  readonly createEdgePredicate: CreateEdgePredicate = createNonEnumDomainEdges;

  constructor(readonly options: trSQL.RdbmsModelTransformerOptions) {
    this.reCtx = gimRDS.rdbmsCtxFactory.rdbmsEngineContext(
      options.spec,
      options.dialect,
    );
  }

  protected column(tc: gimRDS.TableColumn): string {
    let required = tc.column.nullable ? "" : "*";
    let descr = tc.column.primaryKey ? "PK" : "";
    if ("isSelfReference" in tc.column.forAttr) descr = "SR";
    let sqlType = tc.column.references
      ? tc.column.references.table.name(this.reCtx)
      : tc.column.sqlTypes(this.reCtx).nonRefDDL;
    return `        <tr><td port="${
      tc.column.name(this.reCtx)
    }">${required}</td><td>${descr}</td><td>${
      tc.column.name(this.reCtx)
    }</td><td>${sqlType}</td></tr>`;
  }

  protected backRef(
    table: gimRDS.Table,
    backRef: gim.InboundRelationshipBackRef<gim.Entity>,
  ): string {
    const name = infl.toCamelCase(backRef.name.plural);
    const type = backRef.rel.fromAttr.parent
      ? infl.toPascalCase(backRef.rel.fromAttr.parent.name)
      : "SHOULD_NEVER_HAPPEN!";
    return `        <tr><td></td><td>BR</td><td><i>${name}</i></td><td>${type}[]</td></tr>`;
  }

  protected table(table: gimRDS.Table): string[] {
    let columns: string[] = [];
    for (const column of table.columns) {
      columns.push(this.column({ table: table, column: column }));
    }

    let backRefs: string[] = [];
    if (table.entity.backRefs) {
      for (const backRef of table.entity.backRefs) {
        backRefs.push(this.backRef(table, backRef));
      }
    }

    return [
      `  ${table.name(this.reCtx)} [
            label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="4">
                <tr><td colspan="4">${table.name(this.reCtx)}</td></tr>`,
      ...columns,
      ...backRefs,
      `          </table>>
            ]`,
    ];
  }

  protected tables(): string[] {
    let result: string[] = [];
    for (const table of this.options.rdbmsModel.tables) {
      if (!this.createNodePredicate(table)) {
        continue;
      }

      result = result.concat(this.table(table));
    }
    return result;
  }

  protected relationships(): string[] {
    let result: string[] = [];
    for (const rel of this.options.rdbmsModel.relationships) {
      if (!this.createEdgePredicate(this.createNodePredicate, rel)) {
        continue;
      }

      const src = rel.source;
      const ref = rel.references;
      result.push(
        `${ref.table.name(this.reCtx)} -> ${src.table.name(this.reCtx)}:${
          src.column.name(this.reCtx)
        }`,
      );
    }
    return result;
  }

  public transform(context: trSQL.RdbmsModelSqlTransformerContext): void {
    // TODO check out https://voormedia.github.io/rails-erd/gallery.html for
    // nicer Graphviz configurations
    const content = [
      "@startdot dot",
      "digraph G {",
      "  node [shape=none, margin=0]",
      "  edge [arrowhead=crow, arrowtail=none, dir=both]\n",
      ...this.tables(),
      ...this.relationships(),
      "}",
      "@enddot",
    ].join("\n");
    const ph = this.options.persist;
    const artifact = ph.createMutableTextArtifact(context, {
      nature: polyglotArtfNature.plantUmlArtifact,
    });
    artifact.appendText(context, content);
    const pr = ph.persistTextArtifact(
      context,
      this.options.rdbmsModel.imStructure.namespace.identifier,
      artifact,
    );
    context.transformations.modelPrimaryArtifactResult = pr;
  }
}
