import type * as sqlTrCtx from "./context.ts";
import type { governedImTransform as gimTr } from "./deps.ts";
import type * as sqlTr from "./transform.ts";

export * from "./context.ts";
export * from "./dialect.ts";
export * from "./naming.ts";
export * from "./transform.ts";

export class RdbmsSqlTransformer implements gimTr.InfoModelTransformer {
  readonly isInfoModelTransformer = true;

  constructor(readonly options: sqlTr.RdbmsModelTransformerOptions) {
  }

  public transform(ctx: sqlTrCtx.RdbmsModelSqlTransformerContext): void {
    this.options.dialect.persistModel(ctx);
  }
}
