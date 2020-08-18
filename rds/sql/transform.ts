import {
  governedIM as gimc,
  governedImRDS as gimRDS,
  governedImTransform as gimTr,
  specModule as sm,
  artfPersist as ap,
} from "./deps.ts";
import {
  rdbmsCtxFactory,
  RdbmsModelSqlTransformerContext,
} from "./context.ts";

export interface RdbmsModelTransformerDialectOptions {
  readonly isRdbmsModelTransformerDialectOptions: true;
}

export interface RdbmsModelTransformerOptions
  extends gimTr.InfoModelTransformerOptions {
  readonly isRdbmsTransformerOptions: true;
  readonly rdbmsModel: gimRDS.RdbmsModelStruct;
  readonly dialect: gimRDS.Dialect;
  readonly dialectOptions?: RdbmsModelTransformerDialectOptions;
}

export interface RdbmsModelTransformerConstructor {
  new (options: RdbmsModelTransformerOptions): gimTr.InfoModelTransformer;
}

export function transformRdbmsModel(
  transformer: RdbmsModelTransformerConstructor,
  spec: sm.Specification<gimc.InformationModel>,
  dc: gimRDS.Dialect | gimRDS.DialectConstructor,
  ph: ap.PersistenceHandler,
): [RdbmsModelSqlTransformerContext, gimTr.InfoModelTransformer] {
  const dialect = gimRDS.isDialect(dc) ? dc : new dc();
  const rmto: RdbmsModelTransformerOptions = {
    isInfoModelTransformerOptions: true,
    isRdbmsTransformerOptions: true,
    spec: spec,
    model: spec.target,
    rdbmsModel: new gimRDS.RdbmsModelStruct(
      spec,
      dialect,
    ),
    dialect: dialect,
    persist: ph,
  };
  const imt = new transformer(rmto);
  const context = rdbmsCtxFactory.rdbmsModelSqlTransformerContext(rmto);
  imt.transform(context);
  return [context, imt];
}
