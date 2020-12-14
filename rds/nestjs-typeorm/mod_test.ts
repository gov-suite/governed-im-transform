import * as trSqlDia from "../sql/dialect/mod.ts";
import * as trSQL from "../sql/mod.ts";
import {
  governedImTestModel as gimTM,
  specModule as sm,
  testingAsserts as ta,
} from "./deps-test.ts";
import {
  artfPersist as ap,
  governedIM as gim,
  governedImRDS as gimRDS,
} from "./deps.ts";
import * as nestJS from "./mod.ts";

function primaryResultSupplier(
  ctx: gimRDS.RdbmsEngineContext,
): ap.PersistenceResult | undefined {
  return (ctx as trSQL.RdbmsModelSqlTransformerContext).transformations
    .modelPrimaryArtifactResult;
}

Deno.test("NestJS TypeORM Middleware Server Transformer", async () => {
  await testRdbmsModelTransform(
    nestJS.TypeOrmTransformer,
    trSqlDia.PostgreSqlDialect,
    "mod_test.ts.golden",
  );
});

// tests can be run from multiple places so we need to be able to locate our
// golden files from various locations, all the way to the top of the repo
export const testSuiteGoldenFilePaths = [
  ".",
  "./nestjs-typeorm",
  "./rds/nestjs-typeorm",
  "./governed-im-transform/rds/nestjs-typeorm",
];

export interface TestAgainstGolden {
  readonly persistedName?: string;
  readonly goldenFile: string;
}

// deno-lint-ignore require-await
export async function testRdbmsModelTransform(
  transformer: trSQL.RdbmsModelTransformerConstructor,
  dialect: gimRDS.Dialect | gimRDS.DialectConstructor,
  golden?: string | TestAgainstGolden[],
): Promise<ap.PersistenceHandler | undefined> {
  const spec = sm.specFactory.spec<gim.InformationModel>(
    new gimTM.TestModel(),
  );
  const ph = new ap.InMemoryPersistenceHandler();
  const [ctx, imt] = trSQL.transformRdbmsModel(
    transformer,
    spec,
    gimRDS.isDialect(dialect) ? dialect : new dialect(),
    ph,
  );

  // if a file name (string) is given, use that as the "golden file"
  // if a list of golden files is given, use that list
  // if no list is given, assume that each artifact name has a golden file with the same name
  const testAgainst: TestAgainstGolden[] = golden
    ? (typeof golden === "string" ? [{ goldenFile: golden }] : golden)
    : [];
  if (testAgainst.length == 0) {
    for (const phr of ph.results) {
      // if there's a naming strategy, fix up the path
      const path = ctx.dialect.name.inflect() + "_" +
        ctx.dialect.namingStrategy.strategyName.inflect();
      testAgainst.push(
        {
          persistedName: phr.finalArtifactNameLogical,
          goldenFile:
            `sql_test/${path}/${phr.finalArtifactNameLogical}.sql.golden`,
        },
      );
    }
  }

  for (const g of testAgainst) {
    let artifact = undefined;
    if (g.persistedName) {
      artifact = ph.resultsMap.get(g.persistedName);
      ta.assert(
        artifact,
        `artifact ${g.persistedName} not found in peristence handler`,
      );
    } else {
      const primaryResult = ctx.transformations.modelPrimaryArtifactResult;
      ta.assert(primaryResult);
      const primeArtifact = ph.resultsMap.get(
        primaryResult.finalArtifactNameLogical,
      );
      artifact = primeArtifact;
      ta.assert(
        artifact,
        `artifact ${primaryResult.finalArtifactNameLogical} not found in peristence handler`,
      );
    }

    const goldenContent = ap.readFileAsTextFromPaths(
      g.goldenFile,
      testSuiteGoldenFilePaths,
    );
    if (goldenContent) {
      ta.assertEquals(
        goldenContent,
        artifact.artifactText,
      );
    } else {
      console.log(
        `Unable to test '${artifact.finalArtifactNameLogical}': golden file '${g.goldenFile}' not found`,
      );
    }
  }

  return ph;
}
