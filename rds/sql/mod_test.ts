import {
  governedImTestModel as gimTM,
  testingAsserts as ta,
} from "./deps-test.ts";
import {
  artfPersist as ap,
  governedIM as gimc,
  governedImRDS as gimRDS,
  specModule as sm,
} from "./deps.ts";
import * as dia from "./dialect/mod.ts";
import * as mod from "./mod.ts";

Deno.test("SQLite RDBMS SQL Dialect Transformer", async () => {
  const ph = await testRdbmsModelTransform(
    mod.RdbmsSqlTransformer,
    dia.SQLiteDialect,
  );
  ta.assertEquals(
    4,
    ph!.results.length,
    "Expected 4 artifacts to be persisted",
  );
});

Deno.test("PostgreSQL RDBMS SQL Dialect Transformer (default naming strategy)", async () => {
  const ph = await testRdbmsModelTransform(
    mod.RdbmsSqlTransformer,
    dia.PostgreSqlDialect,
  );
  ta.assertEquals(
    8,
    ph!.results.length,
    "Expected 8 artifacts to be persisted",
  );
});

Deno.test(`PostgreSQL RDBMS SQL Dialect Transformer (naming strategy ${dia.PostgreSqlCommonPkColNamedIdDialect.dialectName.inflect()}: convert all PKs to 'id')`, async () => {
  const ph = await testRdbmsModelTransform(
    mod.RdbmsSqlTransformer,
    dia.PostgreSqlCommonPkColNamedIdDialect,
  );
  ta.assertEquals(
    8,
    ph!.results.length,
    "Expected 8 artifacts to be persisted",
  );
});

// tests can be run from multiple places so we need to be able to locate our
// golden files from various locations, all the way to the top of the repo
export const testSuiteGoldenFilePaths = [
  ".",
  "./sql",
  "./rds/sql",
  "./governed-im-transform/rds/sql",
];

export interface TestAgainstGolden {
  readonly persistedName?: string;
  readonly goldenFile: string;
}

// deno-lint-ignore require-await
export async function testRdbmsModelTransform(
  transformer: mod.RdbmsModelTransformerConstructor,
  dialect: gimRDS.Dialect | gimRDS.DialectConstructor,
  golden?: string | TestAgainstGolden[],
): Promise<ap.PersistenceHandler | undefined> {
  const spec = sm.specFactory.spec<gimc.InformationModel>(
    new gimTM.TestModel(),
  );
  const ph = new ap.InMemoryPersistenceHandler();
  const [ctx, imt] = mod.transformRdbmsModel(
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
