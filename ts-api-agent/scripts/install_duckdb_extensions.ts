/**
 * Downloads the DuckDB `httpfs` extension into the image's local extension directory.
 *
 * This is the one place in the whole build that is allowed to reach `extensions.duckdb.org`, and
 * it runs at **image build time**. At run time the serving session forces
 * `autoinstall_known_extensions` and `autoload_known_extensions` off and does a bare
 * `LOAD httpfs`, so a container that reached this point can answer `/ask` with no Internet at
 * all — and a container that did not fails loudly with `HttpfsExtensionUnavailable` instead of
 * quietly fetching a binary from the network on a user's first question.
 *
 * The extension is version-locked to the engine by DuckDB itself: extensions live under
 * `<extension_directory>/v<engine version>/<platform>/`, and an extension built for another
 * engine version will not load. Printing both here makes the pairing visible in the build log,
 * and the assertion below fails the build if `LOAD` cannot then find what `INSTALL` wrote.
 */
import { DuckDBInstance } from '@duckdb/node-api';

const instance = await DuckDBInstance.create(':memory:');
const connection = await instance.connect();

try {
  await connection.run('INSTALL httpfs;');

  // Prove it loads the way production loads it: no autoinstall, no autoload, no network.
  await connection.run(`
    SET autoinstall_known_extensions = false;
    SET autoload_known_extensions = false;
    LOAD httpfs;
  `);

  const [row] = (
    await connection.runAndReadAll(`
      SELECT
        (SELECT library_version FROM pragma_version())                            AS engine,
        (SELECT extension_version FROM duckdb_extensions()
          WHERE extension_name = 'httpfs' AND loaded)                             AS httpfs,
        (SELECT value FROM duckdb_settings() WHERE name = 'extension_directory')  AS directory;
    `)
  ).getRowObjectsJS();

  if (row?.httpfs === undefined || row.httpfs === null) {
    throw new Error('httpfs did not load after INSTALL; the image would fail every /ask');
  }

  console.log(
    `[duckdb-extensions] engine ${String(row.engine)} + httpfs ${String(row.httpfs)} installed ` +
      `into '${String(row.directory) || '<default: $HOME/.duckdb/extensions>'}'`,
  );
} finally {
  connection.disconnectSync();
  instance.closeSync();
}
