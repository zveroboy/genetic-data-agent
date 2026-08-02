/**
 * Builds the versioned ClinVar coordinate snapshot from the committed TSV.
 *
 * The serving path *opens* a snapshot and refuses to start without one — it never builds one
 * lazily on the first request, because a coordinate table materialising mid-flight is a
 * different answer for two requests that arrived a second apart. So the snapshot is produced
 * once, ahead of time: at image build (see `ts-api-agent/Dockerfile`) or by
 * `make reference-snapshot` locally.
 *
 * Reference data, not user data. It is derived from a file tracked in this repository and needs
 * no network, which is what lets the runtime image answer `/ask` with the Internet unplugged.
 */
import {
  buildReferenceDatabase,
  defaultReferenceSnapshotOptions,
} from '../src/infrastructure/database/reference-bootstrap.ts';

const options = defaultReferenceSnapshotOptions();
const snapshot = await buildReferenceDatabase(options);

console.log(
  `[reference-snapshot] built ${snapshot.referenceVersion} (${snapshot.referenceBuild}) with ` +
    `${snapshot.rowCount} coordinate targets at ${snapshot.path}`,
);
