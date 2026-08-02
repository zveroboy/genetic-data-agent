//! Block-parallel decompression of BGZF, the gzip dialect `bgzip` produces and every real
//! genomic pipeline emits.
//!
//! **Why this exists.** A gzip member is one deflate stream: byte *n* cannot be decoded without
//! having decoded byte *n-1*, so `MultiGzDecoder` is strictly sequential and one core is the
//! ceiling. BGZF exists to remove exactly that ceiling. It is legal gzip — any `gunzip` reads it
//! — but the file is a chain of independent members, each holding at most 64 KiB uncompressed,
//! and each member's header carries an extra subfield `BC` giving that member's total size on
//! disk. So the block boundaries can be found by reading 18 bytes per block and seeking, and
//! every block can then be inflated by a different core.
//!
//! ```text
//!   0       2   3    4       8     9    10     12      14      16      18
//! | 1f 8b | 08 | 04 | MTIME | XFL | OS | XLEN | 42 43 | 02 00 | BSIZE | …deflate… | CRC32 | ISIZE |
//! ```
//!
//! The number above each field is that field's byte offset from the start of the member. `CM` is
//! deflate and `FLG` sets `FEXTRA`; `XLEN` is the byte length of the extra area, and inside it the
//! `BC` subfield (id `42 43`, `SLEN` = 2) carries `BSIZE`, the whole block's on-disk size minus
//! one. The trailer's `ISIZE` is the block's exact uncompressed length.
//!
//! **What it does not change.** Plain gzip and uncompressed input never reach this module:
//! [`is_bgzf`] is a positive test on the first member's header, and anything that fails it keeps
//! the sequential decoder it had. The bytes handed to the parser are identical either way — the
//! same VCF, in the same order — because the only thing that happens in parallel is *inflating*
//! blocks, and they are re-assembled strictly in file order before anyone sees them.
//!
//! **Bounded, not eager.** The reader is a pipeline with a hard read-ahead bound: at most
//! [`BgzfReader::max_blocks_in_flight`] blocks exist at once, anywhere — queued, inflating, or
//! waiting to be consumed. The GIAB NA12878 VCF measured here is 2.07 GB uncompressed, which
//! against the format's 64 KiB-per-block ceiling is at least 31 600 blocks; without a bound
//! "decompress the blocks in parallel" would mean holding all of it.
//!
//! No Temporal, no S3, no DuckDB.

use std::fs::File;
use std::io::{self, BufRead, Read};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use flate2::read::GzDecoder;

/// The fixed part of a gzip header, up to and including `XLEN`.
const GZIP_FIXED_HEADER: usize = 12;
/// `CRC32` then `ISIZE`.
const GZIP_TRAILER: usize = 8;
/// `FLG` bit 2: an `EXTRA` field is present. BGZF always sets it.
const FLG_FEXTRA: u8 = 0x04;
/// `CM`: deflate. The only compression method gzip defines.
const CM_DEFLATE: u8 = 0x08;
/// The BGZF extra subfield identifier, `BC`.
const BGZF_SUBFIELD_ID: [u8; 2] = [0x42, 0x43];
/// `BC` carries a two-byte `BSIZE`.
const BGZF_SUBFIELD_LEN: u16 = 2;

/// A BGZF block holds at most 64 KiB uncompressed, by specification. Used to reject an absurd
/// `ISIZE` before it is believed and turned into an allocation.
const MAX_BLOCK_UNCOMPRESSED: usize = 65_536;

/// Blocks read ahead per worker. The read-ahead is what keeps every worker fed; making it large
/// buys nothing once the workers are saturated and only raises the memory ceiling.
const READAHEAD_PER_WORKER: usize = 2;

/// Decompression threads currently alive across the process.
///
/// Observability with a purpose: a Temporal Activity that has been cancelled must not leave
/// worker threads behind, and this is how that is asserted rather than assumed.
static LIVE_THREADS: AtomicUsize = AtomicUsize::new(0);

/// How many BGZF decompression threads are running right now. Zero when no BGZF file is open.
pub fn live_decompression_threads() -> usize {
    LIVE_THREADS.load(Ordering::Acquire)
}

/// Runs `body` on a new thread while [`LIVE_THREADS`] counts it.
fn spawn_counted(name: String, body: impl FnOnce() + Send + 'static) -> io::Result<JoinHandle<()>> {
    LIVE_THREADS.fetch_add(1, Ordering::AcqRel);
    std::thread::Builder::new()
        .name(name)
        .spawn(move || {
            body();
            LIVE_THREADS.fetch_sub(1, Ordering::AcqRel);
        })
        .inspect_err(|_| {
            LIVE_THREADS.fetch_sub(1, Ordering::AcqRel);
        })
}

/// Whether `head` — the first bytes of a file — begins a BGZF member.
///
/// A positive test, not a guess: gzip magic, deflate, `FEXTRA` set, and a well-formed `BC`
/// subfield of the right length inside the declared extra area. Anything else, including plain
/// gzip and a gzip file whose extra field is something other than BGZF's, answers `false` and
/// keeps the sequential decoder.
///
/// `head` shorter than the declared extra area also answers `false` rather than guessing.
pub fn is_bgzf(head: &[u8]) -> bool {
    block_size_from_header(head).is_some()
}

/// The total on-disk size of the block `header` starts, if it is a BGZF block.
fn block_size_from_header(header: &[u8]) -> Option<usize> {
    if header.len() < GZIP_FIXED_HEADER
        || header[0] != 0x1f
        || header[1] != 0x8b
        || header[2] != CM_DEFLATE
        || header[3] & FLG_FEXTRA == 0
    {
        return None;
    }
    let extra_len = u16::from_le_bytes([header[10], header[11]]) as usize;
    let extra = header.get(GZIP_FIXED_HEADER..GZIP_FIXED_HEADER + extra_len)?;
    bsize_from_extra(extra).map(|bsize| bsize as usize + 1)
}

/// Scans the gzip `EXTRA` area for the `BC` subfield and returns its `BSIZE` (block size minus
/// one). The subfield is conventionally first but the area is a list, so it is walked.
fn bsize_from_extra(extra: &[u8]) -> Option<u16> {
    let mut offset = 0;
    while offset + 4 <= extra.len() {
        let id = [extra[offset], extra[offset + 1]];
        let len = u16::from_le_bytes([extra[offset + 2], extra[offset + 3]]) as usize;
        let payload = extra.get(offset + 4..offset + 4 + len)?;
        if id == BGZF_SUBFIELD_ID && len == BGZF_SUBFIELD_LEN as usize {
            return Some(u16::from_le_bytes([payload[0], payload[1]]));
        }
        offset += 4 + len;
    }
    None
}

/// One block's decompressed bytes, or the failure that stopped the stream there.
type BlockResult = io::Result<Vec<u8>>;

/// One block handed to a worker, with the single-use channel its result must come back through.
struct Job {
    block: Vec<u8>,
    result: SyncSender<BlockResult>,
}

/// A `BufRead` over a BGZF file whose blocks are inflated on several threads and re-assembled in
/// file order.
///
/// The pipeline is three parts:
///
/// 1. **One reader thread** walks the file, parsing each block header and reading the block's
///    bytes. This is I/O and 18 bytes of arithmetic per block, so one thread is plenty.
/// 2. **`workers` inflate threads** take blocks off a shared queue and inflate them. This is the
///    part that was the bottleneck and is now parallel.
/// 3. **Ordering by construction.** Before submitting block *n*, the reader pushes that block's
///    single-use result channel onto an ordered queue. The consumer pops those channels in
///    order and blocks on each, so the bytes can only ever be re-assembled in file order — there
///    is no sort, no sequence number to compare, and no way for a fast worker to overtake a slow
///    one. It also makes back-pressure automatic: the ordered queue is bounded, so the reader
///    stalls once the read-ahead is full.
pub struct BgzfReader {
    /// Result channels in file order, one per block.
    slots: Receiver<Receiver<BlockResult>>,
    /// The block currently being handed to the caller, and how much of it has been consumed.
    current: Vec<u8>,
    position: usize,
    /// Set once the stream has ended, by EOF or by an error already reported to the caller.
    finished: bool,
    readahead: usize,
    blocks_in_flight: Arc<AtomicUsize>,
    peak_blocks_in_flight: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
    reader_thread: Option<JoinHandle<()>>,
    inflate_threads: Vec<JoinHandle<()>>,
}

impl BgzfReader {
    /// Starts the pipeline over `file`, which must be positioned at the start of the first block.
    ///
    /// `workers` is clamped to at least one. The read-ahead — and with it the memory ceiling —
    /// is derived from it; see [`BgzfReader::max_blocks_in_flight`].
    pub fn new(file: File, workers: usize) -> io::Result<Self> {
        let workers = workers.max(1);
        let readahead = workers * READAHEAD_PER_WORKER;

        let (job_sender, job_receiver) = sync_channel::<Job>(readahead);
        let (slot_sender, slots) = sync_channel::<Receiver<BlockResult>>(readahead);
        let job_receiver = Arc::new(Mutex::new(job_receiver));
        let shutdown = Arc::new(AtomicBool::new(false));
        let blocks_in_flight = Arc::new(AtomicUsize::new(0));
        let peak_blocks_in_flight = Arc::new(AtomicUsize::new(0));

        let mut inflate_threads = Vec::with_capacity(workers);
        for index in 0..workers {
            let jobs = Arc::clone(&job_receiver);
            inflate_threads.push(spawn_counted(format!("bgzf-inflate-{index}"), move || {
                loop {
                    // The lock is held only across `recv`, never across an inflate, so the
                    // workers hand jobs off to each other rather than serialising the work.
                    let job = {
                        let receiver = jobs.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                        receiver.recv()
                    };
                    let Ok(job) = job else {
                        // The reader thread has finished and dropped the sender.
                        return;
                    };
                    // A closed result channel means the consumer went away; nothing to report to.
                    let _ = job.result.send(inflate_block(&job.block));
                }
            })?);
        }

        let reader_shutdown = Arc::clone(&shutdown);
        let reader_in_flight = Arc::clone(&blocks_in_flight);
        let reader_peak = Arc::clone(&peak_blocks_in_flight);
        let reader_thread = spawn_counted("bgzf-reader".to_string(), move || {
            read_blocks(
                file,
                &job_sender,
                &slot_sender,
                &reader_shutdown,
                &reader_in_flight,
                &reader_peak,
            );
        })?;

        Ok(Self {
            slots,
            current: Vec::new(),
            position: 0,
            finished: false,
            readahead,
            blocks_in_flight,
            peak_blocks_in_flight,
            shutdown,
            reader_thread: Some(reader_thread),
            inflate_threads,
        })
    }

    /// The hard ceiling on blocks existing at once — queued for inflation, being inflated, or
    /// inflated and waiting to be read. This times ~128 KiB (one compressed plus one
    /// decompressed block) is the reader's memory bound, and it does not grow with the file.
    ///
    /// It is the read-ahead plus two: the bounded ordered queue holds `readahead`, the reader
    /// thread may be blocked trying to push one more, and the consumer may be holding one.
    pub fn max_blocks_in_flight(&self) -> usize {
        self.readahead + 2
    }

    /// The most blocks that actually existed at once during this stream. Never exceeds
    /// [`BgzfReader::max_blocks_in_flight`]; that is the property the bound is worth having.
    pub fn peak_blocks_in_flight(&self) -> usize {
        self.peak_blocks_in_flight.load(Ordering::Acquire)
    }

    /// Waits for the next block's bytes, in file order.
    fn next_block(&mut self) -> io::Result<Option<Vec<u8>>> {
        let Ok(slot) = self.slots.recv() else {
            // The reader thread finished and dropped its sender: the file is exhausted.
            self.finished = true;
            return Ok(None);
        };
        let outcome = match slot.recv() {
            Ok(outcome) => outcome,
            // A worker died without answering. Treated as a truncated stream rather than
            // silently as EOF, because losing the tail of a genome must not look like reaching
            // its end.
            Err(_) => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "a BGZF block was never decompressed",
            )),
        };
        self.blocks_in_flight.fetch_sub(1, Ordering::AcqRel);

        match outcome {
            Ok(block) => Ok(Some(block)),
            Err(error) => {
                // The caller is being handed the error now; the stream ends here rather than
                // resuming past the damage.
                self.finished = true;
                Err(error)
            }
        }
    }

    /// Stops the pipeline and joins every thread it started.
    ///
    /// The order matters and is the whole reason this is not just three `join`s:
    ///
    /// 1. Raise the flag, so the reader thread stops at its next block boundary.
    /// 2. Drain the ordered queue. Without this the reader could be blocked forever pushing into
    ///    a full queue and would never look at the flag. `recv` returning `Err` *is* the signal
    ///    that the reader has exited and dropped its sender, so this doubles as the wait.
    /// 3. Join the reader. Dropping its job sender is what disconnects the inflate threads.
    /// 4. Join the inflate threads, which have now seen the disconnect. They can never be
    ///    blocked on a send: each result channel has capacity one and receives exactly one value.
    fn shut_down(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        while self.slots.recv().is_ok() {}
        if let Some(reader_thread) = self.reader_thread.take() {
            let _ = reader_thread.join();
        }
        for worker in self.inflate_threads.drain(..) {
            let _ = worker.join();
        }
    }
}

impl Drop for BgzfReader {
    fn drop(&mut self) {
        self.shut_down();
    }
}

impl BufRead for BgzfReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        while self.position >= self.current.len() {
            if self.finished {
                return Ok(&[]);
            }
            match self.next_block()? {
                // A zero-length block is legal — the 28-byte marker every BGZF file ends with is
                // one — so it is skipped rather than mistaken for the end of the stream.
                Some(block) => {
                    self.current = block;
                    self.position = 0;
                }
                None => return Ok(&[]),
            }
        }
        Ok(&self.current[self.position..])
    }

    fn consume(&mut self, amount: usize) {
        self.position = (self.position + amount).min(self.current.len());
    }
}

impl Read for BgzfReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf()?;
        let taken = available.len().min(buffer.len());
        buffer[..taken].copy_from_slice(&available[..taken]);
        self.consume(taken);
        Ok(taken)
    }
}

/// Walks the file block by block, submitting each one and pushing its result channel onto the
/// ordered queue first, so order is fixed before any work starts.
///
/// Any failure is sent *through the ordered queue* rather than raised out of band, which is what
/// makes an error arrive at the caller at exactly the position in the stream where it happened,
/// with its original [`io::ErrorKind`] — the classification the retry taxonomy is built on.
fn read_blocks(
    file: File,
    jobs: &SyncSender<Job>,
    slots: &SyncSender<Receiver<BlockResult>>,
    shutdown: &AtomicBool,
    in_flight: &AtomicUsize,
    peak_in_flight: &AtomicUsize,
) {
    let mut source = io::BufReader::with_capacity(256 * 1024, file);

    loop {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        let block = match read_block(&mut source) {
            Ok(Some(block)) => block,
            Ok(None) => return,
            Err(error) => {
                // Reported in order, then the stream ends.
                let (sender, receiver) = sync_channel(1);
                let _ = sender.send(Err(error));
                let _ = slots.send(receiver);
                in_flight.fetch_add(1, Ordering::AcqRel);
                return;
            }
        };

        let (sender, receiver) = sync_channel(1);
        let live = in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        peak_in_flight.fetch_max(live, Ordering::AcqRel);

        // Order first, then work. Pushing the slot is also where back-pressure is applied: this
        // blocks once the read-ahead is full, which is what bounds memory.
        if slots.send(receiver).is_err() {
            return;
        }
        if jobs
            .send(Job {
                block,
                result: sender,
            })
            .is_err()
        {
            return;
        }
    }
}

/// Reads one whole BGZF member, header included, so a worker can inflate it without knowing
/// where it came from. `Ok(None)` is a clean end of file.
fn read_block(source: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0u8; GZIP_FIXED_HEADER];
    match read_exact_or_eof(source, &mut header)? {
        0 => return Ok(None),
        read if read < GZIP_FIXED_HEADER => {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("a BGZF block header was cut off after {read} bytes"),
            ))
        }
        _ => {}
    }

    // Checked before `XLEN` is believed. Without this, a plain-gzip member appearing part way
    // through the file would have two bytes of its deflate stream read as an extra-field length,
    // and the failure would come back as a truncated read rather than as what it is.
    if header[0] != 0x1f || header[1] != 0x8b || header[2] != CM_DEFLATE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a member of this BGZF file is not a deflate gzip member",
        ));
    }
    if header[3] & FLG_FEXTRA == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a member of this BGZF file has no extra field, so it cannot be BGZF",
        ));
    }

    let extra_len = u16::from_le_bytes([header[10], header[11]]) as usize;
    let mut extra = vec![0u8; extra_len];
    source.read_exact(&mut extra)?;

    let mut with_extra = Vec::with_capacity(GZIP_FIXED_HEADER + extra_len);
    with_extra.extend_from_slice(&header);
    with_extra.extend_from_slice(&extra);

    // Every member of a BGZF file must itself be BGZF. A file that starts BGZF and then stops
    // being BGZF cannot be split, and quietly reading half of it would be worse than refusing.
    let total = block_size_from_header(&with_extra).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "a member of this BGZF file carries no BC block-size subfield",
        )
    })?;
    if total < GZIP_FIXED_HEADER + extra_len + GZIP_TRAILER {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a BGZF block declares an impossible size of {total} bytes"),
        ));
    }

    let mut block = with_extra;
    block.resize(total, 0);
    source.read_exact(&mut block[GZIP_FIXED_HEADER + extra_len..])?;
    Ok(Some(block))
}

/// `read_exact`, except that reading nothing at all is a clean end of file rather than an error.
/// Returns how many bytes were read.
fn read_exact_or_eof(source: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        match source.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(filled)
}

/// Inflates one complete BGZF member.
///
/// `ISIZE` in the trailer is the exact uncompressed length, so the output buffer is sized once
/// rather than grown. It is checked against the specification's 64 KiB ceiling before it is
/// believed, and against the bytes actually produced afterwards — [`GzDecoder`] already verifies
/// the CRC, and this catches the remaining way a block can lie about itself.
fn inflate_block(block: &[u8]) -> BlockResult {
    // Cannot underflow: `read_block` rejects any block shorter than header + extra + trailer
    // before one is ever handed here, and this function staying panic-free is what the `Drop`
    // liveness argument rests on — a worker that panicked would never answer its channel.
    let trailer_at = block.len() - 4;
    let declared = u32::from_le_bytes([
        block[trailer_at],
        block[trailer_at + 1],
        block[trailer_at + 2],
        block[trailer_at + 3],
    ]) as usize;
    if declared > MAX_BLOCK_UNCOMPRESSED {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a BGZF block declares {declared} uncompressed bytes, above the 64 KiB limit"),
        ));
    }

    // Bounded, not just checked afterwards. `declared` has already been rejected above if it
    // exceeds the 64 KiB ceiling, so it is a legitimate bound on the read — and the source
    // object is user-supplied from S3, where deflate's ~1032:1 ceiling on a 64 KiB member means
    // an unbounded `read_to_end` would transiently allocate ~67 MB per worker before the length
    // check below rejected it. `declared + 1` rather than `declared`, so a block that produces
    // *more* than it promised still fails that check instead of being silently truncated to fit.
    let mut inflated = Vec::with_capacity(declared);
    GzDecoder::new(block)
        .take(declared as u64 + 1)
        .read_to_end(&mut inflated)?;
    if inflated.len() != declared {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "a BGZF block declared {declared} uncompressed bytes but produced {}",
                inflated.len()
            ),
        ));
    }
    Ok(inflated)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;
    use std::sync::Mutex;

    use flate2::write::{DeflateEncoder, GzEncoder};
    use flate2::read::MultiGzDecoder;
    use flate2::Compression;
    use tempfile::TempDir;

    /// Serialises every test that starts a `BgzfReader`.
    ///
    /// [`live_decompression_threads`] counts threads across the whole process, which is what
    /// makes it useful as a metric and useless as a per-test assertion when the harness runs
    /// tests on several threads at once: another test's pipeline would be counted too. Every
    /// test below that starts a reader takes this lock, so the two that assert on the count are
    /// the only reader running while they do.
    static ONE_PIPELINE_AT_A_TIME: Mutex<()> = Mutex::new(());

    fn exclusive() -> std::sync::MutexGuard<'static, ()> {
        ONE_PIPELINE_AT_A_TIME
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Uncompressed bytes per synthetic block. Small, so a modest fixture still spans many
    /// blocks and the ordering has somewhere to go wrong.
    const TEST_BLOCK_PAYLOAD: usize = 1_024;

    /// Assembles one BGZF member by hand, from the layout in this module's header comment.
    ///
    /// Written from the specification rather than by calling anything in this file, so the
    /// reader is tested against the format and not against its own idea of the format.
    fn bgzf_block(payload: &[u8]) -> Vec<u8> {
        let mut deflated = Vec::new();
        DeflateEncoder::new(&mut deflated, Compression::default())
            .write_all(payload)
            .expect("deflate");
        let deflated = {
            let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(payload).expect("deflate");
            encoder.finish().expect("finish deflate")
        };

        // 12-byte fixed header + 6-byte BC subfield + deflate + 8-byte trailer.
        let total = GZIP_FIXED_HEADER + 6 + deflated.len() + GZIP_TRAILER;
        let bsize = u16::try_from(total - 1).expect("a test block fits in BSIZE");

        let mut block = Vec::with_capacity(total);
        block.extend_from_slice(&[0x1f, 0x8b, CM_DEFLATE, FLG_FEXTRA]);
        block.extend_from_slice(&0u32.to_le_bytes()); // MTIME
        block.push(0); // XFL
        block.push(0xff); // OS: unknown
        block.extend_from_slice(&6u16.to_le_bytes()); // XLEN
        block.extend_from_slice(&BGZF_SUBFIELD_ID);
        block.extend_from_slice(&BGZF_SUBFIELD_LEN.to_le_bytes());
        block.extend_from_slice(&bsize.to_le_bytes());
        block.extend_from_slice(&deflated);
        block.extend_from_slice(&crc32(payload).to_le_bytes());
        block.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        assert_eq!(block.len(), total);
        block
    }

    fn crc32(bytes: &[u8]) -> u32 {
        let mut hasher = flate2::Crc::new();
        hasher.update(bytes);
        hasher.sum()
    }

    /// A whole BGZF file: `content` cut into blocks, then the 28-byte empty end-of-file marker
    /// that `bgzip` writes and that this reader must treat as ordinary.
    fn bgzf_bytes(content: &[u8]) -> Vec<u8> {
        let mut file = Vec::new();
        for chunk in content.chunks(TEST_BLOCK_PAYLOAD) {
            file.extend_from_slice(&bgzf_block(chunk));
        }
        file.extend_from_slice(&bgzf_block(b""));
        file
    }

    fn write_temp(directory: &TempDir, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let path = directory.path().join(name);
        std::fs::write(&path, bytes).expect("write fixture");
        path
    }

    /// Text with enough structure that a mis-ordered reassembly is certain to be visible: every
    /// line names its own index.
    fn numbered_lines(count: usize) -> Vec<u8> {
        (0..count)
            .map(|index| format!("line {index:07} {}\n", "payload".repeat(index % 11 + 1)))
            .collect::<String>()
            .into_bytes()
    }

    fn read_all(path: &std::path::Path, workers: usize) -> io::Result<Vec<u8>> {
        let mut reader = BgzfReader::new(File::open(path)?, workers)?;
        let mut out = Vec::new();
        reader.read_to_end(&mut out)?;
        Ok(out)
    }

    // -----------------------------------------------------------------------------------
    // The fixture is real BGZF
    // -----------------------------------------------------------------------------------

    /// If the hand-assembled fixture were not valid gzip, every test below would be testing
    /// nothing. `MultiGzDecoder` — the sequential decoder this module replaces — must read it.
    #[test]
    fn the_synthetic_fixture_is_valid_multi_member_gzip() {
        let content = numbered_lines(500);
        let file = bgzf_bytes(&content);
        assert!(is_bgzf(&file), "the fixture must be recognised as BGZF");

        let mut decoded = Vec::new();
        MultiGzDecoder::new(file.as_slice())
            .read_to_end(&mut decoded)
            .expect("the fixture is valid gzip");
        assert_eq!(decoded, content);
    }

    // -----------------------------------------------------------------------------------
    // Detection
    // -----------------------------------------------------------------------------------

    #[test]
    fn detects_bgzf_and_only_bgzf() {
        assert!(is_bgzf(&bgzf_bytes(b"hello")));

        // Plain gzip: no FEXTRA at all.
        let mut plain = GzEncoder::new(Vec::new(), Compression::default());
        plain.write_all(b"hello").expect("write");
        assert!(!is_bgzf(&plain.finish().expect("finish")));

        // Gzip carrying an extra field that is not BGZF's.
        let mut foreign = bgzf_block(b"hello");
        foreign[GZIP_FIXED_HEADER] = b'X';
        assert!(!is_bgzf(&foreign), "a non-BC subfield is not BGZF");

        for not_gzip in [
            &b""[..],
            &b"\x1f"[..],
            &b"##fileformat=VCFv4.2"[..],
            &[0x1f, 0x8b, 0x08, 0x04][..], // truncated before XLEN
        ] {
            assert!(!is_bgzf(not_gzip), "{not_gzip:?} must not be taken for BGZF");
        }

        // Declares more extra bytes than are present.
        let mut lying = bgzf_block(b"hello");
        lying[10] = 0xff;
        assert!(!is_bgzf(&lying), "a truncated extra area must not be taken for BGZF");
    }

    // -----------------------------------------------------------------------------------
    // Ordering
    // -----------------------------------------------------------------------------------

    /// The invariant the whole design exists for: the bytes out are the bytes in, in order, at
    /// every worker count. 6 000 numbered lines over ~1 KiB blocks is hundreds of blocks, so a
    /// reassembly that trusted completion order would be scrambled beyond any chance of passing.
    #[test]
    fn reassembles_the_stream_in_file_order_at_every_worker_count() {
        let _exclusive = exclusive();
        let directory = TempDir::new().expect("temp dir");
        let content = numbered_lines(6_000);
        let path = write_temp(&directory, "ordered.vcf.gz", &bgzf_bytes(&content));

        for workers in [1, 2, 4, 8, 16] {
            let decoded = read_all(&path, workers).expect("read");
            assert_eq!(
                decoded.len(),
                content.len(),
                "wrong length at {workers} workers"
            );
            assert!(decoded == content, "the stream was reassembled out of order at {workers} workers");
        }
    }

    /// The same bytes the sequential decoder would have produced, for a source that is not
    /// neatly block-aligned.
    #[test]
    fn produces_exactly_what_the_sequential_decoder_produces() {
        let _exclusive = exclusive();
        let directory = TempDir::new().expect("temp dir");
        for size in [0, 1, TEST_BLOCK_PAYLOAD - 1, TEST_BLOCK_PAYLOAD, TEST_BLOCK_PAYLOAD * 3 + 7] {
            let content: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();
            let file = bgzf_bytes(&content);
            let path = write_temp(&directory, &format!("size-{size}.gz"), &file);

            let mut sequential = Vec::new();
            MultiGzDecoder::new(file.as_slice())
                .read_to_end(&mut sequential)
                .expect("sequential decode");

            assert_eq!(read_all(&path, 8).expect("parallel decode"), sequential, "size {size}");
        }
    }

    /// The empty 28-byte block every BGZF file ends with must be skipped, not read as the end of
    /// the stream — otherwise a file would be truncated at its first empty block.
    #[test]
    fn an_empty_block_is_skipped_rather_than_read_as_end_of_file() {
        let _exclusive = exclusive();
        let directory = TempDir::new().expect("temp dir");
        let mut file = bgzf_block(b"before\n");
        file.extend_from_slice(&bgzf_block(b"")); // an empty block in the middle
        file.extend_from_slice(&bgzf_block(b"after\n"));
        file.extend_from_slice(&bgzf_block(b"")); // and the usual terminator
        let path = write_temp(&directory, "empty-inside.gz", &file);

        for workers in [1, 4] {
            assert_eq!(read_all(&path, workers).expect("read"), b"before\nafter\n");
        }
    }

    /// `BufRead::read_line` over the reader must see the same lines whatever the bound, including
    /// the lines that straddle a block boundary — which is where a naive reassembly loses bytes.
    #[test]
    fn lines_that_straddle_block_boundaries_survive() {
        let _exclusive = exclusive();
        let directory = TempDir::new().expect("temp dir");
        let content = numbered_lines(3_000);
        let expected: Vec<&[u8]> = content.split_inclusive(|byte| *byte == b'\n').collect();
        let path = write_temp(&directory, "lines.gz", &bgzf_bytes(&content));

        for workers in [1, 3, 8] {
            let reader = BgzfReader::new(File::open(&path).expect("open"), workers).expect("start");
            let lines: Vec<Vec<u8>> = reader
                .split(b'\n')
                .map(|line| line.expect("line"))
                .collect();
            assert_eq!(lines.len(), expected.len(), "line count at {workers} workers");
            assert_eq!(
                String::from_utf8(lines[0].clone()).unwrap(),
                "line 0000000 payload"
            );
            assert_eq!(
                String::from_utf8(lines[lines.len() - 1].clone()).unwrap(),
                String::from_utf8_lossy(expected[expected.len() - 1])
                    .trim_end_matches('\n')
                    .to_string()
            );
        }
    }

    // -----------------------------------------------------------------------------------
    // Bounded read-ahead
    // -----------------------------------------------------------------------------------

    /// "Decompress the blocks in parallel" must not become "hold the whole genome". The reader
    /// is driven slowly on purpose, so the pipeline has every opportunity to run ahead, and the
    /// count of blocks alive at once still never passes the declared ceiling.
    #[test]
    fn the_read_ahead_is_bounded_however_slowly_the_stream_is_consumed() {
        let _exclusive = exclusive();
        let directory = TempDir::new().expect("temp dir");
        // ~1 500 blocks: far more than any bound below.
        let content = numbered_lines(15_000);
        let path = write_temp(&directory, "bounded.gz", &bgzf_bytes(&content));

        for workers in [1, 4, 16] {
            let mut reader = BgzfReader::new(File::open(&path).expect("open"), workers).expect("start");
            let ceiling = reader.max_blocks_in_flight();
            assert!(ceiling <= 34, "the ceiling itself must stay small, was {ceiling}");

            let mut total = 0usize;
            let mut buffer = [0u8; 97]; // a size unrelated to the block size
            loop {
                let read = reader.read(&mut buffer).expect("read");
                if read == 0 {
                    break;
                }
                total += read;
                assert!(
                    reader.peak_blocks_in_flight() <= ceiling,
                    "{} blocks were in flight at {workers} workers, ceiling {ceiling}",
                    reader.peak_blocks_in_flight()
                );
            }
            assert_eq!(total, content.len());
            assert!(
                reader.peak_blocks_in_flight() > 1 || workers == 1,
                "at {workers} workers the pipeline never actually ran ahead, so the bound is untested"
            );
        }
    }

    // -----------------------------------------------------------------------------------
    // Errors from any thread
    // -----------------------------------------------------------------------------------

    /// A corrupt block must surface as an error at its own position in the stream, with the
    /// `ErrorKind` the retry taxonomy classifies on — content damage is deterministic and must
    /// stay non-retryable, so it may not be reported as a generic I/O fault.
    #[test]
    fn a_corrupt_block_fails_at_its_own_position_with_a_content_error_kind() {
        let _exclusive = exclusive();
        let directory = TempDir::new().expect("temp dir");
        let mut file = bgzf_block(b"good one\n");
        let intact_prefix = file.len();
        let mut damaged = bgzf_block(b"this block will be corrupted\n");
        // Flip a byte inside the deflate payload: the header and BSIZE stay valid, so the block
        // is found and dispatched, and only the inflate fails.
        let payload_at = GZIP_FIXED_HEADER + 6 + 2;
        damaged[payload_at] ^= 0xff;
        file.extend_from_slice(&damaged);
        file.extend_from_slice(&bgzf_block(b"never reached\n"));
        let path = write_temp(&directory, "corrupt.gz", &file);
        assert!(intact_prefix > 0);

        for workers in [1, 4, 16] {
            let mut reader = BgzfReader::new(File::open(&path).expect("open"), workers).expect("start");
            let mut decoded = Vec::new();
            let error = reader
                .read_to_end(&mut decoded)
                .expect_err("a corrupt block must not decode");

            assert!(
                matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput | io::ErrorKind::UnexpectedEof
                ),
                "at {workers} workers a corrupt block gave {:?}, which the taxonomy would retry",
                error.kind()
            );
            // Whatever was decoded before the damage is the good block, never the block after it.
            assert!(
                decoded == b"good one\n" || decoded.is_empty(),
                "at {workers} workers the reader delivered bytes from past the corruption: {decoded:?}"
            );
        }
    }

    /// A member that is gzip but not BGZF, part way through a BGZF file, is refused rather than
    /// half-read. htslib behaves the same way: a BGZF file's members are all BGZF.
    #[test]
    fn a_non_bgzf_member_part_way_through_is_refused() {
        let _exclusive = exclusive();
        let directory = TempDir::new().expect("temp dir");
        let mut file = bgzf_block(b"first\n");
        let mut plain = GzEncoder::new(Vec::new(), Compression::default());
        plain.write_all(b"second\n").expect("write");
        file.extend_from_slice(&plain.finish().expect("finish"));
        let path = write_temp(&directory, "mixed.gz", &file);

        let mut reader = BgzfReader::new(File::open(&path).expect("open"), 4).expect("start");
        let mut decoded = Vec::new();
        let error = reader.read_to_end(&mut decoded).expect_err("must refuse");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(decoded, b"first\n");
    }

    /// A file cut off mid-block is a truncated stream, not a shorter one.
    #[test]
    fn a_truncated_file_is_an_unexpected_eof_not_a_silent_short_read() {
        let _exclusive = exclusive();
        let directory = TempDir::new().expect("temp dir");
        let full = bgzf_bytes(&numbered_lines(200));
        let path = write_temp(&directory, "truncated.gz", &full[..full.len() - 40]);

        for workers in [1, 8] {
            let mut reader = BgzfReader::new(File::open(&path).expect("open"), workers).expect("start");
            let error = reader
                .read_to_end(&mut Vec::new())
                .expect_err("a truncated BGZF file must fail");
            assert_eq!(
                error.kind(),
                io::ErrorKind::UnexpectedEof,
                "at {workers} workers truncation gave {error:?}"
            );
        }
    }

    // -----------------------------------------------------------------------------------
    // Cancellation
    // -----------------------------------------------------------------------------------

    /// Dropping the reader part way through a large file must stop the pipeline and join every
    /// thread it started. A cancelled Activity that returned while sixteen threads kept inflating
    /// a genome would be a leak the Temporal layer could not see.
    #[test]
    fn dropping_the_reader_mid_stream_stops_and_joins_every_thread() {
        let _exclusive = exclusive();
        let directory = TempDir::new().expect("temp dir");
        // Big enough that the pipeline is certainly still working when the reader is dropped.
        let path = write_temp(&directory, "abandoned.gz", &bgzf_bytes(&numbered_lines(40_000)));

        let before = live_decompression_threads();
        for workers in [1, 4, 16] {
            {
                let mut reader =
                    BgzfReader::new(File::open(&path).expect("open"), workers).expect("start");
                let mut buffer = [0u8; 64];
                reader.read_exact(&mut buffer).expect("read a little");
                assert!(
                    live_decompression_threads() > workers,
                    "the reader thread and {workers} inflate threads should all be running"
                );
                // Dropped here, mid-file, with the whole rest of the stream unread.
            }
            assert_eq!(
                live_decompression_threads(),
                before,
                "dropping at {workers} workers left threads behind"
            );
        }
    }

    /// The same, for the ordinary path: reaching the end of the stream also joins everything.
    #[test]
    fn reading_to_the_end_leaves_no_threads_behind() {
        let _exclusive = exclusive();
        let directory = TempDir::new().expect("temp dir");
        let path = write_temp(&directory, "complete.gz", &bgzf_bytes(&numbered_lines(2_000)));
        let before = live_decompression_threads();
        {
            let reader = BgzfReader::new(File::open(&path).expect("open"), 8).expect("start");
            let mut reader = reader;
            reader.read_to_end(&mut Vec::new()).expect("read");
        }
        assert_eq!(live_decompression_threads(), before);
    }
}
