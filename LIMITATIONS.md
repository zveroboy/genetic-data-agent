# Known limitations

What this system deliberately does not do, and why. Everything here was found by running the
thing against real data, not by reading the code — several items are recorded exactly because they
looked like bugs the first time and turned out to be properties of the data.

This file documents; it does not enforce. Where a limitation could mislead a user, the guard is in
code and is named below, because a promise written in Markdown constrains nothing at runtime.

## The dataset

**One sample per dataset, and a multi-sample VCF is refused.** `UserVariant` has no sample field,
the manifest records no sample identity, and the Parquet schema is frozen by a fingerprint, so
"which sample is this" could not be recorded anywhere. Ingesting the first sample of a joint
callset would publish a dataset claiming to be one person's genome without being able to name
whom. *Enforced:* the `#CHROM` header must declare exactly one sample; anything else fails as a
non-retryable `InvalidVcfFormat` naming the count and the `bcftools view -s` remedy. A 1000
Genomes joint VCF (3,202 samples) therefore needs a single-sample extraction first.

**Absence in the VCF is not a reference call.** A GIAB benchmark VCF lists only variants; there is
no "same as reference" record. So a missing position means either "matches the reference" or "was
never assessed", and the two are distinguishable only via the accompanying high-confidence BED,
which this pipeline does not ingest. *Enforced:* every "no genotype" answer says so explicitly and
never claims the reference allele.

**APOE ε cannot be determined from NA12878.** The two markers that define ε2/ε3/ε4 —
rs429358 (chr19:44,908,684) and rs7412 (chr19:44,908,822) — fall inside a 2,780 bp gap between
calls at 44,907,187 and 44,909,967. The surrounding region is covered (27 calls in a 20 kb window),
so this is a local exclusion: the ε region is GC-rich and outside GIAB v4.2.1's high-confidence
set. Three other APOE coordinates (rs405509, rs769450, rs439401) do have calls and are answered.
Published sources report NA12878 as ε3/ε3; that is not derivable from this dataset and this system
will not assert it.

**The dataset covers chromosomes 1–22 only.** No chrX, chrY or chrM, because the source benchmark
VCF has none. Every X-linked target — G6PD among the featured ones — is therefore unanswerable for
a reason that has nothing to do with the sample. *Enforced:* the answer names the absent chromosome
and states that it is a coverage gap, not a finding.

## The reference snapshot

**A gene symbol is not one variant.** `demo-clinvar-grch38-v3` holds 13,853 coordinates over 238
genes: BRCA2 has 2,714, BRCA1 2,271, CYP2D6 215. One question reads at most
`MAX_TARGETS_PER_QUERY` (64) of them, ranked featured-first, then pathogenic and drug-response
classifications, then position. *Enforced:* the answer states how many the snapshot lists and how
many were read, and points at naming an rsID to reach one.

**Common non-pathogenic polymorphisms are absent.** Selection is "Pathogenic/Likely pathogenic with
expert-panel or practice-guideline review" ∪ "drug response" ∪ the featured targets. Well-known
functional variants that are neither — COMT rs4680 is the clearest example — are not in the table,
even where the dataset does carry a call for them. Adding one means editing `FEATURED_TARGETS`,
regenerating the table, bumping `REFERENCE_VERSION` and re-ingesting.

**Naming an unknown rsID falls back to the gene.** Ask about `rs4680` and, since the snapshot has
no such rsID, routing drops to the gene-symbol tier, matches `COMT`, and answers about COMT's other
76 coordinates — "no matching call", which is true of those coordinates and misleading about the
one that was asked for. Known, not fixed: the honest behaviour is to say the rsID is not in the
snapshot.

**A reference version bump invalidates every existing dataset.** The serving path requires the
dataset's `referenceVersion` to equal the open snapshot's, so raising it means re-ingesting.
Nothing about the Parquet actually depends on the snapshot — the content checksum is unchanged
across versions — so a compatibility rule is possible; it was judged not worth the code here.

## The agent

**Only thirteen genes are reachable from plain language.** Lay terms ("coffee", "statin muscle
pain") and the ClinVar condition vocabulary are scoped to the featured targets. The other ~225
genes are reachable by gene symbol or rsID only. The condition index is deliberately scoped: over
the whole table nearly every clinical word stops discriminating, and words that survive do so by
accident — "mean" occurs under exactly one gene, in "increased **mean** platelet volume", and would
route "what does this variant mean?" to that gene with total confidence.

**A model-written answer is checked lexically, not for correctness.** The grounding check reports
rsIDs and gene symbols the tools never returned, and genotypes contradicting what was read. It
cannot see clinical reasoning, dosage advice or causal claims, and zero findings means "the prose
does not contradict the payload", never "the answer is correct". A symbol is only flagged when a
variant-level claim sits beside it, so an invented digit-less symbol carrying a bare genotype
(`Your TPMT genotype is A/A`) is knowingly missed — the alternative flagged `INR` and `PK` as
invented genes, and a warning that misnames a clinical acronym is a warning nobody believes.

**A model-written answer does not mention coordinate coverage.** The tool loop passes variants and
absence notes to the model but not the "64 of 2,714" numbers, so only the deterministic path speaks
the truncation.

**The similarity floor separates topics, not relevance.** `LITERATURE_MIN_SCORE` (0.65) reliably
separates "about genetics" from "not about genetics" — measured margin 0.225 over 362 real
abstracts. It cannot separate "about this question" from "about some other gene": questions about
type 2 diabetes and celiac disease clear it and retrieve genuinely genetic papers that do not
answer them. The citation is therefore labelled related reading and prints its score.

**The literature corpus is 362 abstracts over 13 genes.** Anything outside them retrieves a
nearest neighbour rather than a source. Grow it with `make ingest-pubmed`, which re-measures and
prints the calibration table it was tuned on.

## Operations

**No authentication in front of `/ask`.** Any caller that can reach the port reads any published
dataset by id. The prod overlay binds to loopback and expects a reverse proxy that does not exist
in this repository.

**Genotypes are sent to Cerebras when a key is set.** Tool results — the person's actual variants —
travel in the request body to a third-party API. Acceptable for a demo on public benchmark data;
not acceptable for anyone's real genome without a legal basis and consent.

**Temporal runs `server start-dev` on SQLite and MinIO is a single node.** No TLS, no
authorization, no erasure coding, no backup. Published artifacts are immutable but not durable.

**No dataset deletion.** There is no user model and no erasure path; `datasetId` is the only
handle.

**The UI can only query a dataset it ingested itself.** There is no route listing published
manifests, so looking at an existing dataset means building it again.

**Paid model calls are capped, not metered per caller.** `CEREBRAS_CALLS_PER_WINDOW` bounds the
process, not any individual client; one client can exhaust the window for everyone. Exhaustion is
not an error — the question is answered from the deterministic path instead.
