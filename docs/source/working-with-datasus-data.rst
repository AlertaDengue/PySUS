.. _working-with-datasus-data-gotchas--field-semantics:

Working with DATASUS data: gotchas & field semantics
====================================================

   Practical field notes for anyone building pipelines on DATASUS microdata with PySUS. Every item below is a real trap encountered while processing the SIH-RD, SIH-SP, SIM, SIA-AM, SIA-PA, CNES-EQ
   and CNES-PF systems for all 27 Brazilian states, 2008–2025 (CNES since 2005). DATASUS does not standardize conventions across systems and does not publish a changelog for its file layouts, so most
   of these can only be learned the hard way.

.. contents:: On this page
   :local:
   :depth: 1

.. _1-the-systems-at-a-glance:

1. The systems at a glance
--------------------------

============================================= ==================== ========================================= =============== ========================
System                                        File pattern         Content                                   Granularity     Typical volume
============================================= ==================== ========================================= =============== ========================
**SIH-RD** (hospital admissions, reduced AIH) ``RD{UF}{YYMM}.dbc`` Hospitalizations                          Monthly / state ~500K–2M rec/state/yr
**SIH-SP** (hospital professional services)   ``SP{UF}{YYMM}.dbc`` Secondary professional acts per admission Monthly / state ~1M–5M rec/state/mo
**SIM** (mortality)                           ``DO{UF}{YYYY}.dbc`` Death certificates                        Annual / state  ~50K–300K rec/state/yr
**SIA-AM** (outpatient, APAC medications)     ``AM{UF}{YYMM}.dbc`` High-cost drug dispensations              Monthly / state ~100K–500K rec/state/mo
**SIA-PA** (outpatient production)            ``PA{UF}{YYMM}.dbc`` Outpatient procedures                     Monthly / state **1M–17M+ rec/state/mo**
**CNES-EQ** (facilities, equipment)           ``EQ{UF}{YYMM}.dbc`` Equipment inventory                       Monthly / state ~50K–200K rec/state/mo
**CNES-PF** (facilities, professionals)       ``PF{UF}{YYMM}.dbc`` Health professionals                      Monthly / state ~200K–800K rec/state/mo
============================================= ==================== ========================================= =============== ========================

Each system has its own column names, encodings and date formats. **There is no standardization across them.**

--------------

.. _2-the-dbc-format:

2. The DBC format
-----------------

``.dbc`` is a proprietary compression used only by DATASUS: internally a dBASE ``.dbf`` file compressed with a variant of PKWare's ``blast``/implode. There is no complete official spec.

-  **SIGSEGV on corrupt files.** The C decompressor (``blast.c``, used by ``pyreaddbc``) can crash the host process with a segmentation fault on corrupt or malformed headers. This kills the process —
   ``try/except`` cannot catch it, so a single bad file can abort a whole batch. Running the DBC→DBF step in a subprocess (with retries) contains the blast radius to one file.
-  **Unpredictable expansion ratio.** A 110 MB SIA-PA ``.dbc`` can expand to ~1.1 GB of ``.dbf``. You cannot preallocate based on the compressed size.
-  **Platform/build sensitivity.** ``pyreaddbc`` compiles C; on Alpine-based Docker images it needs build packages (``gcc``, ``musl-dev``), and ARM (Apple Silicon) can surface additional issues.

.. _3-encoding-always-latin-1:

3. Encoding: always latin-1
---------------------------

All DATASUS files use **Latin-1 (ISO-8859-1)**, but routinely contain:

-  embedded NUL bytes (``\x00``) inside character fields,
-  control characters outside the printable range,
-  raw binary where text is expected (corruption).

Decode defensively on every field, and strip NULs before persisting:

.. code:: python

   value = raw_bytes.decode("latin-1", "replace").strip().replace("\x00", "")

.. warning::

   Reading with ``utf-8`` (Python's default) fails — silently or with ``UnicodeDecodeError`` — on a large fraction of files. Always force ``latin-1``.

.. _4-file-naming-and-the-ftp-layout:

4. File naming and the FTP layout
---------------------------------

======= ====================== ================ ===========
System  Name pattern           Example          Year digits
======= ====================== ================ ===========
SIH-RD  ``RD{UF}{YY}{MM}.dbc`` ``RDSP2301.dbc`` **2**
SIH-SP  ``SP{UF}{YY}{MM}.dbc`` ``SPSP2301.dbc`` **2**
SIM     ``DO{UF}{YYYY}.dbc``   ``DOSP2023.dbc`` **4**
SIA-AM  ``AM{UF}{YY}{MM}.dbc`` ``AMSP2301.dbc`` **2**
SIA-PA  ``PA{UF}{YY}{MM}.dbc`` ``PASP2301.dbc`` **2**
CNES-EQ ``EQ{UF}{YY}{MM}.dbc`` ``EQSP2301.dbc`` **2**
CNES-PF ``PF{UF}{YY}{MM}.dbc`` ``PFSP2301.dbc`` **2**
======= ====================== ================ ===========

-  **Two-digit years need a pivot.** SIH/SIA/CNES abbreviate the year, so ``23`` → 2023 requires a pivot (e.g. ``< 50`` → 20xx, else 19xx). **SIM uses four digits** — applying the pivot to SIM
   produces garbage years.
-  **Alphabetic version suffix.** Some files carry a suffix: ``RDSP2301a.dbc``, ``RDSP2301b.dbc``. Filename regexes must allow an optional trailing letter, e.g. ``RD{UF}\d{4}[a-zA-Z]?\.dbc``
   (case-insensitive).
-  **Shared directories.** SIH-RD and SIH-SP live in the *same* FTP directory, distinguished only by the ``RD``/``SP`` prefix; SIA-AM and SIA-PA likewise share a directory (``AM``/``PA``).
-  **Different start dates.** SIH/SIA/SIM begin at ``200801_``; CNES begins earlier at ``200508_`` (May 2005). Pre-2008 SIH/SIA data live elsewhere with a different name format.

FTP base: ``ftp://ftp.datasus.gov.br/dissemin/publicos/``

============ =========================
System       Path
============ =========================
SIH (RD, SP) ``SIHSUS/200801_/Dados``
SIM (DO)     ``SIM/CID10/DORES``
SIA (AM, PA) ``SIASUS/200801_/Dados``
CNES-EQ      ``CNES/200508_/Dados/EQ``
CNES-PF      ``CNES/200508_/Dados/PF``
============ =========================

.. _5-the-datasus-ftp-server:

5. The DATASUS FTP server
-------------------------

-  **Anonymous access**, IIS/Windows-style listings, recommended timeout ~120 s.
-  **Connections drop**, especially on files > 50 MB. Wrap transfers with automatic reconnect + retry rather than failing the whole job on one dropped socket.
-  **Directory listing is expensive.** The SIA directory holds thousands of files (27 states × 12 months × 20+ years × 2 prefixes). Issue a **single ``LIST``** per session and cache
   ``{filename: size}`` instead of one ``LIST``/``SIZE`` per state.
-  **Verify integrity by size.** A cached local file whose byte count differs from the server's is truncated/corrupt — re-download it. Existence alone is not enough (see also the staleness point
   below).
-  **Recent months get revised.** DATASUS retroactively updates the most recent months. A naive "already downloaded → skip" cache silently serves stale data. Always re-fetch a trailing window (e.g.
   the last 6 months) even if a same-named file already exists; older consolidated files can be skipped safely.

.. _6-large-files-and-memory-sia-pa:

6. Large files and memory (SIA-PA)
----------------------------------

SIA-PA is orders of magnitude larger than the other systems:

========== ============= ============== ============
System     ``.dbc`` size ``.dbf`` size  Records/file
========== ============= ============== ============
SIH        5–30 MB       50–300 MB      200K–1.5M
SIM        2–15 MB       20–150 MB      50K–300K
SIA-AM     5–40 MB       50–400 MB      100K–500K
**SIA-PA** **50–110 MB** **0.5–1.1 GB** **5M–17M+**
========== ============= ============== ============

Loading one São Paulo SIA-PA file into pandas to then filter can consume 8–16 GB and OOM. The fix is to **filter before materializing**: scan the fixed-width records at the byte level on just the
column(s) you filter on (e.g. the CID columns), collect matching row indices, and only build a DataFrame from those.

Observed on a real file (SIA-PA, São Paulo, 2023-01, 17.2M records → 1,284 rows with neurological CIDs): full read + filter ≈ **12 min / 14 GB**; byte-level pre-filter ≈ **45 s / 0.8 GB**.

Other practical guards: batch inserts (e.g. 5,000 rows) with savepoints so one bad batch does not roll back the rest, and generous worker timeouts for full 27-state pulls.

.. _7-column-layouts-drift-across-years:

7. Column layouts drift across years
------------------------------------

DATASUS does not version file layouts. Columns are added, renamed or removed silently between years. An ETL written against one year's files may break — months later — on another year's. Audited
deltas between the oldest and newest available files:

-  **SIA-AM:** 50 → 51 columns (``AP_NATJUR`` added ~2017).
-  **SIA-PA:** 54 → 60 (+6: ``PA_INE``, ``PA_NAT_JUR``, ``PA_SRV_C``, ``PA_VL_CF``, ``PA_VL_CL``, ``PA_VL_INC``).
-  **SIH:** 86 → 113 (+27), incl. detailed secondary diagnoses ``DIAGSEC1``–``DIAGSEC9`` and their types ``TPDISEC1``–``TPDISEC9``, ICU markers, finance breakdowns.
-  **SIM:** 54 → 87 (+37, −4). Added: 2010 schooling codes, the death-investigation module, maternal-death detail, codification/version fields. Removed: ``CODBAIOCOR``, ``CODBAIRES``, ``TPASSINA``,
   ``UFINFORM``.

Implications:

1. Access columns defensively (``df.get("COL", default)``), never assume presence.
2. The only reliable source of truth for a year is **the DBC header itself**.
3. Test your pipeline against the **oldest and newest** files before a full run.

.. _8-do-not-trust-third-party-data-dictionaries:

8. Do not trust third-party data dictionaries
---------------------------------------------

Column names in non-official dictionaries, forums and even some semi-official docs do **not** always match the real DBC field names. With ``df.get("wrong_name")`` the result is a silently empty column
— no error, no warning.

Real examples (SIA-PA, verified absent in DBC from 2010 and 2024):

================== =============== ============
Documented (wrong) Actual DBC name Symptom
================== =============== ============
``PA_CNPJCC``      ``PA_CNPJ_CC``  always empty
``PA_CNSPROF``     ``PA_CNSMED``   always empty
``PA_VPABRE``      ``NU_VPA_TOT``  always 0
``PA_PAESSION``    ``NU_PA_TOT``   always 0
``PA_IND_PA``      ``PA_INDICA``   always empty
================== =============== ============

"Phantom" columns are the dual hazard — names that never existed in any year (e.g. SIM ``STNOVA`` confused with ``STDONOVA``; ``NUMERODO``, which has never existed in SIM disseminação files). Because
``df.get(...)`` returns NaN, a researcher concludes "DATASUS never fills this field," when in fact the field name is wrong.

**Rule of thumb:** if a field is 100% null, verify the real column name in the DBC header before concluding the field is unused. Dump the header to check:

.. code:: python

   # field descriptors live in the DBF header right after DBC decompression
   for name, ftype, length, decimals in dbf_fields:
       print(f"{name:20s} type={ftype} len={length}")

.. _9-same-concept-different-column-names:

9. Same concept, different column names
---------------------------------------

The same concept is named differently in every system — a Rosetta table:

====================== =========================== ================== ====================== ============== ============== ============ ============
Concept                SIH-RD                      SIH-SP             SIM                    SIA-AM         SIA-PA         CNES-EQ      CNES-PF
====================== =========================== ================== ====================== ============== ============== ============ ============
Primary CID            ``DIAG_PRINC``              ``SP_CIDPRI``      ``CAUSABAS``           ``AP_CIDPRI``  ``PA_CIDPRI``  —            —
Secondary CID          ``DIAG_SECUN``              —                  ``LINHAA``–``LINHAD``  ``AP_CIDSEC``  ``PA_CIDSEC``  —            —
Associated CID         ``CID_ASSO``                —                  ``LINHAII``            ``AP_CIDCAS``  ``PA_CIDCAS``  —            —
Sex                    ``SEXO``                    —                  ``SEXO``               ``AP_SEXO``    ``PA_SEXO``    —            —
Age                    ``COD_IDADE``\ +\ ``IDADE`` —                  ``IDADE`` (prefixed)   ``AP_NUIDADE`` ``PA_IDADE``   —            —
Residence municipality ``MUNIC_RES``               —                  ``CODMUNRES``          ``AP_MUNPCN``  ``PA_MUNPCN``  —            —
Facility (CNES)        ``CNES``                    ``SP_CNES``        —                      ``AP_CODUNI``  ``PA_CODUNI``  ``CNES``     ``CNES``
Procedure              ``PROC_REA``                ``SP_PROCREA``     —                      ``AP_PRIPAL``  ``PA_PROC_ID`` —            —
Amount (R$)            ``VAL_TOT``                 ``SP_VALATO``      —                      ``AP_VL_AP``   ``PA_VALAPR``  —            —
Race/colour            ``RACA_COR``                —                  ``RACACOR``            ``AP_RACACOR`` ``PA_RACACOR`` —            —
Date/competence        ``DT_INTER`` (YYYYMMDD)     —                  ``DTOBITO`` (DDMMYYYY) competence     competence     ``COMPETEN`` ``COMPETEN``
Record number          ``N_AIH``                   ``SP_NAIH`` (→ RD) —                      —              —              —            —
Occupation (CBO)       ``CBOR``                    ``SP_PF_CBO``      —                      —              ``PA_CBOCOD``  —            ``CBO``
====================== =========================== ================== ====================== ============== ============== ============ ============

.. warning::

   **Date formats are inverted between systems.** SIH uses ``YYYYMMDD``; SIM uses ``DDMMYYYY``. Swapping them produces silently wrong dates (``20230115`` vs ``15012023``).

The SIH carries diagnoses across **up to 15 columns** (``DIAG_PRINC``, ``DIAG_SECUN``, ``DIAGSEC1``–``DIAGSEC9``, ``CID_ASSO``, ``CID_MORTE``, ``CID_NOTIF``) — analyses that read only ``DIAG_PRINC``
miss most of the clinical picture.

.. _10-sex-encoding-three-different-maps:

10. Sex encoding: three different maps
--------------------------------------

.. code:: python

   SEXO_MAP_SIH = {"1": "M", "3": "F", "0": "I", "9": "I"}  # SIH: Female = 3 (!)
   SEXO_MAP_SIM = {"1": "M", "2": "F", "0": "I", "9": "I"}  # SIM: standard numeric
   SEXO_MAP_SIA = {"M": "M", "F": "F", "I": "I"}            # SIA: already letters

.. warning::

   In SIH, **Female is ``3``, not ``2``** — and ``2`` does not exist. Reusing the SIM map for SIH drops ~half of the female admissions to "unknown".

.. _11-age-encoding-three-different-schemes:

11. Age encoding: three different schemes
-----------------------------------------

-  **SIH — two fields.** ``COD_IDADE`` (2 = hours, 3 = months, 4 = years, 5 = ≥100, 0 = ignored) + ``IDADE`` (the value). Ignoring ``COD_IDADE`` turns a 6-month-old (``COD_IDADE=3, IDADE=6``) into a
   6-year-old.
-  **SIM — single prefixed field.** ``IDADE`` packs the code as the first digit: ``"4065"`` → 65 years, ``"3006"`` → 6 months, ``"501"`` → 101 years.
-  **SIA — plain numeric.** ``AP_NUIDADE`` / ``PA_IDADE`` are already in years (may be NaN).

Cap implausible values (e.g. 120 years) to catch corruption.

.. _12-cid-10-codes-validity-multicausal-fields-prefix-matching:

12. CID-10 codes: validity, multicausal fields, prefix matching
---------------------------------------------------------------

-  **~31% garbage.** Across millions of records, a large share of distinct CID values are invalid: ``0000``, empty, binary bytes, no leading letter, trailing letters. Validate before aggregating — a
   valid CID-10 is one uppercase letter + 2–3 digits: ``^[A-Z]\d{2,3}$``. Counting distinct CIDs without this inflates the count by ~⅓.
-  **SIM death certificates are multicausal.** Causes span ``LINHAA``–``LINHAD`` (causal chain) plus ``LINHAII`` (contributing conditions); ``CAUSABAS`` is only the single underlying cause picked by
   the mortality rules. Analyses limited to ``CAUSABAS`` miss conditions recorded as associated/contributing. Moreover, **one field can hold several space-separated CIDs** (``LINHAB = "G200 F032"``),
   so these fields must be tokenized with ``split()``.
-  **Prefix vs exact matching.** A 3-char CID (``G40``) should match ``G400/G401/…``, but a 4-char CID must match exactly: matching ``E104`` by the ``E10`` prefix wrongly captures ``E109``. Only
   3-char codes may be prefix-matched.

.. _13-municipality-codes-6-vs-7-digits:

13. Municipality codes: 6 vs 7 digits
-------------------------------------

IBGE municipality codes have **7 digits** (last is a check digit); DATASUS stores the **first 6**. A direct join to IBGE tables returns zero rows.

::

   IBGE:    3550308  (São Paulo, 7 digits)
   DATASUS: 355030   (São Paulo, 6 digits)

Index your municipality lookup by both the full 7-digit code and its 6-digit prefix. The **first 2 digits are the state (UF)** code (``35`` → São Paulo), which is handy for deriving UF without a
separate field.

.. _14-missing-data-sentinels:

14. Missing-data sentinels
--------------------------

DATASUS rarely uses NULL; empty values are encoded as sentinels:

============ ==================================
Field        "Null" sentinel
============ ==================================
CEP          ``00000000``
CNS          ``000000000000000``
CID          ``0000``
Generic text ``nan``, ``NAN``, ``None``, ``""``
Numeric      ``0``, ``9`` (context-dependent)
============ ==================================

Normalize these to real NULL/NaN on ingest; otherwise counts and joins are skewed.

.. _15-data-types-codes-money-plausibility:

15. Data types: codes, money, plausibility
------------------------------------------

-  **Codes are text, not integers.** ``codigo_ibge``, ``cnes``, ``cep`` must be ``VARCHAR`` — storing them as INTEGER drops leading zeros (``01001000`` → ``1001000``, São Paulo CEPs become invalid).
   The same applies to type inference in Excel/R imports.
-  **Money is decimal, not float.** Store monetary values as ``NUMERIC``/``DECIMAL``; floats introduce ``10.50 → 10.4999…`` drift.
-  **Cap for plausibility** to flag corruption (e.g. amounts above a sane ceiling, length-of-stay above ~10 years).

.. _16-disappearing-fields-ap_tippre:

16. Disappearing fields: AP_TIPPRE
----------------------------------

``AP_TIPPRE`` (provider type: public/private/philanthropic) in SIA stops being populated **from 2016 on** — every record becomes ``00``. Byte-level inspection confirms the zeros come from DATASUS, not
from conversion. From ~2017 the replacement is ``AP_NATJUR`` / ``PA_NAT_JUR`` / ``NAT_JUR`` (legal-nature code, 4 digits, CONCLA/IBGE classification — e.g. ``1023`` State Autarchy, ``3069`` Private
Foundation).

Consequence: provider-type analysis is only viable for 2008–2015 via ``AP_TIPPRE``; 2017+ uses ``*_NATJUR`` (different granularity); **2016 is a blind year** for both.

.. _17-boolean-encodings-sn-vs-01:

17. Boolean encodings: S/N vs 0/1
---------------------------------

Boolean fields are not consistently encoded — sometimes not even within one system:

-  SIA-AM ``AM_GESTANT``, ``AM_TRANSPL`` use **``S``/``N``** (text).
-  SIA-AM ``AP_OBITO``, ``AP_ENCERR``, ``AP_PERMAN``, ``AP_ALTA``, ``AP_TRANSF`` use **``0``/``1``** — in the same files.
-  SIH boolean fields use ``0``/``1``.

.. warning::

   Testing ``== "1"`` on ``AM_GESTANT`` yields zero positives (its values are ``S``/``N``, never ``1``). Never assume the encoding — check the actual values.

.. _18-auxiliary-reference-tables-cnv-cbo-equipment:

18. Auxiliary reference tables (.CNV, CBO, equipment)
-----------------------------------------------------

Several coded fields are meaningless without DATASUS auxiliary tables, which ship as fixed-width ``.CNV`` files inside ``*.zip`` bundles on the FTP (e.g. ``SIASUS/200801_/Auxiliar/TAB_SIA.zip``).

-  **Indigenous ethnicity** (``AP_ETNIA`` / ``PA_ETNIA``): SIASI numeric codes for 264 ethnicities, mapped by ``ETNIA.CNV`` (name in cols 0–39, code in cols 40–50; skip the header line). Special codes
   ``X100``/``X900`` mean "not identified"/"not informed". Only filled for self-declared indigenous patients.
-  **CBO occupation codes** (e.g. ``225125`` = Neurologist): from the Ministry of Labour's CBO table, not the standard DATASUS dictionary. **Normalize length** — CBO appears with 4, 5 or 6 digits
   depending on era/state; filtering without normalizing causes false negatives.
-  **Equipment** (``TIPEQUIP`` + ``CODEQUIP``, CNES-EQ): identify equipment by the **combination** of both fields — ``CODEQUIP`` means different things under different ``TIPEQUIP``.

``.CNV`` files have a proprietary fixed-width format; parse by column offsets, trim, then cast the code.

.. _19-deduplication-and-natural-keys:

19. Deduplication and natural keys
----------------------------------

-  **SIH:** the same AIH can recur across months (admission crossing month boundary, retroactive correction). Dedup on ``(aih_numero, competence)``.
-  **SIA-AM / SIA-PA:** there is **no natural per-record key**. Same patient + same drug + same month can be legitimately distinct records. You cannot dedup by content — track processed files instead
   (mark each DBC as ingested).
-  **SIH-SP:** no natural key; dedup on ``(file, sequence_number)`` — but ``SEQUENCIA`` can be null in older files, leaving those rows un-deduplicated.

.. _20-system-notes-cnes-and-sih-sp:

20. System notes: CNES and SIH-SP
---------------------------------

**CNES is a monthly snapshot, not a transactional log.** Each file is one record per equipment (or professional) per facility. Notable traps:

-  **``COMPETEN`` lives in the data, not the filename.** Read ``df["COMPETEN"].iloc[0]`` (e.g. ``"202301"``); you cannot reliably infer competence from the filename. If the column is missing/empty the
   file should be skipped.
-  **``IND_SUS`` / ``IND_NSUS`` are text ``"1"``/``"0"``.** Summing them directly concatenates strings (``"111"`` instead of ``3``); cast/compare as strings: ``(s == "1").sum()``.
-  **Aggregate before loading** if you want municipality-level facts (e.g. group CNES-EQ by ``(CODUFMUN, TIPEQUIP, CODEQUIP)``).
-  CNES history starts in 2005 (``200508_``), 3 years earlier than SIH/SIA.

**SIH-SP details the SP component of an admission (a parent–child of SIH-RD):**

-  One AIH (RD) maps to **many SP rows** (one per professional act). Never ``GROUP BY`` without accounting for the 1:N relationship.
-  **Load RD before SP.** SP rows whose AIH is absent from the admissions table are silently dropped — order matters.
-  ``SP_CIDPRI`` (the act's CID) can **differ** from the admission's ``DIAG_PRINC`` (e.g. a comorbidity treated during the stay) — useful for comorbidity analysis.
-  The sum of ``SP_VALATO`` for an AIH may not equal the RD's ``VAL_SP``, due to DATASUS rounding/adjustments.

.. _21-population-denominators-ibge:

21. Population denominators (IBGE)
----------------------------------

Rates "per 100,000" need population by at least UF and year, but IBGE censuses run every 10 years (last: 2022) and annual estimates have gaps. IBGE publishes projections via the SIDRA API (table
6579), but some years are missing and must be **interpolated**. Using a 2020 population for 2023 rates underestimates rates in high-growth regions — interpolate between the nearest available years.

.. _22-researcher-checklist:

22. Researcher checklist
------------------------

**Data preparation**

-  ☐ Read DBF/DBC as **latin-1**, never utf-8; strip NUL bytes.
-  ☐ Validate CIDs with ``^[A-Z]\d{2,3}$`` before aggregating.
-  ☐ Use the **correct sex map** for the system (SIH: Female = 3).
-  ☐ Decode age correctly (SIH two-field; SIM prefixed; SIA plain).
-  ☐ Decide 6- vs 7-digit municipality codes and index both.
-  ☐ Treat sentinels (``00000000``, ``0000``, …) as NULL.
-  ☐ Parse the right date format per system (YYYYMMDD vs DDMMYYYY).
-  ☐ Store codes as text (leading zeros), money as decimal.

**Volume & performance**

-  ☐ Never load a full SIA-PA file into memory — pre-filter at the byte level.
-  ☐ Isolate DBC decompression in a subprocess (SIGSEGV protection).
-  ☐ Retry FTP transfers with reconnection; one ``LIST`` per session.
-  ☐ Re-download a trailing window of recent months (retroactive revisions).

**Evolution & compatibility**

-  ☐ Test against the **oldest and newest** files before a full run.
-  ☐ Verify column names against the **real DBC header**, not third-party docs.
-  ☐ If a field is 100% null, suspect a wrong column name first.
-  ☐ ``AP_TIPPRE`` only to 2015; use ``*_NATJUR`` from 2017 (2016 is blind).
-  ☐ Check boolean encoding per field (S/N vs 0/1).
-  ☐ Decode coded fields with the right ``.CNV`` auxiliary table; normalize CBO length.

**Analysis**

-  ☐ Dedup SIH on ``(aih_numero, competence)``; SIA has no natural key.
-  ☐ For SIM, read ``LINHAA``–``LINHAD`` and ``LINHAII``, not only ``CAUSABAS``; tokenize multi-CID fields.
-  ☐ Check IBGE population availability for the year; interpolate gaps.

--------------

*Derived from real ETL experience over SIH, SIH-SP, SIM, SIA-AM, SIA-PA, CNES-EQ and CNES-PF for all 27 Brazilian states (2008–2025; CNES from 2005). Contributed to PySUS as practical guidance for the
community.*
