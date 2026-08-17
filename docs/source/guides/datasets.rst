========
Datasets
========

Reference for every system PySUS serves: the groups (file families)
available on each client, the filename conventions, and where the data
lives.

.. list-table:: Sources per dataset
   :header-rows: 1

   * - Dataset
     - FTP
     - DadosGov
     - S3 catalog
   * - SINAN (disease notifications)
     - ✅ full history
     - ✅ national CSVs (arboviroses, MPOX, hanseníase, tuberculose, sífilis)
     - ✅
   * - SIM (mortality)
     - ✅ per-UF DO/DOR
     - ✅ national `Mortalidade_Geral_<year>`
     - ✅
   * - SINASC (births)
     - ✅ per-UF DN
     - ✅ national `SINASC_<year>` / `DNBR<year>`
     - ✅
   * - SIA (ambulatory)
     - ✅ per-UF/month PA/BI/AD/AM/AN/AQ/AR
     - ⚠️ Fortaleza extracts only (excluded from the sync)
     - ✅
   * - SIH (hospitalizations)
     - ✅ per-UF/month RD/RJ/ER/SP
     - ❌
     - ✅
   * - CIHA (hospital admissions)
     - ✅ per-UF/month
     - ❌
     - ✅
   * - CNES (health facilities)
     - ✅ per-UF/month (DC, EE, EQ, EP, GM, HB, IN, LT, PF, RC, SR, ST)
     - ✅ current national base
     - ✅
   * - PNI (immunizations)
     - ✅ CPNI/DPNI per-UF
     - ✅ national monthly `vacinacao_<mes>_<ano>`
     - ✅
   * - IBGE (population)
     - ✅ POP/ALF/ESCA/ESCB/RENDA/IDOSO/PROJ
     - ❌
     - ✅
   * - COVID19
     - ❌
     - ✅ anonymized cases
     - ⏳

Filename conventions (FTP)
--------------------------

Every FTP file encodes its metadata in the name; PySUS parses it with
per-dataset formatters:

.. list-table::
   :header-rows: 1

   * - Dataset
     - Pattern
     - Example
   * - SINAN
     - ``<GROUP>BR<yy>``
     - ``DENGBR25.dbc`` → DENG, BR, 2025
   * - SIM
     - ``<GROUP><UF><yy>``
     - ``DOAC2501.dbc`` → DO, AC, 2025
   * - SINASC
     - ``DN<UF><yy>``
     - ``DNSP2501.dbc`` → DN, SP, 2025
   * - SIA
     - ``<GROUP><UF><yymm>``
     - ``PAAC2501.dbc`` → PA, AC, 2025-01
   * - SIH
     - ``<GROUP><UF><yymm>``
     - ``RDAC2501.dbc`` → RD, AC, 2025-01
   * - CIHA
     - ``CIHA<UF><yymm>``
     - ``CIHAAC2501.dbc`` → AC, 2025-01
   * - CNES
     - ``<GROUP><UF><yymm>``
     - ``PFMS2501.dbc`` → PF, MS, 2025-01
   * - PNI
     - ``<GROUP><UF><yy>``
     - ``DPNIBR25.dbc`` → DPNI, BR, 2025

Part-split files
^^^^^^^^^^^^^^^^

DATASUS splits large monthly files into parts with ``_1``, ``_2``, ``_3``
suffixes (e.g. ``BISP2504_2.dbc``). PySUS keeps each part as a separate
file with the correct group/year/month metadata.

S3 layout
---------

The catalog stores one Parquet artifact per logical file under a
hierarchical key::

   public/data/<origin>/<dataset>/<group>/<year>/<month>/<state>/<STEM>.parquet

Missing attributes use ``_`` (national files use ``BR`` as the state).
Duplicate logical files are not kept — when the same data arrived from
multiple origins, only the most updated artifact remains.
