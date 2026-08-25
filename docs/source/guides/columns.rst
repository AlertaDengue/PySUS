==========================
Column Metadata & Schemas
==========================

PySUS ships curated YAML schemas for SUS datasets, transcribed from
official data dictionaries. These provide column names, descriptions,
data types, value codes, and field rules.

Available Schemas
-----------------

.. code-block:: python

   from pysus import available_databases

   print(available_databases())
   # ["sim", "sinan"]

Currently available:

- **SIM** — Declaração de Óbito (death certificate)
- **SINAN** — 30 disease notification schemas (dengue, peste, chikungunya, malaria, tuberculosis, etc.)

SINAN Diseases
^^^^^^^^^^^^^^

The SINAN schemas cover these diseases:

.. list-table::
   :header-rows: 1

   * - Disease
     - Schema File
   * - Dengue
     - ``sinan/dengue.yaml``
   * - Chikungunya
     - ``sinan/chikungunya.yaml``
   * - Peste
     - ``sinan/peste.yaml``
   * - Malaria
     - ``sinan/malaria.yaml``
   * - Tuberculosis
     - ``sinan/tuberculose.yaml``
   * - Hanseníase
     - ``sinan/hanseniase.yaml``
   * - Hepatites
     - ``sinan/hepatites.yaml``
   * - Meningite
     - ``sinan/meningite.yaml``
   * - Coqueluche
     - ``sinan/coqueluche.yaml``
   * - Raiva
     - ``sinan/raiva.yaml``
   * - Febre Amarela
     - ``sinan/febre_amarela.yaml``
   * - Chagas
     - ``sinan/chagas.yaml``
   * - Leishmaniose Tegumentar
     - ``sinan/leishmaniose_tegumentar.yaml``
   * - Leishmaniose Visceral
     - ``sinan/leishmaniose_visceral.yaml``
   * - Leptospirose
     - ``sinan/leptospirose.yaml``
   * - Esquistossomose
     - ``sinan/esquistossomose.yaml``
   * - Hantavirose
     - ``sinan/hantavirose.yaml``
   * - Sífilis Congênita
     - ``sinan/sifilis_congenita.yaml``
   * - Sífilis Gestacional
     - ``sinan/sifilis_gestacional.yaml``
   * - Tetano Neonatal
     - ``sinan/teti_neonatal.yaml``
   * - Tetano
     - ``sinan/teti.yaml``
   * - Febre Tifoide
     - ``sinan/febre_tifoide.yaml``
   * - Febre Maculosa
     - ``sinan/febre_maculosa.yaml``
   * - Colera
     - ``sinan/colera.yaml``
   * - Difteria
     - ``sinan/difteria.yaml``
   * - Botulismo
     - ``sinan/botulismo.yaml``
   * - Intoxicação Exógena
     - ``sinan/intoxicacao_exogena.yaml``
   * - Acidente por Animais
     - ``sinan/acidente_por_animais.yaml``

Loading Column Metadata
-----------------------

.. code-block:: python

   from pysus import load_column_metadata

   # Load columns for a specific SINAN disease
   columns = load_column_metadata("sinan", "Dengue")
   for col in columns[:5]:
       print(f"{col['name']}: {col.get('description_pt', '')}")

   # Load columns for SIM death certificate
   columns = load_column_metadata("sim", "Declaracao de Obito - DO")

Each column dict contains:

- ``name`` — column name in the data file
- ``type`` — data type (``string``, ``date``, ``integer``)
- ``description_pt`` — Portuguese description
- ``description_en`` — English description (if available)
- ``required`` — whether the field is mandatory
- ``categories`` — value code mapping (e.g., ``"1-Sim 2-Não 9-Ignorado"``)
- ``characteristics`` — field rules and notes from the official dictionary
- ``format`` — expected format hint (e.g., ``"YYYYMMDD"``)

Searching Columns
-----------------

Search across all datasets and schemas:

.. code-block:: python

   from pysus import search_columns

   # Find a column by name across all datasets
   results = search_columns(query="CON_CLASSI")
   for r in results:
       print(f"{r.dataset}/{r.endpoint}: {r.name}")
       if r.categories:
           print(f"  Categories: {r.categories}")

   # Restrict to a specific dataset
   results = search_columns(dataset="sinan", query="DT_NOTIFIC")

   # Restrict to a specific disease
   results = search_columns(dataset="sinan", endpoint="peste")

Returns a list of :class:`~pysus.api.columns.ColumnInfo` dataclasses with
attributes: ``name``, ``description``, ``description_en``, ``dtype``,
``dataset``, ``endpoint``, ``categories``, ``characteristics``,
``required``, ``format``.

Schema Format
-------------

Each YAML file follows this structure:

.. code-block:: yaml

   dengue:
     - name: FEBRE
       type: string
       description_pt: "Informar qual sinal clínico"
       type_info: "VARCHAR(1)"
       required: true
       categories: "1 – Sim 2 – Não"
       characteristics: "Campo obrigatório"

     - name: DT_NOTIFIC
       field: "dt_notificacao"
       type: date
       description_pt: "Data da notificação"
       required: true
       format: "YYYYMMDD"

Schema fields:

- ``name`` — column name (required)
- ``type`` — ``string``, ``date``, ``integer`` (required)
- ``description_pt`` — Portuguese description
- ``description_en`` — English description
- ``type_info`` — raw SQL type (e.g., ``VARCHAR(1)``)
- ``required`` — mandatory on the official form
- ``categories`` — value code mapping
- ``characteristics`` — field rules, validation notes
- ``format`` — expected format hint
- ``field`` — alternative/alias column name
