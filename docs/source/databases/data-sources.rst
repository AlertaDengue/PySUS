=============
Data Sources
=============

.. toctree::
   :hidden:

   getting_started_pysus

PySUS provides simplified, origin-namespaced functions that return pandas
DataFrames directly. Each origin is reachable through its own namespace —
``pysus.ftp.*``, ``pysus.dadosgov.*``, ``pysus.saude.*`` — so the data source
is explicit:

.. code-block:: python

    import pysus

    # Download SINAN Dengue data (DATASUS FTP, via the S3 catalog mirror)
    df = pysus.ftp.sinan(disease="deng", year=2024)

    # Multiple years
    df = pysus.ftp.sinan(disease="deng", year=[2023, 2024])

    # SINASC births for São Paulo (dados.gov.br)
    df = pysus.dadosgov.sinasc(state="SP", year=2024)

    # SIM mortality data — query the origin server directly
    df = pysus.ftp.sim(state="SP", year=2024, source="origin")

    # SIH hospitalizations
    df = pysus.ftp.sih(state="SP", year=2024, month=[1, 2, 3])

    # CNES health facilities
    df = pysus.ftp.cnes(state="SP", year=2024, month=1)

OpenDataSUS (Saude) functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    import pysus

    # Dengue/Chik/Zika notifications
    df = pysus.saude.arboviroses(disease="dengue", year=2024)

    # Vaccination coverage
    df = pysus.saude.vacinacao(state="SP", year=2024)

    # Hospital and health facility data
    df = pysus.saude.assistencia_saude(state="SP", year=2024)

The legacy flat functions (``pysus.sinan``, ``pysus.arboviroses``, ...) still
work but emit a deprecation warning pointing to the namespaced form.

Function Reference
^^^^^^^^^^^^^^^^^^

Namespaced fetchers per origin. Both ``pysus.ftp.*`` and
``pysus.dadosgov.*`` read the S3 catalog mirror by default.

.. list-table::
    :header-rows: 1

    * - Function
      - Dataset
      - Parameters
    * - ``ftp.sinan(...)`` / ``dadosgov.sinan(...)``
      - Disease Notifications
      - disease (e.g., "DENG", "ZIKA"), year
    * - ``ftp.sinasc(...)`` / ``dadosgov.sinasc(...)``
      - Births
      - state, year
    * - ``ftp.sim(...)`` / ``dadosgov.sim(...)``
      - Mortality
      - state, year
    * - ``ftp.sih(...)``
      - Hospitalizations
      - state, year, month
    * - ``ftp.sia(...)``
      - Ambulatory
      - state, year, month
    * - ``ftp.pni(...)`` / ``dadosgov.pni(...)``
      - Immunizations
      - state, year
    * - ``ftp.ibge(...)``
      - IBGE
      - year
    * - ``ftp.cnes(...)`` / ``dadosgov.cnes(...)``
      - Health Facilities
      - state, year, month
    * - ``ftp.ciha(...)``
      - Hospital Admissions
      - state, year, month

OpenDataSUS (Saude) Functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
    :header-rows: 1

    * - Function
      - Dataset
      - Parameters
    * - ``saude.arboviroses(**kwargs)``
      - Arboviroses (Dengue/Chik/Zika/YF)
      - disease, state, year (via kwargs)
    * - ``saude.vacinacao(**kwargs)``
      - Vaccination Coverage
      - state, year (via kwargs)
    * - ``saude.assistencia_saude(**kwargs)``
      - Hospital/Facility Data
      - state, year (via kwargs)
    * - ``saude.atencao_primaria(**kwargs)``
      - Primary Care (Previne Brasil)
      - state, year (via kwargs)
    * - ``saude.sisvan(**kwargs)``
      - Nutrition Surveillance
      - state, year (via kwargs)
    * - ``saude.sisagua(**kwargs)``
      - Water Quality
      - state, year (via kwargs)
    * - ``saude.bnafar(**kwargs)``
      - Pharmaceutical Assistance
      - state, year (via kwargs)
    * - ``saude.saude_indigena(**kwargs)``
      - Indigenous Health
      - state, year (via kwargs)

The ``source`` parameter is accepted everywhere: ``source="catalog"`` (default)
serves the S3/Parquet mirror; ``source="origin"`` queries the origin server
directly.  The Saude portal has no catalog mirror, so ``saude.*`` always
queries the CKAN portal.

Using the PySUS Client
^^^^^^^^^^^^^^^^^^^^^^

For more control, use the PySUS client directly:

.. code-block:: python

    from pysus import PySUS

    async def main():
        async with PySUS() as pysus:
            # Query catalog
            files = await pysus.query(
                dataset="sinan",
                group="DENG",
                state="SP",
                year=2024,
            )

            # Download files
            for f in files:
                local = await pysus.download(f)

            # Read parquet files
            import glob
            paths = glob.glob("/cache/**/*.parquet")
            df = pysus.read_parquet(paths, mode="union")

read_parquet Modes
^^^^^^^^^^^^^^^^^^

- **union** (default): Includes all columns from any file
- **intersection**: Only common columns across all files
- **strict**: Raises error if schemas don't match

.. code-block:: python

    df = pysus.read_parquet(paths, mode="union")
    df = pysus.read_parquet(paths, mode="intersection")
    df = pysus.read_parquet(paths, mode="strict")

    # With custom SQL
    df = pysus.read_parquet(paths, sql="SELECT * WHERE column > 100")

---

Dataset Descriptions
--------------------

About SINAN
^^^^^^^^^^^

The Information System for Notifiable Diseases (Sinan) is primarily fed by the notification and investigation of cases of diseases and conditions listed in the national list of notifiable diseases. However, states and municipalities are allowed to include other significant health issues in their region, such as filariasis in the municipality of São Paulo. Its effective use allows for dynamic diagnosis of the occurrence of an event in the population, potentially providing insights into the causal explanations of notifiable diseases, as well as indicating risks to which individuals are exposed. This contributes to the identification of the epidemiological reality of a specific geographic area. Its systematic, decentralized use contributes to the democratization of information, enabling all healthcare professionals to access and make it available to the community. Therefore, it is a relevant tool to assist in health planning, defining intervention priorities, and evaluating the impact of interventions.


About SINASC
^^^^^^^^^^^^

The Information System on Live Births (Sistema de Informações sobre Nascidos Vivos or SINASC) was officially implemented starting from 1990 with the aim of collecting data on reported births across the entire national territory and providing birth-related data for all levels of the Healthcare System.

The Ministry of Health's Department of Health Surveillance (Secretaria de Vigilância em Saúde or SVS/MS) manages SINASC at the national level. Specifically, the responsibility for changes in layout, as well as arrangements for printing and distributing the Declaration of Live Birth (DN) forms and the System manuals, lies with the General Coordination of Information and Epidemiological Analysis (Coordenação-Geral de Informações e Análises Epidemiológicas or CGIAE) and the Department of Epidemiological Analysis and Surveillance of Non-Communicable Diseases (Departamento de Análise Epidemiológica e Vigilância de Doenças Não Transmissíveis or DAENT). The implementation of SINASC occurred gradually in all federal units and, since 1994, has been showing a higher number of registrations in many municipalities compared to what is published by the Brazilian Institute of Geography and Statistics (Instituto Brasileiro de Geografia e Estatística or IBGE) based on Civil Registry data. The system also enables the construction of useful indicators for healthcare service management planning.


About SIM
^^^^^^^^^

The Mortality Information System (Sistema de Informações sobre Mortalidade or SIM) was established by DATASUS to regularly collect data on mortality in the country. With the creation of SIM, it became possible to comprehensively capture mortality data to support various levels of public health management. Based on this information, it is possible to conduct analyses of the situation, plan, and evaluate actions and programs in the field of public health.


About SIH
^^^^^^^^^^

The purpose of the AIH (SIHSUS System) is to document all hospitalization-related services that are FINANCED BY SUS and, after processing, generate reports for managers to facilitate payments to healthcare facilities. Additionally, the federal level receives a monthly database of all authorized hospitalizations (whether approved for payment or not) to enable the transfer of Production values for Medium and High complexity, as well as values for CNRAC, FAEC, and University Hospitals, in their various forms of management contracts, to the Health Departments.


About SIA
^^^^^^^^^^

The SIA (Sistema de Informação Ambulatorial) is the system that enables local managers to process information related to outpatient care (non-hospital) recorded in the data collection applications for such services provided by public and private providers, whether contracted or affiliated with SUS.

About PNI
^^^^^^^^^

The PNI (Programa Nacional de Imunizações) information system manages vaccination data across Brazil, tracking immunization coverage, vaccine doses administered, and supporting the monitoring of the National Immunization Program's goals.

About CIHA
^^^^^^^^^^

The CIHA (Comunicação de Informação Hospitalar e Ambulatorial) system manages hospital admission and outpatient information, complementing the SIH system with additional data on hospital care across Brazil.
