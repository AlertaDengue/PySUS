=============
Data Sources
=============

PySUS provides simplified functions that return pandas DataFrames directly:

.. code-block:: python

    from pysus.api import sinan, sinasc, sim, sih, sia, pni, ibge, cnes, ciha

    # Download SINAN Dengue data
    df = sinan(disease="deng", year=2024)

    # Multiple years
    df = sinan(disease="deng", year=[2023, 2024])

    # SINASC births for São Paulo
    df = sinasc(state="SP", year=2024)

    # SIM mortality data
    df = sim(state="SP", year=2024)

    # SIH hospitalizations
    df = sih(state="SP", year=2024, month=[1, 2, 3])

    # CNES health facilities
    df = cnes(state="SP", year=2024, month=1)

Function Reference
^^^^^^^^^^^^^^^^^^

.. list-table::
    :header-rows: 1

    * - Function
      - Dataset
      - Parameters
    * - ``sinan(disease, year)``
      - Disease Notifications
      - disease (e.g., "DENG", "ZIKA"), year
    * - ``sinasc(state, year, group)``
      - Births
      - state, year, group (optional)
    * - ``sim(state, year, group)``
      - Mortality
      - state, year, group (optional)
    * - ``sih(state, year, month, group)``
      - Hospitalizations
      - state, year, month, group (optional)
    * - ``sia(state, year, month, group)``
      - Ambulatory
      - state, year, month, group (optional)
    * - ``pni(state, year, group)``
      - Immunizations
      - state, year, group (optional)
    * - ``ibge(year, group)``
      - IBGE
      - year, group (optional)
    * - ``cnes(state, year, month, group)``
      - Health Facilities
      - state, year, month, group (optional)
    * - ``ciha(state, year, month)``
      - Hospital Admissions
      - state, year, month

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
            df = pysus.read_parquet(paths, mode="union").df()

read_parquet Modes
^^^^^^^^^^^^^^^^^^

- **union** (default): Includes all columns from any file
- **intersection**: Only common columns across all files
- **strict**: Raises error if schemas don't match

.. code-block:: python

    df = pysus.read_parquet(paths, mode="union").df()
    df = pysus.read_parquet(paths, mode="intersection").df()
    df = pysus.read_parquet(paths, mode="strict").df()

    # With custom SQL
    df = pysus.read_parquet(paths, sql="SELECT * WHERE column > 100").df()

---

Dataset Descriptions
-------------------

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
