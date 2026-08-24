"""Portuguese-to-English column name mappings for SUS health datasets.

Each entry maps an uppercase Portuguese column name (as found in DATASUS
files) to a lowercase_snake_case English equivalent.  The mapping is
curated from SINAN, SINASC, SIM, SIH, SIA, PNI, CNES, and CIHA
documentation.

Examples
--------
>>> from pysus.api.mappings import PT_TO_EN
>>> PT_TO_EN.get("DT_NOTIFIC")
'notification_date'
>>> PT_TO_EN.get("SG_UF")
'state'
"""

from __future__ import annotations

import pandas as pd  # type: ignore[import-untyped]

PT_TO_EN: dict[str, str] = {
    # --- Identifiers ---
    "NU_NOTIFIC": "notification_number",
    "NU_ANO": "notification_year",
    "ID_AGRAVO": "disease_code",
    "ID_MUNICIP": "municipality_code",
    "ID_UNIDADE": "facility_code",
    "ID_UF": "state_code",
    "CODMUNRES": "residence_municipality_code",
    "CODMUNOCOR": "occurrence_municipality_code",
    # --- Dates ---
    "DT_NOTIFIC": "notification_date",
    "DT_SIN_PRI": "symptom_onset_date",
    "DT_NASC": "birth_date",
    "DT_INTER": "admission_date",
    "DT_OBITO": "death_date",
    "DT_DIGITA": "entry_date",
    "DT艾滋": "diagnosis_date",
    "DT_TRUNC": "report_date",
    "DT_RETORNO": "return_date",
    "DT_COL_PQ": "specimen_collection_date",
    "DT_VISITA": "visit_date",
    "DT_CRITERIO": "confirmation_date",
    "DT_INVEST": "investigation_date",
    "DT_ENCERRA": "case_closure_date",
    "DT_TRANSD": "transfer_date",
    "DT_RECEBIM": "receipt_date",
    "DT_ALT": "status_change_date",
    "DT_AUDIT": "audit_date",
    "DT_IMUNO": "immunisation_date",
    "DT_DOSE": "dose_date",
    "DT_PROva": "test_date",
    # --- Demographics ---
    "CS_SEXO": "sex",
    "IDADE": "age",
    "ANT_IDADE": "age_at_event",
    "CS_GESTANT": "pregnancy_status",
    "CS_RACA": "race",
    "ANT_RACA": "race_ethnicity",
    "CS_ESCOLAR": "education_level",
    "ESCOLMAE": "mothers_education",
    # --- Geography ---
    "SG_UF": "state",
    "SG_UF_NOT": "notification_state",
    "NM_MUNICIP": "municipality_name",
    "UF": "state",
    # --- Clinical ---
    "CLASSI_FIN": "final_classification",
    "CRITERIO_CONF": "confirmation_criteria",
    "EVOLUCAO": "case_outcome",
    "TP_NOT": "notification_type",
    "SEM_NOT": "epidemiological_week",
    "FEBRE": "fever",
    "MIALGIA": "myalgia",
    "CEFALEIA": "headache",
    "EXANTEMA": "rash",
    "VOMITO": "vomiting",
    "NAUSEA": "nausea",
    "DOR_RETRO": "retro_orbital_pain",
    "DIARREIA": "diarrhea",
    "CONJUNTIV": "conjunctivitis",
    "ARTRALGIA": "arthralgia",
    "ARTRITE": "arthritis",
    "PETEQUIA": "petechiae",
    "LEUCOPENIA": "leukopenia",
    "LACO": "positive_tourniquet_test",
    "DOR_COSTAS": "back_pain",
    "SUPURACAO": "suppuration",
    "HEPATOMEGLIA": "hepatomegaly",
    "ESPLENOMEG": "splenomegaly",
    "CONSCIO": "consciousness_level",
    "COMA": "coma",
    ".Convul": "seizures",
    "INSUFICIENCIA": "insufficiency",
    "HEMORRAGIAS": "hemorrhages",
    "MANCHAS_RUB": "red_spots",
    "PLAQ_MENOR": "low_platelets",
    "HEMATOCRIT": "hematocrit",
    "HEMOGLOBINA": "hemoglobin",
    "HEMATEC": "hematocrit_alt",
    "PROVA_CRUZ": "cross_test",
    "ANTIVIRAL": "antiviral",
    "HOSPITALIZ": "hospitalised",
    "INTERNACAO": "hospitalisation",
    "UTI": "icu",
    "SUPORTE_VENT": "ventilatory_support",
    "AUTO_IMUNE": "autoimmune_disease",
    "HEMATOLOGIC": "haematologic_disease",
    "DIABETES": "diabetes",
    "HIPERTENSA": "hypertension",
    "RENAL": "renal_disease",
    "HEPATICA": "liver_disease",
    "OBESIDADE": "obesity",
    "ALCOOLISMO": "alcoholism",
    "DESC_RESP": "respiratory_distress",
    "DOENCA_HEMAT": "haematological_disease",
    # --- Pregnancy ---
    "ANT_PRE_NA": "prenatal_visits",
    "UF_PRE_NAT": "prenatal_state",
    "MUN_PRE_NA": "prenatal_municipality",
    "UNI_PRE_NA": "prenatal_facility",
    "ANT_TRATAD": "treated",
    # --- Vaccination ---
    "VACINA": "vaccinated",
    "DOSE_1": "dose_1",
    "DOSE_2": "dose_2",
    "DOSE_REF": "booster_dose",
    # --- Lab ---
    "LAB_CONF": "lab_confirmation",
    "PARTO": "delivery_type",
    "PESO": "birth_weight",
    "GESTACAO": "gestational_age",
    "APGAR1": "apgar_1min",
    "APGAR5": "apgar_5min",
    "TIPO_PARTO": "delivery_type",
    "LOC_NASC": "birth_location",
    "IDANOMAL": "congenital_anomaly",
    # --- Hospital ---
    "DIAG_PRINC": "primary_diagnosis",
    "DIAG_SECUN": "secondary_diagnosis",
    "CID_PRIM": "primary_cid",
    "PROC_SOLIC": "requested_procedure",
    "PROC_REALI": "performed_procedure",
    "NAT_JUR": "legal_nature",
    "TP_SAID": "discharge_type",
    "MOT_SAID": "discharge_reason",
    "VAL_TOT": "total_cost",
    "VAL_SH": "hospital_services_cost",
    "VAL_SP": "professional_services_cost",
    "VAL_SAD": "ambulatory_cost",
    "VAL_UTI": "icu_cost",
    "DIAS_PERM": "length_of_stay",
    # --- Mortality ---
    "TIPO_OBITO": "death_type",
    "CAUSA_OBITO": "cause_of_death",
    "CIRURGIA": "surgery",
    "ASSIST_MED": "medical_care",
    "OBITO_PARTO": "death_during_delivery",
    "NECROPSIA": "necropsy",
}


def to_english(
    df: pd.DataFrame,
    store_mapping: bool = True,
) -> pd.DataFrame:
    """Rename Portuguese column names to English equivalents.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with Portuguese column names.
    store_mapping : bool, optional
        When ``True`` (default), stores the original-to-English
        mapping in ``df.attrs["aliases"]`` so it can be reversed
        later.

    Returns
    -------
    DataFrame
        A copy with English column names.

    Examples
    --------
    >>> import pysus
    >>> df = pysus.to_english(df)
    >>> df.attrs["aliases"]  # original → English mapping
    {'DT_NOTIFIC': 'notification_date', ...}
    """
    rename_map = {
        col: PT_TO_EN[col.upper()]
        for col in df.columns
        if col.upper() in PT_TO_EN
    }
    if store_mapping and rename_map:
        df = df.copy()
        df.attrs["aliases"] = rename_map
    return df.rename(columns=rename_map)
