"""Data masking for sensitive columns.

Provides reversible encryption, hashing, and redaction for
sensitive data like CPF, names, and addresses.

Usage::

    from pysus.api.transform.masking import mask_data, unmask_data

    masked_df, key = mask_data(df, columns=["CPF", "NOME"])
    original_df = unmask_data(masked_df, ["CPF", "NOME"], key)
"""

from __future__ import annotations

from typing import Literal

import pandas as pd

# Columns that typically contain sensitive data
SENSITIVE_PATTERNS: list[str] = [
    "CPF",
    "NOME",
    "NASC",
    "RG",
    "ENDERECO",
    "TELEFONE",
    "EMAIL",
    "CEP",
]


def mask_data(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    key: bytes | None = None,
    method: Literal["encrypt", "hash", "redact"] = "encrypt",
) -> tuple[pd.DataFrame, bytes]:
    """Mask sensitive data in DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    columns : list, optional
        Columns to mask (auto-detect if None).
    key : bytes, optional
        Encryption key (generated if None).
    method : str
        Masking method.

    Returns
    -------
    tuple[pd.DataFrame, bytes]
        Masked DataFrame and key for reversal.
    """
    from cryptography.fernet import Fernet

    if key is None:
        key = Fernet.generate_key()

    fernet = Fernet(key)

    if columns is None:
        columns = _detect_sensitive_columns(df)

    df = df.copy()
    for col in columns:
        if col in df.columns:
            if method == "encrypt":
                df[col] = df[col].apply(
                    lambda x: (
                        fernet.encrypt(str(x).encode()).decode()
                        if pd.notna(x)
                        else x
                    )
                )
            elif method == "hash":
                import hashlib

                df[col] = df[col].apply(
                    lambda x: (
                        hashlib.sha256(str(x).encode()).hexdigest()
                        if pd.notna(x)
                        else x
                    )
                )
            elif method == "redact":
                df[col] = df[col].apply(lambda x: "***" if pd.notna(x) else x)

    return df, key


def unmask_data(
    df: pd.DataFrame,
    columns: list[str],
    key: bytes,
) -> pd.DataFrame:
    """Reverse encryption masking.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with masked columns.
    columns : list
        Columns to unmask.
    key : bytes
        Encryption key used for masking.

    Returns
    -------
    pd.DataFrame
        Unmasked DataFrame.
    """
    from cryptography.fernet import Fernet

    fernet = Fernet(key)
    df = df.copy()

    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: (
                    fernet.decrypt(x.encode()).decode()
                    if pd.notna(x) and isinstance(x, str)
                    else x
                )
            )

    return df


def _detect_sensitive_columns(df: pd.DataFrame) -> list[str]:
    """Auto-detect columns containing sensitive data."""
    detected: list[str] = []
    for col in df.columns:
        col_upper = col.upper()
        for pattern in SENSITIVE_PATTERNS:
            if pattern in col_upper:
                detected.append(col)
                break
    return detected
