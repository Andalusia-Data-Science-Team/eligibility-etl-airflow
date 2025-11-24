# src/icd10_utils.py
from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from typing import Dict, Tuple
import re

import pandas as pd
import streamlit as st

# Where to look for the full ICD10 CSV
ICD10_PATHS = [
    # typical repo layout: <root>/data/idc10_disease_full_data.csv
    Path(__file__).resolve().parents[1] / "data" / "idc10_disease_full_data.csv",
    # fallback: mounted path (what you uploaded in Streamlit)
    Path("/mnt/data/idc10_disease_full_data.csv"),
]


@lru_cache(maxsize=1)
def load_icd10_maps() -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Return (code_to_name, name_to_code) using the full ICD10 CSV.

    code_to_name: "D50.9" -> "Iron deficiency anemia, unspecified"
    name_to_code: normalized lower-case name -> code
    """
    df = None
    errs = []

    for p in ICD10_PATHS:
        try:
            if p.exists():
                df = pd.read_csv(
                    p,
                    dtype=str,
                    keep_default_na=False,
                    encoding="utf-8",
                    on_bad_lines="skip",
                )
                break
        except Exception as e:
            errs.append(f"{p}: {e}")

    if df is None or df.empty:
        st.warning("⚠️ ICD10 full table not found/empty. Free-text will be used.")
        return {}, {}

    # CSV columns: diseaseCode, diseaseDescription, (…)
    code_col = "diseaseCode" if "diseaseCode" in df.columns else df.columns[0]
    name_col = "diseaseDescription" if "diseaseDescription" in df.columns else df.columns[1]

    df[code_col] = df[code_col].astype(str).str.strip().str.upper()
    df[name_col] = df[name_col].astype(str).str.strip()

    # main maps
    code_to_name: Dict[str, str] = {
        c: n for c, n in zip(df[code_col], df[name_col]) if c
    }

    # normalize name -> code
    def _norm_name(s: str) -> str:
        return " ".join(str(s).lower().split())

    name_to_code: Dict[str, str] = {}
    for c, n in code_to_name.items():
        key = _norm_name(n)
        if key and key not in name_to_code:
            name_to_code[key] = c

    # also add a few alias forms without punctuation
    for n, c in list(name_to_code.items()):
        alias = re.sub(r"[^a-z0-9 ]+", "", n)
        if alias and alias not in name_to_code:
            name_to_code[alias] = c

    return code_to_name, name_to_code


def _best_match_code(raw_code: str, code_to_name: Dict[str, str]) -> Tuple[str, str]:
    """
    Fuzzy resolver for ICD10 codes.

    Handles cases like:
    - user enters D50  but table has only D50.1 → pick child D50.1
    - user enters D50.1 but only D50 exists   → fall back to parent D50
    - user enters D501 and only D50 exists    → try shortening suffix
    """
    code = (raw_code or "").strip().upper().replace(" ", "")
    if not code:
        return "", ""

    # 1) Exact match
    if code in code_to_name:
        return code, code_to_name[code]

    # 2) If there is a dot, try parent (before dot)
    if "." in code:
        base = code.split(".")[0]
        if base in code_to_name:
            return base, code_to_name[base]

    # 3) If there is no dot, see if there is a child starting with this code + "."
    #    e.g. user types D50, table has D50.1, D50.9, etc.
    candidates = [c for c in code_to_name.keys() if c.startswith(code + ".")]
    if candidates:
        # Choose the shortest child like D50.1 before D50.10
        best = sorted(candidates, key=len)[0]
        return best, code_to_name[best]

    # 4) Try gradually truncating for weird cases like D501 when only D50 exists
    tmp = code
    while len(tmp) > 3:
        tmp = tmp[:-1]
        if tmp in code_to_name:
            return tmp, code_to_name[tmp]

    # nothing found
    return "", ""


def resolve_icd10_pair(icd10_input: str, dx_input: str) -> Tuple[str, str]:
    """
    Given either/both of (icd10 code, diagnosis name) return a consistent (code, name).

    Priority:
    1) If ICD10 is provided -> fuzzy match (_best_match_code)
    2) Else use diagnosis name -> name_to_code / contains search
    3) Fallback → pass through what the user typed
    """
    code_to_name, name_to_code = load_icd10_maps()

    icd10 = (icd10_input or "").strip().upper()
    dx    = (dx_input  or "").strip()

    # If user gave an ICD10 -> try fuzzy resolution
    if icd10 and code_to_name:
        best_code, best_name = _best_match_code(icd10, code_to_name)
        if best_code:
            return best_code, best_name

    # Resolve from diagnosis name if we still don't have a valid code
    if dx and name_to_code:
        key = " ".join(dx.lower().split())

        # exact / normalized match
        if key in name_to_code:
            c = name_to_code[key]
            best_code, best_name = (
                _best_match_code(c, code_to_name) if code_to_name else (c, code_to_name.get(c, dx))
            )
            if best_code:
                return best_code, best_name or dx

        # loose contains match
        if code_to_name:
            match_code = next(
                (c for c, name in code_to_name.items() if key and key in name.lower()),
                None,
            )
            if match_code:
                return match_code, code_to_name.get(match_code, dx)

    # Fallback: unknown, return what user typed
    return icd10, dx