import csv
import json
import re
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ========= CONFIG (edit these two) =========
INPUT_FILE = "dataset.xlsx"
OUTPUT_DIR = "clean_output"
# ==========================================

# general settings
DAYFIRST_HINT = True          # True if dates are usually DD/MM/YYYY
THRESH_NUMERIC = 0.88         # (kept, but numeric inference now requires 100% numeric)
THRESH_DATE = 0.65            # share of parsable rows to accept date
MAX_ROWS_SAMPLE = 20000       # speed cap per column for inference
DECIMAL_CHAR = "."            # set "," if decimals use comma
CURRENCY_CHARS = "£$€¥₹"
NBSP = "\u00A0"
EXCEL_EPOCH = datetime(1899, 12, 30)
EXCEL_DATE_MIN = 20000
EXCEL_DATE_MAX = 60000
BOOL_TRUE = {"true", "t", "yes", "y", "1"}
BOOL_FALSE = {"false", "f", "no", "n", "0"}

NA_MAP = {
    "": np.nan,
    "nan": np.nan,
    "none": np.nan,
    "null": np.nan,
    "N/A": np.nan,
    "n/a": np.nan,
    "-": np.nan,
    "\u2014": np.nan  # em dash as unicode escape
}

COMMON_DATE_PATTERNS = [
    "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y",
    "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d",
    "%d.%m.%Y", "%Y.%m.%d",
    "%d-%b-%Y", "%d %b %Y", "%b %d %Y",
    "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S"
]

def norm_header(s: str) -> str:
    s = str(s).replace(NBSP, " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s

# UPDATED: robust header policy (BQ-safe)
def simple_header(s: str) -> str:
    """
    - Trim extra spaces
    - Keep only alphanumerics; split on any non-alphanum
    - If 1 token -> keep original token casing
    - If 2+ tokens -> snake_case in lowercase
    """
    s_norm = norm_header(s)
    if s_norm == "":
        return s_norm
    tokens = re.findall(r"[A-Za-z0-9]+", s_norm)
    if not tokens:
        return "col"
    if len(tokens) == 1:
        return tokens[0]
    return "_".join(t.lower() for t in tokens)

def strip_cell(x):
    if isinstance(x, str):
        # normalize curly quotes; trim; map common NA strings to NaN
        x = x.replace("“", "\"").replace("”", "\"").replace("’", "'").replace(NBSP, " ").strip()
        return NA_MAP.get(x, x)
    return x

def sample_series(s: pd.Series, n: int) -> pd.Series:
    return s.sample(n, random_state=42) if len(s) > n else s

def detect_id_like(s: pd.Series) -> bool:
    x = s.dropna().astype(str)
    if x.empty:
        return False
    letter_ratio = x.str.contains(r"[A-Za-z]").mean()
    lead0_ratio = x.str.match(r"^0+\d+$").mean()
    unique_ratio = x.nunique(dropna=True) / max(len(x), 1)
    allowed = x.str.match(r"^[A-Za-z0-9\-\_/\.]+$").mean()
    return (letter_ratio > 0.15 or lead0_ratio > 0.05) and unique_ratio > 0.4 and allowed > 0.7

def normalize_numeric_text(s: pd.Series) -> pd.Series:
    x = s.astype(str)
    x = x.str.replace(r"^\((.*)\)$", r"-\1", regex=True)   # (123) -> -123
    x = x.str.replace(r"^(.+)-$", r"-\1", regex=True)      # 123- -> -123
    x = x.str.replace(f"[{re.escape(CURRENCY_CHARS)}{NBSP} ]", "", regex=True)
    x = x.str.replace(",", "", regex=False)                # drop thousands comma
    if DECIMAL_CHAR != ".":
        x = x.str.replace(DECIMAL_CHAR, ".", regex=False)
    x = x.str.replace("%", "", regex=False)
    x = x.replace(NA_MAP)
    return x

def try_numeric(s: pd.Series):
    x = normalize_numeric_text(s)
    num = pd.to_numeric(x, errors="coerce")
    return num, num.notna().mean()

def try_parse_date_patterns(s: pd.Series):
    x = s.astype(str)
    best = None
    best_ratio = -1.0
    for fmt in COMMON_DATE_PATTERNS:
        dt = pd.to_datetime(x, format=fmt, errors="coerce")
        ratio = dt.notna().mean()
        if ratio > best_ratio:
            best_ratio = ratio
            best = dt
            if best_ratio == 1.0:
                break
    return best, best_ratio

def try_parse_date_direction(s: pd.Series, dayfirst: bool):
    dt = pd.to_datetime(s, errors="coerce", dayfirst=dayfirst, infer_datetime_format=False, utc=False)
    return dt, dt.notna().mean()

def try_parse_excel_serial(s: pd.Series):
    num, ratio = try_numeric(s)
    if ratio < 0.7:
        return None, 0.0
    nonnull = num.dropna()
    if nonnull.empty:
        return None, 0.0
    frac = np.modf(nonnull.values)[0]
    intlike_ratio = float(np.mean(frac == 0)) if len(frac) else 0.0
    within_range = ((nonnull >= EXCEL_DATE_MIN) & (nonnull <= EXCEL_DATE_MAX)).mean()
    if intlike_ratio > 0.9 and within_range > 0.9:
        dt = nonnull.apply(lambda v: EXCEL_EPOCH + timedelta(days=int(v)))
        out = pd.Series(pd.NaT, index=num.index, dtype="datetime64[ns]")
        out.loc[nonnull.index] = pd.to_datetime(dt.values)
        return out, out.notna().mean()
    return None, 0.0

def detect_boolean(s: pd.Series) -> bool:
    x = s.dropna().astype(str).str.strip().str.lower()
    if x.empty:
        return False
    return x.isin(BOOL_TRUE | BOOL_FALSE).mean() > 0.9

def coerce_boolean(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.strip().str.lower()
    return x.map(lambda v: True if v in BOOL_TRUE else (False if v in BOOL_FALSE else np.nan)).astype("boolean")

def infer_column(s: pd.Series, name: str):
    """
    Robust inference for client-safe CSV -> BigQuery Autodetect:
    - If any non-null value contains letters OR any token is not numeric-ish -> STRING
    - Otherwise, try boolean, id-like (STRING), date, else numeric only if 100% numeric
    """
    s = s.apply(strip_cell)

    # HARD RULE: letters or non-numeric-ish -> STRING
    non_null = s.dropna().astype(str)
    if not non_null.empty:
        has_letters = non_null.str.contains(r"[A-Za-z]", na=False)
        numeric_ish = non_null.str.match(r'^[\s\+\-]?\(?\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?\)?%?$', na=False)
        if has_letters.any() or (~numeric_ish).any():
            return s.astype(str).str.strip(), "STRING", None

    # boolean
    if detect_boolean(s):
        return coerce_boolean(s), "BOOL", None

    # id-like -> STRING
    if detect_id_like(s):
        return s.astype(str).str.strip(), "STRING", None

    # dates
    ss = sample_series(s, MAX_ROWS_SAMPLE)

    excel_dt, excel_ratio = try_parse_excel_serial(ss)
    if excel_ratio >= THRESH_DATE:
        num, _ = try_numeric(s)
        nonnull = num.dropna()
        out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
        idx = nonnull.index
        out.loc[idx] = [EXCEL_EPOCH + timedelta(days=int(v)) for v in nonnull.loc[idx]]
        return out, "DATE", "%Y-%m-%d"

    pat_dt, pat_ratio = try_parse_date_patterns(ss)
    d1, r1 = try_parse_date_direction(ss, dayfirst=DAYFIRST_HINT)
    d2, r2 = try_parse_date_direction(ss, dayfirst=not DAYFIRST_HINT)
    candidates = [(pat_dt, pat_ratio), (d1, r1), (d2, r2)]
    best_dt, best_ratio = max(candidates, key=lambda t: t[1])

    if best_ratio >= THRESH_DATE:
        if best_dt is pat_dt:
            full_dt, _ = try_parse_date_patterns(s)
        elif best_dt is d1:
            full_dt, _ = try_parse_date_direction(s, dayfirst=DAYFIRST_HINT)
        else:
            full_dt, _ = try_parse_date_direction(s, dayfirst=not DAYFIRST_HINT)

        nonnull = full_dt.dropna()
        bq_type = "TIMESTAMP"
        date_fmt = "%Y-%m-%d %H:%M:%S"
        if not nonnull.empty and (nonnull.dt.time == datetime.min.time()).all():
            bq_type = "DATE"
            date_fmt = "%Y-%m-%d"
        return full_dt, bq_type, date_fmt

    # numeric ONLY IF 100% numeric after normalization
    num, num_ratio = try_numeric(s)
    if num_ratio == 1.0:
        nonnull = num.dropna()
        if len(nonnull) > 0 and np.all(np.modf(nonnull.values)[0] == 0):
            return num.astype("Int64"), "INT64", None
        return num.astype(float), "FLOAT64", None

    # default STRING
    return s.astype(str).str.strip(), "STRING", None

def bq_schema_from_df(df: pd.DataFrame, date_fmt_map: dict) -> list:
    schema = []
    for col in df.columns:
        if col in date_fmt_map and date_fmt_map[col] == "%Y-%m-%d":
            bq = "DATE"
        else:
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                bq = "INT64"
            elif pd.api.types.is_float_dtype(dtype):
                bq = "FLOAT64"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                bq = "TIMESTAMP"
            elif dtype.name == "boolean":
                bq = "BOOL"
            else:
                bq = "STRING"
        schema.append({"name": col, "type": bq, "mode": "NULLABLE"})
    return schema

def format_dates_for_csv(df: pd.DataFrame, date_fmt_map: dict) -> pd.DataFrame:
    out = df.copy()
    for col, fmt in date_fmt_map.items():
        if fmt == "%Y-%m-%d":
            out[col] = out[col].dt.strftime("%Y-%m-%d")
        elif fmt == "%Y-%m-%d %H:%M:%S":
            out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    return out

def reorder_for_bq_autodetect(df: pd.DataFrame, bq_type_map: dict) -> pd.DataFrame:
    """
    Move rows with letters in STRING columns to the top so BigQuery Autodetect
    reliably infers STRING for those columns. Deterministic, no data changes.
    """
    string_cols = [c for c, t in bq_type_map.items() if t == "STRING" and c in df.columns]
    if not string_cols:
        return df
    has_letters = pd.Series(False, index=df.index)
    for c in string_cols:
        s = df[c].astype(str)
        has_letters = has_letters | s.str.contains(r"[A-Za-z]", na=False)
    out = df.copy()
    out["__letters__"] = has_letters.astype(int)
    out = out.sort_values("__letters__", ascending=False).drop(columns="__letters__")
    return out

def write_clean_csv(df: pd.DataFrame, path: Path):
    # Write as-is and tell pandas how to render missing values
    df.to_csv(
        path,
        index=False,
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        doublequote=True,
        lineterminator="\n",
        encoding="utf-8",
        na_rep=""          # <- this renders NaN/NaT/<NA> as empty cells without changing dtypes
    )

def find_unbalanced_quote_lines(path: Path):
    bad = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for i, line in enumerate(f, 1):
            if line.count('"') % 2:
                bad.append(i)
    return bad

def process_sheet(sheet_name: str, df_raw: pd.DataFrame, out_dir: Path):
    # Header cleanup
    df_raw.columns = [simple_header(c) for c in df_raw.columns]

    # Cell cleanup
    df_raw = df_raw.applymap(strip_cell)

    # Inference
    typed = {}
    date_fmt_map = {}
    bq_type_map = {}

    for col in df_raw.columns:
        ser, bq_type, date_fmt = infer_column(df_raw[col], col)
        typed[col] = ser
        bq_type_map[col] = bq_type
        if date_fmt:
            date_fmt_map[col] = date_fmt
        elif bq_type == "TIMESTAMP":
            date_fmt_map[col] = "%Y-%m-%d %H:%M:%S"

    df_clean = pd.DataFrame(typed)
    df_to_write = format_dates_for_csv(df_clean, date_fmt_map)

    # Ensure autodetect sees STRING columns as text early
    df_to_write = reorder_for_bq_autodetect(df_to_write, bq_type_map)

    # Outputs
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", sheet_name).strip("_") or "Sheet"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{safe}.csv"
    schema_path = out_dir / f"{safe}_bq_schema.json"
    summary_path = out_dir / f"{safe}_summary.txt"

    write_clean_csv(df_to_write, csv_path)

    bad_lines = find_unbalanced_quote_lines(csv_path)
    if bad_lines:
        print(f"Warning: {len(bad_lines)} line(s) have unbalanced quotes. Example lines: {bad_lines[:5]}")

    schema = bq_schema_from_df(df_clean, date_fmt_map)
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)

    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Sheet: {sheet_name}\n")
        for col in df_clean.columns:
            f.write(f"- {col}: {bq_type_map[col]}\n")

    print(f"OK: {csv_path.name}, {schema_path.name}, {summary_path.name}")

def process_xlsx(xlsx_path: Path, out_dir: Path):
    sheets = pd.read_excel(
        xlsx_path,
        sheet_name=None,
        dtype=str,
        keep_default_na=False,
        engine="openpyxl"
    )
    for name, df in sheets.items():
        process_sheet(name, df, out_dir)

def process_csv(csv_path: Path, out_dir: Path):
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, engine="python", on_bad_lines="skip")
    process_sheet(csv_path.stem, df, out_dir)

def main():
    in_path = Path(INPUT_FILE)
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    suf = in_path.suffix.lower()
    if suf in {".xlsx", ".xlsm", ".xls"}:
        process_xlsx(in_path, out_dir)
    elif suf == ".csv":
        process_csv(in_path, out_dir)
    else:
        raise ValueError("Unsupported file type. Provide .xlsx or .csv")

if __name__ == "__main__":
    main()