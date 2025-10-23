import csv
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional, List

import numpy as np
import pandas as pd
import argparse

# ========= CONFIG (edit these two) =========
INPUT_FILE = "dataset.xlsx"
OUTPUT_DIR = "clean_output"
# ==========================================

# general settings
DAYFIRST_HINT = True            # True if dates are usually DD/MM/YYYY
THRESH_NUMERIC = 0.88           # share of parsable rows to accept numeric
THRESH_DATE = 0.65              # share of parsable rows to accept date
MAX_ROWS_SAMPLE = 20000         # speed cap per column for inference
DECIMAL_CHAR = "."              # set "," if decimals use comma
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

# ----------------- logging helpers -----------------

class RunLogger:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.log_file = out_dir / "run_log.txt"
        self._ensure_dir()
        self._write_header()

    def _ensure_dir(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def _write_header(self):
        self.log_file.write_text("", encoding="utf-8")  # reset each run

    def log(self, level: str, msg: str, sheet: Optional[str] = None):
        prefix = f"[{level.upper()}]"
        tag = f" [{sheet}]" if sheet else ""
        line = f"{prefix}{tag} {msg}"
        print(line)
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def info(self, msg: str, sheet: Optional[str] = None): self.log("INFO", msg, sheet)
    def success(self, msg: str, sheet: Optional[str] = None): self.log("SUCCESS", msg, sheet)
    def warning(self, msg: str, sheet: Optional[str] = None): self.log("WARNING", msg, sheet)
    def error(self, msg: str, sheet: Optional[str] = None): self.log("ERROR", msg, sheet)

# ---------------------------------------------------

def norm_header(s: str) -> str:
    s = str(s).replace(NBSP, " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def sanitize_header(raw: str) -> str:
    """
    Single word -> keep original casing, remove spaces.
    Multi word -> snake_case lowercase using only letters and digits.
    """
    s = norm_header(raw)
    if s == "":
        return s
    tokens = re.findall(r"[A-Za-z0-9]+", s)
    if len(tokens) <= 1:
        return s.replace(" ", "")
    return "_".join(t.lower() for t in tokens)

def apply_header_policy(headers: List[str], logger: RunLogger, sheet: str) -> List[str]:
    """
    Apply sanitize_header and ensure uniqueness by appending suffixes if needed.
    Logs every change.
    """
    out = []
    seen = {}
    for original in headers:
        target = sanitize_header(original)
        if target != original:
            logger.info(f"Renamed column '{original}' -> '{target}'", sheet)
        base = target or "col"
        if base in seen:
            seen[base] += 1
            uniq = f"{base}_{seen[base]}"
            logger.warning(f"Duplicate column after rename: '{base}', using '{uniq}'", sheet)
            out.append(uniq)
        else:
            seen[base] = 0
            out.append(base)
    return out

def strip_cell(x):
    if isinstance(x, str):
        # normalize curly quotes to straight quotes to avoid CSV quote errors
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

def try_numeric(s: pd.Series) -> Tuple[pd.Series, float]:
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
    Returns: series_converted, logical_type, date_fmt_or_None, stats_dict
    stats_dict keys:
      valid_ratio: float between 0 and 1 for parsed values
      invalid_count: int
      note: short text about what was inferred
    """
    s = s.apply(strip_cell)

    # boolean
    if detect_boolean(s):
        ser = coerce_boolean(s)
        vr = ser.notna().mean()
        return ser, "BOOL", None, {"valid_ratio": float(vr), "invalid_count": int(len(ser) - ser.notna().sum()), "note": "boolean"}

    # id
    if detect_id_like(s):
        ser = s.astype(str).str.strip()
        return ser, "STRING", None, {"valid_ratio": 1.0, "invalid_count": 0, "note": "id-like string"}

    # date inference
    ss = sample_series(s, MAX_ROWS_SAMPLE)

    excel_dt, excel_ratio = try_parse_excel_serial(ss)
    if excel_ratio >= THRESH_DATE:
        num, _ = try_numeric(s)
        nonnull = num.dropna()
        out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
        idx = nonnull.index
        out.loc[idx] = [EXCEL_EPOCH + timedelta(days=int(v)) for v in nonnull.loc[idx]]
        vr = out.notna().mean()
        return out, "DATE", "%Y-%m-%d", {"valid_ratio": float(vr), "invalid_count": int(len(out) - out.notna().sum()), "note": "excel-serial date"}

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
        if not nonnull.empty:
            times = nonnull.dt.time
            all_midnight = (pd.Series([t == datetime.min.time() for t in times]).all())
            if all_midnight:
                bq_type = "DATE"
                date_fmt = "%Y-%m-%d"
        else:
            bq_type = "DATE"
            date_fmt = "%Y-%m-%d"

        vr = full_dt.notna().mean()
        return full_dt, bq_type, date_fmt, {"valid_ratio": float(vr), "invalid_count": int(len(full_dt) - full_dt.notna().sum()), "note": "date/timestamp"}

    # numeric
    num, num_ratio = try_numeric(s)
    if num_ratio >= THRESH_NUMERIC:
        nonnull = num.dropna()
        if len(nonnull) > 0 and np.all(np.modf(nonnull.values)[0] == 0):
            ser = num.astype("Int64")
            vr = ser.notna().mean()
            return ser, "INT64", None, {"valid_ratio": float(vr), "invalid_count": int(len(ser) - ser.notna().sum()), "note": "integer"}
        ser = num.astype(float)
        vr = ser.notna().mean()
        return ser, "FLOAT64", None, {"valid_ratio": float(vr), "invalid_count": int(len(ser) - ser.notna().sum()), "note": "float"}

    # default string
    ser = s.astype(str).str.strip()
    return ser, "STRING", None, {"valid_ratio": 1.0, "invalid_count": 0, "note": "string"}

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

def write_clean_csv(df: pd.DataFrame, path: Path):
    # Quote every field, escape quotes by doubling
    df.to_csv(
        path,
        index=False,
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        doublequote=True,
        lineterminator="\n",
        encoding="utf-8"
    )

def find_unbalanced_quote_lines(path: Path):
    bad = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for i, line in enumerate(f, 1):
            if line.count('"') % 2:
                bad.append(i)
    return bad

# ----------------- validations & checkpoints -----------------

def validate_headers(headers: pd.Index) -> Dict[str, str]:
    issues = {}
    empties = [i for i, h in enumerate(headers) if str(h).strip() == ""]
    if empties:
        issues["empty_names"] = f"Empty column names at positions: {empties}"
    dups = headers[headers.duplicated()].tolist()
    if dups:
        issues["duplicates"] = f"Duplicate column names: {sorted(set(dups))}"
    too_long = [h for h in headers if len(str(h)) > 128]
    if too_long:
        issues["too_long"] = f"Very long column names (over 128 chars): {too_long}"
    return issues

def write_validation_report(path: Path, lines: List[str]):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

# ----------------- main per sheet processing -----------------

def process_sheet(sheet_name: str, df_raw: pd.DataFrame, out_dir: Path, logger: RunLogger):
    """
    Returns a dict summary for this sheet:
      {
        'sheet': str,
        'rows': int,
        'cols': int,
        'warnings': int,
        'errors': int,
        'csv_path': str,
        'schema_path': str,
        'summary_path': str,
        'validation_path': str,
        'is_clean': bool
      }
    """
    warnings_count = 0
    errors_count = 0
    val_lines: List[str] = []
    logger.info("Sheet read into memory", sheet_name)
    val_lines.append(f"Sheet: {sheet_name}")
    val_lines.append(f"Rows: {len(df_raw)}  Cols: {len(df_raw.columns)}")

    # headers: sanitize then validate
    original_headers = list(df_raw.columns)
    df_raw.columns = apply_header_policy(original_headers, logger, sheet_name)

    header_issues = validate_headers(df_raw.columns)
    if header_issues:
        for _, v in header_issues.items():
            logger.warning(f"Header check: {v}", sheet_name)
            val_lines.append(f"[WARNING] {v}")
            warnings_count += 1
    else:
        logger.success("Header check passed", sheet_name)
        val_lines.append("[SUCCESS] Header check passed")

    # clean cells
    logger.info("Normalizing cell text", sheet_name)
    df_raw = df_raw.applymap(strip_cell)

    # type inference
    typed = {}
    date_fmt_map = {}
    bq_type_map = {}
    type_quality = []

    logger.info("Inferring types per column", sheet_name)
    for col in df_raw.columns:
        ser, bq_type, date_fmt, stats = infer_column(df_raw[col], col)
        typed[col] = ser
        bq_type_map[col] = bq_type
        if date_fmt:
            date_fmt_map[col] = date_fmt
        elif bq_type == "TIMESTAMP":
            date_fmt_map[col] = "%Y-%m-%d %H:%M:%S"

        type_quality.append((col, bq_type, stats["valid_ratio"], stats["invalid_count"], stats["note"]))

    df_clean = pd.DataFrame(typed)
    df_to_write = format_dates_for_csv(df_clean, date_fmt_map)

    # report type quality
    ok_cols = 0
    for col, t, vr, ic, note in type_quality:
        if vr >= 0.99:
            logger.success(f"Type {t} inferred for '{col}' with {vr:.2%} valid", sheet_name)
            val_lines.append(f"[SUCCESS] {col}: {t}  valid={vr:.2%}  note={note}")
            ok_cols += 1
        else:
            logger.warning(f"Type {t} inferred for '{col}' with {vr:.2%} valid ({ic} invalid)", sheet_name)
            val_lines.append(f"[WARNING] {col}: {t}  valid={vr:.2%}  invalid={ic}  note={note}")
            warnings_count += 1

    if ok_cols == len(df_clean.columns):
        logger.success("All columns inferred with high confidence", sheet_name)
    else:
        logger.info(f"{ok_cols}/{len(df_clean.columns)} columns inferred with high confidence", sheet_name)

    # output paths
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", sheet_name).strip("_") or "Sheet"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{safe}.csv"
    schema_path = out_dir / f"{safe}_bq_schema.json"
    summary_path = out_dir / f"{safe}_summary.txt"
    validation_path = out_dir / f"{safe}_validation.txt"

    # write CSV
    try:
        write_clean_csv(df_to_write, csv_path)
        logger.success("CSV written ok", sheet_name)
        val_lines.append("[SUCCESS] CSV written")
    except Exception as e:
        logger.error(f"CSV write failed: {e}", sheet_name)
        val_lines.append(f"[ERROR] CSV write failed: {e}")
        errors_count += 1
        write_validation_report(validation_path, val_lines)
        return {
            "sheet": sheet_name, "rows": len(df_raw), "cols": len(df_raw.columns),
            "warnings": warnings_count, "errors": errors_count,
            "csv_path": str(csv_path), "schema_path": str(schema_path),
            "summary_path": str(summary_path), "validation_path": str(validation_path),
            "is_clean": False
        }

    # post write quote check
    bad_lines = find_unbalanced_quote_lines(csv_path)
    if bad_lines:
        logger.warning(f"Unbalanced quote lines detected: {bad_lines[:5]} (showing up to 5)", sheet_name)
        val_lines.append(f"[WARNING] Unbalanced quote lines: {bad_lines[:20]}")
        warnings_count += 1
    else:
        logger.success("Post write quote balance passed", sheet_name)
        val_lines.append("[SUCCESS] Post write quote balance passed")

    # build schema and summary
    schema = bq_schema_from_df(df_clean, date_fmt_map)
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Sheet: {sheet_name}\n")
        for col in df_clean.columns:
            f.write(f"- {col}: {bq_type_map[col]}\n")

    logger.success("Summary and schema written", sheet_name)
    val_lines.append("[SUCCESS] Summary and schema written")

    # save validation report
    write_validation_report(validation_path, val_lines)
    logger.success("Validation report saved", sheet_name)
    logger.info("Completed sheet", sheet_name)

    is_clean = (warnings_count == 0 and errors_count == 0 and len(bad_lines) == 0)
    return {
        "sheet": sheet_name, "rows": len(df_raw), "cols": len(df_raw.columns),
        "warnings": warnings_count, "errors": errors_count,
        "csv_path": str(csv_path), "schema_path": str(schema_path),
        "summary_path": str(summary_path), "validation_path": str(validation_path),
        "is_clean": bool(is_clean)
    }

def process_xlsx(xlsx_path: Path, out_dir: Path, logger: RunLogger) -> List[dict]:
    logger.info(f"Reading workbook: {xlsx_path.name}")
    try:
        sheets = pd.read_excel(
            xlsx_path,
            sheet_name=None,
            dtype=str,
            keep_default_na=False,
            engine="openpyxl"
        )
    except Exception as e:
        logger.error(f"Failed to read workbook: {e}")
        return []
    logger.success(f"Workbook read ok. Sheets: {list(sheets.keys())}")

    summaries = []
    for name, df in sheets.items():
        summaries.append(process_sheet(name, df, out_dir, logger))
    return summaries

def process_csv(csv_path: Path, out_dir: Path, logger: RunLogger) -> List[dict]:
    logger.info(f"Reading CSV: {csv_path.name}")
    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, engine="python", on_bad_lines="skip")
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return []
    logger.success(f"CSV read ok. Rows: {len(df)} Cols: {len(df.columns)}")
    return [process_sheet(csv_path.stem, df, out_dir, logger)]

def main():
    parser = argparse.ArgumentParser(description="Clean and validate Excel or CSV into BigQuery-ready outputs.")
    parser.add_argument("--input", help="Path to .xlsx/.xls/.csv", default=INPUT_FILE)
    parser.add_argument("--out", help="Output folder", default=OUTPUT_DIR)
    args = parser.parse_args()

    in_path = Path(args.input)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    logger = RunLogger(out_dir)

    # Step 0: input exists
    if not in_path.exists():
        logger.error(f"Input file not found at: {in_path.resolve()}")
        return
    logger.success(f"Input file found: {in_path.name}")

    # Step 1..N: sequential, no parallelism
    summaries: List[dict] = []
    suf = in_path.suffix.lower()
    if suf in {".xlsx", ".xlsm", ".xls"}:
        summaries = process_xlsx(in_path, out_dir, logger)
    elif suf == ".csv":
        summaries = process_csv(in_path, out_dir, logger)
    else:
        logger.error("Unsupported file type. Provide .xlsx or .csv")
        return

    # Final overall feedback
    total_errors = sum(s["errors"] for s in summaries if s)
    total_warnings = sum(s["warnings"] for s in summaries if s)
    sheets_clean = all(s.get("is_clean", False) for s in summaries if s)

    logger.info("Run completed. See run_log.txt and per sheet *_validation.txt for details.")
    if suf == ".csv" and sheets_clean and total_errors == 0 and total_warnings == 0:
        ok_msg = "CSV is correct. No issues detected and output matches expected format."
        logger.success(ok_msg)
        (out_dir / "status.txt").write_text(ok_msg + "\n", encoding="utf-8")
    elif total_errors == 0:
        logger.success("Completed with no errors")
        if total_warnings > 0:
            logger.warning(f"Completed with {total_warnings} warnings")
    else:
        logger.error(f"Completed with {total_errors} errors and {total_warnings} warnings")

if __name__ == "__main__":
    main()