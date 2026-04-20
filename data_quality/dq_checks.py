"""
data_quality/dq_checks.py
────────────────────────────────────────────────────────────────────────────────
Custom PySpark-based Data Quality framework.
Runs rule-based checks on Silver DataFrames and produces a DQ report.
────────────────────────────────────────────────────────────────────────────────
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.helpers import load_config, get_logger, utc_now_iso


# ──────────────────────────────────────────────
# DQ Result Dataclass
# ──────────────────────────────────────────────

@dataclass
class DQCheckResult:
    check_name:     str
    column:         str
    check_type:     str
    passed:         bool
    total_records:  int
    failed_records: int
    failure_pct:    float
    threshold:      Optional[float] = None
    message:        str = ""

@dataclass
class DQReport:
    run_id:       str
    run_at:       str
    dataset:      str
    total_checks: int
    passed:       int
    failed:       int
    overall_pass: bool
    results:      List[DQCheckResult] = field(default_factory=list)

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d, indent=2)

    def print_summary(self):
        status = "✅ PASSED" if self.overall_pass else "❌ FAILED"
        print(f"\n{'='*60}")
        print(f"DQ Report  {status}")
        print(f"Run ID   : {self.run_id}")
        print(f"Dataset  : {self.dataset}")
        print(f"Checks   : {self.total_checks} total | {self.passed} passed | {self.failed} failed")
        print(f"{'='*60}")
        for r in self.results:
            icon = "✅" if r.passed else "❌"
            print(f"  {icon} [{r.check_type}] {r.column}: {r.message}")
        print()


# ──────────────────────────────────────────────
# Individual Check Functions
# ──────────────────────────────────────────────

def check_not_null(df: DataFrame, column: str, max_null_pct: float = 5.0) -> DQCheckResult:
    """Ensure null percentage in a column is below threshold."""
    total = df.count()
    null_count = df.filter(F.col(column).isNull()).count()
    null_pct = (null_count / total * 100) if total > 0 else 0.0
    passed = null_pct <= max_null_pct

    return DQCheckResult(
        check_name=f"not_null_{column}",
        column=column,
        check_type="NOT_NULL",
        passed=passed,
        total_records=total,
        failed_records=null_count,
        failure_pct=round(null_pct, 4),
        threshold=max_null_pct,
        message=f"{null_count}/{total} nulls ({null_pct:.2f}%) — threshold {max_null_pct}%"
    )


def check_greater_than(df: DataFrame, column: str, value: float) -> DQCheckResult:
    """Ensure all values in a column are strictly greater than a threshold."""
    total = df.count()
    failed_count = df.filter(
        F.col(column).isNull() | (F.col(column) <= value)
    ).count()
    fail_pct = (failed_count / total * 100) if total > 0 else 0.0
    passed = failed_count == 0

    return DQCheckResult(
        check_name=f"gt_{column}_{value}",
        column=column,
        check_type="GREATER_THAN",
        passed=passed,
        total_records=total,
        failed_records=failed_count,
        failure_pct=round(fail_pct, 4),
        threshold=value,
        message=f"{failed_count} records where {column} <= {value}"
    )


def check_no_duplicates(df: DataFrame, key_columns: list, max_dup_pct: float = 1.0) -> DQCheckResult:
    """Check for duplicate rows based on key columns."""
    total = df.count()
    distinct = df.dropDuplicates(key_columns).count()
    dup_count = total - distinct
    dup_pct = (dup_count / total * 100) if total > 0 else 0.0
    passed = dup_pct <= max_dup_pct

    col_str = ", ".join(key_columns)
    return DQCheckResult(
        check_name=f"no_duplicates_{'_'.join(key_columns)}",
        column=col_str,
        check_type="NO_DUPLICATES",
        passed=passed,
        total_records=total,
        failed_records=dup_count,
        failure_pct=round(dup_pct, 4),
        threshold=max_dup_pct,
        message=f"{dup_count} duplicate rows on [{col_str}] ({dup_pct:.2f}%)"
    )


def check_value_in_range(df: DataFrame, column: str, min_val: float, max_val: float) -> DQCheckResult:
    """Ensure values fall within [min_val, max_val]."""
    total = df.count()
    failed_count = df.filter(
        F.col(column).isNull() |
        (F.col(column) < min_val) |
        (F.col(column) > max_val)
    ).count()
    fail_pct = (failed_count / total * 100) if total > 0 else 0.0
    passed = failed_count == 0

    return DQCheckResult(
        check_name=f"range_{column}",
        column=column,
        check_type="RANGE",
        passed=passed,
        total_records=total,
        failed_records=failed_count,
        failure_pct=round(fail_pct, 4),
        threshold=None,
        message=f"{failed_count} records outside [{min_val}, {max_val}] for {column}"
    )


def check_unique_count(df: DataFrame, column: str, expected_min: int) -> DQCheckResult:
    """Ensure a column has at least `expected_min` distinct values."""
    total = df.count()
    distinct_count = df.select(column).distinct().count()
    passed = distinct_count >= expected_min

    return DQCheckResult(
        check_name=f"unique_count_{column}",
        column=column,
        check_type="UNIQUE_COUNT",
        passed=passed,
        total_records=total,
        failed_records=0 if passed else 1,
        failure_pct=0.0 if passed else 100.0,
        threshold=float(expected_min),
        message=f"{distinct_count} distinct values in {column} (expected >= {expected_min})"
    )


def check_freshness(df: DataFrame, timestamp_col: str, max_lag_minutes: int = 90) -> DQCheckResult:
    """Check that the most recent record is within max_lag_minutes of now."""
    total = df.count()
    latest_row = df.select(F.max(F.col(timestamp_col)).alias("latest")).collect()
    latest_ts = latest_row[0]["latest"]

    if latest_ts is None:
        return DQCheckResult(
            check_name=f"freshness_{timestamp_col}",
            column=timestamp_col,
            check_type="FRESHNESS",
            passed=False,
            total_records=total,
            failed_records=total,
            failure_pct=100.0,
            threshold=float(max_lag_minutes),
            message="No timestamp data found."
        )

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    lag_minutes = (now - latest_ts).total_seconds() / 60
    passed = lag_minutes <= max_lag_minutes

    return DQCheckResult(
        check_name=f"freshness_{timestamp_col}",
        column=timestamp_col,
        check_type="FRESHNESS",
        passed=passed,
        total_records=total,
        failed_records=0 if passed else total,
        failure_pct=0.0 if passed else 100.0,
        threshold=float(max_lag_minutes),
        message=f"Latest record is {lag_minutes:.1f} min old (threshold: {max_lag_minutes} min)"
    )


# ──────────────────────────────────────────────
# DQ Runner
# ──────────────────────────────────────────────

class DataQualityRunner:
    """Runs all DQ checks on a Silver DataFrame and produces a DQReport."""

    def __init__(self, config: dict):
        self.config = config
        self.logger = get_logger("DataQualityRunner", config)
        self.dq_cfg = config.get("data_quality", {})
        self.max_null_pct = self.dq_cfg.get("max_null_pct_allowed", 5.0)
        self.max_dup_pct = self.dq_cfg.get("max_duplicate_pct_allowed", 1.0)

    def run(self, df: DataFrame, dataset_name: str = "silver_layer") -> DQReport:
        import uuid
        results = []

        self.logger.info(f"Running DQ checks on dataset: {dataset_name}")

        # ── Core not-null checks ──
        for col in ["coin_id", "symbol", "name", "current_price", "market_cap", "total_volume"]:
            results.append(check_not_null(df, col, self.max_null_pct))

        # ── Value range checks ──
        results.append(check_greater_than(df, "current_price", 0))
        results.append(check_greater_than(df, "total_volume", 0))
        results.append(check_greater_than(df, "market_cap", 0))

        # ── Pct change sanity (-100% to +10000%) ──
        results.append(check_value_in_range(df, "price_change_pct_24h", -100.0, 10000.0))

        # ── Deduplication check ──
        results.append(check_no_duplicates(df, ["coin_id", "batch_ingested_at"], self.max_dup_pct))

        # ── Variety check: ensure at least 50 distinct coins ──
        results.append(check_unique_count(df, "coin_id", expected_min=50))

        # ── Freshness check ──
        results.append(check_freshness(df, "batch_ingested_at", max_lag_minutes=90))

        passed  = sum(1 for r in results if r.passed)
        failed  = sum(1 for r in results if not r.passed)
        overall = failed == 0

        report = DQReport(
            run_id=str(uuid.uuid4()),
            run_at=utc_now_iso(),
            dataset=dataset_name,
            total_checks=len(results),
            passed=passed,
            failed=failed,
            overall_pass=overall,
            results=results
        )

        report.print_summary()

        # Save JSON report
        report_path = f"/tmp/crypto_pipeline/dq_reports/dq_{dataset_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        Path(report_path).parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w") as f:
            f.write(report.to_json())
        self.logger.info(f"DQ report saved → {report_path}")

        if not overall:
            self.logger.warning(f"DQ FAILED: {failed} check(s) did not pass.")
        else:
            self.logger.info("DQ PASSED: All checks passed ✅")

        return report


# ──────────────────────────────────────────────
# Entry Point (standalone)
# ──────────────────────────────────────────────

def run_dq_on_silver(config: dict = None):
    from pyspark.sql import SparkSession
    config = config or load_config()
    spark = SparkSession.builder.appName("CryptoPipeline_DQ").getOrCreate()

    silver_path = config["storage"]["silver_path"]
    df = spark.read.parquet(silver_path)

    runner = DataQualityRunner(config)
    report = runner.run(df, dataset_name="silver_layer")
    return report


if __name__ == "__main__":
    run_dq_on_silver()
