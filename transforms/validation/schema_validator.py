# /transforms/validation/schema_validator.py
"""
SCHEMA VALIDATOR
================
Validates DataFrames against defined data contracts.
Supports required fields, type checking, enum validation, and business rules.

Usage:
    from transforms.validation.schema_validator import validate, ContractViolation

    result = validate(df, "opportunity")
    if not result.is_valid:
        # Handle violations
        for error in result.errors:
            print(f"ERROR: {error}")
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType, DateType, TimestampType


# =============================================================================
# CONTRACT DEFINITIONS
# =============================================================================

@dataclass
class FieldContract:
    """Contract for a single field."""
    name: str
    dtype: Any  # PySpark DataType
    required: bool = False
    unique: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    valid_values: Optional[Set[str]] = None
    max_length: Optional[int] = None
    regex_pattern: Optional[str] = None


@dataclass
class EntityContract:
    """Contract for an entity (table/dataset)."""
    name: str
    fields: List[FieldContract]
    business_rules: List[str] = field(default_factory=list)


# =============================================================================
# OPPORTUNITY CONTRACT
# =============================================================================

OPPORTUNITY_CONTRACT = EntityContract(
    name="opportunity",
    fields=[
        FieldContract("opportunity_id", StringType(), required=True, unique=True),
        FieldContract("opportunity_name", StringType(), required=True, max_length=255),
        FieldContract("account_id", StringType(), required=True),
        FieldContract("account_name", StringType(), required=False),
        FieldContract("owner_id", StringType(), required=True),
        FieldContract("owner_name", StringType(), required=False),
        FieldContract("amount", DoubleType(), required=True, min_value=0),
        FieldContract("stage_name", StringType(), required=True,
                     valid_values={"Prospecting", "Discovery", "Solution Design",
                                   "Proposal", "Negotiation", "Verbal Commit",
                                   "Closed Won", "Closed Lost"}),
        FieldContract("probability", DoubleType(), required=False, min_value=0, max_value=1),
        FieldContract("close_date", DateType(), required=True),
        FieldContract("created_date", DateType(), required=True),
        FieldContract("forecast_category", StringType(), required=False,
                     valid_values={"Pipeline", "Best Case", "Commit", "Closed", "Omitted"}),
        FieldContract("is_closed", BooleanType(), required=True),
        FieldContract("is_won", BooleanType(), required=True),
        FieldContract("last_modified_date", TimestampType(), required=True),
    ],
    business_rules=[
        "is_won implies stage_name = 'Closed Won'",
        "is_closed implies stage_name in ('Closed Won', 'Closed Lost')",
    ]
)

# =============================================================================
# ACCOUNT CONTRACT
# =============================================================================

ACCOUNT_CONTRACT = EntityContract(
    name="account",
    fields=[
        FieldContract("account_id", StringType(), required=True, unique=True),
        FieldContract("account_name", StringType(), required=True, max_length=255),
        FieldContract("industry", StringType(), required=False),
        FieldContract("segment", StringType(), required=False,
                     valid_values={"Enterprise", "Mid-Market", "SMB", "Velocity"}),
        FieldContract("annual_revenue", DoubleType(), required=False, min_value=0),
        FieldContract("employee_count", IntegerType(), required=False, min_value=0),
        FieldContract("billing_country", StringType(), required=False),
        FieldContract("owner_id", StringType(), required=False),
        FieldContract("created_date", DateType(), required=True),
        FieldContract("last_modified_date", TimestampType(), required=True),
    ]
)

# =============================================================================
# CONTACT CONTRACT
# =============================================================================

CONTACT_CONTRACT = EntityContract(
    name="contact",
    fields=[
        FieldContract("contact_id", StringType(), required=True, unique=True),
        FieldContract("first_name", StringType(), required=False, max_length=100),
        FieldContract("last_name", StringType(), required=True, max_length=100),
        FieldContract("email", StringType(), required=False,
                     regex_pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
        FieldContract("title", StringType(), required=False, max_length=255),
        FieldContract("account_id", StringType(), required=True),
        FieldContract("created_date", DateType(), required=True),
    ]
)

# =============================================================================
# ACTIVITY CONTRACT
# =============================================================================

ACTIVITY_CONTRACT = EntityContract(
    name="activity",
    fields=[
        FieldContract("activity_id", StringType(), required=True, unique=True),
        FieldContract("activity_type", StringType(), required=True,
                     valid_values={"Call", "Email", "Meeting", "Demo", "Task", "Note"}),
        FieldContract("activity_date", DateType(), required=True),
        FieldContract("owner_id", StringType(), required=True),
        FieldContract("is_completed", BooleanType(), required=True),
        FieldContract("created_date", TimestampType(), required=True),
    ],
    business_rules=[
        "At least one of account_id, opportunity_id, or contact_id must be populated"
    ]
)

# Contract registry
CONTRACTS: Dict[str, EntityContract] = {
    "opportunity": OPPORTUNITY_CONTRACT,
    "account": ACCOUNT_CONTRACT,
    "contact": CONTACT_CONTRACT,
    "activity": ACTIVITY_CONTRACT,
}


# =============================================================================
# VALIDATION RESULT
# =============================================================================

@dataclass
class ValidationResult:
    """Result of contract validation."""
    is_valid: bool
    contract_name: str
    total_rows: int
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    field_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "contract_name": self.contract_name,
            "total_rows": self.total_rows,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": self.errors[:10],  # First 10 errors
            "warnings": self.warnings[:10],
        }


class ContractViolation(Exception):
    """Raised when data contract validation fails."""
    def __init__(self, result: ValidationResult):
        self.result = result
        super().__init__(f"Contract '{result.contract_name}' violated: {len(result.errors)} errors")


# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def validate(df: DataFrame, contract_name: str, strict: bool = False) -> ValidationResult:
    """
    Validate a DataFrame against a named contract.

    Args:
        df: DataFrame to validate
        contract_name: Name of contract (e.g., "opportunity")
        strict: If True, raise exception on validation failure

    Returns:
        ValidationResult with errors and warnings
    """
    if contract_name not in CONTRACTS:
        raise ValueError(f"Unknown contract: {contract_name}. Available: {list(CONTRACTS.keys())}")

    contract = CONTRACTS[contract_name]
    errors = []
    warnings = []
    field_stats = {}

    total_rows = df.count()

    # Validate each field
    for field_contract in contract.fields:
        field_name = field_contract.name
        stats = {"exists": False, "null_count": 0, "null_pct": 0}

        # Check field exists
        if field_name not in df.columns:
            if field_contract.required:
                errors.append(f"Missing required field: {field_name}")
            continue

        stats["exists"] = True

        # Check nulls for required fields
        null_count = df.filter(F.col(field_name).isNull()).count()
        stats["null_count"] = null_count
        stats["null_pct"] = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0

        if field_contract.required and null_count > 0:
            errors.append(
                f"Required field '{field_name}' has {null_count} null values ({stats['null_pct']}%)"
            )

        # Check uniqueness
        if field_contract.unique:
            distinct_count = df.select(field_name).distinct().count()
            non_null_count = total_rows - null_count
            if distinct_count < non_null_count:
                duplicate_count = non_null_count - distinct_count
                errors.append(f"Field '{field_name}' has {duplicate_count} duplicate values")

        # Check valid values (enum)
        if field_contract.valid_values:
            invalid_count = df.filter(
                ~F.col(field_name).isin(list(field_contract.valid_values)) &
                F.col(field_name).isNotNull()
            ).count()
            if invalid_count > 0:
                # Get sample invalid values
                sample = df.filter(
                    ~F.col(field_name).isin(list(field_contract.valid_values)) &
                    F.col(field_name).isNotNull()
                ).select(field_name).distinct().limit(5).collect()
                sample_values = [str(row[0]) for row in sample]
                warnings.append(
                    f"Field '{field_name}' has {invalid_count} invalid values. "
                    f"Sample: {sample_values}"
                )

        # Check numeric ranges
        if field_contract.min_value is not None:
            below_min = df.filter(
                F.col(field_name) < field_contract.min_value
            ).count()
            if below_min > 0:
                warnings.append(
                    f"Field '{field_name}' has {below_min} values below minimum ({field_contract.min_value})"
                )

        if field_contract.max_value is not None:
            above_max = df.filter(
                F.col(field_name) > field_contract.max_value
            ).count()
            if above_max > 0:
                warnings.append(
                    f"Field '{field_name}' has {above_max} values above maximum ({field_contract.max_value})"
                )

        # Check string length
        if field_contract.max_length is not None and field_contract.dtype == StringType():
            too_long = df.filter(
                F.length(F.col(field_name)) > field_contract.max_length
            ).count()
            if too_long > 0:
                warnings.append(
                    f"Field '{field_name}' has {too_long} values exceeding max length ({field_contract.max_length})"
                )

        field_stats[field_name] = stats

    result = ValidationResult(
        is_valid=len(errors) == 0,
        contract_name=contract_name,
        total_rows=total_rows,
        errors=errors,
        warnings=warnings,
        field_stats=field_stats
    )

    if strict and not result.is_valid:
        raise ContractViolation(result)

    return result


def validate_and_log(df: DataFrame, contract_name: str, ctx) -> DataFrame:
    """
    Validate DataFrame and log results. Returns DataFrame unchanged.

    Use in transforms:
        df = validate_and_log(df, "opportunity", ctx)
    """
    result = validate(df, contract_name)

    if result.errors:
        ctx.logger.error(f"Contract validation failed: {result.errors}")
    if result.warnings:
        ctx.logger.warning(f"Contract validation warnings: {result.warnings}")

    return df


# =============================================================================
# BUSINESS RULE VALIDATION
# =============================================================================

def validate_opportunity_rules(df: DataFrame) -> List[str]:
    """Validate opportunity-specific business rules."""
    errors = []

    # Rule: is_won implies stage_name = 'Closed Won'
    violated = df.filter(
        (F.col("is_won") == True) &
        (F.col("stage_name") != "Closed Won")
    ).count()
    if violated > 0:
        errors.append(f"{violated} records have is_won=true but stage is not 'Closed Won'")

    # Rule: is_closed implies stage_name in ('Closed Won', 'Closed Lost')
    violated = df.filter(
        (F.col("is_closed") == True) &
        (~F.col("stage_name").isin("Closed Won", "Closed Lost"))
    ).count()
    if violated > 0:
        errors.append(f"{violated} records have is_closed=true but stage is not closed")

    return errors


def validate_activity_rules(df: DataFrame) -> List[str]:
    """Validate activity-specific business rules."""
    errors = []

    # Rule: At least one of account_id, opportunity_id, contact_id must be populated
    if all(col in df.columns for col in ["account_id", "opportunity_id", "contact_id"]):
        orphaned = df.filter(
            F.col("account_id").isNull() &
            F.col("opportunity_id").isNull() &
            F.col("contact_id").isNull()
        ).count()
        if orphaned > 0:
            errors.append(f"{orphaned} activities have no linked account, opportunity, or contact")

    return errors
