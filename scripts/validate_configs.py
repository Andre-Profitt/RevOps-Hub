#!/usr/bin/env python3
"""
Configuration Validation Script
===============================
Validates all configuration files against their schemas.

Usage:
    python scripts/validate_configs.py

Exit codes:
    0 - All configs valid
    1 - Validation errors found
"""

import sys
import json
import importlib.util
from pathlib import Path
from typing import List, Tuple, Optional, Dict
from dataclasses import dataclass
from datetime import datetime


yaml_spec = importlib.util.find_spec("yaml")
if yaml_spec:
    import yaml  # type: ignore
else:
    yaml = None


@dataclass
class ValidationError:
    """Represents a config validation error."""
    file: str
    path: str
    message: str
    severity: str = "error"


class ConfigValidator:
    """Validates configuration files against schemas."""

    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self.errors: List[ValidationError] = []
        self.valid_tiers: List[str] = []
        self._seen_tier_files: Dict[str, str] = {}
        self._seen_customer_ids: Dict[str, str] = {}

    def validate_all(self) -> bool:
        """Validate all configuration files. Returns True if all valid."""
        print("=" * 60)
        print("Configuration Validation")
        print("=" * 60)

        # Load tier names once so customer validation can use canonical values
        self.valid_tiers = self._load_valid_tiers()

        # Validate customer configs
        self._validate_customer_configs()

        # Validate tier configs
        self._validate_tier_configs()

        # Validate schema definitions
        self._validate_schema_definitions()

        # Report results
        return self._report_results()

    def _validate_customer_configs(self):
        """Validate customer configuration files."""
        customer_dir = self.config_dir / "customers"
        if not customer_dir.exists():
            return

        print(f"\nValidating customer configs in {customer_dir}...")

        for config_file in customer_dir.glob("*.yaml"):
            self._validate_customer_config(config_file)

        for config_file in customer_dir.glob("*.json"):
            self._validate_customer_config(config_file)

    def _validate_customer_config(self, path: Path):
        """Validate a single customer config file."""
        print(f"  Checking {path.name}...")

        try:
            config = self._load_config(path)
        except Exception as e:
            self.errors.append(ValidationError(
                file=str(path),
                path="",
                message=f"Failed to parse: {e}"
            ))
            return

        # Required fields
        required = ["customer_id", "customer_name", "tier"]
        for field in required:
            if field not in config:
                self.errors.append(ValidationError(
                    file=str(path),
                    path=field,
                    message=f"Missing required field: {field}"
                ))

        # Validate customer identity fields
        customer_id = config.get("customer_id")
        if customer_id is None:
            customer_id = ""
        if not isinstance(customer_id, str):
            self.errors.append(ValidationError(
                file=str(path),
                path="customer_id",
                message="customer_id must be a string"
            ))
            customer_id = ""
        elif not customer_id.strip():
            self.errors.append(ValidationError(
                file=str(path),
                path="customer_id",
                message="customer_id must not be empty"
            ))

        customer_name = config.get("customer_name")
        if customer_name is None:
            customer_name = ""
        if not isinstance(customer_name, str):
            self.errors.append(ValidationError(
                file=str(path),
                path="customer_name",
                message="customer_name must be a string"
            ))
        elif not customer_name.strip():
            self.errors.append(ValidationError(
                file=str(path),
                path="customer_name",
                message="customer_name must not be empty"
            ))

        # Ensure customer IDs are unique across files
        if isinstance(customer_id, str):
            if customer_id in self._seen_customer_ids:
                self.errors.append(ValidationError(
                    file=str(path),
                    path="customer_id",
                    message=(
                        f"Duplicate customer_id '{customer_id}'. First seen in {self._seen_customer_ids[customer_id]}"
                    )
                ))
            else:
                self._seen_customer_ids[customer_id] = path.name

        # Validate tier
        tier_value = config.get("tier")
        normalized_tiers = {tier.lower(): tier for tier in self.valid_tiers} if self.valid_tiers else {}
        if tier_value:
            if isinstance(tier_value, str):
                if normalized_tiers and tier_value.lower() not in normalized_tiers:
                    self.errors.append(ValidationError(
                        file=str(path),
                        path="tier",
                        message=f"Invalid tier '{tier_value}'. Must be one of: {self.valid_tiers}"
                    ))
            else:
                self.errors.append(ValidationError(
                    file=str(path),
                    path="tier",
                    message="Tier must be a string value"
                ))

        # Validate limits (if present)
        if "limits" in config:
            if isinstance(config["limits"], dict):
                self._validate_limits(path, config["limits"])
            else:
                self.errors.append(ValidationError(
                    file=str(path),
                    path="limits",
                    message="Limits must be provided as an object"
                ))

        # Validate features (if present)
        if "features" in config:
            self._validate_features(path, config["features"])

        # Validate contact emails if present
        contacts = config.get("contacts", {}) if isinstance(config.get("contacts"), dict) else {}
        for contact_key, contact in contacts.items():
            if isinstance(contact, dict) and "email" in contact:
                email = contact.get("email")
                if not isinstance(email, str) or "@" not in email:
                    self.errors.append(ValidationError(
                        file=str(path),
                        path=f"contacts.{contact_key}.email",
                        message="Contact email must be a valid string containing '@'",
                        severity="warning",
                    ))

    def _validate_limits(self, path: Path, limits: dict):
        """Validate limit configuration."""
        if not isinstance(limits, dict):
            self.errors.append(ValidationError(
                file=str(path),
                path="limits",
                message="Limits must be provided as an object"
            ))
            return

        numeric_limits = ["users", "opportunities", "api_calls", "compute_hours"]

        for limit in numeric_limits:
            if limit in limits and limits[limit] is not None:
                if not isinstance(limits[limit], (int, float)) or limits[limit] < 0:
                    self.errors.append(ValidationError(
                        file=str(path),
                        path=f"limits.{limit}",
                        message=f"Limit '{limit}' must be a non-negative number"
                    ))

    def _validate_features(self, path: Path, features: dict):
        """Validate feature configuration."""
        valid_features = [
            "core", "forecasting", "ai_predictions", "custom_reports",
            "api_access", "sso", "data_export", "advanced_analytics"
        ]

        if isinstance(features, list):
            for feature in features:
                if not isinstance(feature, str):
                    self.errors.append(ValidationError(
                        file=str(path),
                        path="features",
                        message="Features must be strings"
                    ))
                    continue

                if feature not in valid_features:
                    self.errors.append(ValidationError(
                        file=str(path),
                        path="features",
                        message=f"Unknown feature: {feature}",
                        severity="warning"
                    ))
        else:
            self.errors.append(ValidationError(
                file=str(path),
                path="features",
                message="Features must be provided as a list"
            ))

    def _validate_tier_configs(self):
        """Validate tier definition files."""
        tier_dir = self.config_dir / "tiers"
        if not tier_dir.exists():
            return

        print(f"\nValidating tier configs in {tier_dir}...")

        tier_files = list(tier_dir.glob("*.json"))
        tier_files.extend(tier_dir.glob("*.yaml"))
        tier_files.extend(tier_dir.glob("*.yml"))

        for config_file in sorted(tier_files):
            self._validate_tier_config(config_file)

    def _validate_tier_config(self, path: Path):
        """Validate a single tier config file."""
        print(f"  Checking {path.name}...")

        try:
            config = self._load_config(path)
        except Exception as e:
            self.errors.append(ValidationError(
                file=str(path),
                path="",
                message=f"Failed to parse: {e}"
            ))
            return

        # Required fields for tier config
        required = ["tier_name", "price_monthly", "limits", "features"]
        for field in required:
            if field not in config:
                self.errors.append(ValidationError(
                    file=str(path),
                    path=field,
                    message=f"Missing required field: {field}"
                ))

        # Enforce unique tier names
        tier_name = config.get("tier_name") or config.get("name")
        if isinstance(tier_name, str):
            normalized_name = tier_name.lower()
            if normalized_name in self._seen_tier_files:
                self.errors.append(ValidationError(
                    file=str(path),
                    path="tier_name",
                    message=(
                        f"Duplicate tier name '{tier_name}'. First defined in {self._seen_tier_files[normalized_name]}"
                    )
                ))
            else:
                self._seen_tier_files[normalized_name] = path.name

        # Validate pricing fields
        for price_field in ["price_monthly", "price_annual"]:
            if price_field in config:
                price_value = config.get(price_field)
                if price_value is not None and (not isinstance(price_value, (int, float)) or price_value < 0):
                    self.errors.append(ValidationError(
                        file=str(path),
                        path=price_field,
                        message=f"{price_field} must be a non-negative number or null"
                    ))

        # Validate limits and features
        limits = config.get("limits")
        if isinstance(limits, dict):
            self._validate_limits(path, limits)
        else:
            self.errors.append(ValidationError(
                file=str(path),
                path="limits",
                message="Limits must be provided as an object"
            ))

        if "features" in config:
            self._validate_features(path, config.get("features"))

    def _load_valid_tiers(self) -> List[str]:
        """Load valid tier names from tier configs (case-insensitive)."""
        tier_dir = self.config_dir / "tiers"
        if not tier_dir.exists():
            return []

        valid_tiers: List[str] = []
        seen: Dict[str, str] = {}
        for config_file in tier_dir.glob("*.*"):
            if config_file.suffix not in {".json", ".yaml", ".yml"}:
                continue

            try:
                config = self._load_config(config_file)
            except Exception as e:
                self.errors.append(ValidationError(
                    file=str(config_file),
                    path="tier_name",
                    message=f"Failed to read tier definition: {e}",
                    severity="warning"
                ))
                continue

            tier_name = config.get("tier_name") or config.get("name")
            candidate_name = str(tier_name) if tier_name else config_file.stem.title()
            normalized_name = candidate_name.lower()
            if normalized_name in seen:
                continue

            seen[normalized_name] = candidate_name
            valid_tiers.append(candidate_name)

        return valid_tiers

    def _validate_schema_definitions(self):
        """Validate schema.py definitions."""
        schema_file = self.config_dir / "schema.py"
        if not schema_file.exists():
            print(f"\nNo schema.py found in {self.config_dir}")
            return

        print(f"\nValidating schema definitions...")

        # Try to import and validate schema
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("schema", schema_file)
            schema_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(schema_module)

            # Check for required schema classes
            required_schemas = ["CustomerConfig", "TierConfig", "FeatureFlags"]
            for schema_name in required_schemas:
                if not hasattr(schema_module, schema_name):
                    self.errors.append(ValidationError(
                        file=str(schema_file),
                        path=schema_name,
                        message=f"Missing schema class: {schema_name}",
                        severity="warning"
                    ))
                else:
                    print(f"  Found schema: {schema_name}")

        except Exception as e:
            self.errors.append(ValidationError(
                file=str(schema_file),
                path="",
                message=f"Failed to load schema: {e}"
            ))

    def _load_config(self, path: Path) -> dict:
        """Load a config file (YAML or JSON)."""
        with open(path) as f:
            if path.suffix in [".yaml", ".yml"]:
                if yaml is None:
                    raise RuntimeError(
                        "PyYAML is required to load YAML config files. Install pyyaml or convert the file to JSON."
                    )
                return yaml.safe_load(f)
            else:
                return json.load(f)

    def _report_results(self) -> bool:
        """Report validation results. Returns True if no errors."""
        print("\n" + "=" * 60)

        errors = [e for e in self.errors if e.severity == "error"]
        warnings = [e for e in self.errors if e.severity == "warning"]

        if errors:
            print(f"FAILED: {len(errors)} error(s) found")
            print("-" * 60)
            for error in errors:
                print(f"ERROR: {error.file}")
                print(f"  Path: {error.path or '(root)'}")
                print(f"  {error.message}")
                print()

        if warnings:
            print(f"WARNINGS: {len(warnings)} warning(s) found")
            print("-" * 60)
            for warning in warnings:
                print(f"WARN: {warning.file}")
                print(f"  Path: {warning.path or '(root)'}")
                print(f"  {warning.message}")
                print()

        if not errors and not warnings:
            print("SUCCESS: All configurations valid!")

        print("=" * 60)
        return len(errors) == 0


def main():
    """Main entry point."""
    # Find config directory
    project_root = Path(__file__).parent.parent
    config_dir = project_root / "configs"

    if not config_dir.exists():
        print(f"Config directory not found: {config_dir}")
        print("Creating empty config directory structure...")
        (config_dir / "customers").mkdir(parents=True, exist_ok=True)
        (config_dir / "tiers").mkdir(parents=True, exist_ok=True)

    validator = ConfigValidator(config_dir)
    success = validator.validate_all()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
