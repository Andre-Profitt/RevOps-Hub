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
from pathlib import Path
from typing import List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


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

    def validate_all(self) -> bool:
        """Validate all configuration files. Returns True if all valid."""
        print("=" * 60)
        print("Configuration Validation")
        print("=" * 60)

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

        # Validate tier (lowercase to match PLAN_DEFINITIONS)
        valid_tiers = ["starter", "growth", "enterprise"]
        tier_value = config.get("tier", "").lower() if config.get("tier") else None
        if tier_value and tier_value not in valid_tiers:
            self.errors.append(ValidationError(
                file=str(path),
                path="tier",
                message=f"Invalid tier '{config['tier']}'. Must be one of: {valid_tiers}"
            ))

        # Validate limits (if present)
        if "limits" in config:
            self._validate_limits(path, config["limits"])

        # Validate features (if present)
        if "features" in config:
            self._validate_features(path, config["features"])

    def _validate_limits(self, path: Path, limits: dict):
        """Validate limit configuration."""
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
                if feature not in valid_features:
                    self.errors.append(ValidationError(
                        file=str(path),
                        path="features",
                        message=f"Unknown feature: {feature}",
                        severity="warning"
                    ))

    def _validate_tier_configs(self):
        """Validate tier definition files."""
        tier_dir = self.config_dir / "tiers"
        if not tier_dir.exists():
            return

        print(f"\nValidating tier configs in {tier_dir}...")

        for config_file in tier_dir.glob("*.yaml"):
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
                if not YAML_AVAILABLE:
                    raise ImportError(
                        "PyYAML is required to parse YAML files. "
                        "Install with: pip install pyyaml"
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

    # Check for yaml dependency if yaml files exist
    yaml_files = list(config_dir.rglob("*.yaml")) + list(config_dir.rglob("*.yml"))
    if yaml_files and not YAML_AVAILABLE:
        print("ERROR: PyYAML is required to validate YAML configuration files.")
        print("Install with: pip install pyyaml")
        print(f"\nFound {len(yaml_files)} YAML file(s) that cannot be validated.")
        sys.exit(1)

    validator = ConfigValidator(config_dir)
    success = validator.validate_all()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
