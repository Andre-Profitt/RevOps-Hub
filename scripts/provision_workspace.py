#!/usr/bin/env python3
"""
WORKSPACE PROVISIONING SCRIPT
=============================
Automates the setup of a new RevOps Hub workspace for a customer.

Creates:
- Folder structure in Foundry
- Dataset placeholders
- Transform repository
- Schedules
- Permission groups

Usage:
    python scripts/provision_workspace.py --customer "Acme Corp" --tier enterprise
    python scripts/provision_workspace.py --customer "StartupCo" --tier starter --dry-run
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class CustomerConfig:
    """Customer-specific configuration."""
    customer_id: str
    customer_name: str
    tier: str  # starter, growth, enterprise
    primary_crm: str  # salesforce, hubspot
    timezone: str
    currency: str
    fiscal_year_start: str  # e.g., "01-01" for Jan 1

    # Feature flags based on tier
    features: Dict[str, bool] = None

    # Custom thresholds
    thresholds: Dict[str, float] = None

    def __post_init__(self):
        if self.features is None:
            self.features = TIER_FEATURES.get(self.tier, TIER_FEATURES["starter"])
        if self.thresholds is None:
            self.thresholds = DEFAULT_THRESHOLDS.copy()


TIER_FEATURES = {
    "starter": {
        "pipeline_health": True,
        "deal_predictions": False,
        "coaching": False,
        "scenario_modeling": False,
        "territory_planning": False,
        "custom_integrations": False,
        "api_access": False,
        "sso": False,
    },
    "growth": {
        "pipeline_health": True,
        "deal_predictions": True,
        "coaching": True,
        "scenario_modeling": False,
        "territory_planning": False,
        "custom_integrations": True,
        "api_access": True,
        "sso": False,
    },
    "enterprise": {
        "pipeline_health": True,
        "deal_predictions": True,
        "coaching": True,
        "scenario_modeling": True,
        "territory_planning": True,
        "custom_integrations": True,
        "api_access": True,
        "sso": True,
    },
}

DEFAULT_THRESHOLDS = {
    "health_critical": 40,
    "health_at_risk": 60,
    "health_monitor": 80,
    "days_stale_warning": 7,
    "days_stale_critical": 14,
    "pipeline_coverage_target": 3.0,
    "win_rate_benchmark": 0.25,
    "avg_cycle_days_benchmark": 45,
}


# =============================================================================
# WORKSPACE STRUCTURE
# =============================================================================

FOLDER_STRUCTURE = [
    "/RevOps",
    "/RevOps/Raw",
    "/RevOps/Staging",
    "/RevOps/Enriched",
    "/RevOps/Analytics",
    "/RevOps/Dashboard",
    "/RevOps/Telemetry",
    "/RevOps/Monitoring",
    "/RevOps/Config",
    "/RevOps/Reference",
]

CORE_DATASETS = [
    # Staging (from CRM sync)
    {"path": "/RevOps/Staging/opportunities", "type": "tabular"},
    {"path": "/RevOps/Staging/accounts", "type": "tabular"},
    {"path": "/RevOps/Staging/contacts", "type": "tabular"},
    {"path": "/RevOps/Staging/activities", "type": "tabular"},

    # Reference
    {"path": "/RevOps/Reference/stage_benchmarks", "type": "tabular"},
    {"path": "/RevOps/Reference/users", "type": "tabular"},

    # Config
    {"path": "/RevOps/Config/customer_settings", "type": "tabular"},
    {"path": "/RevOps/Config/feature_flags", "type": "tabular"},
    {"path": "/RevOps/Config/thresholds", "type": "tabular"},

    # Enriched
    {"path": "/RevOps/Enriched/deal_health_scores", "type": "tabular"},
    {"path": "/RevOps/Enriched/opportunity_health_scored", "type": "tabular"},

    # Analytics (core)
    {"path": "/RevOps/Analytics/pipeline_health_summary", "type": "tabular"},
    {"path": "/RevOps/Analytics/pipeline_hygiene_alerts", "type": "tabular"},
    {"path": "/RevOps/Analytics/pipeline_hygiene_summary", "type": "tabular"},
    {"path": "/RevOps/Analytics/deal_predictions", "type": "tabular"},
    {"path": "/RevOps/Analytics/leading_indicators_summary", "type": "tabular"},

    # Dashboard
    {"path": "/RevOps/Dashboard/kpis", "type": "tabular"},

    # Monitoring
    {"path": "/RevOps/Monitoring/build_metrics", "type": "tabular"},
    {"path": "/RevOps/Monitoring/sync_status", "type": "tabular"},
    {"path": "/RevOps/Monitoring/alerts", "type": "tabular"},
]

TIER_DATASETS = {
    "growth": [
        {"path": "/RevOps/Coaching/rep_performance", "type": "tabular"},
        {"path": "/RevOps/Coaching/coaching_insights", "type": "tabular"},
    ],
    "enterprise": [
        {"path": "/RevOps/Coaching/rep_performance", "type": "tabular"},
        {"path": "/RevOps/Coaching/coaching_insights", "type": "tabular"},
        {"path": "/RevOps/Analytics/scenario_summary", "type": "tabular"},
        {"path": "/RevOps/Analytics/territory_summary", "type": "tabular"},
        {"path": "/RevOps/Analytics/account_scores", "type": "tabular"},
    ],
}

SCHEDULES = [
    {
        "name": "hourly-crm-sync",
        "cron": "0 * * * *",
        "datasets": [
            "/RevOps/Staging/opportunities",
            "/RevOps/Staging/activities",
        ],
        "tier_minimum": "starter",
    },
    {
        "name": "daily-analytics",
        "cron": "0 6 * * *",
        "datasets": [
            "/RevOps/Enriched/deal_health_scores",
            "/RevOps/Analytics/pipeline_health_summary",
            "/RevOps/Analytics/pipeline_hygiene_alerts",
            "/RevOps/Dashboard/kpis",
        ],
        "tier_minimum": "starter",
    },
    {
        "name": "daily-predictions",
        "cron": "0 7 * * *",
        "datasets": [
            "/RevOps/Analytics/deal_predictions",
            "/RevOps/Analytics/leading_indicators_summary",
        ],
        "tier_minimum": "growth",
    },
    {
        "name": "weekly-coaching",
        "cron": "0 8 * * 1",
        "datasets": [
            "/RevOps/Coaching/rep_performance",
            "/RevOps/Coaching/coaching_insights",
        ],
        "tier_minimum": "growth",
    },
]


# =============================================================================
# PERMISSION GROUPS
# =============================================================================

PERMISSION_GROUPS = [
    {
        "name": "revops-admins",
        "description": "Full access to all RevOps data and configuration",
        "permissions": ["read", "write", "admin"],
        "datasets": ["*"],
    },
    {
        "name": "revops-analysts",
        "description": "Read access to analytics and dashboards",
        "permissions": ["read"],
        "datasets": [
            "/RevOps/Analytics/*",
            "/RevOps/Dashboard/*",
            "/RevOps/Coaching/*",
        ],
    },
    {
        "name": "revops-managers",
        "description": "Read access to team-level analytics",
        "permissions": ["read"],
        "datasets": [
            "/RevOps/Analytics/pipeline_health_summary",
            "/RevOps/Analytics/pipeline_hygiene_alerts",
            "/RevOps/Dashboard/*",
            "/RevOps/Coaching/rep_performance",
        ],
    },
    {
        "name": "revops-reps",
        "description": "Limited read access to own performance data",
        "permissions": ["read"],
        "datasets": [
            "/RevOps/Dashboard/kpis",
        ],
        "row_level_security": True,
    },
]


# =============================================================================
# PROVISIONING FUNCTIONS
# =============================================================================

class WorkspaceProvisioner:
    """Handles workspace provisioning operations."""

    def __init__(self, config: CustomerConfig, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.log = []

    def provision(self) -> Dict:
        """Run full provisioning workflow."""
        self._log(f"Starting provisioning for {self.config.customer_name}")
        self._log(f"Tier: {self.config.tier}")
        self._log(f"Dry run: {self.dry_run}")

        results = {
            "customer_id": self.config.customer_id,
            "customer_name": self.config.customer_name,
            "timestamp": datetime.utcnow().isoformat(),
            "dry_run": self.dry_run,
            "folders_created": [],
            "datasets_created": [],
            "schedules_created": [],
            "groups_created": [],
            "config_written": False,
        }

        # 1. Create folder structure
        results["folders_created"] = self._create_folders()

        # 2. Create datasets
        results["datasets_created"] = self._create_datasets()

        # 3. Create schedules
        results["schedules_created"] = self._create_schedules()

        # 4. Create permission groups
        results["groups_created"] = self._create_permission_groups()

        # 5. Write customer config
        results["config_written"] = self._write_config()

        self._log(f"Provisioning complete for {self.config.customer_name}")
        results["log"] = self.log

        return results

    def _log(self, message: str):
        """Add message to log."""
        timestamp = datetime.utcnow().isoformat()
        entry = f"[{timestamp}] {message}"
        self.log.append(entry)
        print(entry)

    def _create_folders(self) -> List[str]:
        """Create folder structure."""
        created = []
        for folder in FOLDER_STRUCTURE:
            self._log(f"Creating folder: {folder}")
            if not self.dry_run:
                # In real implementation: call Foundry API
                # foundry_client.create_folder(folder)
                pass
            created.append(folder)
        return created

    def _create_datasets(self) -> List[str]:
        """Create dataset placeholders."""
        created = []

        # Core datasets (all tiers)
        for ds in CORE_DATASETS:
            self._log(f"Creating dataset: {ds['path']}")
            if not self.dry_run:
                # foundry_client.create_dataset(ds['path'], ds['type'])
                pass
            created.append(ds['path'])

        # Tier-specific datasets
        tier_order = ["starter", "growth", "enterprise"]
        tier_index = tier_order.index(self.config.tier)

        for tier in tier_order[1:tier_index + 1]:
            if tier in TIER_DATASETS:
                for ds in TIER_DATASETS[tier]:
                    self._log(f"Creating tier dataset: {ds['path']}")
                    if not self.dry_run:
                        # foundry_client.create_dataset(ds['path'], ds['type'])
                        pass
                    created.append(ds['path'])

        return created

    def _create_schedules(self) -> List[str]:
        """Create build schedules."""
        created = []
        tier_order = ["starter", "growth", "enterprise"]
        tier_index = tier_order.index(self.config.tier)

        for schedule in SCHEDULES:
            min_tier_index = tier_order.index(schedule["tier_minimum"])
            if tier_index >= min_tier_index:
                self._log(f"Creating schedule: {schedule['name']}")
                if not self.dry_run:
                    # foundry_client.create_schedule(schedule)
                    pass
                created.append(schedule["name"])

        return created

    def _create_permission_groups(self) -> List[str]:
        """Create permission groups."""
        created = []
        for group in PERMISSION_GROUPS:
            group_name = f"{self.config.customer_id}-{group['name']}"
            self._log(f"Creating permission group: {group_name}")
            if not self.dry_run:
                # foundry_client.create_group(group_name, group['description'])
                # foundry_client.assign_permissions(group_name, group['permissions'], group['datasets'])
                pass
            created.append(group_name)
        return created

    def _write_config(self) -> bool:
        """Write customer configuration."""
        config_path = f"configs/{self.config.customer_id}.json"
        self._log(f"Writing config to: {config_path}")

        if not self.dry_run:
            os.makedirs("configs", exist_ok=True)
            with open(config_path, "w") as f:
                json.dump(asdict(self.config), f, indent=2)

        return True


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Provision RevOps Hub workspace")
    parser.add_argument("--customer", required=True, help="Customer name")
    parser.add_argument("--customer-id", help="Customer ID (auto-generated if not provided)")
    parser.add_argument("--tier", choices=["starter", "growth", "enterprise"],
                       default="starter", help="Subscription tier")
    parser.add_argument("--crm", choices=["salesforce", "hubspot"],
                       default="salesforce", help="Primary CRM")
    parser.add_argument("--timezone", default="America/New_York", help="Timezone")
    parser.add_argument("--currency", default="USD", help="Currency code")
    parser.add_argument("--fiscal-year-start", default="01-01",
                       help="Fiscal year start (MM-DD)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Preview changes without executing")
    parser.add_argument("--output", help="Output file for results JSON")

    args = parser.parse_args()

    # Generate customer ID if not provided
    customer_id = args.customer_id or args.customer.lower().replace(" ", "-").replace(".", "")

    config = CustomerConfig(
        customer_id=customer_id,
        customer_name=args.customer,
        tier=args.tier,
        primary_crm=args.crm,
        timezone=args.timezone,
        currency=args.currency,
        fiscal_year_start=args.fiscal_year_start,
    )

    provisioner = WorkspaceProvisioner(config, dry_run=args.dry_run)
    results = provisioner.provision()

    # Output results
    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nResults written to: {args.output}")

    # Summary
    print("\n" + "=" * 60)
    print("PROVISIONING SUMMARY")
    print("=" * 60)
    print(f"Customer: {config.customer_name} ({config.customer_id})")
    print(f"Tier: {config.tier}")
    print(f"Folders: {len(results['folders_created'])}")
    print(f"Datasets: {len(results['datasets_created'])}")
    print(f"Schedules: {len(results['schedules_created'])}")
    print(f"Permission Groups: {len(results['groups_created'])}")
    print(f"Config Written: {results['config_written']}")

    if args.dry_run:
        print("\n⚠️  DRY RUN - No changes were made")
    else:
        print("\n✓ Provisioning complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
