#!/usr/bin/env python3
"""
DATASET DEPENDENCY MANIFEST GENERATOR
=====================================
Extracts dataset dependencies from webapp fetchers and transforms,
generates a machine-readable manifest, and identifies gaps.

Usage:
    python scripts/generate_manifest.py
    python scripts/generate_manifest.py --check  # CI mode - exits non-zero if gaps exist
"""

import json
import re
import sys
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Set, Dict, List, Optional

# Project root
PROJECT_ROOT = Path(__file__).parent.parent

# =============================================================================
# COMMERCIAL DATASETS CONFIGURATION
# =============================================================================
# These 8 canonical datasets form the core commercial data layer

COMMERCIAL_DATASETS = {
    # Inputs (sources for agents, dashboards, billing logic)
    # These paths match what transforms expect
    "/RevOps/Agents/action_feedback": {
        "type": "input",
        "producers": ["webapp_ui", "sdk_api"],
        "consumers": ["ai_training", "quality_dashboard", "ops_agent"],
        "description": "User feedback on agent/action quality",
    },
    "/RevOps/Agents/action_log": {
        "type": "input",
        "producers": ["agent_executor", "action_dispatcher"],
        "consumers": ["usage_tracking", "analytics_dashboard"],
        "description": "Execution logs for agent/action invocations",
    },
    "/RevOps/Audit/notification_events": {
        "type": "input",
        "producers": ["notification_service"],
        "consumers": ["notification_analytics", "cs_console", "ops_agent"],
        "description": "Audit trail of notifications sent",
    },
    "/RevOps/Audit/subscription_events": {
        "type": "input",
        "producers": ["billing_service", "contract_service"],
        "consumers": ["revenue_analytics", "cs_console", "audit_reports"],
        "description": "Audit trail for subscription/billing events",
    },
    "/RevOps/Audit/webhook_delivery_log": {
        "type": "input",
        "producers": ["webhook_dispatcher"],
        "consumers": ["webhook_analytics", "integration_health", "ops_agent"],
        "description": "Audit of outbound webhook delivery attempts",
    },
    # Config datasets
    "/RevOps/Config/user_notification_preferences": {
        "type": "config",
        "producers": ["settings_ui", "onboarding"],
        "consumers": ["notification_service"],
        "description": "Per-user notification preferences",
    },
    "/RevOps/Config/webhook_configs": {
        "type": "config",
        "producers": ["settings_ui", "api"],
        "consumers": ["webhook_dispatcher", "integration_health"],
        "description": "Webhook endpoint configurations",
    },
    "/RevOps/Monitoring/sync_status": {
        "type": "analytics",
        "producers": ["crm_sync", "billing_sync", "usage_sync"],
        "consumers": ["ops_dashboard", "ops_agent", "alerting"],
        "description": "Health status of external system syncs",
    },
}

# Analytics views that consume previously orphaned outputs
# Paths must match what transforms actually produce
ANALYTICS_CONSUMERS = {
    # notification_* outputs are consumed by notification_analytics view
    "/RevOps/Analytics/notification_daily_rollup": ["notification_analytics_view", "ops_dashboard"],
    "/RevOps/Analytics/notification_channel_stats": ["notification_analytics_view"],
    "/RevOps/Analytics/notification_latency_metrics": ["notification_analytics_view"],
    # webhook_analytics consumed by integration_health view
    "/RevOps/Analytics/webhook_analytics": ["integration_health_view", "ops_dashboard"],
    # Orphaned commercial outputs now have consumers
    "/RevOps/Commercial/notification_preferences": ["settings_ui", "notification_service"],
    "/RevOps/Commercial/notification_summary": ["ops_dashboard", "cs_console"],
    "/RevOps/Commercial/webhook_analytics": ["integration_health_view", "ops_dashboard"],
}

@dataclass
class TransformInfo:
    """Information about a transform."""
    file: str
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)

@dataclass
class DatasetManifest:
    """Complete dataset dependency manifest."""
    webapp_datasets: Dict[str, List[str]]        # dataset -> list of modules that use it
    transform_outputs: Dict[str, str]            # dataset -> transform file that produces it
    transform_inputs: Dict[str, List[str]]       # dataset -> list of transforms that consume it
    commercial_datasets: Dict[str, dict]         # commercial dataset path -> metadata
    analytics_consumers: Dict[str, List[str]]    # analytics output -> list of consumers
    gaps: Dict[str, List[str]]                   # gap type -> list of datasets
    build_order: List[List[str]]                 # topologically sorted build tiers


def extract_webapp_datasets() -> Dict[str, List[str]]:
    """Extract dataset paths from webapp fetchers."""
    webapp_dir = PROJECT_ROOT / "webapp" / "src" / "lib" / "foundry"
    datasets: Dict[str, Set[str]] = {}

    for ts_file in webapp_dir.glob("*.ts"):
        if ts_file.name == "index.ts":
            continue

        content = ts_file.read_text()
        module = ts_file.stem

        # Find all /RevOps/... patterns in SQL queries
        matches = re.findall(r'`(/RevOps/[^`\s\\]+)', content)
        for match in matches:
            dataset = match.rstrip('\\')  # Remove trailing backslash if any
            if dataset not in datasets:
                datasets[dataset] = set()
            datasets[dataset].add(module)

    return {k: sorted(list(v)) for k, v in sorted(datasets.items())}


def extract_transform_io() -> Dict[str, TransformInfo]:
    """Extract inputs and outputs from transform decorators."""
    transforms_dir = PROJECT_ROOT / "transforms"
    transforms: Dict[str, TransformInfo] = {}

    for py_file in transforms_dir.rglob("*.py"):
        if py_file.name.startswith("__"):
            continue

        content = py_file.read_text()
        rel_path = str(py_file.relative_to(PROJECT_ROOT))

        info = TransformInfo(file=rel_path)

        # Find Input patterns
        inputs = re.findall(r'Input\s*\(\s*"(/[^"]+)"', content)
        info.inputs = sorted(set(inputs))

        # Find Output patterns
        outputs = re.findall(r'Output\s*\(\s*"(/[^"]+)"', content)
        info.outputs = sorted(set(outputs))

        if info.inputs or info.outputs:
            transforms[rel_path] = info

    return transforms


def build_dataset_mappings(transforms: Dict[str, TransformInfo]):
    """Build mappings from datasets to transforms."""
    outputs: Dict[str, str] = {}
    inputs: Dict[str, List[str]] = {}

    for path, info in transforms.items():
        for output in info.outputs:
            outputs[output] = path
        for inp in info.inputs:
            if inp not in inputs:
                inputs[inp] = []
            inputs[inp].append(path)

    return outputs, inputs


def find_gaps(webapp: Dict[str, List[str]],
              outputs: Dict[str, str],
              inputs: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Identify gaps between webapp expectations and transform outputs."""
    gaps = {
        "missing_transforms": [],      # Webapp needs but no transform produces
        "orphaned_transforms": [],     # Transform produces but nothing consumes
        "missing_inputs": [],          # Transform needs but nothing produces
    }

    all_outputs = set(outputs.keys())
    all_inputs = set(inputs.keys())
    webapp_needs = set(webapp.keys())

    # Datasets with defined consumers (not orphaned)
    has_consumers = set(ANALYTICS_CONSUMERS.keys())

    # Commercial datasets that are known inputs
    commercial_inputs = set(COMMERCIAL_DATASETS.keys())

    # Webapp needs that no transform produces
    for dataset in sorted(webapp_needs - all_outputs):
        gaps["missing_transforms"].append(dataset)

    # Transform outputs that nothing consumes
    # Exclude: webapp consumers, transform inputs, defined analytics consumers
    for dataset in sorted(all_outputs - webapp_needs - all_inputs - has_consumers):
        gaps["orphaned_transforms"].append(dataset)

    # Transform inputs that nothing produces
    for dataset in sorted(all_inputs - all_outputs - commercial_inputs):
        # Exclude raw/sample/reference data that may be uploaded
        if not any(x in dataset for x in ["/Raw/", "/Sample/", "/Scenario/", "/Reference/", "/Commercial/"]):
            gaps["missing_inputs"].append(dataset)

    return gaps


def topological_sort(transforms: Dict[str, TransformInfo]) -> List[List[str]]:
    """Sort transforms into build tiers based on dependencies."""
    outputs, _ = build_dataset_mappings(transforms)

    # Build dependency graph: transform -> transforms it depends on
    deps: Dict[str, Set[str]] = {}
    for path, info in transforms.items():
        deps[path] = set()
        for inp in info.inputs:
            if inp in outputs:
                deps[path].add(outputs[inp])

    # Kahn's algorithm for topological sort with tiers
    in_degree = {t: len(d) for t, d in deps.items()}
    tiers: List[List[str]] = []
    remaining = set(deps.keys())

    while remaining:
        # Find all transforms with no remaining dependencies
        tier = [t for t in remaining if in_degree[t] == 0]
        if not tier:
            # Cycle detected - just add remaining
            tiers.append(sorted(remaining))
            break

        tier.sort()
        tiers.append(tier)

        # Remove this tier and update in-degrees
        for t in tier:
            remaining.remove(t)
            for other in remaining:
                if t in deps[other]:
                    in_degree[other] -= 1

    return tiers


def generate_manifest() -> DatasetManifest:
    """Generate the complete dependency manifest."""
    webapp = extract_webapp_datasets()
    transforms = extract_transform_io()
    outputs, inputs = build_dataset_mappings(transforms)
    gaps = find_gaps(webapp, outputs, inputs)
    build_order = topological_sort(transforms)

    return DatasetManifest(
        webapp_datasets=webapp,
        transform_outputs=outputs,
        transform_inputs={k: sorted(v) for k, v in sorted(inputs.items())},
        commercial_datasets=COMMERCIAL_DATASETS,
        analytics_consumers=ANALYTICS_CONSUMERS,
        gaps=gaps,
        build_order=build_order
    )


def main():
    check_mode = "--check" in sys.argv

    manifest = generate_manifest()

    # Write manifest JSON
    manifest_path = PROJECT_ROOT / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(asdict(manifest), f, indent=2)
    print(f"✓ Generated {manifest_path}")

    # Print summary
    print(f"\n=== Dataset Dependency Summary ===")
    print(f"Webapp datasets:       {len(manifest.webapp_datasets)}")
    print(f"Transform outputs:     {len(manifest.transform_outputs)}")
    print(f"Commercial datasets:   {len(manifest.commercial_datasets)}")
    print(f"Analytics consumers:   {len(manifest.analytics_consumers)}")
    print(f"Build tiers:           {len(manifest.build_order)}")

    # Print commercial dataset breakdown
    comm_by_type = {}
    for path, meta in manifest.commercial_datasets.items():
        dtype = meta.get("type", "unknown")
        comm_by_type[dtype] = comm_by_type.get(dtype, 0) + 1
    if comm_by_type:
        print(f"\n=== Commercial Datasets by Type ===")
        for dtype, count in sorted(comm_by_type.items()):
            print(f"  {dtype}: {count}")

    # Print gaps
    has_gaps = False
    has_critical_gaps = False  # Only missing_transforms are critical
    for gap_type, datasets in manifest.gaps.items():
        if datasets:
            has_gaps = True
            is_critical = gap_type == "missing_transforms"
            if is_critical:
                has_critical_gaps = True
                print(f"\n❌ {gap_type.replace('_', ' ').title()} ({len(datasets)}):")
            else:
                print(f"\n⚠️  {gap_type.replace('_', ' ').title()} ({len(datasets)}):")
            for d in datasets[:10]:  # Show first 10
                print(f"   - {d}")
            if len(datasets) > 10:
                print(f"   ... and {len(datasets) - 10} more")

    if not has_gaps:
        print("\n✓ No gaps found - webapp and transforms are in sync!")
    elif not has_critical_gaps:
        print("\n✓ All webapp datasets have corresponding transforms!")

    # Only fail on missing_transforms - orphaned outputs and missing inputs are warnings
    if check_mode and has_critical_gaps:
        print("\n❌ CI check failed: webapp requires datasets that no transform produces")
        sys.exit(1)

    return manifest


if __name__ == "__main__":
    main()
