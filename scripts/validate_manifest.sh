#!/bin/bash
#
# MANIFEST VALIDATION SCRIPT
# ==========================
# Validates that webapp dataset references match transform outputs.
# Use in CI to catch drift between webapp and transforms.
#
# Usage:
#   ./scripts/validate_manifest.sh
#
# Exit codes:
#   0 - All checks passed
#   1 - Gaps found between webapp and transforms
#   2 - Script error

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=== RevOps Manifest Validation ==="
echo "Project root: $PROJECT_ROOT"
echo ""

# Check Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not found"
    exit 2
fi

# Run the manifest generator in check mode
echo "Running manifest generator..."
cd "$PROJECT_ROOT"

if python3 scripts/generate_manifest.py --check; then
    echo ""
    echo "✅ All manifest checks passed!"
    exit 0
else
    echo ""
    echo "❌ Manifest validation failed - see gaps above"
    echo ""
    echo "To fix:"
    echo "  1. Add missing transforms for datasets the webapp needs"
    echo "  2. Or remove webapp references to datasets that don't exist"
    echo "  3. Run 'python scripts/generate_manifest.py' to regenerate manifest"
    exit 1
fi
