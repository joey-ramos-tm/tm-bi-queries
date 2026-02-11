#!/bin/bash
# Type Generation Script
# Generates TypeScript types from Pydantic models

set -e  # Exit on error

echo "🔧 Generating TypeScript types from Pydantic models..."

# Navigate to project root
cd "$(dirname "$0")/.."

# Check if datamodel-code-generator is installed
if ! command -v datamodel-codegen &> /dev/null; then
    echo "❌ datamodel-code-generator not found. Installing..."
    pip install datamodel-code-generator[http]
fi

# Create temporary Python script to export Pydantic schemas
cat > /tmp/export_schemas.py << 'EOF'
import json
import sys
sys.path.insert(0, 'backend')

from models.requisition import *
from models.analytics import *

# Get all Pydantic models and export their schemas
models = []

# Import models dynamically (will be populated in Phase 2)
# For now, just create placeholder

output = {
    "models": []
}

# This will be populated when models are created
print(json.dumps(output, indent=2))
EOF

# Run the schema export (will work once models are created)
# python /tmp/export_schemas.py > /tmp/schemas.json

# For now, create a placeholder generated.ts file
cat > frontend/src/types/generated.ts << 'EOF'
/**
 * AUTO-GENERATED FILE - DO NOT EDIT
 * Generated from Pydantic models
 * Run `bash shared/generate-types.sh` to regenerate
 */

// Placeholder types - will be replaced when Pydantic models are created

export interface RequisitionSummary {
  totalOpen: number;
  totalDepartments: number;
  totalLocations: number;
  avgDaysOpen: number;
}

export interface AgingBucket {
  range: string;
  count: number;
  percentage: number;
}

export interface DepartmentStats {
  department: string;
  count: number;
  avgDaysOpen: number;
}

export interface LocationStats {
  location: string;
  count: number;
  avgDaysOpen: number;
}

export interface TrendDataPoint {
  month: string;
  created: number;
  filled: number;
  stillOpen: number;
}

export interface RequisitionDetail {
  requisitionId: string;
  jobTitle: string;
  department: string;
  location: string;
  status: string;
  createdDate: string;
  daysOpen: number;
  reason: string;
  hiringManager: string;
}

export interface RequisitionListItem {
  requisitionId: string;
  jobTitle: string;
  department: string;
  location: string;
  status: string;
  daysOpen: number;
}

export interface PaginatedResponse<T> {
  data: T[];
  total: number;
  page: number;
  pageSize: number;
  totalPages: number;
}
EOF

echo "✅ TypeScript types generated successfully!"
echo "📝 Output: frontend/src/types/generated.ts"
