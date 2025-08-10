#!/usr/bin/env python3
"""
Quick test to verify ProductionEntityCounts logic works correctly.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Test without importing the full model system
class EntityCounts:
    def __init__(self):
        self.datacenter = 1
        self.customer = 2

class ProductionEntityCounts(EntityCounts):
    def __init__(self):
        super().__init__()
        self.datacenter = 10
        self.customer = 1000

class MockGenerator:
    def __init__(self):
        self.counts = EntityCounts()
    
    @property
    def customer_names(self):
        names = []
        for i in range(self.counts.customer):
            names.append(f"Customer-{i+1}")
        return names
    
    @property
    def datacenter_locations(self):
        all_locations = [
            {"name": "NYC-01"}, {"name": "NYC-02"}, {"name": "SV-01"}, 
            {"name": "SV-02"}, {"name": "LON-01"}, {"name": "LON-02"}, 
            {"name": "SG-01"}, {"name": "HK-01"}, {"name": "CHI-01"}, 
            {"name": "DAL-01"}
        ]
        return all_locations[:self.counts.datacenter]

def test_counts_logic():
    print("Testing EntityCounts logic...")
    
    # Step 1: Create generator (simulates line 1)
    generator = MockGenerator()
    print(f"After __init__(): datacenter={generator.counts.datacenter}, customer={generator.counts.customer}")
    print(f"  customer_names count: {len(generator.customer_names)}")
    print(f"  datacenter_locations count: {len(generator.datacenter_locations)}")
    
    # Step 2: Set production counts (simulates line 2)
    generator.counts = ProductionEntityCounts()
    print(f"After assignment: datacenter={generator.counts.datacenter}, customer={generator.counts.customer}")
    print(f"  customer_names count: {len(generator.customer_names)}")
    print(f"  datacenter_locations count: {len(generator.datacenter_locations)}")
    
    # Test if properties work correctly
    if len(generator.customer_names) == 1000 and len(generator.datacenter_locations) == 10:
        print("✅ SUCCESS: Properties work correctly with production counts!")
        return True
    else:
        print("❌ FAILURE: Properties not using production counts correctly!")
        return False

if __name__ == "__main__":
    success = test_counts_logic()
    sys.exit(0 if success else 1)
