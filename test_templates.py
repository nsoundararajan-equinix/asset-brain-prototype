#!/usr/bin/env python3
"""
Test script for Asset Brain template system.

Validates that all templates are registered correctly and 
demonstrates the validation capabilities.
"""

import sys
import os

# Add the model directory to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'model'))

from template import EntityKind, RelationshipKind, GlobalRegistry, register_all_templates, get_registered_template_count

def test_template_registration():
    """Test that all templates register successfully."""
    print("Testing template registration...")
    
    # Register all templates
    register_all_templates()
    
    # Verify count
    count = get_registered_template_count()
    expected_count = 8  # We have 8 entity types
    
    if count == expected_count:
        print(f"✅ Successfully registered {count} templates")
    else:
        print(f"❌ Expected {expected_count} templates, got {count}")
        return False
    
    # Verify specific templates exist
    for entity_kind in EntityKind:
        if entity_kind == EntityKind.EK_UNKNOWN:
            continue
        template = GlobalRegistry.get_template(entity_kind)
        if template:
            print(f"✅ Template registered for {entity_kind.name}")
        else:
            print(f"❌ Missing template for {entity_kind.name}")
            return False
    
    return True

def test_valid_subgraph():
    """Test validation of a valid subgraph."""
    print("\nTesting valid subgraph validation...")
    
    # Create a valid subgraph: Customer owns Cage, Cage contains Cabinet
    entities = [
        {
            'id': {'kind': EntityKind.EK_CUSTOMER.value, 'name': 'ACME Corp'},
            'kind': EntityKind.EK_CUSTOMER.value
        },
        {
            'id': {'kind': EntityKind.EK_CAGE.value, 'name': 'Cage-A1'}, 
            'kind': EntityKind.EK_CAGE.value
        },
        {
            'id': {'kind': EntityKind.EK_CABINET.value, 'name': 'CAB-A1-01'},
            'kind': EntityKind.EK_CABINET.value
        }
    ]
    
    relationships = [
        {
            'a': {'kind': EntityKind.EK_CUSTOMER.value, 'name': 'ACME Corp'},
            'z': {'kind': EntityKind.EK_CAGE.value, 'name': 'Cage-A1'},
            'kind': RelationshipKind.RK_OWNS.value
        },
        {
            'a': {'kind': EntityKind.EK_CAGE.value, 'name': 'Cage-A1'},
            'z': {'kind': EntityKind.EK_CABINET.value, 'name': 'CAB-A1-01'},
            'kind': RelationshipKind.RK_CONTAINS.value
        }
    ]
    
    try:
        GlobalRegistry.validate_subgraph(entities, relationships)
        print("✅ Valid subgraph passed validation")
        return True
    except Exception as e:
        print(f"❌ Valid subgraph failed validation: {e}")
        return False

def test_invalid_subgraph():
    """Test validation of an invalid subgraph."""
    print("\nTesting invalid subgraph validation...")
    
    # Create an invalid subgraph: Customer directly contains Cabinet (should go through Cage)
    entities = [
        {
            'id': {'kind': EntityKind.EK_CUSTOMER.value, 'name': 'ACME Corp'},
            'kind': EntityKind.EK_CUSTOMER.value
        },
        {
            'id': {'kind': EntityKind.EK_CABINET.value, 'name': 'CAB-A1-01'},
            'kind': EntityKind.EK_CABINET.value
        }
    ]
    
    relationships = [
        {
            'a': {'kind': EntityKind.EK_CUSTOMER.value, 'name': 'ACME Corp'},
            'z': {'kind': EntityKind.EK_CABINET.value, 'name': 'CAB-A1-01'},
            'kind': RelationshipKind.RK_CONTAINS.value  # Invalid - Customer cannot contain Cabinet
        }
    ]
    
    try:
        GlobalRegistry.validate_subgraph(entities, relationships)
        print("❌ Invalid subgraph incorrectly passed validation")
        return False
    except Exception as e:
        print(f"✅ Invalid subgraph correctly failed validation: {e}")
        return True

def test_power_chain_validation():
    """Test validation of power chain relationships."""
    print("\nTesting power chain validation...")
    
    # Create a valid power chain: Utility -> UPS -> PDU -> Cage -> Cabinet
    entities = [
        {'id': {'kind': EntityKind.EK_UTILITY.value, 'name': 'ConEd'}, 'kind': EntityKind.EK_UTILITY.value},
        {'id': {'kind': EntityKind.EK_UPS.value, 'name': 'UPS-01'}, 'kind': EntityKind.EK_UPS.value},
        {'id': {'kind': EntityKind.EK_PDU.value, 'name': 'PDU-A1'}, 'kind': EntityKind.EK_PDU.value},
        {'id': {'kind': EntityKind.EK_CAGE.value, 'name': 'Cage-A1'}, 'kind': EntityKind.EK_CAGE.value},
        {'id': {'kind': EntityKind.EK_CABINET.value, 'name': 'CAB-A1-01'}, 'kind': EntityKind.EK_CABINET.value}
    ]
    
    relationships = [
        {
            'a': {'kind': EntityKind.EK_UTILITY.value, 'name': 'ConEd'},
            'z': {'kind': EntityKind.EK_UPS.value, 'name': 'UPS-01'},
            'kind': RelationshipKind.RK_FEEDS_POWER_TO.value
        },
        {
            'a': {'kind': EntityKind.EK_UPS.value, 'name': 'UPS-01'},
            'z': {'kind': EntityKind.EK_PDU.value, 'name': 'PDU-A1'},
            'kind': RelationshipKind.RK_FEEDS_POWER_TO.value
        },
        {
            'a': {'kind': EntityKind.EK_PDU.value, 'name': 'PDU-A1'},
            'z': {'kind': EntityKind.EK_CAGE.value, 'name': 'Cage-A1'},
            'kind': RelationshipKind.RK_FEEDS_POWER_TO.value
        },
        {
            'a': {'kind': EntityKind.EK_CAGE.value, 'name': 'Cage-A1'},
            'z': {'kind': EntityKind.EK_CABINET.value, 'name': 'CAB-A1-01'},
            'kind': RelationshipKind.RK_FEEDS_POWER_TO.value
        }
    ]
    
    try:
        GlobalRegistry.validate_subgraph(entities, relationships)
        print("✅ Valid power chain passed validation")
        return True
    except Exception as e:
        print(f"❌ Valid power chain failed validation: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Asset Brain Template System Test\n")
    
    tests = [
        test_template_registration,
        test_valid_subgraph,
        test_invalid_subgraph,
        test_power_chain_validation
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Template system is working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Please check the implementation.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
