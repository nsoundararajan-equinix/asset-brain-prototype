#!/usr/bin/env python3
"""
Test script to verify template-based data generation works.

This script tests a small subset of data to ensure the template
validation system is working properly before running full-scale generation.
"""

import sys
import os
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / 'model'))

def test_template_imports():
    """Test that template imports work correctly."""
    try:
        from template.registry import GlobalRegistry
        from template.template import EntityKind, RelationshipKind
        print("✅ Template imports successful")
        return True
    except ImportError as e:
        print(f"❌ Template import failed: {e}")
        return False

def test_small_scale_generation():
    """Test small-scale entity generation with UxM templates."""
    print("Testing small-scale entity generation...")
    
    try:
        from generators.entity_generator import EntityGenerator
        
        # EntityGenerator now defaults to small test counts
        # Use proper directory structure: test_data_sample/valid/entities
        entities_output_dir = "test_data_sample/valid/entities"
        generator = EntityGenerator(entities_output_dir)
        entities = generator.generate_all_entities()
        
        if not entities:
            print("❌ Failed to generate entities")
            return None
            
        # Print summary
        total = sum(len(e) for e in entities.values())
        print(f"✅ Generated {total} test entities successfully")
        return entities
        
    except Exception as e:
        print(f"❌ Entity generation test failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_relationship_generation(entities):
    """Test relationship generation with template validation using existing entities."""
    if not entities:
        print("❌ No entities provided for relationship testing")
        return False
        
    try:
        from generators.relationship_generator import RelationshipGenerator
        
        print("Testing relationship generation...")
        # Use proper directory structure: test_data_sample/valid/relationships
        relationships_output_dir = "test_data_sample/valid/relationships"
        rel_generator = RelationshipGenerator(entities, relationships_output_dir)
        relationships = rel_generator.generate_all_relationships()
        
        if relationships:
            total = sum(len(r) for r in relationships.values())
            print(f"✅ Generated {total} test relationships successfully")
            return True
        else:
            print("❌ No relationships generated")
            return False
            
    except Exception as e:
        print(f"❌ Relationship generation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def cleanup_test_files():
    """Clean up test output files."""
    try:
        import shutil
        test_dir = Path("test_data_sample")
        if test_dir.exists():
            shutil.rmtree(test_dir)
        print("✅ Test files cleaned up")
    except Exception as e:
        print(f"⚠️ Cleanup warning: {e}")

def main():
    """Run all tests."""
    print("=" * 50)
    print("Asset Brain Template Integration Test")
    print("=" * 50)
    
    tests_passed = 0
    total_tests = 3
    entities = None
    
    # Test 1: Template imports
    if test_template_imports():
        tests_passed += 1
    
    # Test 2: Entity generation
    entities = test_small_scale_generation()
    if entities:
        tests_passed += 1
    
    # Test 3: Relationship generation (reuse entities from test 2)
    if test_relationship_generation(entities):
        tests_passed += 1

    # Keep test files for examination - cleanup disabled
    # cleanup_test_files()
    
    print("\n" + "=" * 50)
    print(f"TEST RESULTS: {tests_passed}/{total_tests} tests passed")
    print("=" * 50)
    
    if tests_passed == total_tests:
        print("🎉 All tests passed! Template integration is working.")
        print("You can now run the full-scale data generation:")
        print("  cd data && python generate_test_data.py")
        return True
    else:
        print("❌ Some tests failed. Please fix the issues before proceeding.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
