#!/usr/bin/env python3
"""
Main production script to generate Asset Brain UxM test data.

Generates enterprise-scale entities and relationships following
benchmark distribution and business rules.

This is the MAIN script for production data generation.
For testing, use test_template_integration.py instead.
"""

import sys
import os
import time
from pathlib import Path
from typing import List, Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from generators.entity_generator import EntityGenerator, ProductionEntityCounts
from generators.relationship_generator import RelationshipGenerator


def generate_test_data():
    """Generate complete test dataset with single validation."""
    start_time = time.time()
    
    print("=" * 60)
    print("Asset Brain UxM Test Data Generation")
    print("=" * 60)

    # Generate entities (includes validation)
    print("\n1. Generating entities with template validation...")
    entities_output_dir = Path(__file__).parent / "test_data_final" / "valid" / "entities"
    entity_generator = EntityGenerator(str(entities_output_dir))
    entity_generator.counts = ProductionEntityCounts()  # Use production-scale counts
    
    entity_data = entity_generator.generate_all_entities()  # This does generate + validate + save
    
    if not entity_data:
        print("✗ Entity generation failed")
        return False
    
    # Generate relationships (uses pre-validated entities)
    print("\n2. Generating relationships with template validation...")
    relationships_output_dir = Path(__file__).parent / "test_data_final" / "valid" / "relationships"
    rel_generator = RelationshipGenerator(entity_data, str(relationships_output_dir))
    relationship_data = rel_generator.generate_all_relationships()  # This does generate + validate + save
    
    if not relationship_data:
        print("✗ Relationship generation failed")
        return False
    
    # Summary
    elapsed_time = time.time() - start_time
    total_entities = sum(len(entities) for entities in entity_data.values())
    total_relationships = sum(len(rels) for rels in relationship_data.values())
    
    print("\n" + "=" * 60)
    print("GENERATION COMPLETE")
    print("=" * 60)
    print(f"Time elapsed: {elapsed_time:.2f} seconds")
    print(f"Total entities: {total_entities:,}")
    print(f"Total relationships: {total_relationships:,}")
    
    print("\nEntity breakdown:")
    for entity_type, entities in entity_data.items():
        print(f"  {entity_type:12}: {len(entities):,}")
    
    print("\nRelationship breakdown:")
    for rel_type, rels in relationship_data.items():
        print(f"  {rel_type:12}: {len(rels):,}")
    
    print("\nOutput files:")
    print(f"  Valid entities: data/test_data_final/valid/entities/*.json")
    print(f"  Valid relationships: data/test_data_final/valid/relationships/*.json")
    print(f"  Invalid entities: data/test_data_final/invalid/entities/*.json")
    print(f"  Invalid relationships: data/test_data_final/invalid/relationships/*.json")
    
    print("\nNext steps:")
    print("  1. Review generated data in data/test_data_final/")
    print("  2. Set up Google Cloud Spanner Graph instance")
    print("  3. Import data using valid relationship JSON files")
    print("  4. Implement API layer for asset operations")
    
    return True


def main():
    """Main entry point."""
    try:
        success = generate_test_data()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n\nGeneration interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n\nGeneration failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
