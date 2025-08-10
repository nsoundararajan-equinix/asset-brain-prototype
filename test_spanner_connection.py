#!/usr/bin/env python3
"""
Test Spanner Graph Connection

Simple script to test connection to Spanner Graph and validate schema.
Run this before the main ingestion pipeline to ensure everything is configured correctly.
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from ingestion.spanner_client import SpannerGraphClient


def test_spanner_connection():
    """Test Spanner Graph connection and basic operations."""
    
    # Get configuration from environment
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    instance_id = os.getenv('SPANNER_INSTANCE_ID') 
    database_id = os.getenv('SPANNER_DATABASE_ID')
    
    if not all([project_id, instance_id, database_id]):
        print("❌ Missing required environment variables:")
        print("   GOOGLE_CLOUD_PROJECT")
        print("   SPANNER_INSTANCE_ID") 
        print("   SPANNER_DATABASE_ID")
        print("\nSet them with:")
        print("   export GOOGLE_CLOUD_PROJECT=your-project")
        print("   export SPANNER_INSTANCE_ID=your-instance")
        print("   export SPANNER_DATABASE_ID=your-database")
        return False
        
    print("🧪 Testing Spanner Graph Connection")
    print("=" * 50)
    print(f"Project: {project_id}")
    print(f"Instance: {instance_id}")
    print(f"Database: {database_id}")
    print()
    
    try:
        # Test connection
        client = SpannerGraphClient(project_id, instance_id, database_id)
        
        print("1. Testing connection...")
        if not client.connect():
            return False
            
        print("\n2. Creating/updating schema...")
        if not client.create_schema():
            return False
            
        print("\n3. Getting database statistics...")
        stats = client.get_stats()
        print(f"   Entities: {stats['total_entities']:,}")
        print(f"   Relationships: {stats['total_relationships']:,}")
        print(f"   Entity Types: {stats['entity_types']}")
        print(f"   Relationship Types: {stats['relationship_types']}")
        
        print("\n4. Testing graph query capabilities...")
        client.test_graph_query()
        
        print("\n5. Disconnecting...")
        client.disconnect()
        
        print("\n✅ All tests passed! Spanner Graph is ready for ingestion.")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure Spanner instance and database exist")
        print("2. Check authentication: gcloud auth application-default login")
        print("3. Verify service account has Spanner permissions")
        print("4. Ensure google-cloud-spanner is installed: pip install google-cloud-spanner")
        return False


if __name__ == "__main__":
    success = test_spanner_connection()
    sys.exit(0 if success else 1)
