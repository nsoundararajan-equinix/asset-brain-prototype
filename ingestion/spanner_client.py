"""
Spanner Graph Client for Asset Brain

Provides connection and data operations for Google Cloud Spanner Graph.
Since our test data is already in UxM format, this client directly 
loads JSON data into Spanner without transformation.
"""

import json
import os
from typing import List, Dict, Any, Optional
from pathlib import Path
import time

try:
    from google.cloud import spanner
    SPANNER_AVAILABLE = True
except ImportError:
    SPANNER_AVAILABLE = False
    raise ImportError(
        "Google Cloud Spanner library is required. Install with: pip install google-cloud-spanner"
    )


class SpannerGraphClient:
    """
    Production Spanner Graph client using Google Cloud Spanner Graph.
    Implements UxM single-entity pattern for Asset Brain.
    """
    
    def __init__(self, project_id: str, instance_id: str, database_id: str):
        if not SPANNER_AVAILABLE:
            raise ImportError("Google Cloud Spanner library not available")
            
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        
        # Spanner client objects
        self.spanner_client = None
        self.instance = None
        self.database = None
        self.connected = False
        
    def connect(self) -> bool:
        """Connect to Spanner Graph."""
        print(f"🔗 Connecting to Spanner Graph:")
        print(f"   Project: {self.project_id}")
        print(f"   Instance: {self.instance_id}")
        print(f"   Database: {self.database_id}")
        
        try:
            # Initialize Spanner client
            self.spanner_client = spanner.Client(project=self.project_id)
            self.instance = self.spanner_client.instance(self.instance_id)
            self.database = self.instance.database(self.database_id)
            
            # Test connection
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql("SELECT 1")
                list(results)  # Force execution
                
            self.connected = True
            print("✅ Connected to Spanner Graph successfully")
            return True
            
        except Exception as e:
            print(f"❌ Failed to connect to Spanner Graph: {e}")
            print("   Make sure:")
            print("   1. Spanner instance and database exist")
            print("   2. Authentication is configured (gcloud auth application-default login)")
            print("   3. Service account has Spanner permissions")
            return False
        
    def create_schema(self) -> bool:
        """Create Asset Brain UxM schema in Spanner Graph."""
        if not self.connected:
            raise Exception("Not connected to Spanner Graph")
            
        print("📋 Creating Asset Brain UxM schema...")
        
        # UxM Schema: Single Assets table + Relationships table + Property Graph
        schema_ddl = [
            # Main Assets table (UxM single-entity pattern)
            """
            CREATE TABLE IF NOT EXISTS Assets (
                asset_id STRING(36) NOT NULL,
                entity_kind STRING(50) NOT NULL,
                entity_data JSON NOT NULL,
                tags ARRAY<STRING(100)>,
                version INT64 NOT NULL,
                created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
                updated_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
                is_deleted BOOL NOT NULL DEFAULT (false)
            ) PRIMARY KEY (asset_id)
            """,
            
            # Relationships table (bidirectional)
            """
            CREATE TABLE IF NOT EXISTS Relationships (
                relationship_id STRING(100) NOT NULL,
                source_asset_id STRING(36) NOT NULL,
                target_asset_id STRING(36) NOT NULL,
                relationship_kind STRING(50) NOT NULL,
                relationship_data JSON,
                tags ARRAY<STRING(100)>,
                version INT64 NOT NULL,
                created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
                updated_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
                is_deleted BOOL NOT NULL DEFAULT (false),
                FOREIGN KEY (source_asset_id) REFERENCES Assets (asset_id),
                FOREIGN KEY (target_asset_id) REFERENCES Assets (asset_id)
            ) PRIMARY KEY (relationship_id)
            """,
            
            # Property Graph definition
            """
            CREATE OR REPLACE PROPERTY GRAPH AssetGraph
            NODE TABLES (
                Assets 
                KEY (asset_id)
                LABEL AssetNode
                PROPERTIES (asset_id, entity_kind, entity_data, tags, version, created_at, updated_at)
            )
            EDGE TABLES (
                Relationships
                KEY (relationship_id)
                SOURCE KEY (source_asset_id) REFERENCES Assets (asset_id)
                DESTINATION KEY (target_asset_id) REFERENCES Assets (asset_id)
                LABEL RelationshipEdge
                PROPERTIES (relationship_id, relationship_kind, relationship_data, tags, version, created_at, updated_at)
            )
            """
        ]
        
        try:
            for i, ddl in enumerate(schema_ddl, 1):
                print(f"   Executing DDL {i}/{len(schema_ddl)}...")
                operation = self.database.update_ddl([ddl])
                operation.result(timeout=240)  # Wait up to 4 minutes for DDL
                
            print("✅ UxM schema created successfully")
            return True
            
        except Exception as e:
            print(f"❌ Failed to create schema: {e}")
            return False
        
    def batch_insert_entities(self, entities: List[Dict[str, Any]]) -> Dict[str, int]:
        """Batch insert or update entities into Assets table."""
        if not self.connected:
            raise Exception("Not connected to Spanner Graph")
            
        if not entities:
            return {'inserted': 0, 'errors': 0, 'warnings': 0}
            
        try:
            with self.database.batch() as batch:
                for entity in entities:
                    # Ensure tags is a list
                    tags = entity.get('tags', [])
                    if not isinstance(tags, list):
                        tags = []
                        
                    # Insert or update into Assets table (UxM pattern)
                    batch.insert_or_update(
                        table='Assets',
                        columns=[
                            'asset_id', 'entity_kind', 'entity_data', 'tags', 
                            'version', 'created_at', 'updated_at', 'is_deleted'
                        ],
                        values=[[  # Wrap in another list to make it a list of rows
                            entity['asset_id'],
                            entity['entity_kind'],
                            json.dumps(entity['entity_data']),  # Use json.dumps instead of JsonObject
                            tags,
                            entity.get('version', 1),
                            spanner.COMMIT_TIMESTAMP,
                            spanner.COMMIT_TIMESTAMP,
                            entity.get('is_deleted', False)
                        ]]
                    )
                    
            return {
                'inserted': len(entities),
                'errors': 0,
                'warnings': 0
            }
            
        except Exception as e:
            import traceback
            print(f"❌ Batch insert entities failed: {e}")
            print("Full traceback:")
            traceback.print_exc()
            print(f"Entity data being processed: {entities[:1]}")  # Show first entity for debugging
            return {
                'inserted': 0,
                'errors': len(entities),
                'warnings': 0
            }
        
    def batch_insert_relationships(self, relationships: List[Dict[str, Any]]) -> Dict[str, int]:
        """Batch insert or update relationships into Relationships table."""
        if not self.connected:
            raise Exception("Not connected to Spanner Graph")
            
        if not relationships:
            return {'inserted': 0, 'errors': 0, 'warnings': 0}
            
        try:
            with self.database.batch() as batch:
                for rel in relationships:
                    # Ensure tags is a list
                    tags = rel.get('tags', [])
                    if not isinstance(tags, list):
                        tags = []
                        
                    # Insert or update into Relationships table
                    batch.insert_or_update(
                        table='Relationships',
                        columns=[
                            'relationship_id', 'source_asset_id', 'target_asset_id',
                            'relationship_kind', 'relationship_data', 'tags',
                            'version', 'created_at', 'updated_at', 'is_deleted'
                        ],
                        values=[[  # Wrap in another list to make it a list of rows
                            rel['relationship_id'],
                            rel['source_asset_id'],
                            rel['target_asset_id'],
                            rel['relationship_kind'],
                            json.dumps(rel.get('relationship_data', {})),  # Use json.dumps instead of JsonObject
                            tags,
                            rel.get('version', 1),
                            spanner.COMMIT_TIMESTAMP,
                            spanner.COMMIT_TIMESTAMP,
                            rel.get('is_deleted', False)
                        ]]
                    )
                    
            return {
                'inserted': len(relationships),
                'errors': 0,
                'warnings': 0
            }
            
        except Exception as e:
            import traceback
            print(f"❌ Batch insert relationships failed: {e}")
            print("Full traceback:")
            traceback.print_exc()
            print(f"Relationship data being processed: {relationships[:1]}")  # Show first relationship for debugging
            return {
                'inserted': 0,
                'errors': len(relationships),
                'warnings': 0
            }
    
    def batch_upsert_entities_smart(self, entities: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Smart upsert that preserves created_at for existing entities.
        This method checks if entities exist and preserves their original created_at timestamp.
        """
        if not self.connected:
            raise Exception("Not connected to Spanner Graph")
            
        if not entities:
            return {'inserted': 0, 'updated': 0, 'errors': 0, 'warnings': 0}
            
        try:
            inserted_count = 0
            updated_count = 0
            
            # Get existing entities to preserve created_at
            asset_ids = [entity['asset_id'] for entity in entities]
            existing_entities = {}
            
            if asset_ids:
                with self.database.snapshot() as snapshot:
                    query = f"""
                    SELECT asset_id, created_at 
                    FROM Assets 
                    WHERE asset_id IN UNNEST(@asset_ids)
                    """
                    results = snapshot.execute_sql(
                        query, 
                        params={'asset_ids': asset_ids},
                        param_types={'asset_ids': spanner.param_types.Array(spanner.param_types.STRING)}
                    )
                    for row in results:
                        existing_entities[row[0]] = row[1]  # asset_id -> created_at
            
            with self.database.batch() as batch:
                for entity in entities:
                    # Ensure tags is a list
                    tags = entity.get('tags', [])
                    if not isinstance(tags, list):
                        tags = []
                    
                    # Determine if this is an insert or update
                    asset_id = entity['asset_id']
                    if asset_id in existing_entities:
                        # Update: preserve original created_at
                        created_at = existing_entities[asset_id]
                        updated_count += 1
                    else:
                        # Insert: use current timestamp
                        created_at = spanner.COMMIT_TIMESTAMP
                        inserted_count += 1
                        
                    # Insert or update into Assets table
                    batch.insert_or_update(
                        table='Assets',
                        columns=[
                            'asset_id', 'entity_kind', 'entity_data', 'tags', 
                            'version', 'created_at', 'updated_at', 'is_deleted'
                        ],
                        values=[[
                            entity['asset_id'],
                            entity['entity_kind'],
                            json.dumps(entity['entity_data']),
                            tags,
                            entity.get('version', 1),
                            created_at,  # Preserve original or use current timestamp
                            spanner.COMMIT_TIMESTAMP,  # Always update updated_at
                            entity.get('is_deleted', False)
                        ]]
                    )
                    
            return {
                'inserted': inserted_count,
                'updated': updated_count,
                'errors': 0,
                'warnings': 0
            }
            
        except Exception as e:
            import traceback
            print(f"❌ Smart upsert entities failed: {e}")
            print("Full traceback:")
            traceback.print_exc()
            return {
                'inserted': 0,
                'updated': 0,
                'errors': len(entities),
                'warnings': 0
            }
        
    def get_stats(self) -> Dict[str, int]:
        """Get database statistics."""
        if not self.connected:
            raise Exception("Not connected to Spanner Graph")
            
        try:
            # Count entities
            with self.database.snapshot() as snapshot:
                entity_results = snapshot.execute_sql("SELECT COUNT(*) as count FROM Assets WHERE is_deleted = false")
                entity_count = list(entity_results)[0][0]
                
            # Count relationships  
            with self.database.snapshot() as snapshot:
                rel_results = snapshot.execute_sql("SELECT COUNT(*) as count FROM Relationships WHERE is_deleted = false")
                rel_count = list(rel_results)[0][0]
                
            # Count entity types
            with self.database.snapshot() as snapshot:
                entity_type_results = snapshot.execute_sql("SELECT COUNT(DISTINCT entity_kind) as count FROM Assets WHERE is_deleted = false")
                entity_types = list(entity_type_results)[0][0]
                
            # Count relationship types
            with self.database.snapshot() as snapshot:
                rel_type_results = snapshot.execute_sql("SELECT COUNT(DISTINCT relationship_kind) as count FROM Relationships WHERE is_deleted = false")
                rel_types = list(rel_type_results)[0][0]
                
            return {
                'total_entities': entity_count,
                'total_relationships': rel_count,
                'entity_types': entity_types,
                'relationship_types': rel_types
            }
            
        except Exception as e:
            print(f"❌ Failed to get stats: {e}")
            return {
                'total_entities': 0,
                'total_relationships': 0,
                'entity_types': 0,
                'relationship_types': 0
            }
        
    def test_graph_query(self) -> bool:
        """Test basic graph traversal query."""
        if not self.connected:
            raise Exception("Not connected to Spanner Graph")
            
        try:
            print("🔍 Testing graph traversal query...")
            
            # Test query: Find power relationships
            graph_query = """
            GRAPH AssetGraph
            MATCH (source:AssetNode)-[rel:RelationshipEdge]->(target:AssetNode)
            WHERE rel.relationship_kind LIKE '%FEEDS_POWER_TO%'
            RETURN source.asset_id AS source_id, source.entity_kind AS source_type, 
                   target.asset_id AS target_id, target.entity_kind AS target_type
            LIMIT 5
            """
            
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(graph_query)
                results_list = list(results)
                
                if results_list:
                    print(f"✅ Graph query successful - found {len(results_list)} power relationships")
                    for row in results_list[:3]:  # Show first 3
                        print(f"   {row[1]} ({row[0]}) -> {row[3]} ({row[2]})")
                else:
                    print("⚠️ Graph query successful but no power relationships found")
                    
            return True
            
        except Exception as e:
            print(f"❌ Graph query failed: {e}")
            return False
        
    def disconnect(self):
        """Disconnect from Spanner Graph."""
        if self.connected:
            print("🔌 Disconnecting from Spanner Graph...")
            self.spanner_client = None
            self.instance = None
            self.database = None
            self.connected = False
            print("✅ Disconnected successfully")
