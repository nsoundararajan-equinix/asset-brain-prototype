"""
Asset Data Loader for Asset Brain

Loads UxM-formatted JSON test data and prepares it for Spanner Graph ingestion.
Since our test data is already in UxM format, this primarily handles 
file I/O and batching for efficient database operations.
"""

import json
import os
from typing import List, Dict, Any, Iterator
from pathlib import Path
import time

class AssetDataLoader:
    """
    Loads Asset Brain test data from JSON files.
    Data is already in UxM format, so no transformation needed.
    """
    
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.entity_dir = self.data_dir / "valid" / "entities"
        self.relationship_dir = self.data_dir / "valid" / "relationships"
        
        # Verify directories exist
        if not self.entity_dir.exists():
            raise FileNotFoundError(f"Entity directory not found: {self.entity_dir}")
        if not self.relationship_dir.exists():
            raise FileNotFoundError(f"Relationship directory not found: {self.relationship_dir}")
            
        print(f"📂 Data loader initialized:")
        print(f"   Entities: {self.entity_dir}")
        print(f"   Relationships: {self.relationship_dir}")
        
    def load_entities(self, batch_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
        """
        Load entities in batches from JSON files.
        
        Args:
            batch_size: Number of entities per batch
            
        Yields:
            Batches of entity dictionaries (already in UxM format)
        """
        entity_files = [
            "customer.json", "product.json", "datacenter.json", "utility.json",
            "ups.json", "pdu.json", "cage.json", "cabinet.json"
        ]
        
        total_entities = 0
        
        for filename in entity_files:
            file_path = self.entity_dir / filename
            if not file_path.exists():
                print(f"⚠️ Entity file not found: {filename}")
                continue
                
            print(f"📖 Loading entities from {filename}...")
            
            with open(file_path, 'r') as f:
                entities = json.load(f)
                
            # Process in batches
            batch = []
            for entity in entities:
                # Transform to Spanner format
                spanner_entity = {
                    'asset_id': f"{entity['id']['kind']}_{entity['id']['name']}",
                    'entity_kind': self._map_entity_kind(entity['id']['kind']),
                    'entity_data': entity,  # Store the entire original entity as JSON
                    'tags': entity.get('tags', []),
                    'version': entity.get('version', 1),
                    'is_deleted': entity.get('is_deleted', False)
                }
                batch.append(spanner_entity)
                
                if len(batch) >= batch_size:
                    yield batch
                    total_entities += len(batch)
                    batch = []
                    
            # Yield remaining entities
            if batch:
                yield batch
                total_entities += len(batch)
                
            print(f"   Loaded {len(entities)} entities from {filename}")
            
        print(f"✅ Total entities loaded: {total_entities:,}")
        
    def _map_entity_kind(self, kind_id: int) -> str:
        """Map numeric entity kind to string representation."""
        kind_mapping = {
            1: "CUSTOMER",
            2: "PRODUCT", 
            3: "DATACENTER",
            4: "UTILITY",
            5: "UPS",
            6: "PDU",
            7: "CAGE",
            8: "CABINET"
        }
        return kind_mapping.get(kind_id, f"UNKNOWN_{kind_id}")
        
    def _map_relationship_kind(self, kind_id: int) -> str:
        """Map numeric relationship kind to string representation."""
        kind_mapping = {
            1: "OWNS",
            2: "FULFILLS", 
            3: "CONTAINS",
            4: "FEEDS_POWER_TO",
            5: "PROVIDES_SERVICE_TO",
            6: "MANAGED_BY",
            7: "BACKUP_FOR",
            8: "ALLOCATED_TO"
        }
        return kind_mapping.get(kind_id, f"UNKNOWN_{kind_id}")
        
    def load_relationships(self, batch_size: int = 5000) -> Iterator[List[Dict[str, Any]]]:
        """
        Load relationships in batches from JSON files.
        
        Args:
            batch_size: Number of relationships per batch
            
        Yields:
            Batches of relationship dictionaries (already in UxM format)
        """
        relationship_files = [
            "ownership_relationships.json",
            "service_relationships.json", 
            "containment_relationships.json",
            "power_relationships.json"
        ]
        
        total_relationships = 0
        
        for filename in relationship_files:
            file_path = self.relationship_dir / filename
            if not file_path.exists():
                print(f"⚠️ Relationship file not found: {filename}")
                continue
                
            print(f"🔗 Loading relationships from {filename}...")
            
            with open(file_path, 'r') as f:
                relationships = json.load(f)
                
            # Process in batches
            batch = []
            for relationship in relationships:
                # Transform to Spanner format
                spanner_relationship = {
                    'relationship_id': f"rel_{relationship['a']['kind']}_{relationship['a']['name']}_to_{relationship['z']['kind']}_{relationship['z']['name']}_{relationship['kind']}",
                    'source_asset_id': f"{relationship['a']['kind']}_{relationship['a']['name']}",
                    'target_asset_id': f"{relationship['z']['kind']}_{relationship['z']['name']}",
                    'relationship_kind': self._map_relationship_kind(relationship['kind']),
                    'relationship_data': relationship,  # Store the entire original relationship as JSON
                    'tags': relationship.get('tags', []),
                    'version': relationship.get('version', 1),
                    'is_deleted': relationship.get('is_deleted', False)
                }
                batch.append(spanner_relationship)
                
                if len(batch) >= batch_size:
                    yield batch
                    total_relationships += len(batch)
                    batch = []
                    
            # Yield remaining relationships
            if batch:
                yield batch
                total_relationships += len(batch)
                
            print(f"   Loaded {len(relationships)} relationships from {filename}")
            
        print(f"✅ Total relationships loaded: {total_relationships:,}")
        
    def get_data_stats(self) -> Dict[str, Any]:
        """Get statistics about the available data."""
        stats = {
            'entity_files': {},
            'relationship_files': {},
            'total_entities': 0,
            'total_relationships': 0
        }
        
        # Count entities
        for entity_file in self.entity_dir.glob("*.json"):
            with open(entity_file, 'r') as f:
                entities = json.load(f)
                count = len(entities)
                stats['entity_files'][entity_file.name] = count
                stats['total_entities'] += count
                
        # Count relationships
        for rel_file in self.relationship_dir.glob("*.json"):
            with open(rel_file, 'r') as f:
                relationships = json.load(f)
                count = len(relationships)
                stats['relationship_files'][rel_file.name] = count
                stats['total_relationships'] += count
                
        return stats
        
    def validate_data_format(self) -> bool:
        """
        Validate that data is in proper UxM format.
        Quick spot check of a few records.
        """
        print("🔍 Validating UxM format...")
        
        # Check entity format
        customer_file = self.entity_dir / "customer.json"
        if customer_file.exists():
            with open(customer_file, 'r') as f:
                customers = json.load(f)
                if customers:
                    entity = customers[0]
                    required_fields = ['id', 'version', 'tags', 'properties', 'created_at', 'updated_at']
                    for field in required_fields:
                        if field not in entity:
                            print(f"❌ Missing required field '{field}' in entity")
                            return False
                    
                    # Check EntityID format
                    if 'kind' not in entity['id'] or 'name' not in entity['id']:
                        print("❌ Invalid EntityID format - missing 'kind' or 'name'")
                        return False
                        
        # Check relationship format
        ownership_file = self.relationship_dir / "ownership_relationships.json"
        if ownership_file.exists():
            with open(ownership_file, 'r') as f:
                relationships = json.load(f)
                if relationships:
                    rel = relationships[0]
                    required_fields = ['a', 'z', 'kind', 'version', 'tags', 'created_at', 'updated_at']
                    for field in required_fields:
                        if field not in rel:
                            print(f"❌ Missing required field '{field}' in relationship")
                            return False
                            
        print("✅ Data format validation passed - UxM format confirmed")
        return True
