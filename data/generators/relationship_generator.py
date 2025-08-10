"""
Relationship generator for Asset Brain UxM test data.

Generates bidirectional relationships between entities following
business rules and template validation.
"""

import csv
import json
import random
import sys
import time
from typing import Dict, List, Any, Set, Tuple
from datetime import datetime
import os
from pathlib import Path

# Add model path for template system
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root / 'model'))

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from model.template.registry import GlobalRegistry
from model.template.template import EntityKind, RelationshipKind
from model.template import register_all_templates


class RelationshipGenerator:
    """Generates UxM relationships with business rule validation."""
    
    def __init__(self, entity_data: Dict[str, List[Dict]], output_dir: str = "data/test_data/valid/relationships"):
        self.entity_data = entity_data
        self.output_dir = output_dir
        # Smart invalid_dir derivation
        if "/valid/" in output_dir:
            # Production case: test_data/valid/relationships -> test_data/invalid/relationships
            self.invalid_dir = output_dir.replace("/valid/", "/invalid/")
        else:
            # Test case: test_output -> test_output/invalid
            self.invalid_dir = os.path.join(output_dir, "invalid")
            
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(self.invalid_dir, exist_ok=True)
        
        # Initialize template registry for validation
        register_all_templates()  # Load all templates
        self.registry = GlobalRegistry
        
        # Timing metrics
        self.metrics = {
            'generation_times': {},
            'validation_times': {},
            'total_start_time': None,
            'total_end_time': None
        }
        
        # Track generated relationships to avoid duplicates
        self.generated_relationships = []
        self.relationship_set: Set[str] = set()
        
        # Define forward and reverse relationships for network team's validation approach
        self.forward_relationships = {
            RelationshipKind.RK_OWNS,
            RelationshipKind.RK_CONTAINS, 
            RelationshipKind.RK_FULFILLS,
            RelationshipKind.RK_FEEDS_POWER_TO
        }
        
        self.reverse_relationships = {
            RelationshipKind.RK_OWNED_BY,
            RelationshipKind.RK_CONTAINED_BY,
            RelationshipKind.RK_FULFILLED_BY, 
            RelationshipKind.RK_POWERED_BY
        }
    
    def _create_relationship_pair(self, source_entity_id: Dict, target_entity_id: Dict, 
                                 forward_kind: RelationshipKind, reverse_kind: RelationshipKind) -> List[Dict[str, Any]]:
        """Create bidirectional relationship pair following network team's JSON pattern."""
        timestamp = datetime.now().isoformat()
        
        # Forward relationship (source -> target) using network team's format
        forward_rel = {
            "version": 1,
            "kind": forward_kind.name,
            "a": source_entity_id,  # Source entity as structured object
            "z": target_entity_id,  # Target entity as structured object  
            "tags": ["TG_SYSTEM_GENERATED", "TG_BENCHMARK_SUITE"],
            "created_at": timestamp,
            "updated_at": timestamp
        }
        
        # Reverse relationship (target -> source)  
        reverse_rel = {
            "version": 1,
            "kind": reverse_kind.name,
            "a": target_entity_id,  # Target becomes source
            "z": source_entity_id,  # Source becomes target
            "tags": ["TG_SYSTEM_GENERATED", "TG_BENCHMARK_SUITE"],
            "created_at": timestamp,
            "updated_at": timestamp
        }
        
        return [forward_rel, reverse_rel]
    
    def generate_ownership_relationships(self) -> List[Dict[str, Any]]:
        """Generate customer OWNS cage and cabinet relationships using template validation."""
        relationships = []
        customers = self.entity_data['customer']
        cages = self.entity_data['cage']
        cabinets = self.entity_data['cabinet']
        
        # Customer OWNS Cage relationships
        for cage in cages:
            customer = random.choice(customers)
            
            rel_pair = self._create_relationship_pair(
                customer['id'], cage['id'],
                RelationshipKind.RK_OWNS, RelationshipKind.RK_OWNED_BY
            )
            relationships.extend(rel_pair)
        
        # Customer OWNS Cabinet relationships (some cabinets may have direct customer ownership)
        for cabinet in cabinets:
            if random.random() < 0.3:  # 30% of cabinets have direct customer ownership
                customer = random.choice(customers)
                
                rel_pair = self._create_relationship_pair(
                    customer['id'], cabinet['id'],
                    RelationshipKind.RK_OWNS, RelationshipKind.RK_OWNED_BY
                )
                relationships.extend(rel_pair)
        
        print(f"  Generated {len(relationships)} ownership relationships")
        return relationships
    
    def generate_service_relationships(self) -> List[Dict[str, Any]]:
        """Generate product FULFILLS cage/cabinet relationships using template validation."""
        start_time = time.time()
        relationships = []
        products = self.entity_data['product']
        cages = self.entity_data['cage']
        cabinets = self.entity_data['cabinet']
        
        # Assign service tiers to cages
        for cage in cages:
            product = random.choice(products)
            
            # Correct direction: Cage FULFILLS Product (cage provides service level to product)
            rel_pair = self._create_relationship_pair(
                cage['id'], product['id'],
                RelationshipKind.RK_FULFILLS, RelationshipKind.RK_FULFILLED_BY
            )
            relationships.extend(rel_pair)
        
        # Assign service tiers to cabinets (80% inherit from cage, 20% have independent service)
        for cabinet in cabinets:
            if random.random() < 0.2:
                product = random.choice(products)
                
                # Correct direction: Cabinet FULFILLS Product  
                rel_pair = self._create_relationship_pair(
                    cabinet['id'], product['id'],
                    RelationshipKind.RK_FULFILLS, RelationshipKind.RK_FULFILLED_BY
                )
                relationships.extend(rel_pair)
        
        end_time = time.time()
        self.metrics['generation_times']['service'] = end_time - start_time
        print(f"  Generated {len(relationships)} service relationships in {end_time - start_time:.2f}s")
        return relationships
    
    def generate_containment_relationships(self) -> List[Dict[str, Any]]:
        """Generate physical containment relationships using template validation."""
        relationships = []
        
        # DataCenter CONTAINS Utility
        datacenters = self.entity_data['datacenter']
        utilities = self.entity_data['utility']
        
        for utility in utilities:
            datacenter_id = utility['properties']['datacenter_id']
            
            rel_pair = self._create_relationship_pair(
                datacenter_id, utility['id'],
                RelationshipKind.RK_CONTAINS, RelationshipKind.RK_CONTAINED_BY
            )
            relationships.extend(rel_pair)
        
        # DataCenter CONTAINS UPS
        ups_systems = self.entity_data['ups']
        
        for ups in ups_systems:
            # Find datacenter through utility relationship
            utility_id = ups['properties']['utility_id']
            utility = next(u for u in utilities if u['id']['kind'] == utility_id['kind'] and u['id']['name'] == utility_id['name'])
            datacenter_id = utility['properties']['datacenter_id']
            
            rel_pair = self._create_relationship_pair(
                datacenter_id, ups['id'],
                RelationshipKind.RK_CONTAINS, RelationshipKind.RK_CONTAINED_BY
            )
            relationships.extend(rel_pair)
        
        # DataCenter CONTAINS PDU
        pdus = self.entity_data['pdu']
        
        for pdu in pdus:
            # Find datacenter through UPS relationship
            ups_id = pdu['properties']['ups_id']
            ups = next(u for u in ups_systems if u['id']['kind'] == ups_id['kind'] and u['id']['name'] == ups_id['name'])
            utility_id = ups['properties']['utility_id']
            utility = next(u for u in utilities if u['id']['kind'] == utility_id['kind'] and u['id']['name'] == utility_id['name'])
            datacenter_id = utility['properties']['datacenter_id']
            
            rel_pair = self._create_relationship_pair(
                datacenter_id, pdu['id'],
                RelationshipKind.RK_CONTAINS, RelationshipKind.RK_CONTAINED_BY
            )
            relationships.extend(rel_pair)
        
        # DataCenter CONTAINS Cage
        cages = self.entity_data['cage']
        
        for cage in cages:
            datacenter_id = cage['properties']['datacenter_id']
            
            rel_pair = self._create_relationship_pair(
                datacenter_id, cage['id'],
                RelationshipKind.RK_CONTAINS, RelationshipKind.RK_CONTAINED_BY
            )
            relationships.extend(rel_pair)
        
        # DataCenter CONTAINS Cabinet
        cabinets = self.entity_data['cabinet']
        
        for cabinet in cabinets:
            # Find datacenter through cage relationship
            cage_id = cabinet['properties']['cage_id']
            cage = next(c for c in cages if c['id']['kind'] == cage_id['kind'] and c['id']['name'] == cage_id['name'])
            datacenter_id = cage['properties']['datacenter_id']
            
            rel_pair = self._create_relationship_pair(
                datacenter_id, cabinet['id'],
                RelationshipKind.RK_CONTAINS, RelationshipKind.RK_CONTAINED_BY
            )
            relationships.extend(rel_pair)
        
        # Cage CONTAINS Cabinet
        for cabinet in cabinets:
            cage_id = cabinet['properties']['cage_id']
            
            rel_pair = self._create_relationship_pair(
                cage_id, cabinet['id'],
                RelationshipKind.RK_CONTAINS, RelationshipKind.RK_CONTAINED_BY
            )
            relationships.extend(rel_pair)
        
        print(f"  Generated {len(relationships)} containment relationships")
        return relationships
    
    def generate_power_relationships(self) -> List[Dict[str, Any]]:
        """Generate power chain relationships using template validation."""
        relationships = []
        
        # Utility FEEDS_POWER_TO PDU (direct connection)
        utilities = self.entity_data['utility']
        pdus = self.entity_data['pdu']
        ups_systems = self.entity_data['ups']
        cages = self.entity_data['cage']
        cabinets = self.entity_data['cabinet']
        
        # Group PDUs by datacenter for proper power distribution
        pdu_by_datacenter = {}
        for pdu in pdus:
            ups_id = pdu['properties']['ups_id']
            ups = next(u for u in ups_systems if u['id']['kind'] == ups_id['kind'] and u['id']['name'] == ups_id['name'])
            utility_id = ups['properties']['utility_id']
            utility = next(u for u in utilities if u['id']['kind'] == utility_id['kind'] and u['id']['name'] == utility_id['name'])
            datacenter_id = utility['properties']['datacenter_id']
            
            # Create string key from EntityID for dictionary grouping
            datacenter_key = f"{datacenter_id['kind']}_{datacenter_id['name']}"
            
            if datacenter_key not in pdu_by_datacenter:
                pdu_by_datacenter[datacenter_key] = []
            pdu_by_datacenter[datacenter_key].append(pdu)
        
        # Utility FEEDS_POWER_TO PDU
        for utility in utilities:
            datacenter_id = utility['properties']['datacenter_id']
            datacenter_key = f"{datacenter_id['kind']}_{datacenter_id['name']}"
            datacenter_pdus = pdu_by_datacenter.get(datacenter_key, [])
            
            # Each utility feeds power to multiple PDUs in its datacenter
            num_pdus = min(5, len(datacenter_pdus))  # Each utility feeds up to 5 PDUs
            if datacenter_pdus:
                selected_pdus = random.sample(datacenter_pdus, min(num_pdus, len(datacenter_pdus)))
                
                for pdu in selected_pdus:
                    rel_pair = self._create_relationship_pair(
                        utility['id'], pdu['id'],
                        RelationshipKind.RK_FEEDS_POWER_TO, RelationshipKind.RK_POWERED_BY
                    )
                    relationships.extend(rel_pair)
        
        # UPS FEEDS_POWER_TO PDU
        for pdu in pdus:
            ups_id = pdu['properties']['ups_id']
            
            rel_pair = self._create_relationship_pair(
                ups_id, pdu['id'],
                RelationshipKind.RK_FEEDS_POWER_TO, RelationshipKind.RK_POWERED_BY
            )
            relationships.extend(rel_pair)
        
        # PDU FEEDS_POWER_TO Cage
        for cage in cages:
            datacenter_id = cage['properties']['datacenter_id']
            datacenter_key = f"{datacenter_id['kind']}_{datacenter_id['name']}"
            available_pdus = pdu_by_datacenter.get(datacenter_key, [])
            
            if available_pdus:
                # Each cage gets 1-3 PDUs for redundancy
                num_pdus = random.randint(1, min(3, len(available_pdus)))
                selected_pdus = random.sample(available_pdus, num_pdus)
                
                for pdu in selected_pdus:
                    rel_pair = self._create_relationship_pair(
                        pdu['id'], cage['id'],
                        RelationshipKind.RK_FEEDS_POWER_TO, RelationshipKind.RK_POWERED_BY
                    )
                    relationships.extend(rel_pair)
        
        # Cage FEEDS_POWER_TO Cabinet
        for cabinet in cabinets:
            cage_id = cabinet['properties']['cage_id']
            
            rel_pair = self._create_relationship_pair(
                cage_id, cabinet['id'],
                RelationshipKind.RK_FEEDS_POWER_TO, RelationshipKind.RK_POWERED_BY
            )
            relationships.extend(rel_pair)
        
        print(f"  Generated {len(relationships)} power relationships")
        return relationships
    
    def _get_complementary_relationship(self, rel_kind: RelationshipKind) -> RelationshipKind:
        """Get the complementary relationship type for validation."""
        complementary_map = {
            RelationshipKind.RK_OWNS: RelationshipKind.RK_OWNED_BY,
            RelationshipKind.RK_OWNED_BY: RelationshipKind.RK_OWNS,
            RelationshipKind.RK_CONTAINS: RelationshipKind.RK_CONTAINED_BY,
            RelationshipKind.RK_CONTAINED_BY: RelationshipKind.RK_CONTAINS,
            RelationshipKind.RK_FULFILLS: RelationshipKind.RK_FULFILLED_BY,
            RelationshipKind.RK_FULFILLED_BY: RelationshipKind.RK_FULFILLS,
            RelationshipKind.RK_FEEDS_POWER_TO: RelationshipKind.RK_POWERED_BY,
            RelationshipKind.RK_POWERED_BY: RelationshipKind.RK_FEEDS_POWER_TO,
        }
        return complementary_map.get(rel_kind, rel_kind)

    def validate_and_separate_relationships(self, relationships: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Validate relationships using network team's forward/reverse approach."""
        start_time = time.time()
        print("Validating relationships against templates...")
        
        valid_relationships = []
        invalid_relationships = []
        validation_errors = []
        
        # Build a lookup for existing forward relationships to implement network team's logic
        forward_relationships_lookup = set()
        
        for rel in relationships:
            try:
                rel_kind = RelationshipKind[rel['kind']]
                if rel_kind in self.forward_relationships:
                    # Add null checks before accessing dictionary keys
                    if rel.get('a') and rel.get('z') and isinstance(rel['a'], dict) and isinstance(rel['z'], dict):
                        a_key = f"{rel['a']['kind']}_{rel['a']['name']}"
                        z_key = f"{rel['z']['kind']}_{rel['z']['name']}"
                        forward_key = f"{a_key}__{rel_kind.name}__{z_key}"
                        forward_relationships_lookup.add(forward_key)
            except:
                continue
        
        for rel in relationships:
            try:
                # Get relationship kind from string name
                rel_kind = RelationshipKind[rel['kind']]
                
                # Get source and target entity IDs (now using a/z format)
                source_entity_id = rel['a']  # 'a' is source in network team's format
                target_entity_id = rel['z']  # 'z' is target in network team's format
                
                # Add null checks to prevent NoneType errors
                if not source_entity_id or not target_entity_id:
                    error_msg = f"Missing entity ID: source={source_entity_id}, target={target_entity_id}"
                    validation_errors.append(error_msg)
                    rel['validation_error'] = error_msg
                    invalid_relationships.append(rel)
                    continue
                
                if not isinstance(source_entity_id, dict) or not isinstance(target_entity_id, dict):
                    error_msg = f"Invalid entity ID format: source={type(source_entity_id)}, target={type(target_entity_id)}"
                    validation_errors.append(error_msg)
                    rel['validation_error'] = error_msg
                    invalid_relationships.append(rel)
                    continue
                
                # Network team's validation approach
                if rel_kind in self.forward_relationships:
                    # Forward relationship: Always validate against templates
                    validation_result = self._validate_forward_relationship(
                        source_entity_id, target_entity_id, rel_kind
                    )
                elif rel_kind in self.reverse_relationships:
                    # Reverse relationship: Only validate if corresponding forward doesn't exist
                    forward_kind = self._get_complementary_relationship(rel_kind)
                    z_key = f"{target_entity_id['kind']}_{target_entity_id['name']}"
                    a_key = f"{source_entity_id['kind']}_{source_entity_id['name']}"
                    forward_key = f"{z_key}__{forward_kind.name}__{a_key}"
                    
                    if forward_key in forward_relationships_lookup:
                        # Forward relationship exists, skip validation for reverse
                        valid_relationships.append(rel)
                        continue
                    else:
                        # No forward relationship found, validate this reverse relationship
                        validation_result = self._validate_forward_relationship(
                            source_entity_id, target_entity_id, rel_kind
                        )
                else:
                    # Unknown relationship type
                    error_msg = f"Unknown relationship type: {rel_kind.name}"
                    validation_errors.append(error_msg)
                    rel['validation_error'] = error_msg
                    invalid_relationships.append(rel)
                    continue
                
                # Process validation result
                if validation_result['valid']:
                    valid_relationships.append(rel)
                else:
                    rel['validation_error'] = validation_result['error']
                    validation_errors.append(validation_result['error'])
                    invalid_relationships.append(rel)
                    
            except Exception as e:
                error_msg = f"Validation error for relationship: {str(e)}"
                validation_errors.append(error_msg)
                rel['validation_error'] = error_msg
                invalid_relationships.append(rel)
        
        end_time = time.time()
        self.metrics['validation_times']['relationships'] = end_time - start_time
        
        print(f"✅ Relationship validation completed in {end_time - start_time:.2f}s")
        print(f"  Valid relationships: {len(valid_relationships):,}")
        print(f"  Invalid relationships: {len(invalid_relationships):,}")
        
        if len(invalid_relationships) > 0:
            print(f"  First 5 validation errors:")
            for error in validation_errors[:5]:
                print(f"    - {error}")
        
        return valid_relationships, invalid_relationships
    
    def _validate_forward_relationship(self, source_entity_id: Dict, target_entity_id: Dict, 
                                     rel_kind: RelationshipKind) -> Dict[str, Any]:
        """Validate a forward relationship against templates."""
        try:
            # Find source and target entities by matching EntityID structure
            source_entity = None
            target_entity = None
            
            for entity_type, entities in self.entity_data.items():
                for entity in entities:
                    # Match using EntityID structure {kind: X, name: Y}
                    if (entity['id']['kind'] == source_entity_id['kind'] and 
                        entity['id']['name'] == source_entity_id['name']):
                        source_entity = entity
                    if (entity['id']['kind'] == target_entity_id['kind'] and 
                        entity['id']['name'] == target_entity_id['name']):
                        target_entity = entity
            
            if not source_entity:
                return {
                    'valid': False,
                    'error': f"Source entity {source_entity_id} not found"
                }
                
            if not target_entity:
                return {
                    'valid': False, 
                    'error': f"Target entity {target_entity_id} not found"
                }
            
            source_kind = EntityKind(source_entity['id']['kind'])
            target_kind = EntityKind(target_entity['id']['kind'])
            
            # Get templates for validation
            source_template = self.registry.get_template(source_kind)
            target_template = self.registry.get_template(target_kind)
            
            if not source_template:
                return {
                    'valid': False,
                    'error': f"No template for source entity kind: {source_kind.name}"
                }
                
            if not target_template:
                return {
                    'valid': False,
                    'error': f"No template for target entity kind: {target_kind.name}"
                }
            
            # Validate relationship is allowed from source perspective
            source_can_create = (
                rel_kind in source_template.allowed_child_relationships and 
                target_kind in source_template.allowed_children
            )
            
            # Validate that target can accept the COMPLEMENTARY relationship from source
            complementary_rel_kind = self._get_complementary_relationship(rel_kind)
            target_can_accept = (
                complementary_rel_kind in target_template.allowed_parent_relationships and
                source_kind in target_template.allowed_parents
            )
            
            if not source_can_create:
                return {
                    'valid': False,
                    'error': f"Source {source_kind.name} cannot create {rel_kind.name} relationship with {target_kind.name}"
                }
                
            if not target_can_accept:
                return {
                    'valid': False,
                    'error': f"Target {target_kind.name} cannot accept {complementary_rel_kind.name} relationship from {source_kind.name} (complementary to {rel_kind.name})"
                }
            
            return {'valid': True, 'error': None}
            
        except Exception as e:
            return {
                'valid': False,
                'error': f"Validation exception: {str(e)}"
            }

    def save_relationships_to_json(self, relationships: List[Dict[str, Any]], filename: str, is_invalid: bool = False):
        """Save relationships to JSON file using network team's format."""
        if is_invalid:
            file_path = os.path.join(self.invalid_dir, filename)
        else:
            file_path = os.path.join(self.output_dir, filename)
        
        if not relationships:
            return
        
        with open(file_path, 'w') as f:
            json.dump(relationships, f, indent=2)
        
        status = "invalid" if is_invalid else "valid"
        print(f"  Saved {len(relationships)} {status} relationships to {file_path}")

    def save_relationships_to_csv(self, relationships: List[Dict[str, Any]], filename: str, is_invalid: bool = False):
        """Legacy CSV save method - now redirects to JSON."""
        # Convert CSV filename to JSON
        json_filename = filename.replace('.csv', '.json')
        self.save_relationships_to_json(relationships, json_filename, is_invalid)

    def print_timing_metrics(self):
        """Print detailed timing metrics for relationships."""
        if self.metrics['total_end_time'] is None:
            print("⚠️ Timing metrics incomplete due to early termination")
            return
            
        total_time = self.metrics['total_end_time'] - self.metrics['total_start_time']
        
        print("\n" + "=" * 60)
        print("RELATIONSHIP TIMING METRICS")
        print("=" * 60)
        print(f"Total relationship generation time: {total_time:.2f}s")
        
        print("\nRelationship generation times:")
        for rel_type, duration in self.metrics['generation_times'].items():
            print(f"  {rel_type:12}: {duration:.2f}s")
        
        if 'validation_times' in self.metrics:
            print(f"\nValidation time: {self.metrics['validation_times'].get('relationships', 0):.2f}s")
        
        if 'save_time' in self.metrics:
            print(f"File save time: {self.metrics['save_time']:.2f}s")
        
        # Calculate breakdown percentages
        generation_time = sum(self.metrics['generation_times'].values())
        validation_time = self.metrics['validation_times'].get('relationships', 0)
        save_time = self.metrics.get('save_time', 0)
        
        print(f"\nTime breakdown:")
        print(f"  Generation: {generation_time:.2f}s ({generation_time/total_time*100:.1f}%)")
        print(f"  Validation: {validation_time:.2f}s ({validation_time/total_time*100:.1f}%)")
        print(f"  File I/O:   {save_time:.2f}s ({save_time/total_time*100:.1f}%)")
        print("=" * 60)
    
    def generate_all_relationships(self) -> Dict[str, List[Dict[str, Any]]]:
        """Generate all relationship types with template validation."""
        print("Generating Asset Brain relationships using UxM templates...")
        
        all_relationships = {}
        
        print("1. Generating ownership relationships...")
        ownership = self.generate_ownership_relationships()
        all_relationships['ownership'] = ownership
        
        print("2. Generating service relationships...")
        service = self.generate_service_relationships()
        all_relationships['service'] = service
        
        print("3. Generating containment relationships...")
        containment = self.generate_containment_relationships()
        all_relationships['containment'] = containment
        
        print("4. Generating power relationships...")
        power = self.generate_power_relationships()
        all_relationships['power'] = power
        
        # Combine all relationships
        combined_relationships = []
        for rel_type, rels in all_relationships.items():
            combined_relationships.extend(rels)
        
        # Validate and separate relationships
        print("\n5. Validating relationships against templates...")
        valid_rels, invalid_rels = self.validate_and_separate_relationships(combined_relationships)
        
        if not valid_rels:
            print("  WARNING: No valid relationships generated!")
        
        # Save valid relationships
        print("\n6. Saving relationships to files...")
        save_start = time.time()
        
        for rel_type, rels in all_relationships.items():
            # Separate valid/invalid for each type using relationship content matching
            valid_type_rels = []
            invalid_type_rels = []
            
            for rel in rels:
                # Create a unique key for matching since we no longer use IDs
                rel_key = f"{rel['a']['kind']}_{rel['a']['name']}__{rel['kind']}__{rel['z']['kind']}_{rel['z']['name']}"
                is_valid = any(
                    f"{v['a']['kind']}_{v['a']['name']}__{v['kind']}__{v['z']['kind']}_{v['z']['name']}" == rel_key 
                    for v in valid_rels
                )
                if is_valid:
                    valid_type_rels.append(rel)
                else:
                    # Find the invalid version with error
                    invalid_rel = next(
                        (inv for inv in invalid_rels if 
                         f"{inv['a']['kind']}_{inv['a']['name']}__{inv['kind']}__{inv['z']['kind']}_{inv['z']['name']}" == rel_key), 
                        rel
                    )
                    invalid_type_rels.append(invalid_rel)
            
            # Save individual relationship types as JSON files
            if valid_type_rels:
                self.save_relationships_to_json(valid_type_rels, f"{rel_type}_relationships.json")
            
            if invalid_type_rels:
                self.save_relationships_to_json(invalid_type_rels, f"{rel_type}_relationships_invalid.json", is_invalid=True)
        
        # Note: No combined "all_relationships" file - each type gets its own file
        
        self.metrics['save_time'] = time.time() - save_start
        
        # End total timing
        self.metrics['total_end_time'] = time.time()
        
        # Print summary
        print(f"\n✅ Generated relationships (valid/invalid):")
        for rel_type, rels in all_relationships.items():
            valid_count = sum(1 for rel in rels if any(
                f"{v['a']['kind']}_{v['a']['name']}__{v['kind']}__{v['z']['kind']}_{v['z']['name']}" == 
                f"{rel['a']['kind']}_{rel['a']['name']}__{rel['kind']}__{rel['z']['kind']}_{rel['z']['name']}"
                for v in valid_rels
            ))
            invalid_count = len(rels) - valid_count
            print(f"  {rel_type}: {valid_count:,} valid, {invalid_count:,} invalid")
        
        print(f"\nTotal: {len(valid_rels):,} valid, {len(invalid_rels):,} invalid relationships")
        
        # Ensure timing metrics are set before printing
        if self.metrics['total_end_time'] is None:
            self.metrics['total_end_time'] = time.time()
        
        # Print timing metrics
        try:
            self.print_timing_metrics()
        except Exception as e:
            print(f"Warning: Could not print timing metrics: {e}")
        
        self.generated_relationships = valid_rels
        return all_relationships


if __name__ == "__main__":
    # Load entity data (example usage)
    import sys
    sys.path.append('.')
    
    from entity_generator import EntityGenerator
    
    # Generate entities first
    entity_gen = EntityGenerator()
    entity_data = entity_gen.generate_all_entities()
    
    # Generate relationships
    rel_gen = RelationshipGenerator(entity_data)
    rel_gen.generate_all_relationships()
