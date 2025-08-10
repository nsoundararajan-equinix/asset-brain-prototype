"""
Entity generator for Asset Brain UxM test data.

Generates enterprise-scale entities following the benchmark distribution
and UxM patterns with proper business logic using the template validation system.
"""

import json
import random
import sys
import time
from typing import Dict, List, Any, Tuple
import os
from datetime import datetime
from pathlib import Path

# Add model path for template system
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root / 'model'))

from model.template.registry import GlobalRegistry
from model.template.template import EntityKind, RelationshipKind
from model.template import register_all_templates


class EntityCounts:
    """Target entity counts - defaults to small test values."""
    def __init__(self):
        self.datacenter = 1
        self.utility = 2       # 2 per datacenter (small test)
        self.ups = 2           # 1 per utility (small test)
        self.pdu = 4           # 2 per UPS (small test)
        self.cage = 2          # 2 per datacenter (small test)
        self.cabinet = 4       # 2 per cage (small test)
        self.customer = 2      # Multi-tenant (small test)
        self.product = 2       # Service tiers


class ProductionEntityCounts(EntityCounts):
    """Production-scale entity counts matching benchmark suite."""
    def __init__(self):
        super().__init__()
        self.datacenter = 10
        self.utility = 40      # 4 per datacenter
        self.ups = 200         # 5 per utility  
        self.pdu = 5000        # 25 per UPS
        self.cage = 5000       # 500 per datacenter
        self.cabinet = 50000   # 10 per cage
        self.customer = 1000   # Multi-tenant
        self.product = 2       # Service tiers


class EntityGenerator:
    """Generates UxM entities at enterprise scale using template validation."""
    
    def __init__(self, output_dir: str = "data/test_data/valid/entities"):
        self.output_dir = output_dir
        # Smart invalid_dir derivation
        if "/valid/" in output_dir:
            # Production case: test_data/valid/entities -> test_data/invalid/entities
            self.invalid_dir = output_dir.replace("/valid/", "/invalid/")
        else:
            # Test case: test_output -> test_output/invalid
            self.invalid_dir = os.path.join(output_dir, "invalid")
        
        self.counts = EntityCounts()
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
        
        # Business data pools
        self.regions = ["US-East", "US-West", "EU-West", "APAC", "US-Central"]
        # Note: customer_names and datacenter_locations are now properties that use current counts
        
        # Entity storage for relationship generation
        self.generated_entities = {
            'datacenter': [],
            'utility': [],
            'ups': [],
            'pdu': [],
            'cage': [],
            'cabinet': [],
            'customer': [],
            'product': []
        }
        
    def _generate_customer_names(self) -> List[str]:
        """Generate realistic customer company names."""
        prefixes = ["Tech", "Global", "Enterprise", "Digital", "Cloud", "Data", "Secure", "Prime", "Elite", "Quantum"]
        suffixes = ["Corp", "Systems", "Solutions", "Technologies", "Enterprises", "Group", "Holdings", "Industries", "Networks", "Labs"]
        
        names = []
        for i in range(self.counts.customer):
            if i < len(prefixes) * len(suffixes):
                prefix = prefixes[i // len(suffixes)]
                suffix = suffixes[i % len(suffixes)]
                names.append(f"{prefix} {suffix}")
            else:
                # Generate additional names with numbers
                prefix = random.choice(prefixes)
                suffix = random.choice(suffixes)
                names.append(f"{prefix} {suffix} {i}")
        
        return names
    
    def _generate_datacenter_locations(self) -> List[Dict[str, str]]:
        """Generate datacenter locations across regions based on counts."""
        all_locations = [
            {"name": "NYC-01", "location": "New York, NY", "region": "US-East"},
            {"name": "NYC-02", "location": "Secaucus, NJ", "region": "US-East"},
            {"name": "SV-01", "location": "San Jose, CA", "region": "US-West"},
            {"name": "SV-02", "location": "Santa Clara, CA", "region": "US-West"},
            {"name": "LON-01", "location": "London, UK", "region": "EU-West"},
            {"name": "LON-02", "location": "Slough, UK", "region": "EU-West"},
            {"name": "SG-01", "location": "Singapore", "region": "APAC"},
            {"name": "HK-01", "location": "Hong Kong", "region": "APAC"},
            {"name": "CHI-01", "location": "Chicago, IL", "region": "US-Central"},
            {"name": "DAL-01", "location": "Dallas, TX", "region": "US-Central"}
        ]
        # Return only the number of datacenters specified in counts
        return all_locations[:self.counts.datacenter]
    
    @property
    def datacenter_locations(self):
        """Get datacenter locations based on current counts."""
        return self._generate_datacenter_locations()
    
    @property
    def customer_names(self):
        """Get customer names based on current counts."""
        return self._generate_customer_names()
    
    def _create_base_entity(self, entity_kind: EntityKind, name: str, **properties) -> Dict[str, Any]:
        """Create base UxM entity structure following network team's EntityID pattern."""
        
        entity = {
            "id": {
                "kind": entity_kind.value,  # Use enum value for kind
                "name": name               # Unique name within the kind
            },
            "version": 1,
            "tags": ["TG_SYSTEM_GENERATED", "TG_BENCHMARK_SUITE"],
            "properties": properties,
            "created_at": "2025-08-07T00:00:00Z",
            "updated_at": "2025-08-07T00:00:00Z"
        }
        
        return entity
    
    def _time_generation(self, entity_type: str, generation_func) -> List[Dict[str, Any]]:
        """Wrapper to time entity generation."""
        start_time = time.time()
        result = generation_func()
        end_time = time.time()
        self.metrics['generation_times'][entity_type] = end_time - start_time
        print(f"  Generated {len(result)} {entity_type} entities in {end_time - start_time:.2f}s")
        return result
    
    def generate_products(self) -> List[Dict[str, Any]]:
        """Generate 2 product service tiers."""
        products = []
        
        # Standard service tier
        standard = self._create_base_entity(
            EntityKind.EK_PRODUCT,
            "Standard Colocation Service",
            description="Standard colocation service with basic power and cooling",
            service_tier="standard",
            power_density_kw_per_cabinet=5.0,
            cooling_redundancy="N+1",
            sla_uptime=99.9
        )
        products.append(standard)
        
        # Premium service tier  
        premium = self._create_base_entity(
            EntityKind.EK_PRODUCT,
            "Premium Colocation Service",
            description="Premium colocation with enhanced power density and redundancy",
            service_tier="premium",
            power_density_kw_per_cabinet=10.0,
            cooling_redundancy="2N",
            sla_uptime=99.99
        )
        products.append(premium)
        
        self.generated_entities['product'] = products
        return products
    
    def generate_customers(self) -> List[Dict[str, Any]]:
        """Generate customer entities."""
        customers = []
        
        # Use all the customer names (already sized to self.counts.customer)
        for i, name in enumerate(self.customer_names):
            customer = self._create_base_entity(
                EntityKind.EK_CUSTOMER,
                name,
                account_id=f"ACCT-{i+1:06d}",
                region=random.choice(self.regions),
                contract_type=random.choice(["ENTERPRISE", "SMB", "STARTUP"]),
                active_since="2020-01-01T00:00:00Z",
                billing_contact=f"billing@{name.lower().replace(' ', '')}.com"
            )
            customers.append(customer)
        
        self.generated_entities['customer'] = customers
        return customers
    
    def generate_datacenters(self) -> List[Dict[str, Any]]:
        """Generate datacenter entities."""
        datacenters = []
        
        # Use all the datacenter locations (already sized to self.counts.datacenter)
        for location in self.datacenter_locations:
            datacenter = self._create_base_entity(
                EntityKind.EK_DATACENTER,
                location["name"],
                location=location["location"],
                region=location["region"],
                total_space_sqft=random.randint(50000, 200000),
                power_capacity_mw=random.randint(10, 50),
                tier_certification="TIER_III",
                operational_since="2018-01-01T00:00:00Z"
            )
            datacenters.append(datacenter)
        
        self.generated_entities['datacenter'] = datacenters
        return datacenters
    
    def generate_utilities(self) -> List[Dict[str, Any]]:
        """Generate utility entities (4 per datacenter, or exact count for testing)."""
        utilities = []
        
        # For testing with small counts, generate exact number
        if self.counts.utility <= 10:  # Small test scenario
            for i in range(self.counts.utility):
                # Use first datacenter if available, otherwise create simple utility
                datacenter = self.generated_entities['datacenter'][0] if self.generated_entities['datacenter'] else None
                if datacenter:
                    utility_name = f"{datacenter['id']['name']}-UTIL-{i+1:02d}"
                    datacenter_id = datacenter['id']
                else:
                    utility_name = f"TEST-UTIL-{i+1:02d}"
                    datacenter_id = None
                
                utility = self._create_base_entity(
                    EntityKind.EK_UTILITY,
                    utility_name,
                    datacenter_id=datacenter_id,
                    utility_provider=random.choice(["ConEd", "PG&E", "Southern Company", "UK Power Networks"]),
                    voltage_primary_kv=random.choice([13.8, 22.0, 35.0]),
                    capacity_mw=random.randint(5, 15),
                    redundancy_level="N+1",
                    service_entrance_type="PRIMARY"
                )
                utilities.append(utility)
        else:
            # Production scenario: 4 utilities per datacenter
            for datacenter in self.generated_entities['datacenter']:
                for i in range(4):  # 4 utilities per datacenter
                    utility_name = f"{datacenter['id']['name']}-UTIL-{i+1:02d}"
                    
                    utility = self._create_base_entity(
                        EntityKind.EK_UTILITY,
                        utility_name,
                        datacenter_id=datacenter['id'],
                        utility_provider=random.choice(["ConEd", "PG&E", "Southern Company", "UK Power Networks"]),
                        voltage_primary_kv=random.choice([13.8, 22.0, 35.0]),
                        capacity_mw=random.randint(5, 15),
                        redundancy_level="N+1",
                        service_entrance_type="PRIMARY"
                    )
                    utilities.append(utility)
        
        self.generated_entities['utility'] = utilities
        return utilities
    
    def generate_ups_systems(self) -> List[Dict[str, Any]]:
        """Generate UPS entities (5 per utility, or exact count for testing)."""
        ups_systems = []
        
        # For testing with small counts, generate exact number
        if self.counts.ups <= 10:  # Small test scenario
            for i in range(self.counts.ups):
                # Use first utility if available, otherwise create simple UPS
                utility = self.generated_entities['utility'][0] if self.generated_entities['utility'] else None
                if utility:
                    ups_name = f"{utility['id']['name']}-UPS-{i+1:02d}"
                    utility_id = utility['id']
                else:
                    ups_name = f"TEST-UPS-{i+1:02d}"
                    utility_id = None
                
                ups = self._create_base_entity(
                    EntityKind.EK_UPS,
                    ups_name,
                    utility_id=utility_id,
                    manufacturer=random.choice(["Schneider Electric", "Eaton", "Vertiv", "ABB"]),
                    model=f"Model-{random.randint(100, 999)}",
                    capacity_kva=random.choice([500, 750, 1000, 1500, 2000]),
                    battery_runtime_minutes=random.randint(10, 30),
                    efficiency_percent=random.uniform(94.0, 98.0),
                    redundancy_configuration="N+1"
                )
                ups_systems.append(ups)
        else:
            # Production scenario: 5 UPS per utility
            for utility in self.generated_entities['utility']:
                for i in range(5):  # 5 UPS per utility
                    ups_name = f"{utility['id']['name']}-UPS-{i+1:02d}"
                    
                    ups = self._create_base_entity(
                        EntityKind.EK_UPS,
                        ups_name,
                        utility_id=utility['id'],
                        manufacturer=random.choice(["Schneider Electric", "Eaton", "Vertiv", "ABB"]),
                        model=f"Model-{random.randint(100, 999)}",
                        capacity_kva=random.choice([500, 750, 1000, 1500, 2000]),
                        battery_runtime_minutes=random.randint(10, 30),
                        efficiency_percent=random.uniform(94.0, 98.0),
                        redundancy_configuration="N+1"
                    )
                    ups_systems.append(ups)
        
        self.generated_entities['ups'] = ups_systems
        return ups_systems
    
    def generate_pdus(self) -> List[Dict[str, Any]]:
        """Generate PDU entities (25 per UPS, or exact count for testing)."""
        pdus = []
        
        # For testing with small counts, generate exact number
        if self.counts.pdu <= 50:  # Small test scenario
            for i in range(self.counts.pdu):
                # Use first UPS if available, otherwise create simple PDU
                ups = self.generated_entities['ups'][0] if self.generated_entities['ups'] else None
                if ups:
                    pdu_name = f"{ups['id']['name']}-PDU-{i+1:03d}"
                    ups_id = ups['id']
                else:
                    pdu_name = f"TEST-PDU-{i+1:03d}"
                    ups_id = None
                
                pdu = self._create_base_entity(
                    EntityKind.EK_PDU,
                    pdu_name,
                    ups_id=ups_id,
                    manufacturer=random.choice(["Raritan", "APC", "Server Technology", "Geist"]),
                    model=f"PDU-{random.randint(1000, 9999)}",
                    input_voltage=random.choice([208, 240, 415, 480]),
                    output_outlets=random.randint(24, 48),
                    max_current_amps=random.randint(20, 60),
                    phase_configuration=random.choice(["1P", "3P"]),
                    monitoring_capable=True
                )
                pdus.append(pdu)
        else:
            # Production scenario: 25 PDUs per UPS
            for ups in self.generated_entities['ups']:
                for i in range(25):  # 25 PDUs per UPS
                    pdu_name = f"{ups['id']['name']}-PDU-{i+1:03d}"
                    
                    pdu = self._create_base_entity(
                        EntityKind.EK_PDU,
                        pdu_name,
                        ups_id=ups['id'],
                        manufacturer=random.choice(["Raritan", "APC", "Server Technology", "Geist"]),
                        model=f"PDU-{random.randint(1000, 9999)}",
                        input_voltage=random.choice([208, 240, 415, 480]),
                        output_outlets=random.randint(24, 48),
                        max_current_amps=random.randint(20, 60),
                        phase_configuration=random.choice(["1P", "3P"]),
                        monitoring_capable=True
                    )
                    pdus.append(pdu)
        
        self.generated_entities['pdu'] = pdus
        return pdus
    
    def generate_cages(self) -> List[Dict[str, Any]]:
        """Generate cage entities (500 per datacenter, or exact count for testing)."""
        cages = []
        
        # For testing with small counts, generate exact number
        if self.counts.cage <= 20:  # Small test scenario
            for i in range(self.counts.cage):
                # Use first datacenter if available, otherwise create simple cage
                datacenter = self.generated_entities['datacenter'][0] if self.generated_entities['datacenter'] else None
                if datacenter:
                    cage_name = f"{datacenter['id']['name']}-CAGE-{i+1:04d}"
                    datacenter_id = datacenter['id']
                else:
                    cage_name = f"TEST-CAGE-{i+1:04d}"
                    datacenter_id = None
                
                cage = self._create_base_entity(
                    EntityKind.EK_CAGE,
                    cage_name,
                    datacenter_id=datacenter_id,
                    size_sqft=random.choice([100, 200, 300, 500, 1000]),
                    max_cabinets=random.randint(4, 20),
                    power_allocation_kw=random.randint(20, 200),
                    cooling_zone=f"ZONE-{random.randint(1, 8)}",
                    access_level=random.choice(["CUSTOMER", "MANAGED", "HYBRID"])
                )
                cages.append(cage)
        else:
            # Production scenario: 500 cages per datacenter
            for datacenter in self.generated_entities['datacenter']:
                for i in range(500):  # 500 cages per datacenter
                    cage_name = f"{datacenter['id']['name']}-CAGE-{i+1:04d}"
                    
                    cage = self._create_base_entity(
                        EntityKind.EK_CAGE,
                        cage_name,
                        datacenter_id=datacenter['id'],
                        size_sqft=random.choice([100, 200, 300, 500, 1000]),
                        max_cabinets=random.randint(4, 20),
                        power_allocation_kw=random.randint(20, 200),
                        cooling_zone=f"ZONE-{random.randint(1, 8)}",
                        access_level=random.choice(["CUSTOMER", "MANAGED", "HYBRID"])
                    )
                    cages.append(cage)
        
        self.generated_entities['cage'] = cages
        return cages
    
    def generate_cabinets(self) -> List[Dict[str, Any]]:
        """Generate cabinet entities (10 per cage, or exact count for testing)."""
        cabinets = []
        
        # For testing with small counts, generate exact number
        if self.counts.cabinet <= 20:  # Small test scenario
            for i in range(self.counts.cabinet):
                # Use first cage if available, otherwise create simple cabinet
                cage = self.generated_entities['cage'][0] if self.generated_entities['cage'] else None
                if cage:
                    cabinet_name = f"{cage['id']['name']}-CAB-{i+1:02d}"
                    cage_id = cage['id']
                else:
                    cabinet_name = f"TEST-CAB-{i+1:02d}"
                    cage_id = None
                
                cabinet = self._create_base_entity(
                    EntityKind.EK_CABINET,
                    cabinet_name,
                    cage_id=cage_id,
                    height_ru=random.choice([42, 45, 47, 48]),
                    width_inches=random.choice([19, 23, 24]),
                    depth_inches=random.choice([36, 42, 48]),
                    power_strips=random.randint(2, 8),
                    max_weight_lbs=random.randint(2000, 4000),
                    ventilation_type=random.choice(["FRONT_TO_BACK", "SIDE_TO_SIDE"])
                )
                cabinets.append(cabinet)
        else:
            # Production scenario: 10 cabinets per cage
            for cage in self.generated_entities['cage']:
                cabinet_count = min(10, cage['properties']['max_cabinets'])
                
                for i in range(cabinet_count):
                    cabinet_name = f"{cage['id']['name']}-CAB-{i+1:02d}"
                    
                    cabinet = self._create_base_entity(
                        EntityKind.EK_CABINET,
                        cabinet_name,
                        cage_id=cage['id'],
                        height_ru=random.choice([42, 45, 47, 48]),
                        width_inches=random.choice([19, 23, 24]),
                        depth_inches=random.choice([36, 42, 48]),
                        power_strips=random.randint(2, 8),
                        max_weight_lbs=random.randint(2000, 4000),
                        ventilation_type=random.choice(["FRONT_TO_BACK", "SIDE_TO_SIDE"])
                    )
                    cabinets.append(cabinet)
        
        self.generated_entities['cabinet'] = cabinets
        return cabinets
    
    def validate_and_separate_entities(self) -> Tuple[Dict[str, List], Dict[str, List]]:
        """Validate entities and separate valid from invalid ones."""
        start_time = time.time()
        print("Validating generated entities against templates...")
        
        valid_entities = {}
        invalid_entities = {}
        total_entities = 0
        validation_errors = []
        
        for entity_type, entities in self.generated_entities.items():
            valid_entities[entity_type] = []
            invalid_entities[entity_type] = []
            
            for entity in entities:
                total_entities += 1
                try:
                    # Validate entity has proper structure
                    if 'id' not in entity:
                        error_msg = f"Entity missing 'id' field"
                        validation_errors.append(error_msg)
                        entity['validation_error'] = error_msg
                        invalid_entities[entity_type].append(entity)
                        continue
                    
                    if not isinstance(entity['id'], dict) or 'kind' not in entity['id']:
                        error_msg = f"Entity {entity.get('id', 'unknown')} has invalid EntityID structure"
                        validation_errors.append(error_msg)
                        entity['validation_error'] = error_msg
                        invalid_entities[entity_type].append(entity)
                        continue
                    
                    entity_kind = EntityKind(entity['id']['kind'])
                    template = self.registry.get_template(entity_kind)
                    
                    if not template:
                        error_msg = f"No template found for entity kind: {entity_kind.name}"
                        validation_errors.append(error_msg)
                        entity['validation_error'] = error_msg
                        invalid_entities[entity_type].append(entity)
                        continue
                    
                    # If we get here, entity is valid
                    valid_entities[entity_type].append(entity)
                    
                except Exception as e:
                    error_msg = f"Validation error for entity {entity.get('id', 'unknown')}: {str(e)}"
                    validation_errors.append(error_msg)
                    entity['validation_error'] = error_msg
                    invalid_entities[entity_type].append(entity)
        
        end_time = time.time()
        self.metrics['validation_times']['entities'] = end_time - start_time
        
        # Count totals
        total_valid = sum(len(entities) for entities in valid_entities.values())
        total_invalid = sum(len(entities) for entities in invalid_entities.values())
        
        print(f"✅ Entity validation completed in {end_time - start_time:.2f}s")
        print(f"  Valid entities: {total_valid:,}")
        print(f"  Invalid entities: {total_invalid:,}")
        
        if total_invalid > 0:
            print(f"  First 5 validation errors:")
            for error in validation_errors[:5]:
                print(f"    - {error}")
        
        return valid_entities, invalid_entities
    
    def save_entities_to_files(self, valid_entities: Dict[str, List], invalid_entities: Dict[str, List]):
        """Save valid and invalid entities to separate files."""
        start_time = time.time()
        
        # Save valid entities
        for entity_type, entities in valid_entities.items():
            if entities:
                file_path = os.path.join(self.output_dir, f"{entity_type}.json")
                with open(file_path, 'w') as f:
                    json.dump(entities, f, indent=2)
                print(f"  Saved {len(entities)} valid {entity_type} entities to {file_path}")
        
        # Save invalid entities
        for entity_type, entities in invalid_entities.items():
            if entities:
                file_path = os.path.join(self.invalid_dir, f"{entity_type}_invalid.json")
                with open(file_path, 'w') as f:
                    json.dump(entities, f, indent=2)
                print(f"  Saved {len(entities)} invalid {entity_type} entities to {file_path}")
        
        end_time = time.time()
        self.metrics['save_time'] = end_time - start_time
        print(f"File save completed in {end_time - start_time:.2f}s")
    
    def print_timing_metrics(self):
        """Print detailed timing metrics."""
        total_time = self.metrics['total_end_time'] - self.metrics['total_start_time']
        
        print("\n" + "=" * 60)
        print("TIMING METRICS")
        print("=" * 60)
        print(f"Total generation time: {total_time:.2f}s")
        
        print("\nEntity generation times:")
        for entity_type, duration in self.metrics['generation_times'].items():
            print(f"  {entity_type:12}: {duration:.2f}s")
        
        if 'validation_times' in self.metrics:
            print(f"\nValidation time: {self.metrics['validation_times'].get('entities', 0):.2f}s")
        
        if 'save_time' in self.metrics:
            print(f"File save time: {self.metrics['save_time']:.2f}s")
        
        # Calculate breakdown percentages
        generation_time = sum(self.metrics['generation_times'].values())
        validation_time = self.metrics['validation_times'].get('entities', 0)
        save_time = self.metrics.get('save_time', 0)
        
        print(f"\nTime breakdown:")
        print(f"  Generation: {generation_time:.2f}s ({generation_time/total_time*100:.1f}%)")
        print(f"  Validation: {validation_time:.2f}s ({validation_time/total_time*100:.1f}%)")
        print(f"  File I/O:   {save_time:.2f}s ({save_time/total_time*100:.1f}%)")
        print("=" * 60)
    
    def generate_all_entities(self):
        """Generate all entity types in proper order with final template validation."""
        self.metrics['total_start_time'] = time.time()
        print("Generating Asset Brain test entities using UxM templates...")
        
        print("1. Generating products...")
        self._time_generation('product', self.generate_products)
        
        print("2. Generating customers...")
        self._time_generation('customer', self.generate_customers)
        
        print("3. Generating datacenters...")
        self._time_generation('datacenter', self.generate_datacenters)
        
        print("4. Generating utilities...")
        self._time_generation('utility', self.generate_utilities)
        
        print("5. Generating UPS systems...")
        self._time_generation('ups', self.generate_ups_systems)
        
        print("6. Generating PDUs...")
        self._time_generation('pdu', self.generate_pdus)
        
        print("7. Generating cages...")
        self._time_generation('cage', self.generate_cages)
        
        print("8. Generating cabinets...")
        self._time_generation('cabinet', self.generate_cabinets)
        
        # Validate and separate entities
        print("\n9. Validating entities against templates...")
        valid_entities, invalid_entities = self.validate_and_separate_entities()
        
        # Save entities to files
        print("\n10. Saving entities to files...")
        self.save_entities_to_files(valid_entities, invalid_entities)
        
        # Update stored entities to only include valid ones
        self.generated_entities = valid_entities
        
        self.metrics['total_end_time'] = time.time()
        
        # Print summary
        total_valid = sum(len(entities) for entities in valid_entities.values())
        total_invalid = sum(len(entities) for entities in invalid_entities.values())
        
        print(f"\n✅ Entity generation completed:")
        print(f"  Valid entities: {total_valid:,}")
        print(f"  Invalid entities: {total_invalid:,}")
        for entity_type, entities in valid_entities.items():
            if entities:
                print(f"    {entity_type}: {len(entities):,}")
        
        # Print timing metrics
        self.print_timing_metrics()
        
        return valid_entities


if __name__ == "__main__":
    generator = EntityGenerator()
    generator.generate_all_entities()
