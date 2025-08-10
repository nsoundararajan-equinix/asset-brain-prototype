"""
Template registry for Asset Brain validation system.

Central registry following the network team's template pattern for 
enterprise-grade validation of asset relationships and hierarchies.
"""

from typing import Dict, List, Optional, Set
from .template import EntityTemplate, EntityKind, RelationshipKind


class TemplateRegistry:
    """
    Central registry for entity validation templates.
    
    Manages templates and provides validation services for the Asset Brain system.
    Following the network team's registry pattern.
    """
    
    def __init__(self):
        self._templates: Dict[EntityKind, EntityTemplate] = {}
    
    def register_template(self, template: EntityTemplate) -> None:
        """Register an entity template for validation."""
        self._templates[template.entity_kind] = template
    
    def get_template(self, entity_kind: EntityKind) -> Optional[EntityTemplate]:
        """Get the template for a specific entity kind."""
        return self._templates.get(entity_kind)
    
    def validate_subgraph(self, entities: List[dict], relationships: List[dict]) -> None:
        """
        Validate a subgraph of entities and relationships.
        
        Args:
            entities: List of entity dictionaries with 'id' and 'kind' fields
            relationships: List of relationship dictionaries with 'a', 'z', 'kind' fields
            
        Raises:
            ValueError: If validation fails
        """
        # Validate all entities have registered templates
        for entity in entities:
            entity_kind = EntityKind(entity['kind'])
            template = self.get_template(entity_kind)
            if not template:
                raise ValueError(f"No template registered for entity kind: {entity_kind.name}")
        
        # Validate all relationships
        for relationship in relationships:
            self._validate_relationship(relationship, entities)
    
    def _validate_relationship(self, relationship: dict, entities: List[dict]) -> None:
        """Validate a single relationship against business rules."""
        a_kind = EntityKind(relationship['a']['kind'])
        z_kind = EntityKind(relationship['z']['kind'])
        rel_kind = RelationshipKind(relationship['kind'])
        
        # Get templates for both entities
        a_template = self.get_template(a_kind)
        z_template = self.get_template(z_kind)
        
        if not a_template or not z_template:
            raise ValueError(f"Missing templates for relationship {a_kind.name} -> {z_kind.name}")
        
        # Check if A can have this relationship with Z
        if z_kind in a_template.allowed_children:
            if rel_kind not in a_template.allowed_child_relationships:
                raise ValueError(
                    f"Source {a_kind.name} cannot create {rel_kind.name} relationship with {z_kind.name}"
                )
        else:
            raise ValueError(
                f"Source {a_kind.name} cannot have relationship with {z_kind.name} (not in allowed children)"
            )
        
        # Check if Z can accept this relationship from A
        if a_kind in z_template.allowed_parents:
            if rel_kind not in z_template.allowed_parent_relationships:
                raise ValueError(
                    f"Target {z_kind.name} cannot accept {rel_kind.name} relationship from {a_kind.name}"
                )
        else:
            raise ValueError(
                f"Target {z_kind.name} cannot accept relationship from {a_kind.name} (not in allowed parents)"
            )


# Global registry instance (singleton pattern)
GlobalRegistry = TemplateRegistry()
