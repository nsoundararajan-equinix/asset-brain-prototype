"""Cage template registration for Asset Brain UxM model."""

from ..template import EntityTemplate, EntityKind, RelationshipKind
from ..registry import GlobalRegistry

def register_cage_template() -> None:
    """Register Cage entity template with business rules."""
    template = EntityTemplate(
        entity_kind=EntityKind.EK_CAGE,
        allowed_parents=[EntityKind.EK_CUSTOMER, EntityKind.EK_DATACENTER, EntityKind.EK_PDU],  
        allowed_parent_relationships=[RelationshipKind.RK_OWNED_BY, RelationshipKind.RK_CONTAINED_BY, RelationshipKind.RK_POWERED_BY],
        allowed_children=[EntityKind.EK_CABINET],  # Cages contain cabinets
        allowed_child_relationships=[RelationshipKind.RK_CONTAINS, RelationshipKind.RK_FEEDS_POWER_TO]
    )
    GlobalRegistry.register_template(template)
