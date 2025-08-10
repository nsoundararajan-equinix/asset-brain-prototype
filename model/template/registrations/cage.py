"""Cage template registration for Asset Brain UxM model."""

from ..template import EntityTemplate, EntityKind, RelationshipKind
from ..registry import GlobalRegistry

def register_cage_template() -> None:
    """Register Cage entity template with business rules."""
    template = EntityTemplate(
        entity_kind=EntityKind.EK_CAGE,
        allowed_parents=[
            EntityKind.EK_CUSTOMER,     # Cages owned by customers
            EntityKind.EK_DATACENTER,   # Cages contained in datacenters
            EntityKind.EK_PDU,          # Cages powered by PDUs
            EntityKind.EK_PRODUCT       # Cages fulfill products
        ],  
        allowed_parent_relationships=[
            RelationshipKind.RK_OWNED_BY, 
            RelationshipKind.RK_CONTAINED_BY, 
            RelationshipKind.RK_POWERED_BY,
            RelationshipKind.RK_FULFILLS
        ],
        allowed_children=[EntityKind.EK_CABINET, EntityKind.EK_PRODUCT],  # Cages contain cabinets and fulfill products
        allowed_child_relationships=[RelationshipKind.RK_CONTAINS, RelationshipKind.RK_FEEDS_POWER_TO, RelationshipKind.RK_FULFILLS]
    )
    GlobalRegistry.register_template(template)
