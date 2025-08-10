"""Cabinet template registration for Asset Brain UxM model."""

from ..template import EntityTemplate, EntityKind, RelationshipKind
from ..registry import GlobalRegistry

def register_cabinet_template() -> None:
    """Register Cabinet entity template with business rules."""
    template = EntityTemplate(
        entity_kind=EntityKind.EK_CABINET,
        allowed_parents=[
            EntityKind.EK_CUSTOMER,     # Cabinets owned by customers
            EntityKind.EK_DATACENTER,   # Cabinets contained in datacenters
            EntityKind.EK_CAGE,         # Cabinets contained in cages
            EntityKind.EK_PRODUCT       # Cabinets fulfill products
        ],  
        allowed_parent_relationships=[
            RelationshipKind.RK_OWNED_BY,
            RelationshipKind.RK_CONTAINED_BY, 
            RelationshipKind.RK_POWERED_BY,
            RelationshipKind.RK_FULFILLS
        ],
        allowed_children=[EntityKind.EK_PRODUCT],  # Cabinets fulfill products
        allowed_child_relationships=[RelationshipKind.RK_FULFILLS]
    )
    GlobalRegistry.register_template(template)
