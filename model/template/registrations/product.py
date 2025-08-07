"""Product template registration for Asset Brain UxM model."""

from ..template import EntityTemplate, EntityKind, RelationshipKind
from ..registry import GlobalRegistry

def register_product_template() -> None:
    """Register Product entity template with business rules."""
    template = EntityTemplate(
        entity_kind=EntityKind.EK_PRODUCT,
        allowed_parents=[EntityKind.EK_CUSTOMER],  # Products owned by customers
        allowed_parent_relationships=[RelationshipKind.RK_OWNED_BY],
        allowed_children=[],  # Products are leaf entities
        allowed_child_relationships=[]
    )
    GlobalRegistry.register_template(template)
