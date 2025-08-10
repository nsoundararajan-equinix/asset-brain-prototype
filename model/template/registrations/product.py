"""Product template registration for Asset Brain UxM model."""

from ..template import EntityTemplate, EntityKind, RelationshipKind
from ..registry import GlobalRegistry

def register_product_template() -> None:
    """Register Product entity template with business rules."""
    template = EntityTemplate(
        entity_kind=EntityKind.EK_PRODUCT,
        allowed_parents=[EntityKind.EK_CUSTOMER, EntityKind.EK_CAGE, EntityKind.EK_CABINET],  # Products owned by customers and fulfilled by assets
        allowed_parent_relationships=[RelationshipKind.RK_OWNED_BY, RelationshipKind.RK_FULFILLED_BY],
        allowed_children=[EntityKind.EK_CAGE, EntityKind.EK_CABINET],  # Products fulfilled by assets
        allowed_child_relationships=[RelationshipKind.RK_FULFILLED_BY]
    )
    GlobalRegistry.register_template(template)
