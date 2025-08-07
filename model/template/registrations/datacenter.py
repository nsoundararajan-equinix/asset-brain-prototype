"""DataCenter template registration for Asset Brain UxM model."""

from ..template import EntityTemplate, EntityKind, RelationshipKind
from ..registry import GlobalRegistry

def register_datacenter_template() -> None:
    """Register DataCenter entity template with business rules."""
    template = EntityTemplate(
        entity_kind=EntityKind.EK_DATACENTER,
        allowed_parents=[EntityKind.EK_CUSTOMER],  # DataCenters owned by customers
        allowed_parent_relationships=[RelationshipKind.RK_OWNED_BY],
        allowed_children=[EntityKind.EK_CAGE, EntityKind.EK_UTILITY],  # Contains cages, has utilities
        allowed_child_relationships=[RelationshipKind.RK_CONTAINS, RelationshipKind.RK_OWNS]
    )
    GlobalRegistry.register_template(template)
