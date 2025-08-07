"""Utility template registration for Asset Brain UxM model."""

from ..template import EntityTemplate, EntityKind, RelationshipKind
from ..registry import GlobalRegistry

def register_utility_template() -> None:
    """Register Utility entity template with business rules."""
    template = EntityTemplate(
        entity_kind=EntityKind.EK_UTILITY,
        allowed_parents=[EntityKind.EK_DATACENTER],  # Utilities serve datacenters
        allowed_parent_relationships=[RelationshipKind.RK_CONTAINED_BY],
        allowed_children=[EntityKind.EK_UPS],  # Utilities feed UPS systems
        allowed_child_relationships=[RelationshipKind.RK_FEEDS_POWER_TO]
    )
    GlobalRegistry.register_template(template)
