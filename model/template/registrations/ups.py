"""UPS template registration for Asset Brain UxM model."""

from ..template import EntityTemplate, EntityKind, RelationshipKind
from ..registry import GlobalRegistry

def register_ups_template() -> None:
    """Register UPS entity template with business rules."""
    template = EntityTemplate(
        entity_kind=EntityKind.EK_UPS,
        allowed_parents=[EntityKind.EK_UTILITY],  # UPS powered by utilities
        allowed_parent_relationships=[RelationshipKind.RK_POWERED_BY],
        allowed_children=[EntityKind.EK_PDU],  # UPS feeds PDUs
        allowed_child_relationships=[RelationshipKind.RK_FEEDS_POWER_TO]
    )
    GlobalRegistry.register_template(template)
