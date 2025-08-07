"""PDU template registration for Asset Brain UxM model."""

from ..template import EntityTemplate, EntityKind, RelationshipKind
from ..registry import GlobalRegistry

def register_pdu_template() -> None:
    """Register PDU entity template with business rules."""
    template = EntityTemplate(
        entity_kind=EntityKind.EK_PDU,
        allowed_parents=[EntityKind.EK_UPS],  # PDU powered by UPS
        allowed_parent_relationships=[RelationshipKind.RK_POWERED_BY],
        allowed_children=[EntityKind.EK_CAGE],  # PDU feeds cages
        allowed_child_relationships=[RelationshipKind.RK_FEEDS_POWER_TO]
    )
    GlobalRegistry.register_template(template)
