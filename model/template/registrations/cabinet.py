"""Cabinet template registration for Asset Brain UxM model."""

from ..template import EntityTemplate, EntityKind, RelationshipKind
from ..registry import GlobalRegistry

def register_cabinet_template() -> None:
    """Register Cabinet entity template with business rules."""
    template = EntityTemplate(
        entity_kind=EntityKind.EK_CABINET,
        allowed_parents=[EntityKind.EK_CAGE],  # Cabinets contained in cages
        allowed_parent_relationships=[RelationshipKind.RK_CONTAINED_BY, RelationshipKind.RK_POWERED_BY],
        allowed_children=[],  # Cabinets are leaf entities
        allowed_child_relationships=[]
    )
    GlobalRegistry.register_template(template)
