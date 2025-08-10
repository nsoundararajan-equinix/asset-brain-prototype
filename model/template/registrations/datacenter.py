"""DataCenter template registration for Asset Brain UxM model."""

from ..template import EntityTemplate, EntityKind, RelationshipKind
from ..registry import GlobalRegistry

def register_datacenter_template() -> None:
    """Register DataCenter entity template with business rules."""
    template = EntityTemplate(
        entity_kind=EntityKind.EK_DATACENTER,
        allowed_parents=[],  # DataCenters are top-level infrastructure entities
        allowed_parent_relationships=[],
        allowed_children=[
            EntityKind.EK_UTILITY,      # Datacenters contain utilities
            EntityKind.EK_UPS,          # Datacenters contain UPS systems
            EntityKind.EK_PDU,          # Datacenters contain PDUs
            EntityKind.EK_CAGE,         # Datacenters contain cages
            EntityKind.EK_CABINET       # Datacenters contain cabinets
        ],  
        allowed_child_relationships=[RelationshipKind.RK_CONTAINS]
    )
    GlobalRegistry.register_template(template)
