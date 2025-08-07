"""
Customer template registration for Asset Brain UxM model.

Defines business rules and validation logic for Customer entities.
"""

from ..template import EntityTemplate, RelationshipTemplate, EntityKind, RelationshipKind
from ..registry import GlobalRegistry

def register_customer_template() -> None:
    """Register Customer entity template with business rules."""
    template = EntityTemplate(
        entity_kind=EntityKind.EK_CUSTOMER,
        allowed_parents=[],  # Customer is a top-level commercial entity
        allowed_parent_relationships=[],  # No parent relationships
        allowed_children=[
            EntityKind.EK_PRODUCT,      # Customers can own products/services
            EntityKind.EK_CAGE,         # Customers can lease cages
            EntityKind.EK_DATACENTER    # Customers can own datacenters
        ],
        allowed_child_relationships=[
            RelationshipKind.RK_OWNS,           # Customer owns assets
            RelationshipKind.RK_FULFILLED_BY    # Customer fulfilled by products
        ]
    )
    GlobalRegistry.register_template(template)
