"""
Asset Brain template system for UxM validation.

This module provides the core template classes for validating 
entity relationships and business rules in the Asset Brain system.
"""

from .template import EntityTemplate, RelationshipTemplate, EntityKind, RelationshipKind
from .registry import GlobalRegistry, TemplateRegistry

# Import all registration functions directly
from .registrations.customer import register_customer_template
from .registrations.product import register_product_template
from .registrations.datacenter import register_datacenter_template
from .registrations.utility import register_utility_template
from .registrations.ups import register_ups_template
from .registrations.pdu import register_pdu_template
from .registrations.cage import register_cage_template
from .registrations.cabinet import register_cabinet_template

def register_all_templates() -> None:
    """Register all entity templates for Asset Brain.
    
    This function should be called once during application startup
    to register all business rules and validation templates.
    """
    register_customer_template()
    register_product_template()
    register_datacenter_template()
    register_utility_template()
    register_ups_template()
    register_pdu_template()
    register_cage_template()
    register_cabinet_template()

def get_registered_template_count() -> int:
    """Get the number of registered templates."""
    return len(GlobalRegistry._templates)

__all__ = [
    'EntityTemplate',
    'RelationshipTemplate', 
    'EntityKind',
    'RelationshipKind',
    'GlobalRegistry',
    'TemplateRegistry',
    'register_all_templates',
    'get_registered_template_count'
]
