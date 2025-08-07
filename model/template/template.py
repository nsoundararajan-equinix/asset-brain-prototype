"""
Core template classes for Asset Brain UxM validation system.

Defines the structure for entity and relationship validation templates,
following the network team's template pattern for enterprise-grade validation.
"""

from dataclasses import dataclass
from typing import List, Set
from enum import IntEnum


# Enums matching the protobuf definitions
class EntityKind(IntEnum):
    """Entity types in the Asset Brain system."""
    EK_UNKNOWN = 0
    EK_CUSTOMER = 1
    EK_PRODUCT = 2
    EK_DATACENTER = 3
    EK_UTILITY = 4
    EK_UPS = 5
    EK_PDU = 6
    EK_CAGE = 7
    EK_CABINET = 8


class RelationshipKind(IntEnum):
    """Relationship types in the Asset Brain system."""
    RK_UNKNOWN = 0
    RK_OWNS = 1
    RK_OWNED_BY = 2
    RK_FULFILLS = 3
    RK_FULFILLED_BY = 4
    RK_CONTAINS = 5
    RK_CONTAINED_BY = 6
    RK_FEEDS_POWER_TO = 7
    RK_POWERED_BY = 8


@dataclass
class EntityTemplate:
    """
    Template defining validation rules for an entity type.
    
    Following the network team's template pattern for business rule validation.
    """
    entity_kind: EntityKind
    allowed_parents: List[EntityKind]
    allowed_parent_relationships: List[RelationshipKind]
    allowed_children: List[EntityKind]
    allowed_child_relationships: List[RelationshipKind]


@dataclass
class RelationshipTemplate:
    """
    Template defining validation rules for a relationship type.
    
    Specifies which entity types can participate in this relationship.
    """
    relationship_kind: RelationshipKind
    allowed_source_kinds: Set[EntityKind]
    allowed_target_kinds: Set[EntityKind]
    is_hierarchical: bool = False  # True if this implies containment/ownership
