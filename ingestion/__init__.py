"""
Asset Brain Ingestion Pipeline

This module provides data ingestion capabilities for loading 
Asset Brain test data into Google Cloud Spanner Graph.

Since our test data is already in UxM format, no transformation needed.
"""

from .spanner_client import SpannerGraphClient
from .data_loader import AssetDataLoader
from .pipeline import IngestionPipeline

__all__ = [
    'SpannerGraphClient',
    'AssetDataLoader', 
    'IngestionPipeline'
]
