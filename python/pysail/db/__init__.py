"""Utilities for database access."""

from .jdbc import read_arrow_batches

__all__ = ["read_arrow_batches"]
