"""
Enrichment configuration for upstream CVE sources.

Like inventory's enrichment config - defines external API sources.
"""

from .definitions import (
    UpstreamSourceType,
    UpstreamSourceConfiguration,
    UpstreamSourcesConfig,
)
from .sources.upstream_sources import upstream_sources_config

__all__ = [
    "UpstreamSourceType",
    "UpstreamSourceConfiguration",
    "UpstreamSourcesConfig",
    "upstream_sources_config",
]
