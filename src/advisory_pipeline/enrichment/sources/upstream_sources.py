from ..definitions import (
    UpstreamSourceType,
    UpstreamSourceConfiguration,
    UpstreamSourcesConfig,
)

# Import all source implementations
from ..sources.nvd import nvd_config


# Central configuration - exact inventory pattern
# All sources in a list, loop through them
upstream_sources_config = UpstreamSourcesConfig(
    sources=[
        nvd_config,  # Priority 1 - NVD (works with CVE ID only)
    ]
)


# Export for easy importing
__all__ = [
    "UpstreamSourceConfiguration",
    "UpstreamSourcesConfig",
    "UpstreamSourceType",
    "upstream_sources_config",
]
