from ..definitions import UpstreamSourceConfiguration, UpstreamSourceType
from ..apis.nvd.nvd_client import enrich_from_nvd
from ..schemas.raw_nvd_schema import raw_nvd_schema
from ..queries.nvd_normalization import normalize_nvd


nvd_config = UpstreamSourceConfiguration(
    name="nvd",
    source_type=UpstreamSourceType.NVD,
    priority=1,
    schema=raw_nvd_schema,
    enrichment_function=enrich_from_nvd,
    normalization_function=normalize_nvd,
)
