import logging
from pathlib import Path

import hydra
import pandas as pd
from hydra.utils import instantiate
from omegaconf import DictConfig, OmegaConf

from src.processing import Pipeline

logger = logging.getLogger(__name__)


def process(
    out_path: Path,
    type: str,
    pipeline: Pipeline,
):
    logger.info(f"Processing: {type}")

    for path in sorted(out_path.rglob(f"{type}-*")):
        logger.info(f"Loading: {path}")
        df = pd.read_parquet(path)
        df = pipeline(df)
        df.to_parquet(path)


@hydra.main(config_path="config", config_name="config", version_base="1.2")
def main(config: DictConfig):
    logger.info(OmegaConf.to_yaml(config))

    out_path = Path(config.output_path)

    document_pipeline = instantiate(config.document_pipeline)
    process(out_path, "document", document_pipeline)

    query_pipeline = instantiate(config.query_pipeline)
    process(out_path, "query", query_pipeline)


if __name__ == "__main__":
    main()
