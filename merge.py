import logging
from pathlib import Path

import hydra
import pandas as pd
from hydra.utils import instantiate
from omegaconf import DictConfig, OmegaConf

from src.processing import Pipeline

logger = logging.getLogger(__name__)


def merge(
    out_path: Path,
    type: str,
    pipeline: Pipeline,
    delete_parts_after_merge: bool,
):
    frames = []
    logger.info(f"Merging: {type}")

    for path in out_path.rglob(f"{type}-*"):
        logger.info(f"Loading: {path}")
        df = pd.read_parquet(path)
        df = pipeline(df)
        frames.append(df)

    if len(frames) > 0:
        path = out_path / f"baidu-ultr-{type}.parquet"
        df = pd.concat(frames)
        df.to_parquet(path)
        logger.info(f"Created: {path}")

    if delete_parts_after_merge:
        for path in out_path.rglob(f"{type}-*"):
            path.unlink()
            logger.info(f"Deleted: {path}")


@hydra.main(config_path="config", config_name="config", version_base="1.2")
def main(config: DictConfig):
    logger.info(OmegaConf.to_yaml(config))

    out_path = Path(config.output_path)

    document_pipeline = instantiate(config.document_pipeline)
    merge(out_path, "document", document_pipeline, config.delete_parts_after_merge)

    query_pipeline = instantiate(config.query_pipeline)
    merge(out_path, "query", query_pipeline, config.delete_parts_after_merge)


if __name__ == "__main__":
    main()
