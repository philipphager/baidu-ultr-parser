import logging
from pathlib import Path

import hydra
from omegaconf import DictConfig, OmegaConf

from src.dataset import Dataset

logger = logging.getLogger(__name__)


@hydra.main(config_path="config", config_name="config", version_base="1.2")
def main(config: DictConfig):
    logger.info(OmegaConf.to_yaml(config))

    in_path = Path(config.dataset_path)
    out_path = Path(config.output_path)
    out_path.mkdir(parents=True, exist_ok=True)

    path = in_path / f"part-{config.part:05d}.gz"
    assert path.exists(), f"Cannot find file: {path}"
    logger.info(f"Parsing file: {path}")

    dataset = Dataset(
        path,
        config.query_columns,
        config.document_columns,
    )

    query_df, document_df = dataset.parse()

    if len(query_df) > 0:
        query_df.to_parquet(out_path / f"query-{path.stem}.parquet")

    if len(document_df) > 0:
        document_df.to_parquet(out_path / f"document-{path.stem}.parquet")


if __name__ == "__main__":
    main()
