import logging
from pathlib import Path

import hydra
from hydra.utils import instantiate
from omegaconf import DictConfig, OmegaConf

from src.dataset import TestDataset

logger = logging.getLogger(__name__)


@hydra.main(config_path="config", config_name="config", version_base="1.2")
def main(config: DictConfig):
    logger.info(OmegaConf.to_yaml(config))

    in_path = Path(config.dataset_path)
    out_path = Path(config.output_path)
    out_path.mkdir(parents=True, exist_ok=True)

    path = in_path / "test_data.txt"
    assert path.exists(), f"Cannot find file: {path}"
    logger.info(f"Parsing file: {path}")

    dataset = TestDataset(
        path,
        config.test_columns,
    )

    df = dataset.parse()

    pipeline = instantiate(config.test_pipeline)
    df = pipeline(df)
    df.to_parquet(out_path / f"test.parquet")


if __name__ == "__main__":
    main()
