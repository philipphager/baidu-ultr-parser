import logging
from pathlib import Path

import hydra
import pandas as pd
from omegaconf import DictConfig, OmegaConf

logger = logging.getLogger(__name__)


@hydra.main(config_path="config", config_name="config", version_base="1.2")
def main(config: DictConfig):
    logger.info(OmegaConf.to_yaml(config))

    in_path = Path(config.output_path) / "train_clicks/"
    out_path = Path(config.output_path) / "train_clicks/"
    dfs = []

    for path in sorted(in_path.rglob(f"part-*")):
        dfs.append(pd.read_parquet(path))

    df = pd.concat(dfs)
    df.to_parquet(out_path / "train.parquet")


if __name__ == "__main__":
    main()
