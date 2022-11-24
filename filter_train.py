import logging
from pathlib import Path

import hydra
import pandas as pd
from omegaconf import DictConfig, OmegaConf

logger = logging.getLogger(__name__)


@hydra.main(config_path="config", config_name="config", version_base="1.2")
def main(config: DictConfig):
    logger.info(OmegaConf.to_yaml(config))

    in_path = Path(config.output_path)
    out_path = Path(config.output_path) / "train_clicks/"
    out_path.mkdir(parents=True, exist_ok=True)

    test_path = in_path / "test.parquet"
    query_path = in_path / f"query-part-{config.part:05d}.parquet"
    document_path = in_path / f"document-part-{config.part:05d}.parquet"

    test_df = pd.read_parquet(test_path)
    query_df = pd.read_parquet(query_path)
    document_df = pd.read_parquet(document_path)

    df = document_df.merge(query_df, on=["qid"])
    filtered_df = df.merge(
        test_df[["query_md5", "title_md5"]], on=["query_md5", "title_md5"]
    )
    filtered_df.to_parquet(out_path / f"part-{config.part:05d}.parquet")


if __name__ == "__main__":
    main()
