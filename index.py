import dbm
import logging
import shelve
from pathlib import Path

import hydra
import pandas as pd
from omegaconf import DictConfig, OmegaConf

logger = logging.getLogger(__name__)


def index(
    out_path: Path,
    type: str,
    column: str,
):
    logger.info(f"Processing: {type}")

    db_path = out_path / f"{type}-{column}"

    with dbm.open(db_path, "c") as db:
        db["max_idx"] = str(0)

    for path in sorted(out_path.rglob(f"{type}-*"))[:10]:
        df = pd.read_parquet(path)

        with dbm.open(db_path, "c") as db:
            for value in df[column].values:
                if value not in db:
                    db[str(value)] = str(db["max_idx"])
                    db["max_idx"] = str(int(db["max_idx"]) + 1)


@hydra.main(config_path="config", config_name="config", version_base="1.2")
def main(config: DictConfig):
    logger.info(OmegaConf.to_yaml(config))

    out_path = Path(config.output_path)
    index(out_path, "document", "url_md5")


if __name__ == "__main__":
    main()
