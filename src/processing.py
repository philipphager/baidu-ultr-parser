import logging
import shelve
from pathlib import Path
from typing import List, Dict

from ordered_set import OrderedSet

logger = logging.getLogger(__name__)


class Step:
    def __call__(self, df):
        raise NotImplementedError()


class Pipeline:
    def __init__(self, steps: List[Step]):
        self.steps = steps

    def __call__(self, df):
        for step in self.steps:
            df = step(df)
            assert df is not None, "Processing returned empty dataframe"

        return df


class EncodeLabel(Step):
    def __init__(self, cache_path: str, column: str):
        self.cache_path = Path(cache_path)
        self.column = column
        self.max_id = 0

    def __call__(self, df):
        logger.debug("Preprocessing: Encoding url_md5 as integer ids")

        with shelve.open(str(self.cache_path / self.column)) as db:
            df[self.column] = df[self.column].map(lambda x: self._fit_predict(x, db))

        return df

    def _fit_predict(self, label, db):

        if label not in db:
            db[label] = self.max_id
            self.max_id += 1

        return db[label]


class RenameColumns(Step):
    def __init__(self, column_mapping: Dict[str, str]):
        self.column_mapping = column_mapping

    def __call__(self, df):
        logger.debug("Preprocessing: Renaming columns")
        df = df.rename(columns=self.column_mapping)
        return df
