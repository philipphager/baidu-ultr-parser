import logging
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
    def __init__(self, column: str):
        self.column = column
        self.label2id = OrderedSet()
        self.max_id = 0

    def __call__(self, df):
        logger.debug("Preprocessing: Encoding url_md5 as integer ids")
        df[self.column] = df[self.column].map(self._fit_predict)

        return df

    def _fit_predict(self, label):
        if label not in self.label2id:
            self.label2id.add(label)

        return self.label2id.index(label)


class RenameColumns(Step):
    def __init__(self, column_mapping: Dict[str, str]):
        self.column_mapping = column_mapping

    def __call__(self, df):
        logger.debug("Preprocessing: Renaming columns")
        df = df.rename(columns=self.column_mapping)
        return df
