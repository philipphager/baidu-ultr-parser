import pandas as pd
import hashlib
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


class Step:
    def __call__(self, df):
        raise NotImplementedError()


class Pipeline:
    def __init__(self, steps: List[Step]):
        self.steps = steps

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        for step in self.steps:
            df = step(df)
            assert df is not None, "Processing returned empty dataframe"

        return df


class EncodeLabel(Step):
    def __init__(self, column: str):
        self.column = column
        self.label2id = {}
        self.max_id = 0

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.debug("Preprocessing: Encoding url_md5 as integer ids")
        df[self.column] = df[self.column].map(self._fit_predict)

        return df

    def _fit_predict(self, label) -> int:
        if label not in self.label2id:
            self.label2id[label] = self.max_id
            self.max_id += 1

        return self.label2id[label]


class RenameColumns(Step):
    def __init__(self, column_mapping: Dict[str, str]):
        self.column_mapping = column_mapping

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.debug("Preprocessing: Renaming columns")
        df = df.rename(columns=self.column_mapping)
        return df


class HashTokens(Step):
    def __init__(self, column: str, unique_tokens: bool = False):
        self.column = column
        self.unique_tokens = unique_tokens

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.debug(f"Preprocessing: Hashing tokens in {self.column} using md5")
        df[f"{self.column}_md5"] = df[self.column].map(self.md5)
        return df

    def md5(self, text: str) -> str:
        tokens = text.split("\x01")

        if self.unique_tokens:
            tokens = sorted(set(tokens))

        hashed_tokens = hashlib.md5(str(tokens).encode())
        return hashed_tokens.hexdigest()
