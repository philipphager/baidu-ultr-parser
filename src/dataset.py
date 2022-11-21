import gzip
from pathlib import Path
from typing import List, Union, Optional

import pandas as pd

from src.const import QUERY_COLUMNS, DOCUMENT_COLUMNS


class Dataset:
    def __init__(
        self,
        path: Union[Path, str],
        query_columns: Optional[List[str]],
        document_columns: Optional[List[str]],
    ):
        self.path = path
        self.query_columns = query_columns if query_columns is not None else []
        self.document_columns = document_columns if query_columns is not None else []
        self.query_idx = self.get_colum_idx(QUERY_COLUMNS, query_columns)
        self.document_idx = self.get_colum_idx(DOCUMENT_COLUMNS, document_columns)

    @staticmethod
    def get_colum_idx(columns, selected_columns):
        for c in selected_columns:
            assert c in columns, f"Column: {c} not found in: {columns}"

        return [i for i, c in enumerate(columns) if c in selected_columns]

    def parse(self):
        query_rows = []
        document_rows = []
        query_id = None

        with gzip.open(self.path, "rt") as f:
            for line in f:
                columns = line.replace("\n", "").split("\t")
                is_query = len(columns) <= len(QUERY_COLUMNS)

                if is_query:
                    # Store current query_id for subsequent documents
                    query_id = columns[0]

                    if len(self.query_idx) > 0:
                        # Keep only selected columns
                        selected_columns = [columns[i] for i in self.query_idx]
                        query_rows.append(selected_columns)
                else:
                    if len(self.document_idx) > 0:
                        # Keep only selected columns and always add query_id
                        selected_columns = [columns[i] for i in self.document_idx]
                        document_rows.append([query_id] + selected_columns)

        query_df = pd.DataFrame(
            query_rows,
            columns=self.query_columns,
        )
        document_df = pd.DataFrame(
            document_rows,
            columns=["qid"] + self.document_columns,
        )

        return query_df, document_df
