# Baidu WSDM Cup 2023 Parser

Parse and preprocess the Baidu unbiased-learning-to-rank dataset for the WSDM Cup 2023 to a .parquet file. The parser uses hydra and can be used on SLURM for parsing multiple files in parallel. 

## Installation
1. Create conda environment: `conda env create -f environment.yaml`
2. Activate environment: `conda activate baidu-ultr`

## Setup & Configuration
1. You can configure the parser with: `config/config.yaml`
2. First, set `dataset_path` to a directory containing all `part-*.gz` files of the dataset. 
3. Next, set `output_path` to a directory to store intermediary parsing outputs and the final merged dataset.
4. You can add which columns to import for each search query or document, we will automatically add the qid for each parsed document. If you don't want to parse query features, you can leave the column list empty.
5. Each part will be parsed into a dataframe, which can be post-processed using a custom pipeline. Create and add steps under: `document_pipeline` or `query_pipeline`.

## Run
1. Parse all dataset parts in parallel: `python parse.py -m file=range(0,2000)`
2. Merge all resulting parts and perform post-processing: `python merge.py`

## Columns
The dataset contains features per search query and per document in each displayed SERP. Find the full documentation of the column meaning here: https://aistudio.baidu.com/aistudio/competition/detail/534/0/introduction

### Query dataset
```yaml
query_columns:
  - qid
  - query
  - query_reformulation
```

### Document dataset
```yaml
document_columns:
  - pos
  - url_md5
  - title
  - abstract
  - multimedia_type
  - click
  - skip
  - serp_height
  - displayed_time
  - displayed_time_middle
  - first_click
  - displayed_count
  - sero_max_show_height
  - slipoff_count_after_click
  - dwelling_time
  - displayed_time_top
  - sero_to_top
  - displayed_count_top
  - displayed_count_bottom
  - slipoff_count
  - final_click
  - displayed_time_bottom
  - click_count
  - displayed_count_2
  - last_click
  - reverse_display_count
  - displayed_count_middle
```
