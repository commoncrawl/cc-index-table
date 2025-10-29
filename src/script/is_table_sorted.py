from collections import defaultdict

import pyarrow.parquet as pq
import argparse

from urllib.parse import urlparse
from urllib.request import urlopen
import boto3
import gzip
from tqdm.auto import tqdm


def are_parquet_file_row_groups_sorted(pf: pq.ParquetFile, column_name: str) -> bool:
    sort_column_index = next(i for i, name in enumerate(pf.schema.names)
                             if name == column_name)

    # keep track of min/max in this ParquetFile
    whole_min = None
    whole_max = None
    prev_max = None
    for row_group_index in range(pf.num_row_groups):
        row_group = pf.metadata.row_group(row_group_index)
        column = row_group.column(sort_column_index)
        if prev_max is not None and prev_max > column.statistics.min:
            # internally unsorted
            print(f"row group {row_group_index} is not sorted on {column_name}: '{column.statistics.min}' <= '{prev_max}' ; stopping")
            return False, None, None
        whole_min = column.statistics.min if whole_min is None else column.statistics.min
        whole_max = column.statistics.max if whole_max is None else column.statistics.max
    return True, whole_min, whole_max


def is_full_table_sorted(file_or_s3_url_list_ordered: list[str], sort_column_name: str) -> bool:
    is_sorted = True
    prev_max = None
    prev_file_or_url = None
    status = defaultdict(int)
    with tqdm(file_or_s3_url_list_ordered) as pbar:
        for file_or_url in pbar:
            pf = pq.ParquetFile(file_or_url)
            this_is_sorted, pf_min, pf_max = are_parquet_file_row_groups_sorted(pf, column_name=sort_column_name)
            if not this_is_sorted:
                print(
                    f"Row groups are *internally* not sorted in file {file_or_url}"
                )
                is_sorted = False
                status['internally_unsorted'] += 1

            if prev_max is not None and prev_max > pf_min:
                print(f"{prev_file_or_url} is not sorted with respect to {file_or_url}: '{prev_max}' > '{pf_min}'")
                status['filewise_unsorted'] += 1
            pbar.set_postfix(status)
            prev_max = pf_max
            prev_file_or_url = file_or_url
    return is_sorted


def is_gzip(content: bytes) -> bool:
    return content[:2] == b'\x1f\x8b'


def read_file_list(path_or_url: str, prefix: str) -> list[str]:
    parsed = urlparse(path_or_url)
    if parsed.scheme == "s3":
        s3 = boto3.client("s3")
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read()
    elif parsed.scheme in ("http", "https"):
        with urlopen(path_or_url) as f:
            content = f.read()
    else:
        with open(path_or_url, "r") as f:
            content = f.read()

    if is_gzip(content):
        content = gzip.decompress(content)
    lines = content.decode("utf-8").split("\n")
    return [prefix + line.strip() for line in lines if len(line.strip()) > 0]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check if a collection of Parquet files, considered as a whole, is sorted. Exit code is 0 if sorted, 1 if not sorted.")
    parser.add_argument("files_or_s3_urls_file", type=str, help="URI or path to a text file containing a list of paths or S3 URLs, one per line, in the expected sorted order.")
    parser.add_argument("--prefix", type=str, default="s3://commoncrawl/", help="Prefix to prepend to entries read from the file (default: 's3://commoncrawl/')")
    parser.add_argument("--column", type=str, default="url_surtkey", help="Column name to check sorting against (default: 'url_surtkey')")

    args = parser.parse_args()

    files = read_file_list(args.files_or_s3_urls_file, prefix=args.prefix)
    is_sorted = is_full_table_sorted(files, sort_column_name=args.column)
    if is_sorted:
        print("✅ Files are sorted")
        exit(0)
    else:
        print("❌ Files are NOT sorted")
        exit(1)
