from collections import defaultdict

import pyarrow.parquet as pq
import argparse

from urllib.parse import urlparse
import boto3
import gzip
from tqdm.auto import tqdm


def are_parquet_file_row_groups_min_max_ordered(pf: pq.ParquetFile, column_name: str) -> bool:
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
            print(f"row group {row_group_index} min is not strictly increasing w.r.t previous row group max on {column_name}: '{column.statistics.min}' <= '{prev_max}' ; stopping")
            return False
        whole_min = column.statistics.min if whole_min is None else min(column.statistics.min, whole_min)
        whole_max = column.statistics.max if whole_max is None else max(column.statistics.max, whole_max)
        prev_max = column.statistics.max
    return True


def are_all_parts_min_max_ordered(file_or_s3_url_list: list[str], sort_column_name: str) -> bool:
    is_sorted = True
    status = defaultdict(int)
    with tqdm(file_or_s3_url_list) as pbar:
        for file_or_url in pbar:
            pf = pq.ParquetFile(file_or_url)
            this_is_sorted = are_parquet_file_row_groups_min_max_ordered(pf, column_name=sort_column_name)
            if not this_is_sorted:
                print(
                    f"Row groups are *internally* not ordered by min/max in file {file_or_url}"
                )
                is_sorted = False
                status['internally_unsorted'] += 1

            pbar.set_postfix(status)
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
    else:
        with open(path_or_url, "rb") as f:
            content = f.read()

    if is_gzip(content):
        content = gzip.decompress(content)
    lines = content.decode("utf-8").split("\n")
    return [prefix + line.strip() for line in lines if len(line.strip()) > 0]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check if row groups within parquet files have strictly increasing non-overlapping min/max ranges. Exit code is 0 if sorted, 1 if not sorted.")
    parser.add_argument("files_or_s3_urls_file", type=str, help="path or s3:// URI to a text file containing a list of paths, to check; used in combination with --prefix to recover individual file paths.")
    parser.add_argument("--prefix", type=str, default="s3://commoncrawl/", help="Prefix to prepend to entries read from the file (default: 's3://commoncrawl/')")
    parser.add_argument("--column", type=str, default="url_surtkey", help="Column name to check against (default: 'url_surtkey')")

    args = parser.parse_args()

    files = read_file_list(args.files_or_s3_urls_file, prefix=args.prefix)
    is_sorted = are_all_parts_min_max_ordered(files, sort_column_name=args.column)
    if is_sorted:
        print("✅ Files are sorted")
        exit(0)
    else:
        print("❌ Files are NOT sorted")
        exit(1)
