"""Execute an Athena query over multiple crawls."""

import argparse
import logging
import os
import re
import sys

from collections import Counter, defaultdict
from urllib.parse import urljoin, urlparse

import pandas as pd

from pyathena import connect
from pyathena.util import RetryConfig


logging.basicConfig(level='INFO',
                    format='%(asctime)s %(levelname)s %(name)s: %(message)s')


rx_crawl_id = re.compile(r'CC-MAIN-20[123][0-9]-(?:[0-4][0-9]|5[12])')


def query_execute(crawl, cursor, args):
    query_template = open(args.query_template, encoding='UTF-8').read()
    if '{crawl}' in query_template:
        query = query_template.format(crawl=crawl)
    else:
        logging.info("The query template does not contain the placeholder '{crawl}'")
        logging.info("Trying to match crawl identifiers...")
        query = rx_crawl_id.sub(crawl, query_template)
        
    logging.info("Athena query to create temporary export table:\n%s", query)

    cursor.execute(query)
    logging.info("Creation of temporary export table: %s", cursor.result_set.state)

    logging.info("Athena query ID %s: %s",
                 cursor.query_id,
                 cursor.result_set.state)
    logging.info("       data_scanned_in_bytes: %d",
                 cursor.result_set.data_scanned_in_bytes)
    logging.info("       total_execution_time_in_millis: %d",
                 cursor.result_set.total_execution_time_in_millis)

def parse_arguments(args):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--database',
                        help='Name of the database', default='ccindex')
    parser.add_argument('--output_base_name',
                        help='Base name of output files, without the suffix .csv.gz', default='results')
    parser.add_argument('--s3_staging_dir',
                        help='Staging directory on S3 used for query results and metadata.'
                        'A subdirectory for each crawl dataset is created.')
    parser.add_argument('query_template',
                        help='Query template.'
                        'The query template should contain a placeholder for the crawls to be processed: {crawl}.'
                        'Alternatively, any crawl identifiers (CC-MAIN-YYYY-WW) are replaced by the actual crawl ID.')
    parser.add_argument('crawl_data_set', nargs='+',
                        help='Common Crawl crawl dataset(s) to process, e.g., CC-MAIN-2022-33')
    args = parser.parse_args(args)

    # remove trailing slash
    args.s3_output_location = args.s3_staging_dir.rstrip('/')

    return args

def main(args):
    args = parse_arguments(args)

    retry_config = RetryConfig(attempt=3)
    
    for crawl in args.crawl_data_set:
        cursor = connect(s3_staging_dir=args.s3_staging_dir + '/' + crawl.lower(),
                         retry_config=retry_config,
                         region_name="us-east-1").cursor()
        query_execute(crawl, cursor, args)


if __name__ == '__main__':
    main(sys.argv[1:])
