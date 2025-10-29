from unittest.mock import MagicMock
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'script'))
from is_table_sorted import are_parquet_file_row_groups_sorted


def _create_mock_parquet_file(column_name: str, row_groups_stats: list[tuple]):
    """
    Helper to create a mock ParquetFile with specified row group statistics.

    Args:
        column_name: Name of the column to sort by
        row_groups_stats: List of (min, max) tuples for each row group
    """
    mock_pf = MagicMock()
    mock_pf.schema.names = [column_name, 'data']
    mock_pf.num_row_groups = len(row_groups_stats)

    mock_row_groups = []
    for min_val, max_val in row_groups_stats:
        mock_row_group = MagicMock()
        mock_column = MagicMock()
        mock_column.statistics.min = min_val
        mock_column.statistics.max = max_val
        mock_row_group.column.return_value = mock_column
        mock_row_groups.append(mock_row_group)

    mock_pf.metadata.row_group.side_effect = lambda i: mock_row_groups[i]
    return mock_pf


# Tests for sorted row groups

def test_single_row_group_sorted():
    """Test with a single row group (trivially sorted)"""
    mock_pf = _create_mock_parquet_file(
        'url_surtkey',
        [('com,example)/page1', 'com,example)/page3')]
    )

    is_sorted, min_val, max_val = are_parquet_file_row_groups_sorted(mock_pf, 'url_surtkey')

    assert is_sorted is True
    assert min_val is not None
    assert max_val is not None


def test_multiple_row_groups_strictly_increasing():
    """Test with multiple row groups in strictly increasing order"""
    mock_pf = _create_mock_parquet_file(
        'url_surtkey',
        [
            ('com,aaa)/', 'com,bbb)/'),
            ('com,ccc)/', 'com,ddd)/'),
            ('com,eee)/', 'com,fff)/')
        ]
    )

    is_sorted, min_val, max_val = are_parquet_file_row_groups_sorted(mock_pf, 'url_surtkey')

    assert is_sorted is True
    assert min_val is not None
    assert max_val is not None


def test_boundary_case_adjacent_values():
    """Test with row groups that have adjacent but non-overlapping values"""
    mock_pf = _create_mock_parquet_file(
        'url',
        [
            ('com,example)/a', 'com,example)/z'),
            ('com,example,aaa)/', 'com,example,zzz)/')
        ]
    )

    is_sorted, min_val, max_val = are_parquet_file_row_groups_sorted(mock_pf, 'url')

    assert is_sorted is True
    assert min_val is not None
    assert max_val is not None


def test_two_row_groups_strictly_increasing_strings():
    """Test with two row groups with string values in strictly increasing order"""
    mock_pf = _create_mock_parquet_file(
        'url_surtkey',
        [
            ('com,apple)/', 'com,banana)/'),
            ('com,cherry)/', 'com,date)/')
        ]
    )

    is_sorted, min_val, max_val = are_parquet_file_row_groups_sorted(mock_pf, 'url_surtkey')

    assert is_sorted is True
    assert min_val is not None
    assert max_val is not None


def test_many_row_groups_strictly_increasing():
    """Test with many row groups, all strictly increasing"""
    row_groups = [
        ('com,aaa)/', 'com,aaa,zzz)/'),
        ('com,bbb)/', 'com,bbb,zzz)/'),
        ('com,ccc)/', 'com,ccc,zzz)/'),
        ('com,ddd)/', 'com,ddd,zzz)/'),
        ('com,eee)/', 'com,eee,zzz)/'),
    ]
    mock_pf = _create_mock_parquet_file('url_surtkey', row_groups)

    is_sorted, min_val, max_val = are_parquet_file_row_groups_sorted(mock_pf, 'url_surtkey')

    assert is_sorted is True
    assert min_val is not None
    assert max_val is not None


# Tests for non-sorted row groups

def test_two_row_groups_overlapping():
    """Test with two row groups where second min is less than first max (overlapping)"""
    mock_pf = _create_mock_parquet_file(
        'url_surtkey',
        [
            ('a', 'd'),
            ('b', 'e')
        ]
    )

    is_sorted, min_val, max_val = are_parquet_file_row_groups_sorted(mock_pf, 'url_surtkey')

    assert is_sorted is False
    assert min_val is None
    assert max_val is None


def test_row_groups_completely_out_of_order():
    """Test with row groups in descending order"""
    mock_pf = _create_mock_parquet_file(
        'url_surtkey',
        [
            ('z', 'zz'),
            ('a', 'b')  # completely before the first group
        ]
    )

    is_sorted, min_val, max_val = are_parquet_file_row_groups_sorted(mock_pf, 'url_surtkey')

    assert is_sorted is False
    assert min_val is None
    assert max_val is None


def test_multiple_row_groups_with_middle_unsorted():
    """Test with multiple row groups where the middle one breaks the sort order"""
    mock_pf = _create_mock_parquet_file(
        'url_surtkey',
        [
            ('a', 'b'),
            ('z', 'zz'),  # correctly sorted so far
            ('c', 'd')  # breaks ordering (min 'c' < previous max 'zz')
        ]
    )

    is_sorted, min_val, max_val = are_parquet_file_row_groups_sorted(mock_pf, 'url_surtkey')

    assert is_sorted is False
    assert min_val is None
    assert max_val is None


def test_row_groups_equal_boundary_allowed():
    """Test that row groups where second min equals first max are allowed (>= not >)"""
    mock_pf = _create_mock_parquet_file(
        'url_surtkey',
        [
            ('a', 'b'),
            ('b', 'c')  # min equals prev_max - this is allowed
        ]
    )

    is_sorted, min_val, max_val = are_parquet_file_row_groups_sorted(mock_pf, 'url_surtkey')

    assert is_sorted is True
    assert min_val is not None
    assert max_val is not None


def test_slight_overlap_in_middle():
    """Test detecting overlap in the middle of many row groups"""
    mock_pf = _create_mock_parquet_file(
        'url_surtkey',
        [
            ('a', 'az'),
            ('b', 'bz'),
            ('c', 'cz'),
            ('ba', 'baz'),  # overlaps with previous ('ba' < 'c')
            ('d', 'dz'),
        ]
    )

    is_sorted, min_val, max_val = are_parquet_file_row_groups_sorted(mock_pf, 'url_surtkey')

    assert is_sorted is False
    assert min_val is None
    assert max_val is None


