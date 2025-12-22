import random
from unittest.mock import MagicMock, patch

from util.are_part_min_max_increasing import are_parquet_file_row_groups_min_max_ordered, are_all_parts_min_max_ordered


def _create_mock_parquet_file(column_name: str, row_groups_stats: list[tuple[str, str]]):
    mock_pf = MagicMock()
    mock_pf.schema.names = [column_name]
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


def test_single_row_group_sorted():
    mock_pf = _create_mock_parquet_file('url_surtkey', [('a', 'b')])
    is_sorted = are_parquet_file_row_groups_min_max_ordered(mock_pf, column_name='url_surtkey')
    assert is_sorted


def test_row_groups_sorted():
    all_row_groups_stats = [('a', 'b'), ('c', 'd'), ('e', 'f'), ('g', 'h')]
    for n in range(1, len(all_row_groups_stats)):
        row_groups_stats = all_row_groups_stats[:n]
        mock_pf = _create_mock_parquet_file('url_surtkey', row_groups_stats)
        is_sorted = are_parquet_file_row_groups_min_max_ordered(mock_pf, column_name='url_surtkey')
        assert is_sorted


def test_row_groups_unsorted():
    all_row_groups_stats = [('a', 'b'), ('c', 'd'), ('e', 'f'), ('g', 'h')]
    count = 0
    while count < 100:
        for n in range(2, len(all_row_groups_stats)):
            row_groups_stats = all_row_groups_stats[:n].copy()
            random.shuffle(row_groups_stats)
            if row_groups_stats == all_row_groups_stats[:n]:
                # shuffle resulted in same order, try again
                continue

            mock_pf = _create_mock_parquet_file('url_surtkey', row_groups_stats)
            is_sorted = are_parquet_file_row_groups_min_max_ordered(mock_pf, column_name='url_surtkey')
            assert not is_sorted

        count += 1


def test_row_groups_null_stats_inorder():
    row_groups = [('a', 'b'), (None, None), ('c', 'd')]
    mock_pf = _create_mock_parquet_file('url_surtkey', row_groups)
    is_sorted = are_parquet_file_row_groups_min_max_ordered(mock_pf, column_name='url_surtkey')
    assert is_sorted


def test_row_groups_null_stats_out_of_order():
    row_groups = [('c', 'd'), (None, None), ('a', 'b')]
    mock_pf = _create_mock_parquet_file('url_surtkey', row_groups)
    is_sorted = are_parquet_file_row_groups_min_max_ordered(mock_pf, column_name='url_surtkey')
    assert not is_sorted


def test_row_groups_overlapping_min_max():
    row_groups = [('a', 'b'), ('b', 'd'), ('e', 'f'), ('g', 'h')]
    mock_pf = _create_mock_parquet_file('url_surtkey', row_groups)
    is_sorted = are_parquet_file_row_groups_min_max_ordered(mock_pf, column_name='url_surtkey')
    assert is_sorted


def test_row_groups_overlapping():
    row_groups = [('a', 'c'), ('b', 'd')]
    mock_pf = _create_mock_parquet_file('url_surtkey', row_groups)
    is_sorted = are_parquet_file_row_groups_min_max_ordered(mock_pf, column_name='url_surtkey')
    assert not is_sorted


def test_ordered_files_sorted():
    files_config = {
        '/data/a': [('aaa', 'bbb'), ('bbc', 'ccc')],
        '/data/b': [('ccd', 'ddd'), ('dde', 'eee')],
        '/data/c': [('eef', 'fff'), ('ffg', 'ggg')],
    }

    def mock_parquet_file(path):
        return _create_mock_parquet_file('url_surtkey', files_config[path])

    with patch('pyarrow.parquet.ParquetFile', side_effect=mock_parquet_file):
        result = are_all_parts_min_max_ordered(['/data/a', '/data/b', '/data/c'], 'url_surtkey')
        assert result


def test_ordered_files_unsorted():
    files_config = {
        '/data/a': [('aaa', 'bbb'), ('bbc', 'ccc')],
        '/data/b': [('ccd', 'ddd'), ('dde', 'eee')],
        '/data/c': [('eef', 'fff'), ('ffg', 'ggg')],
    }

    def mock_parquet_file(path):
        return _create_mock_parquet_file('url_surtkey', files_config[path])

    with patch('pyarrow.parquet.ParquetFile', side_effect=mock_parquet_file):
        result = are_all_parts_min_max_ordered(['/data/a', '/data/c', '/data/b'], 'url_surtkey')
        assert result  # we don't care about the order of files

