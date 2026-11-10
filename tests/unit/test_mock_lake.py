from data_engineering.config import SOURCE_NAMES
from data_engineering.load import REG_COLS, _SOURCE_COLUMNS
from data_engineering.mock_lake import ROWS, SCHEMAS


def test_each_source_has_the_columns_the_load_reads():
    assert set(SCHEMAS) == set(SOURCE_NAMES)
    for name, columns in _SOURCE_COLUMNS.items():
        assert SCHEMAS[name].fieldNames() == list(columns)
    assert SCHEMAS["reg"].fieldNames() == list(REG_COLS)


def test_every_source_has_rows():
    assert set(ROWS) == set(SOURCE_NAMES)
    for name, rows in ROWS.items():
        width = len(SCHEMAS[name].fields)
        assert rows
        assert {len(row) for row in rows} == {width}
