import pytest
from unittest.mock import MagicMock

@pytest.mark.unit
def test_spark_cleaning_mock():
    """
    Test transformation logic without requiring real Spark.
    """

    # Mock a Spark DataFrame
    mock_df = MagicMock()
    mock_df.dropna.return_value = "cleaned_df"

    # Your transformation logic
    cleaned = mock_df.dropna()

    assert cleaned == "cleaned_df"
    mock_df.dropna.assert_called_once()
