"""Unit tests for the is_daraja_default_date function in mpesakit.utils.dates."""

import pytest
from mpesakit.utils.dates import is_daraja_default_date


@pytest.mark.parametrize(
    "input_date,expected",
    [
        # Standard default sentinel dates (should return True)
        ("01-01-1900", True),
        ("01-01-1900T00:00:00", True),  # With timestamp suffix if returned by API

        # With whitespace anomalies (should still normalize and match)
        (" 01-01-1900 ", True),
        ("\t01-01-1900\n", True),
        ("01-01- 1900", True),  # Spaces inside

        # Missing, empty, or whitespace-only inputs (treated as default/missing -> True)
        ("", True),
        ("   ", True),
        (None, True),

        # Valid / non-default dates (should return False)
        ("15-06-2023", False),
        ("2023-06-15", False),
        (" 12-12-2024 ", False),
        ("01-01-2023", False),  # Different year

        # Non-string inputs (defensive handling returns True)
        (19000101, True),
        ([], True),
        ({}, True),
    ],
)
def test_is_daraja_default_date(input_date, expected):
    """Test the is_daraja_default_date function with various date inputs and edge cases."""
    assert is_daraja_default_date(input_date) == expected
