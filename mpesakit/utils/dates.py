"""Utility functions for handling date validation in the M-Pesa SDK."""

DEFAULT_DARAJA_DATE = "01-01-1900"


def is_daraja_default_date(date_str: str | None) -> bool:
    """Check if a date string is missing, empty, or represents Safaricom's default sentinel date.

    - If it is None or not a string, return True (treated as default/missing)
    - Strips whitespace and handles spacing anomalies safely
    - Checks if it starts with '01-01-1900'

    Args:
        date_str (str | None): The date string to check.

    Returns:
        bool: True if it is the default sentinel date or missing, False otherwise.
    """
    if not isinstance(date_str, str):
        return True

    cleaned = date_str.strip().replace(" ", "")
    if not cleaned:
        return True

    return cleaned.startswith(DEFAULT_DARAJA_DATE)
