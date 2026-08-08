"""Utility functions for IP whitelisting, specifically for M-Pesa IP ranges."""

import ipaddress
from typing import List, Optional

# M-Pesa IP addresses (individual IPs).
#
# Source: Safaricom's official "Callback and IP Whitelisting" list at
# https://developer.safaricom.co.ke/apis/GettingStarted (verified 2026-08-08).
# 196.201.212.128 and 196.201.212.132 are not on that published list but are
# kept here as a superset for backward compatibility.
MPESA_IP_ADDRESSES = {
    ipaddress.ip_address("196.201.214.200"),
    ipaddress.ip_address("196.201.214.206"),
    ipaddress.ip_address("196.201.213.114"),
    ipaddress.ip_address("196.201.214.207"),
    ipaddress.ip_address("196.201.214.208"),
    ipaddress.ip_address("196.201.213.44"),
    ipaddress.ip_address("196.201.212.127"),
    ipaddress.ip_address("196.201.212.128"),
    ipaddress.ip_address("196.201.212.129"),
    ipaddress.ip_address("196.201.212.132"),
    ipaddress.ip_address("196.201.212.136"),
    ipaddress.ip_address("196.201.212.138"),
    ipaddress.ip_address("196.201.212.69"),
    ipaddress.ip_address("196.201.212.74"),
}


def is_mpesa_ip_allowed(
    ip_address: str, allowed_ips: Optional[List[str]] = None
) -> bool:
    """Check if an IP address is in the M-Pesa allowed IP addresses.

    Args:
        ip_address (str): The IP address to check
        allowed_ips (List[str], optional): Custom IP addresses.
                                         Defaults to M-Pesa IPs.

    Returns:
        bool: True if IP is allowed, False otherwise

    Example:
        >>> is_mpesa_ip_allowed('196.201.214.200')
        True
        >>> is_mpesa_ip_allowed('192.168.1.1')
        False
    """
    try:
        ip_obj = ipaddress.ip_address(ip_address)

        if allowed_ips is not None:
            # Convert custom IPs to set of ip_address objects
            ip_set = {ipaddress.ip_address(ip) for ip in allowed_ips}
            return ip_obj in ip_set
        else:
            # Use pre-computed set for better performance
            return ip_obj in MPESA_IP_ADDRESSES

    except (ValueError, ipaddress.AddressValueError):
        return False
