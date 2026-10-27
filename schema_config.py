"""
Schema Mapping Configuration
=============================
This file defines how to map different data sources to our standard schema.

Standard Schema (What our model expects):
- user_id: string
- amount: float
- location: string
- timestamp: long
- home_loc: string
"""

# Our standard schema that the ML model was trained on
STANDARD_SCHEMA = {
    "user_id": "string",
    "amount": "double",
    "location": "string",
    "timestamp": "double",
    "home_loc": "string",
    "is_fraud": "long"  # Only in training data
}

# Schema mappings for different data sources
# Format: "our_field": "their_field"
SCHEMA_MAPPINGS = {
    
    # Our synthetic data (producer.py) - already in correct format
    "synthetic": {
        "user_id": "user_id",
        "amount": "amount",
        "location": "location",
        "timestamp": "timestamp",
        "home_loc": "home_loc",
        "is_fraud": "is_fraud"
    },
    
    # Example: Bank A uses different field names
    "bank_a": {
        "user_id": "customer_number",
        "amount": "transaction_amount_usd",
        "location": "merchant_country",
        "timestamp": "event_time",
        "home_loc": "billing_address_country"
    },
    
    # Example: Bank B uses completely different names
    "bank_b": {
        "user_id": "cust_id",
        "amount": "amt",
        "location": "country",
        "timestamp": "tx_timestamp",
        "home_loc": "home_country"
    },
    
    # Example: Payment Gateway (Stripe/PayPal style)
    "payment_gateway": {
        "user_id": "customer.id",  # Nested field
        "amount": "amount_total",
        "location": "shipping.country",  # Nested field
        "timestamp": "created",
        "home_loc": "billing.country"  # Nested field
    }
}

# Data type conversions (if needed)
TYPE_CONVERSIONS = {
    "amount": "double",  # Ensure it's a number
    "timestamp": "double",  # Ensure it's a unix timestamp
}

# Location code mappings (ISO codes to full names)
LOCATION_MAPPINGS = {
    "US": "USA",
    "NG": "Nigeria",
    "IN": "India",
    "GB": "UK",
    "CN": "China",
    "RU": "Russia",
    # Add more as needed
}


def get_mapping(source_name):
    """
    Get the schema mapping for a specific data source.
    
    Args:
        source_name: Name of the data source (e.g., 'bank_a', 'synthetic')
    
    Returns:
        Dictionary mapping our fields to their fields
    """
    if source_name not in SCHEMA_MAPPINGS:
        raise ValueError(
            f"Unknown data source: {source_name}. "
            f"Available sources: {list(SCHEMA_MAPPINGS.keys())}"
        )
    return SCHEMA_MAPPINGS[source_name]


def validate_required_fields(data, source_name):
    """
    Check if all required fields are present in the data.
    
    Args:
        data: Dictionary of the incoming data
        source_name: Name of the data source
    
    Returns:
        List of missing fields (empty if all present)
    """
    mapping = get_mapping(source_name)
    missing_fields = []
    
    for our_field, their_field in mapping.items():
        if our_field != "is_fraud":  # is_fraud is optional for real-time data
            if their_field not in data:
                missing_fields.append(their_field)
    
    return missing_fields
