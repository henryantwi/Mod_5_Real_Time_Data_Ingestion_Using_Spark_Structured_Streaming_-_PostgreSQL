"""
Pydantic Models for E-Commerce Event Validation
================================================
This module defines Pydantic models for validating e-commerce event data
before writing to PostgreSQL. Uses row-level validation with error logging.

Usage:
    from models import EcommerceEvent, validate_event
"""

from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Optional, Literal, List, Tuple, Dict, Any
from datetime import datetime
import logging

# Configure logging for validation errors
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Valid values for constrained fields
VALID_EVENT_TYPES = ["view", "add_to_cart", "remove_from_cart", "purchase", "wishlist"]
VALID_DEVICE_TYPES = ["mobile", "desktop", "tablet"]


class EcommerceEvent(BaseModel):
    """
    Pydantic model for validating e-commerce event data.
    
    Validates all fields according to business rules:
    - Required fields must be present and non-empty
    - IDs must follow the expected format (EVT_, USER_, PROD_, SESS_)
    - Event type must be one of the allowed values
    - Prices must be non-negative
    - Quantities must be at least 1
    """
    
    # Required fields
    event_id: str = Field(..., min_length=5, description="Unique event identifier (EVT_*)")
    user_id: str = Field(..., min_length=5, description="User identifier (USER_*)")
    event_type: Literal["view", "add_to_cart", "remove_from_cart", "purchase", "wishlist"]
    product_id: str = Field(..., min_length=3, description="Product identifier (PROD*)")
    product_name: str = Field(..., min_length=1, description="Product name")
    event_timestamp: datetime  # Automatically parses string to datetime
    
    # Optional fields with constraints
    product_category: Optional[str] = None
    product_price: Optional[float] = Field(None, ge=0, description="Price must be >= 0")
    quantity: Optional[int] = Field(None, ge=1, description="Quantity must be >= 1")
    session_id: Optional[str] = None
    device_type: Optional[Literal["mobile", "desktop", "tablet"]] = None

    @field_validator("event_id")
    @classmethod
    def validate_event_id_format(cls, v: str) -> str:
        """Ensure event_id starts with 'EVT_'."""
        if not v.startswith("EVT_"):
            raise ValueError("event_id must start with 'EVT_'")
        return v

    @field_validator("user_id")
    @classmethod
    def validate_user_id_format(cls, v: str) -> str:
        """Ensure user_id starts with 'USER_'."""
        if not v.startswith("USER_"):
            raise ValueError("user_id must start with 'USER_'")
        return v

    @field_validator("product_id")
    @classmethod
    def validate_product_id_format(cls, v: str) -> str:
        """Ensure product_id starts with 'PROD'."""
        if not v.startswith("PROD"):
            raise ValueError("product_id must start with 'PROD'")
        return v

    class Config:
        # Allow population by field name (for Spark Row compatibility)
        populate_by_name = True


def validate_event(row_dict: Dict[str, Any]) -> Tuple[bool, Optional[Dict], Optional[str]]:
    """
    Validate a single event row using the Pydantic model.
    
    Args:
        row_dict: Dictionary containing event data
        
    Returns:
        Tuple of (is_valid, validated_data, error_message)
        - is_valid: True if validation passed
        - validated_data: The validated data dict (None if invalid)
        - error_message: Error description (None if valid)
    """
    try:
        validated = EcommerceEvent(**row_dict)
        return True, validated.model_dump(), None
    except ValidationError as e:
        error_msg = "; ".join([f"{err['loc'][0]}: {err['msg']}" for err in e.errors()])
        return False, None, error_msg


def validate_batch(rows: List[Dict[str, Any]]) -> Tuple[List[Dict], List[Dict]]:
    """
    Validate a batch of event rows.
    
    Args:
        rows: List of dictionaries containing event data
        
    Returns:
        Tuple of (valid_rows, invalid_rows)
        - valid_rows: List of validated event dictionaries
        - invalid_rows: List of dicts with 'data' and 'error' keys
    """
    valid_rows = []
    invalid_rows = []
    
    for row_dict in rows:
        is_valid, validated_data, error_msg = validate_event(row_dict)
        
        if is_valid:
            valid_rows.append(validated_data)
        else:
            invalid_rows.append({
                "data": row_dict,
                "error": error_msg
            })
            logger.warning(f"Validation failed: {error_msg} | Data: {row_dict}")
    
    return valid_rows, invalid_rows


def log_validation_summary(batch_id: int, valid_count: int, invalid_count: int) -> None:
    """Log a summary of batch validation results."""
    total = valid_count + invalid_count
    if invalid_count > 0:
        logger.warning(
            f"Batch {batch_id}: {invalid_count}/{total} records failed validation "
            f"({valid_count} valid, {invalid_count} rejected)"
        )
    else:
        logger.info(f"Batch {batch_id}: All {total} records passed validation")
