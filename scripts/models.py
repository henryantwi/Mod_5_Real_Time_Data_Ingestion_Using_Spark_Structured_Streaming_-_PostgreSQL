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
import os
import re
import json

# =============================================================================
# Logging Configuration
# =============================================================================
# Log directory - uses /opt/spark/work-dir/logs in Docker, or local logs/ folder
LOG_DIR = os.environ.get("VALIDATION_LOG_DIR", "/opt/spark/work-dir/logs")
LOG_FILE = os.path.join(LOG_DIR, "validation_errors.log")

# Create log directory if it doesn't exist
os.makedirs(LOG_DIR, exist_ok=True)

# Configure console logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create a dedicated file handler for validation errors
validation_file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
validation_file_handler.setLevel(logging.WARNING)
validation_file_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
)
logger.addHandler(validation_file_handler)

# Separate logger for detailed JSON error logs (for analysis)
error_logger = logging.getLogger("validation_errors")
error_logger.setLevel(logging.WARNING)
error_json_handler = logging.FileHandler(
    os.path.join(LOG_DIR, "validation_errors_detailed.json"), 
    mode='a', 
    encoding='utf-8'
)
error_json_handler.setFormatter(logging.Formatter('%(message)s'))
error_logger.addHandler(error_json_handler)


# Valid values for constrained fields
VALID_EVENT_TYPES = ["view", "add_to_cart", "remove_from_cart", "purchase", "wishlist"]
VALID_DEVICE_TYPES = ["mobile", "desktop", "tablet"]

# UUID pattern for validation
# Standard UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
UUID_PATTERN = re.compile(r'^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$')


class EcommerceEvent(BaseModel):
    """
    Pydantic model for validating e-commerce event data.
    
    Validates all fields according to business rules:
    - Required fields must be present and non-empty
    - UUID fields must be in standard format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    - product_id: PROD prefix (e.g., PROD001)
    - Event type must be one of the allowed values
    - Prices must be non-negative
    - Quantities must be at least 1
    """
    
    # Required fields with UUID-based identifiers
    # Standard UUIDs are 36 characters
    event_id: str = Field(..., min_length=36, max_length=36, description="Unique event identifier (UUID format)")
    user_id: str = Field(..., min_length=36, max_length=36, description="User identifier (UUID format)")
    event_type: Literal["view", "add_to_cart", "remove_from_cart", "purchase", "wishlist"]
    product_id: str = Field(..., min_length=3, description="Product identifier (PROD*)")
    product_name: str = Field(..., min_length=1, description="Product name")
    event_timestamp: datetime  # Automatically parses string to datetime
    
    # Optional fields with constraints
    product_category: Optional[str] = None
    product_price: Optional[float] = Field(None, ge=0, description="Price must be >= 0")
    quantity: Optional[int] = Field(None, ge=1, description="Quantity must be >= 1")
    session_id: Optional[str] = Field(None, min_length=36, max_length=36, description="Session identifier (UUID format)")
    device_type: Optional[Literal["mobile", "desktop", "tablet"]] = None

    @field_validator("event_id")
    @classmethod
    def validate_event_id_format(cls, v: str) -> str:
        """Validate event_id is a valid UUID format."""
        if not UUID_PATTERN.match(v):
            raise ValueError("event_id must be a valid UUID (format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)")
        return v

    @field_validator("user_id")
    @classmethod
    def validate_user_id_format(cls, v: str) -> str:
        """Validate user_id is a valid UUID format."""
        if not UUID_PATTERN.match(v):
            raise ValueError("user_id must be a valid UUID (format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)")
        return v

    @field_validator("product_id")
    @classmethod
    def validate_product_id_format(cls, v: str) -> str:
        """Ensure product_id starts with 'PROD'."""
        if not v.startswith("PROD"):
            raise ValueError("product_id must start with 'PROD'")
        return v

    @field_validator("session_id")
    @classmethod
    def validate_session_id_format(cls, v: Optional[str]) -> Optional[str]:
        """Validate session_id is a valid UUID format (if provided)."""
        if v is None:
            return v
        if not UUID_PATTERN.match(v):
            raise ValueError("session_id must be a valid UUID (format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)")
        return v

    class Config:
        # Allow population by field name (for Spark Row compatibility)
        populate_by_name = True


def _serialize_for_json(obj: Any) -> Any:
    """Convert non-JSON-serializable objects to strings."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def _log_validation_error_to_file(row_dict: Dict[str, Any], error_msg: str, batch_id: Optional[int] = None) -> None:
    """
    Log a validation error to the detailed JSON log file.
    
    Args:
        row_dict: The original data that failed validation
        error_msg: The validation error message
        batch_id: Optional batch identifier
    """
    # Create a JSON-serializable copy of the data
    serializable_data = {}
    for key, value in row_dict.items():
        serializable_data[key] = _serialize_for_json(value)
    
    error_record = {
        "timestamp": datetime.now().isoformat(),
        "batch_id": batch_id,
        "error": error_msg,
        "data": serializable_data
    }
    error_logger.warning(json.dumps(error_record))


def validate_event(row_dict: Dict[str, Any], batch_id: Optional[int] = None) -> Tuple[bool, Optional[Dict], Optional[str]]:
    """
    Validate a single event row using the Pydantic model.
    
    Args:
        row_dict: Dictionary containing event data
        batch_id: Optional batch identifier for logging
        
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
        # Log to file
        _log_validation_error_to_file(row_dict, error_msg, batch_id)
        return False, None, error_msg


def validate_batch(rows: List[Dict[str, Any]], batch_id: Optional[int] = None) -> Tuple[List[Dict], List[Dict]]:
    """
    Validate a batch of event rows.
    
    Args:
        rows: List of dictionaries containing event data
        batch_id: Optional batch identifier for logging
        
    Returns:
        Tuple of (valid_rows, invalid_rows)
        - valid_rows: List of validated event dictionaries
        - invalid_rows: List of dicts with 'data' and 'error' keys
    """
    valid_rows = []
    invalid_rows = []
    
    for row_dict in rows:
        is_valid, validated_data, error_msg = validate_event(row_dict, batch_id)
        
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
    """Log a summary of batch validation results to both console and file."""
    total = valid_count + invalid_count
    if invalid_count > 0:
        msg = (
            f"Batch {batch_id}: {invalid_count}/{total} records failed validation "
            f"({valid_count} valid, {invalid_count} rejected)"
        )
        logger.warning(msg)
        
        # Also log batch summary to JSON file
        summary_record = {
            "timestamp": datetime.now().isoformat(),
            "type": "batch_summary",
            "batch_id": batch_id,
            "total_records": total,
            "valid_count": valid_count,
            "invalid_count": invalid_count
        }
        error_logger.warning(json.dumps(summary_record))
    else:
        logger.info(f"Batch {batch_id}: All {total} records passed validation")


def get_log_file_paths() -> Dict[str, str]:
    """Return the paths to the log files for external reference."""
    return {
        "log_directory": LOG_DIR,
        "validation_errors_log": LOG_FILE,
        "validation_errors_json": os.path.join(LOG_DIR, "validation_errors_detailed.json")
    }
