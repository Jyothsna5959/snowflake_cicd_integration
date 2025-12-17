import datetime
from typing import Dict, Any, Optional

def parse_copy_into_result(copy_into_response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse Snowflake COPY INTO response and return a dictionary
    ready for audit table insert.
    """
    if copy_into_response.get("status") != "SUCCESS":
        return None

    results = copy_into_response.get("results")
    if not results or len(results) == 0:
        return None

    (
        file_name,
        load_status,
        row_count,
        rows_loaded,
        errors_seen,
        errors_limit,
        *_,
    ) = results[0]

    return {
        "file_name": file_name,
        "load_status": load_status,
        "row_count": row_count,
        "rows_loaded": rows_loaded,
        "errors_seen": errors_seen,
        "errors_limit": errors_limit,
        "load_time": datetime.datetime.utcnow()
    }