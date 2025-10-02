from datetime import datetime
from typing import Any, List
from athena_udf import BaseAthenaUDF
from pyarrow import Schema # Keep the Schema import, as this is the style you use

class CustomUDFHandler(BaseAthenaUDF):
    
    # --- Existing (Working) UDF Logic ---
    # Keep your existing handle_athena_record for UPPER, if it's currently working.
    # If your date UDFs and UPPER UDF are meant to be in the *same* Lambda, 
    # we need a single handler to route all calls. Let's assume you're moving 
    # everything to this single, robust handler style.

    # --- Low-Level Date Logic Functions (Internal) ---

    @staticmethod
    def _datediff_logic(end_date: datetime, start_date: datetime) -> int:
        """Internal logic for DATEDIFF: calculates difference in full days."""
        # Convert to date objects to ignore the time component, as is standard
        time_delta = end_date.date() - start_date.date()
        return time_delta.days

    @staticmethod
    def _date_trunc_logic(unit: str, input_date: datetime) -> datetime:
        """Internal logic for DATE_TRUNC: truncates to the unit."""
        unit = unit.strip().upper()
        
        if unit == 'MONTH':
            # Truncate to the first day of the month at midnight
            return input_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        elif unit == 'YEAR':
            # Truncate to the first day of the year at midnight
            return input_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        
        return input_date # Default to return original if unit is unsupported

    # --- The Single, High-Confidence Handler ---
    @staticmethod
    def handle_athena_record(input_schema: Schema, output_schema: Schema, arguments: List[Any]):
        """
        The single entry point that Athena calls for *all* registered UDFs 
        that point to this Lambda's ARN.
        """
        # 1. Determine which UDF Athena is calling
        function_name = input_schema.metadata.get(b'function_name', b'').decode('utf-8')

        if function_name == 'DATEDIFF_IMPA':
            # arguments[0] is EndDate (TIMESTAMP), arguments[1] is StartDate (TIMESTAMP)
            # The low-level protocol passes Python datetime objects for TIMESTAMP.
            end_date = arguments[0]
            start_date = arguments[1]
            return CustomUDFHandler._datediff_logic(end_date, start_date)
            
        elif function_name == 'DATE_TRUNC_IMPA':
            # arguments[0] is Unit (VARCHAR/STRING), arguments[1] is Date (TIMESTAMP)
            unit = arguments[0]
            input_date = arguments[1]
            return CustomUDFHandler._date_trunc_logic(unit, input_date)
        
        elif function_name == 'UPPER_IMPA':
             # Re-implement your working UPPER UDF logic here for completeness
            text = arguments[0]
            return None if text is None else str(text).upper()

        else:
            # Fallback for an unknown function name
            raise ValueError(f"Unknown UDF requested: {function_name}")


# This line remains crucial for the Lambda initialization.
lambda_handler = CustomUDFHandler(use_threads=False).lambda_handler
