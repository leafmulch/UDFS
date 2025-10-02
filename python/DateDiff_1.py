
from datetime import datetime, date
from typing import Any
from athena_udf import BaseAthenaUDF
from pyarrow import Schema

class DateDiffUDF(BaseAthenaUDF):
    @staticmethod
    def handle_athena_record(input_schema: Schema, output_schema: Schema, arguments: list[Any]):
        if len(arguments) < 2 or arguments[0] is None or arguments[1] is None:
            return None

        end_date = arguments[0]
        start_date = arguments[1]

        # Handle PyArrow scalars
        if hasattr(end_date, 'as_py'):
            end_date = end_date.as_py()
        if hasattr(start_date, 'as_py'):
            start_date = start_date.as_py()

        # Convert to date if it's a datetime
        if isinstance(end_date, datetime):
            end_date = end_date.date()
        if isinstance(start_date, datetime):
            start_date = start_date.date()

        # If strings, parse them
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').date()
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').date()

        # Now both should be date objects
        time_delta = end_date - start_date
        return time_delta.days

def lambda_handler(event, context):
    return DateDiffUDF(use_threads=False).lambda_handler(event, context)
