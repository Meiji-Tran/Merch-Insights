# Import packages
import datetime
import os
import warnings

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# Make directory for parquet files if it doesn't exist
os.makedirs("parquets", exist_ok=True)

# Make directory for plots (png files) if it doesn't exist
os.makedirs("plots", exist_ok=True)



# Dictionary of dates
dates = {
    "start_date": "2024-05-01",
    "end_date": "2025-04-30",
    "pp_start_date": "2022-10-01",
}


# Set date range function
def set_dates(con, dates: dict):
    # Check if dates are valid
    for date in dates.values():
        try:
            datetime.date.fromisoformat(date)
        except ValueError:
            raise ValueError("Incorrect date format, should be YYYY-MM-DD")

    # Set date range
    for k, v in dates.items():
        with con.cursor() as cursor:
            cursor.execute(f"""SET {k} = '{v}';""")

    # Gather necessary items to construct the query
    select = [(i + 1, " AS ", date) for i, date in enumerate(dates)]
    values = ["$" + date for date in dates]

    # Construct SELECT clause of query
    select_query = ""
    for col in select:
        line = "".join(str(j) for j in col)
        line = "$" + line
        # Add a comma if it's not the last date we're selecting
        if col[0] != len(select):
            line += ",\n    "
        else:
            line += "\n"
        select_query += line

    # Construct FROM/VALUES clause of query
    values_query = "(" + ", ".join(values) + ")"

    # Final date checking query
    query = f"SELECT\n    {select_query}FROM VALUES{values_query}\n"

    # Return table of dates to confirm
    with warnings.catch_warnings():
        warnings.filterwarnings(
            action="ignore", message=".*SQLAlchemy.*", category=UserWarning
        )
        df = pd.read_sql_query(query, con)
        display(df)
