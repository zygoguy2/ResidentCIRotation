import pandas as pd
from sqlalchemy import create_engine, text

def print_sql_table(sql_query):
    """
    Executes a SQL query and prints the results in a formatted table using pandas,
    with column widths adjusted to the largest entry.

    Args:
        conn: The psycopg2 database connection object.
        sql_query: The SQL query string to execute.
    """
    try:
        engine = create_engine(
            "postgresql+psycopg2://resident:secure_password_here@pirotationpostgres-1.clug0i4i67w8.us-east-2.rds.amazonaws.com:5432/postgres"
        )
        with engine.connect() as conn:
            df = pd.read_sql(sql_query, conn)
        
        if df.empty:
            print("No results found for the query.")
            return
        
        # Convert float columns that should be int to string (preserves NULLs as "None")
        for col in df.columns:
            if df[col].dtype == 'float64' and df[col].isna().any():
                df[col] = df[col].fillna('NULL').astype(str)
        
        # Calculate maximum width for each column
        column_widths = [int(max(len(str(col)), df[col].astype(str).str.len().max())) 
                        for col in df.columns]
        
        # Format header
        header_format_string = " | ".join(f"{{:<{width}}}" for width in column_widths)
        print(header_format_string.format(*df.columns))
        
        # Print separator line
        separator_length = sum(column_widths) + 3 * (len(df.columns) - 1)
        print("-" * separator_length)
        
        # Print rows
        row_format_string = " | ".join(f"{{:<{width}}}" for width in column_widths)
        for _, row in df.iterrows():
            print(row_format_string.format(*(str(item) for item in row)))
    
    except Exception as e:
        print(f"Error executing SQL query: {e}")

def get_data(SQL_query) -> pd.DataFrame:
    """
    Fetch data from database using a SQL query.
    
    Parameters:
    -----------
    SQL_query : str
        SQL query to execute
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing the query results
    """
    engine = create_engine(
        "postgresql+psycopg2://resident:secure_password_here@pirotationpostgres-1.clug0i4i67w8.us-east-2.rds.amazonaws.com:5432/postgres"
    )
    with engine.connect() as conn:
        df = pd.read_sql(SQL_query, conn)
    return df

def get_qc_data(analyzer_name):
    """
    Fetch QC and patient results data from database.
    
    Parameters:
    -----------
    analyzer_name : str
        Name of the analyzer (e.g., 'Sodium, Plasma')
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame with columns: timestamp, qc_value, patient_result
    """
    engine = create_engine(
        "postgresql+psycopg2://resident:secure_password_here@pirotationpostgres-1.clug0i4i67w8.us-east-2.rds.amazonaws.com:5432/postgres"
    )

    query = text(f"""
    SELECT 
        qc.timestamp,
        qc.result
    FROM qc_results qc
    JOIN analyzers a ON qc.analyzer = a.id
    WHERE a.name = '{analyzer_name}'
    ORDER BY qc.timestamp, qc.result;
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    return df

def get_reference_ranges(analyzer_name):
    """
    Fetch reference range data from database for a given analyzer.
    
    Parameters:
    -----------
    analyzer_name : str
        Name of the analyzer (e.g., 'Sodium, Plasma')
    
    Returns:
    --------
    tuple (ref_lower, ref_upper) reference range values
    """
    engine = create_engine(
        "postgresql+psycopg2://resident:secure_password_here@pirotationpostgres-1.clug0i4i67w8.us-east-2.rds.amazonaws.com:5432/postgres"
    )

    # FILL IN THE QUERY TO FETCH REFERENCE RANGES BASED ON THE ANALYZER NAME
    query = f"""
    Select rr_lower_bound, rr_upper_bound from analyzers where name = '{analyzer_name}'"""

    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
        if result:
            ref_lower, ref_upper = result
            return ref_lower, ref_upper
        else:
            return None, None

def westgard_check (analyzer_name):
    """
    Placeholder for Westgard rules implementation.
    """

    engine = create_engine(
        "postgresql+psycopg2://resident:secure_password_here@pirotationpostgres-1.clug0i4i67w8.us-east-2.rds.amazonaws.com:5432/postgres"
    )

    # FILL IN THE QUERY TO FETCH REFERENCE RANGES BASED ON THE ANALYZER NAME
    query = f"""
    
    """

    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
        if result:
            ref_lower, ref_upper = result
            return ref_lower, ref_upper
        else:
            return None, None


    pass

def perform_westguard_analysis(data: pd.Series, mean: float, sd: float) -> tuple[pd.Series, pd.Series]:
    # Our rules that we write will live here.
    # The code here will be developed in the cells below.
    # And this function will eventually live in src/qc.py
    warnings_mask = (data > mean + 2*sd) | (data < mean - 2*sd) # Example 1S2 rule
    failures_mask = (data > mean + 3*sd) | (data < mean - 3*sd) # Example 1S3 rule
    return warnings_mask, failures_mask