from rich.console import Console
from rich.table import Table
from typing import *

import numpy as np
import psycopg
import pymssql
import logging
import inspect
import json
import re

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# with open('./assets/private/database_connection_information.json', 'r') as f:
#     database_connection_info = json.load(f)
#     postgreSQL_connection_info = database_connection_info['postgreSQL_connection_info']


# def introduction(function_name: Optional[Union[str, callable]] = None) -> None:
#     table = Table(show_header=True, header_style="bold magenta")
#     if function_name is None:
#         table.add_column("Function Name", style="dim", width=40)
#         table.add_column("Description")
#         table.add_row("get_postgreSQL_connection", "Connects to PostgreSQL using the provided connection information.")
#         table.add_row("fetch_data_from_postgreSQL", "Fetches data from a PostgreSQL table.")
#         table.add_row("fetch_vector_data", "Fetches vector data from a PostgreSQL table.")
#         table.add_row("get_mssql_connection", "Connects to MSSQL using the provided connection information.")
#         table.add_row("fetch_data_from_mssql", "Fetches data from a MSSQL table.")
#         table.add_row("get_chat_history_from_mssql", "Fetches chat history from a MSSQL table.")
#     else:
#         if callable(function_name):
#             function_name = function_name.__name__
#         table.add_column("Parameter", style="dim", width=30)
#         table.add_column("Description")
#         if function_name == 'get_postgreSQL_connection':
#             table.add_row("postgreSQL_connection_info", "Dictionary containing PostgreSQL connection information.")
#             table.add_row("hostaddr", "Host address of the PostgreSQL server.")
#             table.add_row("port", "Port number of the PostgreSQL server.")
#             table.add_row("dbname", "Name of the PostgreSQL database.")
#             table.add_row("user", "Username for the PostgreSQL database.")
#             table.add_row("password", "Password for the PostgreSQL database.")
#         elif function_name == 'fetch_data_from_postgreSQL':
#             table.add_row("connection", "PostgreSQL connection object.")
#             table.add_row("table_name", "Name of the table to fetch data from.")
#             table.add_row("query", "Custom SQL query to execute. If provided, overrides the table_name.")
#             table.add_row("query_arguments", "Arguments in the query.")
#             table.add_row("tail", "Additional SQL conditions to append to the query.")
#             table.add_row("tail_argument_key_value", "Additional parameters for the tail conditions.")
#         elif function_name == 'fetch_vector_data':
#             table.add_row("vector_table_name", "Name of the table to fetch vector data from.")
#             table.add_row("text_table_name", "Name of the table to fetch text data from.")
#             table.add_row("image_table_name", "Name of the table to fetch image data from.")
#             table.add_row("connection", "PostgreSQL connection object.")
#             table.add_row("method", "Method for vector comparison ('inner_product' or 'euclidean_distance').")
#             table.add_row("top_k", "Number of top results to return.")
#             table.add_row("score_threshold", "Minimum score threshold for results.")
#             table.add_row("where_arguments", "Additional filtering conditions.")
#         elif function_name == 'get_mssql_connection':
#             table.add_row("mssql_connection_info", "Dictionary containing MSSQL connection information.")
#             table.add_row("server", "Server address of the MSSQL server.")
#             table.add_row("user", "Username for the MSSQL database.")
#             table.add_row("password", "Password for the MSSQL database.")
#             table.add_row("database", "Name of the MSSQL database.")
#             table.add_row("port", "Port number of the MSSQL server.")
#         elif function_name == 'fetch_data_from_mssql':
#             table.add_row("connection", "MSSQL connection object.")
#             table.add_row("table_name", "Name of the table to fetch data from.")
#             table.add_row("query", "Custom SQL query to execute. If provided, overrides the table_name.")
#             table.add_row("query_arguments", "Arguments in the query.")
#             table.add_row("tail", "Additional SQL conditions to append to the query.")
#             table.add_row("tail_argument_key_value", "Additional parameters for the tail conditions.")
#         elif function_name == 'get_chat_history_from_mssql':
#             table.add_row("connection", "MSSQL connection object.")
#             table.add_row("conversation_id", "Conversation ID to filter the chat history.")
#             table.add_row("table_name", "Name of the table to fetch chat history from.")
#             table.add_row("order_column", "Column name to order the chat history.")
#             table.add_row("limit", "Number of chat history records to fetch.")
#     console = Console()
#     console.print(table)
    


# Set up PostgreSQL helpers
def get_postgreSQL_connection(
    postgreSQL_connection_info: Optional[dict] = None,
    hostaddr: Optional[str] = None,
    port: Optional[int] = None,
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> psycopg.Connection:
    """
    Connects to PostgreSQL using the provided connection information.
    Args:
        postgreSQL_connection_info (dict): Dictionary containing PostgreSQL connection information.
        hostaddr (str): Host address of the PostgreSQL server.
        port (int): Port number of the PostgreSQL server.
        dbname (str): Name of the PostgreSQL database.
        user (str): Username for the PostgreSQL database.
        password (str): Password for the PostgreSQL database.
        
    Returns:
        psycopg.Connection: PostgreSQL connection object.
    """
    if postgreSQL_connection_info is None:
        if None in [hostaddr, port, dbname, user, password]:
            raise ValueError("Either 'postgreSQL_connection_info' or all connection parameters must be provided.")
        postgreSQL_connection_info = {
            "hostaddr": hostaddr,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password
        }
    
    for key, item in {'hostaddr': hostaddr, 'port': port, 'dbname': dbname, 'user': user, 'password': password}.items():
        if item is None and not key in postgreSQL_connection_info:
            raise ValueError(f"Connection parameter '{key}' is missing.")
    
    hostaddr = hostaddr if hostaddr else postgreSQL_connection_info.get('hostaddr')
    port = port if port else postgreSQL_connection_info.get('port')
    dbname = dbname if dbname else postgreSQL_connection_info.get('dbname')
    user = user if user else postgreSQL_connection_info.get('user')
    password = password if password else postgreSQL_connection_info.get('password')

    hostaddr = f"hostaddr={hostaddr}"
    port = f"port={port}"
    dbname = f"dbname={dbname}"
    user = f"user={user}"
    password = f"password={password}"
    connection_str = f'{hostaddr} {port} {dbname} {user} {password}'
    
    connection = psycopg.connect(connection_str)
    return connection
  
def fetch_data_from_postgreSQL(
    connection: psycopg.Connection,
    postgreSQL_connection_info: Optional[dict] = None,
    query: Optional[str] = None,
    table_name: Optional[str] = None,
    query_arguments: Optional[list] = None,
    tail: Optional[str] = None,
    tail_argument_key_value: Optional[dict] = None,
    to_pandas: bool = False
) -> list[dict]:
    """
    Fetches data from a PostgreSQL table.
    Args:
        connection (psycopg.Connection): PostgreSQL connection object.
        table_name (str): Name of the table to fetch data from.
        query (Optional[str]): Custom SQL query to execute. If provided, overrides the table_name.
        query_arguments (Optional[list]): Arguments in the query.
        tail (Optional[str]): Additional SQL conditions to append to the query.
        tail_argument_key_value (Optional[dict]): Additional parameters for the tail conditions.
        # TIP: A parameter, "query" has first priority then "table_name".
        #      And "tail" has high priority then "tail_argument_key_value".
        to_pandas (bool): If True, returns the result as a pandas DataFrame.
    Returns:
        list[dict]: List of dictionaries containing the fetched data.
    """
    if connection.closed and postgreSQL_connection_info is not None:
        connection = get_postgreSQL_connection(postgreSQL_connection_info)
    
    with connection.cursor() as cursor:
        if query_arguments is None:
            query_arguments = []

        if query is not None:
            if query_arguments:
                cursor.execute(query, tuple(query_arguments))
            else:
                cursor.execute(query)
        else:
            arguments = []
            if query_arguments:
                query_str = ', '.join(query_arguments)
            else:
                query_str = ''

            if tail is not None:
                tail_str = tail
            else:
                if tail_argument_key_value is not None:
                    tail_str = ' AND '.join([f'{k} = %s' for k in tail_argument_key_value.keys()])
                    tail_str = f' WHERE {tail_str}'
                    arguments.extend(tail_argument_key_value.values())
                else:
                    tail_str = ''

            if query_str:
                sql_query = f'SELECT {query_str} FROM {table_name}'
            else:
                sql_query = f'SELECT * FROM {table_name}'

            if tail_str:
                if tail_str.strip()[-1] == ';':
                    sql_query += f' {tail_str}'
                else:
                    sql_query += f' {tail_str};'
            else:
                sql_query += ';'

            if arguments:
                cursor.execute(sql_query, tuple(arguments))
            else:
                cursor.execute(sql_query)
        
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    
    if to_pandas:
        import pandas as pd
        df = pd.DataFrame(rows, columns=columns)
        return df

    return [dict(zip(columns, row)) for row in rows]


def fetch_vector_data(
    vector: np.ndarray,
    vector_table_name: str,
    text_table_name: str,
    image_table_name: str,
    postgreSQL_connection_info: Optional[dict] = None,
    connection: Optional[psycopg.Connection] = None,
    method: str = 'inner_product',
    top_k: int = 3,
    score_threshold: float = 0.3,
    where_arguments: Optional[dict] = None
) -> list[np.ndarray]:
    """
    Fetches vector data from a PostgreSQL table.
    Args:
        vector_table_name (str): Name of the table to fetch vector data from.
        text_table_name (str): Name of the table to fetch text data from.
        image_table_name (str): Name of the table to fetch image data from.
        connection (psycopg.Connection, Optional): PostgreSQL connection object.
        method (str): Method for vector comparison ('inner_product' or 'euclidean_distance').
                        <-> - L2 distance
                        <#> - (negative) inner product
                        <=> - cosine distance
                        <+> - L1 distance
                        <~> - Hamming distance (binary vectors)
                        <%> - Jaccard distance (binary vectors)
        top_k (int): Number of top results to return.
        score_threshold (float): Minimum score threshold for results.
        where_arguments (Optional[dict]): Additional filtering conditions.
    Returns:
        list[np.ndarray]: List of numpy arrays containing the fetched vector data.
    """
    if connection is None:
        if postgreSQL_connection_info is None:
            raise ValueError("Either 'connection' or 'postgreSQL_connection_info' must be provided.")
        connection = get_postgreSQL_connection(postgreSQL_connection_info)
    
    additional = '1 - '
    if not isinstance(method, str):
        raise ValueError(f"Unsupported method: {method}")
    method = method.replace(' ', '_').lower()
    if method == 'inner_product':
        # inner product is nagative value, so we need to multiply by -1
        sign = '<#>'
        additional = '-1 * '
    elif method == 'euclidean_distance' or method == 'l2_distance':
        sign = '<->'
    elif method == 'cosine_distance':
        sign = '<=>'
    elif method == 'l1_distance':
        sign = '<+>'
    elif method == 'hamming_distance':
        sign = '<~>'
    elif method == 'jaccard_distance':
        sign = '<%>'
    else:
        raise ValueError(f"Unsupported method: {method}")
    if not isinstance(vector, str):
        vector = np.array2string(vector, separator=',', threshold=np.inf).replace('\n', '')
    
    vector_table_query = f"""
    SELECT uuid, {additional}(embedding {sign} %s) as {method} 
    FROM {vector_table_name} 
    WHERE {additional}(embedding {sign} %s) >= %s
    """
    query_arguments = [vector, vector, score_threshold]
    if where_arguments and isinstance(where_arguments, dict):
        # If query_arguments is provided, we can use it to filter the results
        # query_arguments example: {'column1': ['value1', 'value2'], 'column2': ['value3', 'value4']}
        where_clause = []
        in_clauses = []
        for key in where_arguments.keys():
            for value in where_arguments[key]:
                in_clauses.append(f"%s")
                query_arguments.append(value)
            where_clause.append(f"{key} IN ({', '.join(in_clauses)})")
        where_clause = ' AND '.join(where_clause)
        
        vector_table_query = f'{vector_table_query} AND {where_clause}'

    vector_table_query = f'{vector_table_query} ORDER BY {method} DESC LIMIT {top_k}'

    query = f"""
SELECT data.* 
FROM 
(
    SELECT "text".*, vector.{method} 
    FROM {text_table_name} AS "text"
    JOIN (
        {vector_table_query}
    ) AS vector ON text.uuid = vector.uuid 
    WHERE vector.{method} IS NOT NULL
) as data
;
"""
    data = fetch_data_from_postgreSQL(
        connection=connection,
        query=query,
        query_arguments=query_arguments
    )
    
    data_uuid_list = [item['uuid'] for item in data]
    if len(data_uuid_list) == 0:
        return data, {}
    tail_str = ', '.join([f"'{uuid}'" for uuid in data_uuid_list])
    tail_str = f' WHERE uuid IN ({tail_str});'
    images = fetch_data_from_postgreSQL(
        connection=connection,
        table_name=image_table_name,
        tail=tail_str
    )
    if images:
        images = {
            image['image_name']: {
                'image_original_name': image['image_original_name'],
                'image_base64': image['image'],
                'document_uuid': image['uuid'],
            } for image in images
        }
    else:
        images = {}
    
    return data, images


def get_mssql_connection(
    mssql_connection_info: Optional[dict] = None,
    server: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    port: Optional[int] = None
) -> pymssql.Connection:
    """
    Connects to MSSQL using the provided connection information.
    Args:
        mssql_connection_info (dict): Dictionary containing MSSQL connection information.
    Returns:
        pymssql.Connection: MSSQL connection object.
    """
    if mssql_connection_info is None:
        if None in [server, user, password, database, port]:
            raise ValueError("Either 'mssql_connection_info' or all connection parameters must be provided.")
        mssql_connection_info = {
            "server": server,
            "user": user,
            "password": password,
            "database": database,
            "port": port
        }
    for key, item in {'server': server, 'user': user, 'password': password, 'database': database, 'port': port}.items():
        if item is None and not key in mssql_connection_info:
            raise ValueError(f"Connection parameter '{key}' is missing.")


    server = server if server else mssql_connection_info['server']
    user = user if user else mssql_connection_info['user']
    password = password if password else mssql_connection_info['password']
    database = database if database else mssql_connection_info['database']
    port = port if port else mssql_connection_info['port']

    connection = pymssql.connect(server=server, user=user, password=password, database=database, port=port)
    return connection

def fetch_data_from_mssql(
    connection: pymssql.Connection,
    mssql_connection_info: Optional[dict] = None,
    query: Optional[str] = None,
    table_name: Optional[str] = None,
    query_arguments: Optional[list] = None,
    tail: Optional[str] = None,
    tail_argument_key_value: Optional[dict] = None
) -> list[dict]:
    """
    Fetches data from a MSSQL table.
    Args:
        connection (pymssql.Connection): MSSQL connection object.
        table_name (str): Name of the table to fetch data from.
        query (Optional[str]): Custom SQL query to execute. If provided, overrides the table_name.
        query_arguments (Optional[list]): Arguments in the query.
        tail (Optional[str]): Additional SQL conditions to append to the query.
        tail_argument_key_value (Optional[dict]): Additional parameters for the tail conditions.
        # TIP: A parameter, "query" has first priority then "table_name".
        #      And "tail" has high priority then "tail_argument_key_value".
    Returns:
        list[dict]: List of dictionaries containing the fetched data.
    """
    if connection.closed and mssql_connection_info is not None:
        connection = get_mssql_connection(mssql_connection_info)
    
    with connection.cursor(as_dict=True) as cursor:
        if query_arguments is None:
            query_arguments = []

        if query is not None:
            if query_arguments:
                cursor.execute(query, tuple(query_arguments))
            else:
                cursor.execute(query)
        else:
            arguments = []
            if query_arguments:
                query_str = ', '.join(query_arguments)
            else:
                query_str = ''

            if tail is not None:
                tail_str = tail
            else:
                if tail_argument_key_value is not None:
                    tail_str = ' AND '.join([f'{k} = %s' for k in tail_argument_key_value.keys()])
                    tail_str = f' WHERE {tail_str}'
                    arguments.extend(tail_argument_key_value.values())
                else:
                    tail_str = ''

            if query_str:
                sql_query = f'SELECT {query_str} FROM {table_name}'
            else:
                sql_query = f'SELECT * FROM {table_name}'

            if tail_str:
                if tail_str.strip()[-1] == ';':
                    sql_query += f'{tail_str}'
                else:
                    sql_query += f'{tail_str};'
            else:
                sql_query += ';'
            if arguments:
                cursor.execute(sql_query, tuple(arguments))
            else:
                cursor.execute(sql_query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    connection.close()
    
    return [dict(zip(columns, row)) for row in rows]

def get_chat_history_from_mssql(
    connection: pymssql.Connection,
    conversation_id: str,
    table_name: str = 'dbo.TB_CHATBOT_HISTORY',
    order_column: str = 'DATE_TIME',
    limit: int = 6
) -> list[dict]:
    """
    Fetches chat history from a MSSQL table.
    Args:
        connection (pymssql.Connection): MSSQL connection object.
        table_name (str): Name of the table to fetch chat history from.
        conversation_id (str): Conversation ID to filter the chat history.
        order_column (str): Column name to order the chat history.
        limit (int): Number of chat history records to fetch.
    Returns:
        list[dict]: List of dictionaries containing the fetched chat history.
    """
    with connection.cursor(as_dict=True) as cursor:
        query = f"""
        SELECT TOP {limit} *
        FROM {table_name}
        WHERE CHAT_ID = %s
        ORDER BY {order_column} DESC;
        """
        cursor.execute(query, (conversation_id,))
        rows = cursor.fetchall()
        rows = rows[::-1]
        
        for chat in rows:
            document_uuids = []
            if chat['Q_A_TYPE'] == 'Q':
                chat_attach_image_id = chat['CHAT_ATTACH_IMAGE_ID']
                if chat_attach_image_id is None or chat_attach_image_id == '':
                    continue
                query = f"""
                SELECT ATTACH_IMAGE FROM dbo.TB_CHATBOT_IMAGE WHERE ATTACH_IMAGE_ID = %s ORDER BY {order_column} ASC;
                """
                cursor.execute(query, (chat_attach_image_id,))
                image_rows = cursor.fetchall()
                if len(image_rows) == 0:
                    continue
                attach_images = [img_row['ATTACH_IMAGE'] for img_row in image_rows if img_row['ATTACH_IMAGE'] is not None and img_row['ATTACH_IMAGE'] != '']
                chat['ATTACH_IMAGES'] = attach_images
            else:
                chat_response_id = chat['CHAT_RESPONSE_ID']
                if chat_response_id is None or chat_response_id == '':
                    continue
                query = f"""
                SELECT CONTENT, ATTACH_IMAGE_ID
                FROM dbo.TB_CHATBOT_RESPONSE
                WHERE RESPONSE_ID = %s;
                """
                cursor.execute(query, (chat_response_id,))
                document_rows = cursor.fetchall()
                if len(document_rows) == 0:
                    continue
                document_contents = []
                document_images = []
                for document_row in document_rows:
                    attach_image_id = document_row['ATTACH_IMAGE_ID']
                    query = f"""
                    SELECT ATTACH_IMAGE FROM dbo.TB_CHATBOT_IMAGE WHERE ATTACH_IMAGE_ID = %s ORDER BY {order_column} ASC;
                    """
                    cursor.execute(query, (attach_image_id,))
                    image_rows = cursor.fetchall()
                    if len(image_rows) == 0:
                        continue
                    attach_images = [img_row['ATTACH_IMAGE'] for img_row in image_rows if img_row['ATTACH_IMAGE'] is not None and img_row['ATTACH_IMAGE'] != '']
                    document_images.append(attach_images)
                    document_contents.append(document_row['CONTENT'])
                chat['DOCUMENT_CONTENTS'] = document_contents
                chat['DOCUMENT_IMAGES'] = document_images
                
    connection.close()
    return rows
        

UTILITY_FUNCTIONS = {
    "get_postgreSQL_connection": get_postgreSQL_connection,
    "fetch_data_from_postgreSQL": fetch_data_from_postgreSQL,
    "fetch_vector_data": fetch_vector_data,
    "get_mssql_connection": get_mssql_connection,
    "fetch_data_from_mssql": fetch_data_from_mssql,
    "get_chat_history_from_mssql": get_chat_history_from_mssql,
}



# --- Helper function to parse docstrings ---
def _parse_docstring_for_summary_and_params(docstring: Optional[str]) -> tuple[str, Dict[str, str]]:
    """
    Parses a Google-style docstring to extract the summary and parameter descriptions.

    Args:
        docstring (str, optional): The docstring of the function.

    Returns:
        tuple[str, dict]: A tuple containing the summary (first line) and a dictionary
                          of parameter names to their descriptions.
    """
    if not docstring:
        return "No description available.", {}

    lines = docstring.strip().split('\n')
    summary = lines[0].strip()
    param_descriptions = {}
    in_args_section = False

    for line in lines[1:]:
        stripped_line = line.strip()
        if stripped_line.lower().startswith("args:") or stripped_line.lower().startswith("parameters:"):
            in_args_section = True
            continue
        if in_args_section:
            # Matches patterns like "param_name (type): description" or "param_name: description"
            match = re.match(r"(\w+)\s*(?:\([^)]+\))?:\s*(.*)", stripped_line)
            if match:
                param_name = match.group(1)
                param_desc = match.group(2).strip()
                param_descriptions[param_name] = param_desc
            elif stripped_line and not stripped_line.startswith(' '): # End of args section if new block starts
                in_args_section = False
        if not in_args_section and stripped_line and not stripped_line.startswith(' '):
            # If we've passed the args section and hit another non-indented line, stop parsing params
            break
    return summary, param_descriptions

# --- Refactored introduction function ---
def database_utils_introduction(function_name: Optional[Union[str, Callable]] = None) -> None:
    """
    Provides an introduction to available utility functions or details about a specific function.

    Args:
        function_name (str or callable, optional): The name of the function (string) or the function object itself
                                                   to get detailed parameter information for. If None, lists all functions.
    """
    console = Console()
    table = Table(show_header=True, header_style="bold magenta", expand=True)

    if function_name is None:
        table.add_column("Function Name", style="cyan")
        table.add_column("Description", style="dim")
        for name, func_obj in UTILITY_FUNCTIONS.items():
            summary, _ = _parse_docstring_for_summary_and_params(func_obj.__doc__)
            table.add_row(name, summary)
    else:
        if callable(function_name):
            func_name_str = function_name.__name__
        else:
            func_name_str = function_name

        func_obj = UTILITY_FUNCTIONS.get(func_name_str)

        if not func_obj:
            console.print(f"[bold red]Error:[/bold red] Function '{func_name_str}' not found in utility functions.")
            return

        summary, param_docs = _parse_docstring_for_summary_and_params(func_obj.__doc__)

        console.print(f"[bold green]Function: {func_name_str}[/bold green]")
        console.print(f"[dim]Description: {summary}[/dim]\n")

        table.add_column("Parameter", style="cyan")
        table.add_column("Type", style="magenta")
        table.add_column("Default Value", style="green")
        table.add_column("Description", style="dim")

        signature = inspect.signature(func_obj)
        for param_name, param_obj in signature.parameters.items():
            param_type = str(param_obj.annotation) if param_obj.annotation is not inspect.Parameter.empty else "Any"
            param_default = str(param_obj.default) if param_obj.default is not inspect.Parameter.empty else "Required"
            param_description = param_docs.get(param_name, "No description available in docstring.")
            table.add_row(param_name, param_type, param_default, param_description)

    console.print(table)


# 주요 함수 소개
print("--- 주요 함수 (아래 표시된 함수 말고 더 있음. 확인: database_utils_introduction(함수 이름)) ---")
database_utils_introduction(get_postgreSQL_connection)
database_utils_introduction(fetch_data_from_postgreSQL)
database_utils_introduction(fetch_vector_data)
database_utils_introduction(get_mssql_connection)
database_utils_introduction(fetch_data_from_mssql)