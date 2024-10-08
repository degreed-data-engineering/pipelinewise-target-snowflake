"""CSV file format functions"""
import gzip
import json
import os

from typing import Callable, Dict, List
from tempfile import mkstemp

from target_snowflake import flattening


def create_copy_sql(table_name: str,
                    stage_name: str,
                    upload_key: str,
                    file_format_name: str,
                    columns: List):
    """Generate a CSV compatible snowflake COPY INTO command"""
    p_columns = ', '.join([c['name'] for c in columns])

    return f"COPY INTO {table_name} ({p_columns}) " \
           f"FROM '@{stage_name}/{upload_key}' " \
           f"FILE_FORMAT = (format_name='{file_format_name}')"


def create_merge_sql(table_name: str,
                     stage_name: str,
                     upload_key: str,
                     file_format_name: str,
                     columns: List,
                     pk_merge_condition: str) -> str:
    """Generate a CSV compatible snowflake MERGE INTO command"""
    p_source_columns = ', '.join([f"{c['trans']}(${i + 1}) {c['name']}" for i, c in enumerate(columns)])
    p_update = ', '.join([f"{c['name']}=s.{c['name']}" for c in columns])
    p_insert_cols = ', '.join([c['name'] for c in columns])
    p_insert_values = ', '.join([f"s.{c['name']}" for c in columns])

    return f"MERGE INTO {table_name} t USING (" \
           f"SELECT {p_source_columns} " \
           f"FROM '@{stage_name}/{upload_key}' " \
           f"(FILE_FORMAT => '{file_format_name}')) s " \
           f"ON {pk_merge_condition} " \
           f"WHEN MATCHED THEN UPDATE SET {p_update} " \
           "WHEN NOT MATCHED THEN " \
           f"INSERT ({p_insert_cols}) " \
           f"VALUES ({p_insert_values})"


def prep_csv_row_string(flatten_record, column):
    """
    Preparing the CSV row for Snowflake. Adding formatting for quotes and '\' characters within each record
    """
    record = flatten_record[column] if column in flatten_record else None

    if column in flatten_record and (record == 0 or record):
        # Replace all \ characters with \\ so they will be recognized as characters and not escape sequences
        record_string = str(record).replace("\\", "\\\\")
        # Wrap all records in "", replace all internal quotation marks with \" to ensure they do not terminate a string
        record_string = "\"" + record_string.replace("\"", "\\\"") + "\""
    else:
        record_string = ''

    return record_string


def record_to_csv_line(record: dict,
                       schema: dict,
                       data_flattening_max_level: int = 0) -> str:
    """
    Transforms a record message to a CSV line
    Args:
        record: Dictionary that represents a csv line. Dict key is column name, value is the column value
        schema: JSONSchema of the record
        data_flattening_max_level: Max level of auto flattening if a record message has nested objects. (Default: 0)
    Returns:
        string of csv line
    """
    flatten_record = flattening.flatten_record(record, schema, max_level=data_flattening_max_level)

    # pipelinewise-target-snowflake uses json.dump() here. We're using list comprehension to
    # handle escape sequences like '\t', '\n', '\r', etc. before they get to Snowflake
    # Todo: This is currently a hacky way to get the behavior we want.
    # I assume there are better ways to handle this.

    csv_string = ','.join(
        [
            prep_csv_row_string(flatten_record, column)
            for column in schema
        ]
    )

    return csv_string


def write_records_to_file(outfile,
                          records: Dict,
                          schema: Dict,
                          record_to_csv_line_transformer: Callable,
                          data_flattening_max_level: int = 0) -> None:
    """
    Writes a record message to a given file

    Args:
        outfile: An open file object
        records: List of dictionaries that represents a batch of singer record messages
        schema: JSONSchema of the records
        record_to_csv_line_transformer: Function that transforms dictionary to a CSV string line
        data_flattening_max_level: Max level of auto flattening if a record message has nested objects. (Default: 0)

    Returns:
        None
    """
    for record in records.values():
        csv_line = record_to_csv_line_transformer(record, schema, data_flattening_max_level)
        outfile.write(bytes(csv_line + '\n', 'UTF-8'))


def records_to_file(records: Dict,
                    schema: Dict,
                    suffix: str = 'csv',
                    prefix: str = 'batch_',
                    compression: bool = False,
                    dest_dir: str = None,
                    data_flattening_max_level: int = 0):
    """
    Transforms a list of dictionaries with records messages to a CSV file

    Args:
        records: List of dictionaries that represents a batch of singer record messages
        schema: JSONSchema of the records
        suffix: Generated filename suffix
        prefix: Generated filename prefix
        compression: Gzip compression enabled or not (Default: False)
        dest_dir: Directory where the CSV file will be generated. (Default: OS specificy temp directory)
        data_flattening_max_level: Max level of auto flattening if a record message has nested objects. (Default: 0)

    Returns:
        Absolute path of the generated CSV file
    """
    if dest_dir:
        os.makedirs(dest_dir, exist_ok=True)

    if compression:
        file_suffix = f'.{suffix}.gz'
    else:
        file_suffix = f'.{suffix}'

    filedesc, filename = mkstemp(suffix=file_suffix, prefix=prefix, dir=dest_dir)

    # Using gzip or plain file object
    if compression:
        with open(filedesc, 'wb') as outfile:
            with gzip.GzipFile(filename=filename, mode='wb',fileobj=outfile) as gzipfile:
                write_records_to_file(gzipfile, records, schema, record_to_csv_line, data_flattening_max_level)
    else:
        with open(filedesc, 'wb') as outfile:
            write_records_to_file(outfile, records, schema, record_to_csv_line, data_flattening_max_level)

    return filename
