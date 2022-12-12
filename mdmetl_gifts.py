import logging
log = logging.getLogger('mdmetl_gifts')

from mdmetl_helper import find_file, find_filename_date, csv_field_conversion
from mdmetl_gifts_client_special import gift_csv_client_special_handling
import csv
from mdm_databases import MSSql
import sys
from datetime import datetime
import mdmetl_constants as C

log.info("Inside mdmetl_gifts now... setting up")

db = MSSql()

def process_gift_file(client, clargs, data_date):

    log.info(f'Start process_gift_file for {client}')

    right_now = datetime.now()

    if clargs.verbose:
        print(f'Gift processing for {client} gift file.')

    # Generate a temp tablename
    temp_staging_tablename = client.upper() + '_gift_staging_' + right_now.strftime('%Y%m%dT%H%M%S')
    log.info(f'temp staging tablename: {temp_staging_tablename}')

    if clargs.verbose:
        print(f'Creating new temp table: {temp_staging_tablename}')

    # Create the table
    qry_create_temp_gift_table = C.TEMP_GIFT_CREATE_TABLE.replace('{tablename}', temp_staging_tablename)
    # Execute the query only if all columns are present (below)

    # Find the file
    filemask = f'{client}_gift*.csv'

    csv_filename = find_file(filemask, clargs.folder)
    if csv_filename is None:
        log.warning(f'No file found for filemask={filemask} in folder {clargs.folder}')
        return
    csv_drops_filename = csv_filename[:len(csv_filename) - 4] + '-drops.csv'

    log.info(f'Processing file: {csv_filename}')

    filename_date = find_filename_date(csv_filename)


    if data_date is not None:
        gift_data_date = data_date
        if clargs.verbose:
            print(f'Using argument date of {data_date:%Y-%m-%d} as data date.')
        log.info(f'Using argument date of {data_date:%Y-%m-%d} as data date.')
    else:
        gift_data_date = filename_date
        if clargs.verbose:
            print(f'Using filename date of {filename_date:%Y-%m-%d} as data date.')
        log.info(f'Using filename date of {filename_date:%Y-%m-%d} as data date.')

    # Get RawGiftFileColumnMap for client
    column_map = db.get_RawGiftFileColumnMap(client)

    # CSV
    required_csv_columns = [c['ClientColumn'] for c in column_map if c['MDMColumn'] is not None]

    # SQL database
    columns_to_use_for_insert = ['Client'] + [c['MDMColumn'] for c in column_map if c['MDMColumn'] is not None]
    sql_insert_columns = ','.join(columns_to_use_for_insert)

    qry_value_placeholders = '?,' * len(required_csv_columns) + '?'     # Required columns  + 1 for the client code

    insert_qry = f'insert into Temp.{temp_staging_tablename} (' + sql_insert_columns + ') values (' \
                 + qry_value_placeholders + ')'

    csv_column_to_sql_column_map = {c['ClientColumn']: c['MDMColumn'] for c in column_map if c['MDMColumn'] is not None}
    csv_column_to_data_type_map = {c['ClientColumn']: c['ColumnType'] for c in column_map if c['MDMColumn'] is not None}

    # These will be used below for applying general rules to gift files
    csv_donorid_csv_row_column_no = [*csv_column_to_sql_column_map.values()].index('DonorID')
    csv_giftdate_csv_row_column_no = [*csv_column_to_sql_column_map.values()].index('GiftDate')
    csv_giftamount_csv_row_column_no = [*csv_column_to_sql_column_map.values()].index('GiftAmount')

    # TODO - Check for corrupt CSV file by making sure all rows have the correct column count

    # Verify file (columns) are valid
    with open(csv_drops_filename, 'w', newline='') as client_drops_file:
        with open(csv_filename, 'r', newline='', encoding='utf-8-sig') as client_file:
            csv_in = csv.DictReader(client_file, delimiter=',')

            columns_not_present = []
            row_count = 0  # DictReader swallows up the first row, so we don't have to worry about that.
            dropped_row_count = 0
            insert_rows = []

            for raw_row in csv_in:
                row_count += 1

                # Apply client specific rules to the row
                row = gift_csv_client_special_handling(client, row_count+1, raw_row)
                # If the returned row is None then we will skip the row
                if row is None:
                    dropped_row_count += 1
                    log.warning(f'Row {row_count+1} skipped.')
                    continue

                # Only need to do this check the first time through
                # Check that all the required columns are there
                if row_count == 1:

                    # Populate the columns_not_present with the columns that are not there
                    for i in required_csv_columns:
                        if i not in row.keys():
                            columns_not_present.append(i)

                    # Verify all Columns exist in gift file.
                    #   columns_not_present should be empty
                    if columns_not_present:
                        msg = 'Not all columns present in source file.'
                        msg += '\nColumns not present: '
                        for c in columns_not_present:
                            msg += c
                        log.error(msg)
                        log.error('No processing has taken place.  Exiting process_gift_file.')

                        # This return
                        sys.exit(1)

                    # All is well with the input csv, set up the drop rows file
                    csv_out = csv.DictWriter(client_drops_file, fieldnames=row.keys(), dialect='excel')
                    csv_out.writeheader()

                # Build the row for insertion into the staging table, starting with the client as the 0 position
                insert_row = [client]
                for req_col in required_csv_columns:
                    insert_row.append(csv_field_conversion(csv_column_to_data_type_map[req_col],
                                                           row[req_col],
                                                           row=row_count + 1))

                if clargs.verbose:
                    if row_count%10000 == 0:
                        print(f'\rReading CSV row {row_count+1:>10,}', end='', flush=True)

                # General rules
                # Drop the row if no donorid
                # Note: the +1 accounts for the added Client value at the front
                if insert_row[csv_donorid_csv_row_column_no+1] is None:
                    log.warning(f'***Dropped no donorid row {row_count+1:>10,}: ', row)
                    dropped_row_count += 1
                    csv_out.writerow(row)
                    continue
                if insert_row[csv_giftdate_csv_row_column_no+1] is None:
                    log.warning(f'***Dropped no GiftDate row {row_count+1:>10,}: ', row)
                    dropped_row_count += 1
                    csv_out.writerow(row)
                    continue
                if insert_row[csv_giftamount_csv_row_column_no+1] is None:
                    log.warning(f'***Dropped no GiftAmount row {row_count+1:>10,}: ', row)
                    dropped_row_count += 1
                    csv_out.writerow(row)
                    continue

                insert_rows.append(insert_row)

        # Done with csv file.  insert_rows loaded with data
        if clargs.verbose:
            print(f'\rInserting rows into staging table Temp.{temp_staging_tablename}\n', flush=True)

        # Create Staging table and dd the list of rows
        db.execute_simple_query(qry_create_temp_gift_table)
        db.insert_rows(insert_qry, insert_rows)

    # Provide processing stats
    status = [
        f'Total Data Rows    = {row_count:>10,}',
        f'Dropped Rows       = {dropped_row_count:>10,}',
        f'Staging Table Rows = {len(insert_rows):>10,}'
    ]

    for s in status:
        if clargs.verbose:
            print(s)
        log.info(s)

    # Determine stats for file

    # Add to Batch table with date

    # Process gift data mapping with FileMaker and Impressions table
