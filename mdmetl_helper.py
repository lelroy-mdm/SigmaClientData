# mdmetl_helper
import logging
log = logging.getLogger('mdmetl_helper')

import glob
import os
from datetime import datetime
from decimal import Decimal


def csv_field_conversion(conv_type, data, row=None):

    try:
        if data is None or (isinstance(data, str) and data == ''):
            return None

        if isinstance(data, str) and len(data)>255:
            log.warning(f'Column data in row {row} was length {len(data)} and has been truncated to 255 chars.')
            data = data[:255]

        if conv_type.upper() == 'TEXT':
            return str(data).strip()

        if conv_type.upper() == 'MONEY':
            m = str(data).strip().replace('$','').replace(',','')
            return None if m == '' else Decimal(m)

        if conv_type.upper() == 'NUMBER':
            m = str(data).strip()
            return None if m == '' else Decimal(m)

        if conv_type.upper()[:4] == 'DATE':
            # if the object inside data is already a datetime, then just return it
            if isinstance(data, datetime):
                return data
            # DATE1 = standard database yyyy-mm-dd format
            if str(conv_type)[4:5] == '1':
                return datetime.strptime(data, '%Y-%m-%d')
            elif str(conv_type)[4:5] == '2':
                return datetime.strptime(data, '%m/%d/%Y')
            elif str(conv_type)[4:5] ==  '3':
                return datetime.strptime(data, '%m/%d/%y')
            elif str(conv_type)[4:5] ==  '4':
                return datetime.strptime(data.replace(' 00:00:00',''), '%m/%d/%Y')
            else:
                raise Exception('Date config error.  Contact developer.')

    except Exception as e:
        log.warning(f'csv_conversion_error for converting :{data}: to a {conv_type} on row {row}. {e}')
        print()
        return f'error-{data}'


def find_file(filemask, source_path):

    result = glob.glob(os.path.join(source_path, filemask))

    if len(result)>1:
        print(f'{len(result)} files found looking for filemask: {filemask}')
        print(f'# Filename')
        for fileno, file in enumerate(result):
            print(f'{fileno} {file}')
        print(f'----------------')
        ans = input('Choose number or <enter> to skip this file:')
        if ans=='':
            return None
        try:
            return result[int(ans)]
        except:
            print('Invalid choice.  Skipping this file.')
            return None

    else:
        return result[0]


def find_filename_date(filename):

    try:
        end_pos = filename.upper().find('.CSV')
        start_pos = end_pos - 8
        fn_date_str = filename[start_pos:end_pos]
        fn_date = datetime.strptime(fn_date_str, '%Y%m%d')
        return fn_date
    except:
        return None
