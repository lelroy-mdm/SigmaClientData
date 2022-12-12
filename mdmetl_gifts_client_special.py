

def gift_csv_client_special_handling(client, row_number, row):

    # Special handling for specific clients

    # Returning None is used to skip the row

    #   HGM row 2 is a "totals row" and needs to be skipped/ignored
    #   GSM, GLM row 2 is a repeat of row1 and needs to be skipped/ignored
    if client in ('HGM', 'GSM', 'GLM') and row_number == 2:
        row = None

    return row
