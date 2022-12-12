##########
#
# Sigma Client Data
#
# Started: 9/8/2022
#
##########

import logging
log = logging.getLogger('mdmetl')
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    filename='mdmetl.log',
                    filemode='w')

import argparse
import sys
from datetime import datetime
import os
from mdmetl_gifts import process_gift_file
from mdmetl_donors import process_donor_file

log.info('*** mdmetl.py started')




argparser = argparse.ArgumentParser(
    prog='MDMETL',
    description='MDM Client Gift and Donor processor.',
    epilog='(version 0.1)'
)

argparser.add_argument('-c', '--client', action='append', type=str, nargs='*', required=True,
                       help='Client Identifier to process')
argparser.add_argument('-d', '--donor', action='store_true',
                       help='Process only the donor and associated file(s)')
argparser.add_argument('-g', '--gift', action='store_true',
                       help='Process only the gift file')
argparser.add_argument('-j', '--date', action='store', type=str,
                       help='Use the date as the data/pull date yyyy-mm-dd')
argparser.add_argument('-v', '--verbose', action='store_true',
                       help='Increase verbosity')
argparser.add_argument('-t', '--test', action='store_true',
                       help='Test donor/gift file only -- will not update DB')
argparser.add_argument('-f', '--folder', action='store', type=str,
                       default=r'\\mdmsynology\Data\_General\CampaignProduction\SourceData\InputTest',
                       help=r'Specify the client data folder.  '
                            r'Default=\\mdmsynology\Data\_General\CampaignProduction\SourceData\InputTest')

clargs = argparser.parse_args()
log.info(f'Command line args: {clargs}')

def main():
    print('Sigma Client Data\n')

    if clargs.client is None:
        print('There are no clients to process.  Quitting.')
        sys.exit(1)     # Exit with error

    data_date = None
    if clargs.date != None:
        if len(clargs.date)!=10:
            print('Data date not in correct yyyy-mm-dd format.  Quitting.')
            sys.exit(1)
        try:
            data_date = datetime.strptime(clargs.date, '%Y-%m-%d')
        except:
            pass
        if data_date is None:
            print('Data date unable to convert to datetime.  Quitting.')
            sys.exit(1)
        if data_date.year < 2022 or data_date.year > datetime.now().year+1:
            print('Data date year does not pass sanity check.  Quitting.')
            sys.exit(1)

        if clargs.folder != None and not os.path.isdir(clargs.folder):
            print(f'{clargs.folder} is not a valid folder.  Quitting.')


    clients = clargs.client[0]

    for client in clients:
        print(f'Current client: {client}\n')

        # Do gift if -g selected or neither selected
        if clargs.gift or (not clargs.gift and not clargs.donor):
            process_gift_file(client, clargs, data_date)

        # Do donor if -d selected or neither selected
        if clargs.donor or (not clargs.gift and not clargs.donor):
            process_donor_file(client, clargs, data_date)

    print('Finished processing all client(s).')
    sys.exit(0)


if __name__ == '__main__':
    main()
