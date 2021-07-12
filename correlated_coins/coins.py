import argparse
import correlated_coins

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='********************************************************************************************************\n'+
                                                '** Binance correlated coins finder                                                                    **\n'+
                                                '**  • The program will calculate the correlation of all the coins listed in the "used_coins" file     **\n'+
                                                '**    and will show the ones with correlation falling between "correlation_greater_than" and          **\n'+
                                                '**    "correlation_less_than" config entries                                                          **\n'+
                                                '**  • Unwanted coins can be listed in "ignored_coins". Those will not be shown in the results         **\n'+
                                                '********************************************************************************************************\n',
                                                formatter_class=argparse.RawTextHelpFormatter)

  parser._action_groups.pop()

  # Data Gathering
  required = parser.add_argument_group('Data Gathering')
  required.add_argument('-H', '--update-coins-history', action='store_true',help='Updates the historical price of all the coins in Binance.')
  required.add_argument('-c', '--update-top-coins', action='store_true',help='Updates "used_coins" file with the 100 best coins in CoinGecko.')

  # Correlation Arguments
  required = parser.add_argument_group('Correlation calculation')
  required.add_argument('-A', '--all-correlated-values', action='store_true',help='Correlation values of all coins in "used_coins" file.')
  required.add_argument('-a', '--one-correlated-values',metavar='<coin>',nargs=1,help='Correlation values of all coins in "used_coins" file with one.')
  required.add_argument('-L', '--all-correlated-list',action='store_true',help='List of all correlated coins in "used_coins" file.')
  required.add_argument('-l', '--one-correlated-list',metavar='<coin>',nargs=1,help='List of all correlated coins in "used_coins" file with one.')
  required.add_argument('-G', '--all-correlated-grouped', action='store_true',help='List of all correlated coins in "used_coins" file grouped by their relationship.')

  # Optionnal Arguments
  optional = parser.add_argument_group('Optionnal arguments')
  optional.add_argument('-s','--start-datetime',metavar='<datetime>',nargs=1,help='Fetch historical data from date/time\n - e.g 2020-12-31.23:59:59')
  optional.add_argument('-e','--end-datetime',metavar='<datetime>',nargs=1,help='Fetch historical data until date/time\n - e.g 2020-12-15.00:00:00')
  optional.add_argument('-o','--date-offset',metavar='<integer>',nargs=1,help='Fetch historical data until start time minus [-] offset in days\n - e.g (2020-12-31.23:59:59 - 7 days) = 2020-12-24.23:59:59')
  optional.add_argument('-p','--paired-coin',metavar='<coin>',nargs=1,help='Coin that will be paired with all existing coins on Binance in the process of downloading the history data.')

  args = parser.parse_args()
  
  if not (args.all_correlated_grouped or args.all_correlated_list or args.all_correlated_values or args.one_correlated_list or args.one_correlated_values or args.update_coins_history or args.update_top_coins):
    parser.print_help()
    exit()


#print(vars(args))

correlated_coins.main(vars(args))