import os, sys, math, subprocess, psutil

from sqlalchemy.sql.elements import Null

from binance_trade_bot.auto_trader import AutoTrader
from binance_trade_bot.database import Pair, Coin, Trade
from binance_trade_bot import warmup_database

from correlated_coins import correlated_coins
from datetime import datetime, timezone, timedelta
from time import sleep
from collections import defaultdict
from typing import List

from sqlalchemy.orm import Session, aliased


class Strategy(AutoTrader):

    def initialize(self):
        self.logger.info(f'{self.manager.now().astimezone(tz=None)}')
        self.config.REGENERATE_COIN_LIST = (self.manager.now() + timedelta(minutes=30)).replace(second=0,microsecond=0)

        if len(self.config.SUPPORTED_COIN_LIST) > 2:
            self.logger.info(f'Keeping current coin list until next refresh at {self.config.REGENERATE_COIN_LIST.astimezone(tz=None)}')
            self.logger.info(f"Current coin list : {self.config.SUPPORTED_COIN_LIST}")
        else:
            self.generate_new_coin_list()

        self.logger.info(f'Updating Minimum Quantity ...')
        self.config.START_AMOUNT = {}
        self.set_minimum_quantity()

        self.clean_small_balances()

        self.initialize_trade_thresholds()

        self.reinit_threshold = self.manager.now().replace(second=0, microsecond=0)

    def restart_program(self):
        """Restarts the current program, with file objects and descriptors
          cleanup
        """

        try:
            p = psutil.Process(os.getpid())
            for handler in p.get_open_files() + p.connections():
                os.close(handler.fd)
        except Exception as e:
            #self.logger.error(e)
            pass

        python = sys.executable
        os.execl(python, python, *sys.argv)

    def scout(self):
        base_time: datetime = self.manager.now()
        allowed_idle_time = self.reinit_threshold

        if base_time >= allowed_idle_time:
            self.re_initialize_trade_thresholds()
            self.reinit_threshold = self.manager.now().replace(second=0, microsecond=0) + timedelta(minutes=1)

        if base_time >= self.config.REGENERATE_COIN_LIST:
            self.generate_new_coin_list()
            #self.initialize_trade_thresholds()
            #self.logger.info(f'Updating Minimum Quantity ...')
            #self.set_minimum_quantity()
            #self.config.REGENERATE_COIN_LIST += timedelta(days=1)
            #self.logger.info(f'Next refresh at {self.config.REGENERATE_COIN_LIST.astimezone(tz=None)}')
            #self.clean_small_balances()

        #check if previous buy order failed. If so, bridge scout for a new coin.
        if self.failed_buy_order:
            self.bridge_scout()

        """
        Scout for potential jumps from the current coin to another coin
        """
        current_coin = self.db.get_current_coin()
        if current_coin is None:
            self.initialize_current_coin()
            current_coin = self.db.get_current_coin()

        if current_coin.symbol not in self.config.SUPPORTED_COIN_LIST:
            if self.manager.get_currency_balance(current_coin.symbol) > self.manager.get_min_qty(current_coin.symbol, self.config.BRIDGE.symbol):
                self.logger.info(f"Selling {current_coin} as it was removed from 'SUPPORTED_COIN_LIST'")         
                self.manager.sell_alt(
                    current_coin, self.config.BRIDGE, self.manager.get_sell_price(current_coin + self.config.BRIDGE)
                )
            self.bridge_scout()

        # Display on the console, the current coin+Bridge, so users can see *some* activity and not think the bot has
        # stopped. Not logging though to reduce log size.
        # print(
        #     f"{self.manager.now()} - CONSOLE - INFO - I am scouting the best trades. "
        #     f"Current coin: {current_coin + self.config.BRIDGE} ",
        #     end="\r",
        # )

        current_coin_price = self.manager.get_sell_price(current_coin + self.config.BRIDGE)

        if current_coin_price is None:
            self.logger.info("Skipping scouting... current coin {} not found".format(current_coin + self.config.BRIDGE))
            return

        self._jump_to_best_coin(current_coin, current_coin_price)
        

    def generate_new_coin_list(self):
        new_coin_list = []
        self.logger.info("Updating coin_list ...")
        try:
            correlated_coins.main({
                'update_coins_history':
                True,
                'update_top_coins':
                True,
                'all_correlated_list':
                True #,
                #'start_datetime': [
                #    str(self.manager.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7))
                #],
                #'end_datetime': [
                #    str(self.manager.now().replace(hour=0, minute=0, second=0, microsecond=0))
                #]
            })
        except Exception as e:
            self.logger.info(f'Unable to generate "supported_coin_list" : {e}')
            try:
                if len(self.config.SUPPORTED_COIN_LIST) > 2:
                    self.logger.info(f'Keeping current coin list until next refresh')
                    self.logger.info(f"Coin list : {self.config.SUPPORTED_COIN_LIST}")
                    return
            except:
                self.logger.info(f'Empty coin list - Aborting!')
                sys.exit()

        # Get supported coin list from supported_coin_list file
        if os.path.exists("supported_coin_list"):
            with open("supported_coin_list") as rfh:
                for line in rfh:
                    line = line.strip()
                    if not line or line.startswith(
                            "#") or line in new_coin_list:
                        continue
                    new_coin_list.append(line)

        if len(new_coin_list) < 6:
            self.logger.info(f'Keeping current coin list until next refresh (New list too short)')
            if len(self.config.SUPPORTED_COIN_LIST) > 0:
                self.logger.info(f"Coin list : {self.config.SUPPORTED_COIN_LIST}")
                return
            else:
                self.logger.info(f'Empty coin list - Aborting!')
                sys.exit()

        # keep coinlist if no changes
        if sorted(self.config.SUPPORTED_COIN_LIST) == sorted(new_coin_list):
          self.logger.info(f"Coin list unchanged... skipping restart")
          return

        # Add current coin back in supported_coin_list if not already there
        current_coin = self.db.get_current_coin()
        if current_coin is not None and current_coin.symbol not in new_coin_list:
            self.logger.info(f"Adding {current_coin} back to 'SUPPORTED_COIN_LIST'")   
            try:
                new_coin_list.append(current_coin.symbol)      
                with open('supported_coin_list', 'a') as writer:
                        writer.write(current_coin.symbol+'\n')                            
            except Exception as e:
                self.logger.info(f'Unable to update "supported_coin_list" : {e}')

        # compare and show coin list differences
        if len(self.config.SUPPORTED_COIN_LIST) > 0:
            removed = list(set(self.config.SUPPORTED_COIN_LIST) - set(new_coin_list))
            if len(removed) > 0:
                self.logger.info(f"Removed: {removed}")

            added = list(set(new_coin_list) - set(self.config.SUPPORTED_COIN_LIST ))
            if len(added) > 0:
                self.logger.info(f"Added: {added}")

        self.config.SUPPORTED_COIN_LIST = new_coin_list
        try:
            self.db.set_coins(self.config.SUPPORTED_COIN_LIST)
            self.logger.info(f"New Coin List: {self.config.SUPPORTED_COIN_LIST}")
        except Exception as e:
            self.logger.info(f'Unable to update database with "supported_coin_list" : {e}')

        self.logger.info(f'Sleeping 30 seconds and restart ...')
        sleep(30)
        self.restart_program()

    def bridge_scout(self):
        current_coin = self.db.get_current_coin()
        if self.manager.get_currency_balance(current_coin.symbol) > self.manager.get_min_qty(
            current_coin.symbol, self.config.BRIDGE.symbol
        ):
            # Only scout if we don't have enough of the current coin
            return
        
        self.logger.info(f'bridge_scout ...')

        """
        If we have any bridge coin leftover, buy a coin with it that we won't immediately trade out of
        """
        bridge_balance = self.manager.get_currency_balance(self.config.BRIDGE.symbol)

        for coin in self.db.get_coins():
            coin_price = self.manager.get_sell_price(coin + self.config.BRIDGE)

            if coin_price is None:
                continue

            ratio_dict, _ = self._get_ratios(coin, coin_price)
            ratio_dict = {k: v for k, v in ratio_dict.items() if v > 0}
            # if we have any viable options, pick the one with the biggest ratio
            if ratio_dict:
                if len(ratio_dict) > 1:
                    pairs = sorted(ratio_dict.items(), key=lambda x: x[1], reverse=True)
                else:
                    pairs = [max(ratio_dict, key=ratio_dict.get)]

                for pair in pairs:
                    if isinstance(pair, tuple):
                        best_pair = pair[0]
                    else:
                        best_pair = pair

                    to_coin_price = self.manager.get_buy_price(best_pair.to_coin.symbol + self.config.BRIDGE.symbol)
                    bridge_balance = self.manager.get_currency_balance(self.config.BRIDGE.symbol) + self.estimate_bridge_balance_from_current_coin()
                    order_quantity = self.manager._buy_quantity(best_pair.to_coin.symbol, self.config.BRIDGE.symbol, bridge_balance, to_coin_price)

                    coin_tick = self.manager.get_alt_tick(best_pair.to_coin.symbol, self.config.BRIDGE.symbol)
                    minimum_quantity = self.config.START_AMOUNT[best_pair.to_coin.symbol]
                    fee = minimum_quantity * self.manager.get_fee(best_pair.to_coin, self.config.BRIDGE, False)
                    minimum_order = math.floor((minimum_quantity + fee) * 10 ** coin_tick) / float(10 ** coin_tick)

                    if minimum_order > 0:
                        pct_gain = ((order_quantity - minimum_order) / minimum_order) * 100
                    else:
                        pct_gain = 0

                    #self.logger.info(f"BRIDGE_SCOUT: {coin.symbol} -> {best_pair.to_coin.symbol} | Order : ({minimum_quantity}) -> ({order_quantity}) ({round(pct_gain,2)}%)")

                    if order_quantity > minimum_order:
                        self.logger.info(f"BRIDGE_SCOUT: Buy {best_pair.to_coin.symbol} | Order : ({minimum_quantity}) -> ({order_quantity}) ({round(pct_gain,2)}%)")
                        result = self.manager.buy_alt(best_pair.to_coin, self.config.BRIDGE, to_coin_price, True)
                        if result is not None:
                            self.db.set_current_coin(best_pair.to_coin)
                            self.failed_buy_order = False
                            return coin
                        else:
                            self.failed_buy_order = True
                    else:
                        continue


    def initialize_current_coin(self):
        """
        Decide what is the current coin, and set it up in the DB.
        """
        if self.db.get_current_coin() is None:
            current_coin_symbol = self.config.CURRENT_COIN_SYMBOL
            if not current_coin_symbol:
                current_coin_symbol = self.config.SUPPORTED_COIN_LIST[0]

            self.logger.info(f"Setting initial coin to {current_coin_symbol}")

            if current_coin_symbol not in self.config.SUPPORTED_COIN_LIST:
                sys.exit("***\nERROR!\nSince there is no backup file, a proper coin name must be provided at init\n***")
            self.db.set_current_coin(current_coin_symbol)

            # if we don't have a configuration, we selected a coin at random... Buy it so we can start trading.
            if self.config.CURRENT_COIN_SYMBOL == "":
                current_coin = self.db.get_current_coin()
                self.logger.info(f"Purchasing {current_coin} to begin trading")
                self.manager.buy_alt(
                    current_coin, self.config.BRIDGE, self.manager.get_buy_price(current_coin + self.config.BRIDGE)
                )
                self.logger.info("Ready to start trading")
            else:
                current_balance = self.manager.get_currency_balance(current_coin_symbol)
                sell_price = self.manager.get_sell_price(current_coin_symbol + self.config.BRIDGE.symbol)
                if current_balance is not None and current_balance * sell_price < self.manager.get_min_notional(current_coin_symbol, self.config.BRIDGE.symbol):
                    self.logger.info(f"Purchasing {current_coin_symbol} to begin trading")
                    current_coin = self.db.get_current_coin()
                    self.manager.buy_alt(
                        current_coin, self.config.BRIDGE, self.manager.get_buy_price(current_coin + self.config.BRIDGE)
                    )
                    self.logger.info("Ready to start trading")



    def re_initialize_trade_thresholds(self):
        """
        Re-initialize all the thresholds ( hard reset - as deleting db )
        """
        #updates all ratios
        #print('************INITIALIZING RATIOS**********')
        session: Session
        with self.db.db_session() as session:
            c1 = aliased(Coin)
            c2 = aliased(Coin)
            for pair in session.query(Pair).all():
                if not pair.from_coin.enabled or not pair.to_coin.enabled:
                    continue
                #self.logger.debug(f"Initializing {pair.from_coin} vs {pair.to_coin}", False)

                from_coin_price = self.manager.get_sell_price(pair.from_coin + self.config.BRIDGE)
                if from_coin_price is None:
                    self.logger.debug(
                        "Skipping initializing {}, symbol not found".format(pair.from_coin + self.config.BRIDGE),
                        False
                    )
                    continue

                to_coin_price = self.manager.get_buy_price(pair.to_coin + self.config.BRIDGE)
                if to_coin_price is None:
                    self.logger.debug(
                        "Skipping initializing {}, symbol not found".format(pair.to_coin + self.config.BRIDGE),
                        False
                    )
                    continue

                pair.ratio = (pair.ratio *self.config.RATIO_ADJUST_WEIGHT + from_coin_price / to_coin_price)  / (self.config.RATIO_ADJUST_WEIGHT + 1)



    def initialize_trade_thresholds(self):
        """
        Initialize the buying threshold of all the coins for trading between them
        """
        session: Session
        with self.db.db_session() as session:
            pairs = session.query(Pair).filter(Pair.ratio.is_(None)).all()
            grouped_pairs = defaultdict(list)
            for pair in pairs:
                if pair.from_coin.enabled and pair.to_coin.enabled:
                    grouped_pairs[pair.from_coin.symbol].append(pair)

            price_history = {}

            init_weight = self.config.RATIO_ADJUST_WEIGHT
            
            #Binance api allows retrieving max 1000 candles
            if init_weight > 100:
                init_weight = 100

            self.logger.info(f"Using last {init_weight} candles to initialize ratios")

            base_date = self.manager.now().replace(second=0, microsecond=0)
            start_date = base_date - timedelta(minutes=init_weight*2)
            end_date = base_date - timedelta(minutes=1)

            start_date_str = start_date.strftime('%Y-%m-%d %H:%M')
            end_date_str = end_date.strftime('%Y-%m-%d %H:%M')

            self.logger.info(f"Starting ratio init: Start Date: {start_date}, End Date {end_date}")
            for from_coin_symbol, group in grouped_pairs.items():

                if from_coin_symbol not in price_history.keys():
                    price_history[from_coin_symbol] = []
                    for result in  self.manager.binance_client.get_historical_klines(f"{from_coin_symbol}{self.config.BRIDGE_SYMBOL}", "1m", start_date_str, end_date_str, limit=init_weight*2):
                        price = float(result[1])
                        price_history[from_coin_symbol].append(price)

                for pair in group:                  
                    to_coin_symbol = pair.to_coin.symbol
                    if to_coin_symbol not in price_history.keys():
                        price_history[to_coin_symbol] = []

                        try:
                            for result in self.manager.binance_client.get_historical_klines(f"{to_coin_symbol}{self.config.BRIDGE_SYMBOL}", "1m", start_date_str, end_date_str, limit=init_weight*2):                           
                                price = float(result[1])
                                price_history[to_coin_symbol].append(price)
                        except:
                            self.logger.info(f"Skip initialization. Could not fetch data for {to_coin_symbol}{self.config.BRIDGE_SYMBOL}")
                            continue

                    if len(price_history[from_coin_symbol]) != init_weight*2:
                        self.logger.info(len(price_history[from_coin_symbol]))
                        self.logger.info(f"Skip initialization. Could not fetch last {init_weight * 2} prices for {from_coin_symbol}")
                        continue
                    if len(price_history[to_coin_symbol]) != init_weight*2:
                        self.logger.info(f"Skip initialization. Could not fetch last {init_weight * 2} prices for {to_coin_symbol}")
                        continue
                    
                    sma_ratio = 0.0
                    for i in range(init_weight):
                        sma_ratio += price_history[from_coin_symbol][i] / price_history[to_coin_symbol][i]
                    sma_ratio = sma_ratio / init_weight

                    cumulative_ratio = sma_ratio
                    for i in range(init_weight, init_weight * 2):
                        cumulative_ratio = (cumulative_ratio * init_weight + price_history[from_coin_symbol][i] / price_history[to_coin_symbol][i]) / (init_weight + 1)

                    pair.ratio = cumulative_ratio


    def _jump_to_best_coin(self, coin: Coin, coin_price: float, excluded_coins: List[Coin] = []):
        """
        Given a coin, search for a coin to jump to
        """
        ratio_dict, prices = self._get_ratios(coin, coin_price, excluded_coins)

        # keep only ratios bigger than zero
        ratio_dict = {k: v for k, v in ratio_dict.items() if v > 0}

        # if we have any viable options, pick the one with the biggest ratio
        if ratio_dict:
            if len(ratio_dict) > 1:
                pairs = sorted(ratio_dict.items(), key=lambda x: x[1], reverse=True)
            else:
                pairs = [max(ratio_dict, key=ratio_dict.get)]

            for pair in pairs:
                if isinstance(pair, tuple):
                    best_pair = pair[0]
                else:
                    best_pair = pair

                to_coin_price = self.manager.get_buy_price(best_pair.to_coin.symbol + self.config.BRIDGE.symbol)
                bridge_balance = self.manager.get_currency_balance(self.config.BRIDGE.symbol) + self.estimate_bridge_balance_from_current_coin()
                order_quantity = self.manager._buy_quantity(best_pair.to_coin.symbol, self.config.BRIDGE.symbol, bridge_balance, to_coin_price)

                coin_tick = self.manager.get_alt_tick(best_pair.to_coin.symbol, self.config.BRIDGE.symbol)
                minimum_quantity = self.config.START_AMOUNT[best_pair.to_coin.symbol]
                fee = minimum_quantity * self.manager.get_fee(best_pair.to_coin, self.config.BRIDGE, False)
                minimum_order = math.floor((minimum_quantity + fee) * 10 ** coin_tick) / float(10 ** coin_tick)

                if minimum_order > 0:
                    pct_gain = ((order_quantity - minimum_order) / minimum_order) * 100
                else:
                    pct_gain = 0

                if order_quantity > minimum_order and pct_gain > 1.25:
                    self.logger.info(f"Jump to {best_pair.to_coin} | Order : ({minimum_quantity}) -> ({order_quantity}) ({round(pct_gain,2)}%)")
                    self.transaction_through_bridge(best_pair, coin_price, prices[best_pair.to_coin_id])
                    break
                else:
                    #self.logger.info(f"Skip | {best_pair.from_coin.symbol} -> {best_pair.to_coin.symbol} | Order : ({order_quantity}) / Min. Order : ({minimum_quantity})")
                    continue



    def set_minimum_quantity(self):
        # calculate estimated bridge balance from current coin
        bridge_balance_from_coin = self.estimate_bridge_balance_from_current_coin()

        new_start_amount = self.manager.get_currency_balance(self.config.BRIDGE.symbol) + bridge_balance_from_coin
        self.logger.info(f"{self.config.BRIDGE} START_AMOUNT: {new_start_amount}")

        try: 
            old_start_amount = self.config.START_AMOUNT[self.config.BRIDGE.symbol]
            percent_change = ((new_start_amount - old_start_amount) / old_start_amount) * 100
            if old_start_amount > new_start_amount:
                self.logger.info(f"Lost {round(percent_change,2)}% ... Keeping {self.config.BRIDGE} START_AMOUNT unchanged")
            else:
                self.logger.info(f"Gained {round(percent_change,2)}% ... Updating {self.config.BRIDGE} START_AMOUNT")
                self.config.START_AMOUNT[self.config.BRIDGE.symbol] = new_start_amount    
        except:
            self.config.START_AMOUNT[self.config.BRIDGE.symbol] = new_start_amount    

        try:
            session: Session
            with self.db.db_session() as session:
                for coin in session.query(Coin).all():
                    if coin.enabled:
                        try:
                            trade = session.query(Trade).filter(Trade.alt_coin_id == coin.symbol).filter(Trade.selling == False).filter(Trade.alt_trade_amount != None).order_by(Trade.datetime.desc()).limit(1).one().info()
                            minimum_quantity = float(trade['alt_trade_amount'])
                        except Exception as e:
                            self.logger.info(f"Unable to read last trade Amount for {coin.symbol} - {e}")
                            self.logger.info(f"Using Bridge START_AMOUNT ({new_start_amount}) as base for Minimum Quantity")
                            from_coin_price = self.manager.get_ticker_price(coin.symbol + self.config.BRIDGE.symbol)
                            minimum_quantity = self.manager._buy_quantity(coin.symbol, self.config.BRIDGE.symbol, new_start_amount, from_coin_price)
                            coin_tick = self.manager.get_alt_tick(coin.symbol, self.config.BRIDGE.symbol)
                            minimum_quantity = math.floor(minimum_quantity * 10 ** coin_tick) / float(10 ** coin_tick)

                        if coin.symbol != self.config.BRIDGE.symbol:
                            if coin.symbol in list(self.config.START_AMOUNT):
                                if minimum_quantity >= self.config.START_AMOUNT[coin.symbol]:
                                    self.config.START_AMOUNT[coin.symbol] = minimum_quantity
                                    self.logger.info(f"Updating START_AMOUNT for {coin.symbol} : {minimum_quantity}")
                                else:
                                    self.logger.info(f"Skipping START_AMOUNT for {coin.symbol} as saved value ({self.config.START_AMOUNT[coin.symbol]}) is greater than minimum_quantity ({minimum_quantity})")
                            else:
                                self.config.START_AMOUNT[coin.symbol] = minimum_quantity
                                self.logger.info(f"Setting START_AMOUNT for {coin.symbol} : {minimum_quantity}")

        except Exception as e:
            self.logger.info(f"Unable to save minimum quantity - {e}")
            return



    def estimate_bridge_balance_from_current_coin(self):
        coin_to_bridge_balance = 0
        current_coin = self.db.get_current_coin()
        if current_coin is not None:
            if self.manager.get_currency_balance(current_coin.symbol) > self.manager.get_min_notional(current_coin.symbol, self.config.BRIDGE.symbol):
                sell_quantity = self.manager._sell_quantity(current_coin.symbol, self.config.BRIDGE.symbol)
                sell_price = self.manager.get_sell_price(current_coin.symbol + self.config.BRIDGE.symbol)
                fee = sell_quantity * self.manager.get_fee(current_coin, self.config.BRIDGE, True)
                coin_to_bridge_balance = (sell_quantity - fee ) * sell_price

                #coin_tick = self.manager.get_alt_tick(current_coin.symbol, self.config.BRIDGE.symbol)
                #coin_to_bridge_balance = math.floor(((sell_quantity - fee ) * sell_price) * 10 ** coin_tick) / float(10 ** coin_tick)
                """
                print(f"SELL_QUANTITY: {sell_quantity}")
                print(f"SELL_PRICE: {sell_price}")
                print(f"FEE: {fee}")
                print(f"COIN_TO_BRIDGE_BALANCE: {coin_to_bridge_balance}")
                """
            #else:
            #    self.logger.info(f"Not enough {current_coin} to estimate bridge balance ...")


        return round(coin_to_bridge_balance,8)

    def clean_small_balances(self):
        self.logger.info(f'Cleaning small balances ...')
        try:
          dustlist = []
          sellList = []
          current_coin = self.db.get_current_coin()
          if current_coin is not None:
            for asset in self.manager.binance_client.get_account()["balances"]:  
              if asset['asset'] not in [current_coin.symbol, self.config.BRIDGE.symbol, 'BNB'] and float(asset['free']) > 0:
                if self.manager.get_currency_balance(asset['asset']) * self.manager.get_sell_price(asset['asset'] + 'BTC')  < 0.0003:
                  dustlist.append(asset['asset'])
                else:
                  if self.manager.get_currency_balance(asset['asset']) > self.manager.get_min_qty(asset['asset'], self.config.BRIDGE.symbol):
                    sellList.append(asset['asset'])

          if len(dustlist) > 0:
            self.logger.info(f"Converting : {dustlist}")
            partially_order = None
            while partially_order is None:
              partially_order = self.manager.binance_client.transfer_dust(asset=','.join(dustlist))
          
          if len(sellList) > 0:
            self.logger.info(f"Selling : {sellList}")
        except Exception as e:
          self.logger.warning(f"Unable to convert small balances : {e}")
