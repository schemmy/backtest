from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
import os.path  # To manage paths
import sys  # To find out the script name (in argv[0])

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the backtrader platform
import backtrader as bt
from indicator.kdj import KDJ
from indicator.bbi import BBI
from data_api.yahoo_api import download_stock_data


# Create a Stratey
class TestStrategy(bt.Strategy):
    params = (
        ('maperiod', 15),
        ('stoploss', 0.03),  # 3% stop loss
    )

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close

        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None
        self.stoporder = None  # To keep track of stop loss order

        # Track consecutive days above BBI
        self.days_above_bbi = 0
        self.days_below_bbi = 0
        self.has_above_bbi = False
        self.sell_count = 0

        # Add a MovingAverageSimple indicator
        # self.sma = bt.indicators.SimpleMovingAverage(
        #     self.datas[0], period=self.params.maperiod)
        self.bbi = BBI(self.datas[0])
        
        self.kdj = KDJ(self.datas[0])

        # Indicators for the plotting show
        # bt.indicators.ExponentialMovingAverage(self.datas[0], period=25)
        # bt.indicators.WeightedMovingAverage(self.datas[0], period=25,
                                            # subplot=True)
        # bt.indicators.StochasticSlow(self.datas[0])
        # bt.indicators.MACDHisto(self.datas[0])
        # rsi = bt.indicators.RSI(self.datas[0])
        # bt.indicators.SmoothedMovingAverage(rsi, period=10)
        # bt.indicators.ATR(self.datas[0], plot=False)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
                
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))
                
                # Cancel stop loss order if it exists
                if self.stoporder:
                    self.cancel(self.stoporder)
                    self.stoporder = None
                    self.stop_price = None

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
            
            # If stop loss order was canceled, reset it
            if order == self.stoporder:
                self.stoporder = None

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):
        # Simply log the closing price of the series from the reference
        # self.log('Close, %.2f, J, %.2f' % (self.dataclose[0], self.kdj.l.j[0]))

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Update days above BBI counter
        if self.position and self.dataclose[0] > self.bbi.l.bbi[0]:
            self.days_above_bbi += 1
        else:
            self.days_above_bbi = 0

        # Update days above BBI counter
        if self.position and self.sell_count > 0 and self.dataclose[0] < self.bbi.l.bbi[0]:
            self.days_below_bbi += 1
        else:
            self.days_below_bbi = 0

        # Check if we are in the market
        if not self.position:
            # Not yet ... we MIGHT BUY if ...
            if self.kdj.l.j[0] < 0:
                # BUY, BUY, BUY!!! (with all possible default parameters)
                # self.log('BUY CREATE, %.2f' % self.dataclose[0])
                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()
                self.has_above_bbi = False
                self.sell_count = 0
                self.days_below_bbi = 0
                # Create stop loss order
                self.stop_price = self.dataclose[0] * (1.0 - self.params.stoploss)
                
        else:
            if self.days_above_bbi >= 2 and self.sell_count == 0:
                self.log('SELL CREATE jian, %.2f' % self.dataclose[0])
                self.sell_count += 1
                # Self half
                self.order = self.sell(size=int(self.position.size / 2))

            elif self.days_below_bbi >= 2:
                self.log('SELL CREATE zhisun, %.2f' % self.dataclose[0])
                self.order = self.sell()
            
            elif self.stop_price and self.dataclose[0] < self.stop_price:
                self.log('SELL CREATE stop, %.2f' % self.dataclose[0])
                self.order = self.sell()

            if self.position.size == 0:
                self.sell_count = 0
                self.has_above_bbi = False
                self.days_below_bbi = 0
                self.days_above_bbi = 0

if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(TestStrategy)

    # Datas are in a subfolder of the samples. Need to find where the script is
    # because it could have been called from anywhere
    modpath = os.path.dirname(os.path.abspath(sys.argv[0]))

    symbol = 'GILD'
    datapath = os.path.join(modpath, '../data/stock_data/us/{}.csv'.format(symbol.lower()))

    # Check if data file exists, if not download it
    if not os.path.exists(datapath):
        print(f"Data file not found at {datapath}, downloading...")
        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(datapath), exist_ok=True)
        # Download the data
        download_stock_data(symbol, period='5y')

    # Create a Data Feed
    data = bt.feeds.GenericCSVData(
        dataname=datapath,
        # Do not pass values before this date
        fromdate=datetime.datetime(2010, 1, 1),
        # Do not pass values before this date
        todate=datetime.datetime(2025, 8, 23),
        # Do not pass values after this date
        reverse=False,
        # Column mappings
        dtformat='%Y-%m-%d',  # Date format
        datetime=0,  # Date column index
        open=4,      # Open column index
        high=2,      # High column index
        low=3,       # Low column index
        close=1,     # Close column index
        volume=5,    # Volume column index
        openinterest=-1)  # No open interest column

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)

    # Set our desired cash start
    cerebro.broker.setcash(20000.0)

    # Add a FixedSize sizer according to the stake
    cerebro.addsizer(bt.sizers.PercentSizer, percents=100)

    # Set the commission
    cerebro.broker.setcommission(commission=0.0)

    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    cerebro.run()

    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    
    # Plot the result
    cerebro.plot()
