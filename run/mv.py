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
                
                # Create stop loss order
                self.stop_price = self.buyprice * (1.0 - self.params.stoploss)
                self.stoporder = self.sell(exectype=bt.Order.Stop, price=self.stop_price)
                
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
        self.log('Close, %.2f, J, %.2f' % (self.dataclose[0], self.kdj.l.j[0]))

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Update days above BBI counter
        if self.dataclose[0] > self.bbi.l.bbi[0]:
            self.days_above_bbi += 1
        else:
            self.days_above_bbi = 0

        # Check if we are in the market
        if not self.position:
            # Not yet ... we MIGHT BUY if ...
            if self.kdj.l.j[0] < 0:
                # BUY, BUY, BUY!!! (with all possible default parameters)
                # self.log('BUY CREATE, %.2f' % self.dataclose[0])
                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()
        else:
            if self.days_above_bbi >= 2:
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log('SELL CREATE, %.2f' % self.dataclose[0])
                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell()


if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(TestStrategy)

    # Datas are in a subfolder of the samples. Need to find where the script is
    # because it could have been called from anywhere
    modpath = os.path.dirname(os.path.abspath(sys.argv[0]))

    symbol = 'MSFT'
    datapath = os.path.join(modpath, '../data/{}.csv'.format(symbol.lower()))

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
        fromdate=datetime.datetime(2025, 1, 1),
        # Do not pass values before this date
        todate=datetime.datetime(2025, 5, 30),
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
    cerebro.broker.setcash(1000.0)

    # Add a FixedSize sizer according to the stake
    cerebro.addsizer(bt.sizers.PercentSizer, percents=80)

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
