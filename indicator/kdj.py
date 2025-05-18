import backtrader as bt
from .ema_adjust import ExponentialMovingAverageAdjust

class KDJ(bt.Indicator):
    '''
    This generic indicator doesn't assume the data feed has the components
    ``high``, ``low`` and ``close``. It needs three data sources passed to it,
    which whill considered in that order. (following the OHLC standard naming)
    '''
    lines = ('j',)
    params = dict(
        pk=9,
        pd=3,
        pdslow=3,
        mv=ExponentialMovingAverageAdjust,
        slowav=None,
    )

    def __init__(self):
        # Get highest from period k from 1st data
        highest = bt.ind.Highest(self.data0.high, period=self.p.pk)
        # Get lowest from period k from 2nd data
        lowest = bt.ind.Lowest(self.data0.low, period=self.p.pk)

        # Apply the formula to get raw K
        RSV = 100.0 * (self.data0.close - lowest) / (highest - lowest)

        # The standard k in the indicator is a smoothed versin of K
        k = self.p.mv(RSV, period=self.p.pd)

        # Smooth k => d
        d = self.p.mv(k, period=self.p.pdslow)

        self.l.j = 3. * k - 2. * d
