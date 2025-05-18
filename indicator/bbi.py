from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import backtrader as bt

class BBI(bt.Indicator):
    lines = ('bbi',)
    params = (
        ('ma1', 3),
        ('ma2', 6),
        ('ma3', 12),
        ('ma4', 24),
    )

    plotinfo = dict(
        subplot=False,  # Plot on the main chart
        plotname='BBI',
        plotlinelabels=True,
        plotlinevalues=True,
    )

    plotlines = dict(
        bbi=dict(
            color='orange',
            linestyle='-',
        ),
    )

    def __init__(self):
        super(BBI, self).__init__()
        
        # Calculate different moving averages
        ma1 = bt.indicators.SMA(self.data, period=self.p.ma1)
        ma2 = bt.indicators.SMA(self.data, period=self.p.ma2)
        ma3 = bt.indicators.SMA(self.data, period=self.p.ma3)
        ma4 = bt.indicators.SMA(self.data, period=self.p.ma4)
        
        # Calculate BBI as the average of all MAs
        self.lines.bbi = (ma1 + ma2 + ma3 + ma4) / 4.0 