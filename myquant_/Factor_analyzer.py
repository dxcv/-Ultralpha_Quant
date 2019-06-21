import Dataview
import pandas as pd
import numpy as np
import Dataapi

class FactorAnalyzer(object):

    def __init__(self, dataview,
                 groupby=None, weights=None,
                 quantiles = 10,  periods=(1, 5, 10),
                 delay = 0,
                 max_loss=0.25):

        self.factor = dataview.load_factors
        self.factors_list = dataview.factors

        if delay == 0:
            self.prices = dataview.pre_data_['close']
        elif delay == 1:
            self.prices = dataview.pre_data_['open']
        else:
            raise ValueError("delay must be 0 or 1.")


        # self.universe = universe
        # self.fields = fields

        self.groupby = groupby
        self._quantiles = quantiles

        self.weights = weights
        self._periods = periods

        self._max_loss = max_loss

        self._cleaned_factor_data = self.data_clean_and_data_processing()

    def data_clean_and_data_processing(self):

        factor_data = self.factor
        stocks = factor_data.index.get_level_values('asset')
        start_date = min(factor_data.index.get_level_values('date'))
        end_date = max(factor_data.index.get_level_values('date'))
        # if hasattr(self.prices, "__call__"):
        #     prices = self.prices(securities=stocks,
        #                          start_date=start_date,
        #                          end_date=end_date).append(
        #              self.prices(securities=stocks,
        #                          end_date=end_date,
        #                          count=max(self._periods) + 1)
        #             )
        # self._prices = prices
        forward_returns = self.compute_forward_returns(
            self.factor,
            self.prices,
            self._periods
        )

        return self.data_clean(forward_returns)

    def compute_forward_returns(self,factor,prices,periods = (1,5,10)):

        factor_dateindex = factor.index.levels[0]
        factor_dateindex = factor_dateindex.intersection(prices.index.levels[0])
        if len(factor_dateindex) == 0:
            raise ValueError("Factor and prices indices don't match: make sure "
                         "they have the same convention in terms of datetimes "
                         "and symbol-names")

        factor_assetindex = factor.index.levels[1]
        factor_assetindex = factor_assetindex.intersection(
                                                         prices.index.levels[1])
        if len(factor_dateindex) == 0:
            raise ValueError("Factor and prices indices don't match: make sure "
                         "they have the same convention in terms of STOCKS "
                         )
        forward_returns = pd.DataFrame(
            index=pd.MultiIndex
                .from_product([factor_dateindex,factor_assetindex],
                              names=['date', 'asset'])
        )
        for period in periods:
            delta = prices.groupby(level='asset').pct_change(period)
            delta = delta.groupby(level='asset').shift(-period)
            forward_returns['period_{p}'.format(p=period)] = delta

        # print(forward_returns)
        return forward_returns


    def data_clean(self,forward_returns):

        initial_amount = float(len(self.factor.index))
        groupby = self.groupby
        weights = self.weights
        factor_copy = self.factor.copy()
        merged_data = forward_returns.copy()
        # print('length minus = ',len(factor_copy.index)-len(merged_data.index))
        factors_list = self.factors_list
        for k in factors_list:
            merged_data[k] = factor_copy[k]

        if self.groupby is not None:
            if isinstance(groupby, dict):
                diff = set(factor_copy.index.get_level_values(
                    'asset')) - set(groupby.keys())
                if len(diff) > 0:
                    raise KeyError(
                        "Assets {} not in group mapping".format(
                            list(diff)))

                ss = pd.Series(groupby)
                groupby = pd.Series(index=factor_copy.index,
                                    data=ss[factor_copy.index.get_level_values(
                                        'asset')].values)
            elif isinstance(groupby, pd.DataFrame):
                index_group = groupby.index
                groupby = groupby.stack().loc[factor_copy.index]
                diff2 =   set(index_group) - set(factor_copy.index)
                if len(diff2)>0:
                    print("Assets {} not in group mapping".format(
                            list(diff2)))
            else:
                raise TypeError('group must be dict or DataFrame.')
            merged_data['group'] = groupby

        if self.weights is not None:
            if isinstance(weights, dict):
                diff = set(factor_copy.index.get_level_values(
                    'asset')) - set(weights.keys())
                if len(diff) > 0:
                    raise KeyError(
                        "Assets {} not in weights mapping".format(
                            list(diff)))

                ww = pd.Series(weights)
                weights = pd.Series(index=factor_copy.index,
                                    data=ww[factor_copy.index.get_level_values(
                                        'asset')].values)
            elif isinstance(weights, pd.DataFrame):
                weights = weights.stack()
            merged_data['weights'] = weights

        merged_data = merged_data.dropna()

        no_raise = False if self._max_loss == 0 else True
        quantile_data = self.quantize_factor(
            factor_data=merged_data,
            quantiles=self._quantiles,
            _no_raise= no_raise
        )
        # print( quantile_data )
        # factors_quantized_list = quantile_data.columns
        for k in list(quantile_data.columns):
            merged_data[k] = quantile_data[k]

        merged_data = merged_data.dropna()

        binning_amount = float(len(merged_data.index))

        tot_loss = (initial_amount - binning_amount) / initial_amount

        class MaxLossExceededError(Exception):
            pass

        if tot_loss > self._max_loss:
            message = ("max_loss (%.1f%%) 超过 %.1f%%"
                       % (self._max_loss * 100, tot_loss * 100))
            raise MaxLossExceededError(message)

        return merged_data



    def quantize_factor(self,factor_data, quantiles=5, by_group=False,_no_raise = True):


        def quantile_calc(x, _quantiles, _no_raise=True, _bins=None, _zero_aware=False):
            try:
                if _quantiles is not None and _bins is None and not _zero_aware:
                    return pd.qcut(x, _quantiles, labels=False) + 1
                elif _quantiles is not None and _bins is None and _zero_aware:
                    pos_quantiles = pd.qcut(x[x >= 0], _quantiles // 2,
                                            labels=False) + _quantiles // 2 + 1
                    neg_quantiles = pd.qcut(x[x < 0], _quantiles // 2,
                                            labels=False) + 1
                    return pd.concat([pos_quantiles, neg_quantiles]).sort_index()
                elif _bins is not None and _quantiles is None and not _zero_aware:
                    return pd.cut(x, _bins, labels=False) + 1
                elif _bins is not None and _quantiles is None and _zero_aware:
                    pos_bins = pd.cut(x[x >= 0], _bins // 2,
                                      labels=False) + _bins // 2 + 1
                    neg_bins = pd.cut(x[x < 0], _bins // 2,
                                      labels=False) + 1
                    return pd.concat([pos_bins, neg_bins]).sort_index()
            except Exception as e:
                if _no_raise:
                    return pd.Series(index=x.index)
                raise e

        grouper = [factor_data.index.get_level_values('date')]
        if by_group:
            if 'group' not in factor_data.columns:
                raise ValueError('只有输入了 groupby 参数时 binning_by_group 才能为 True')
            grouper.append('group')

        factor_quantile = pd.DataFrame()
        for k in self.factors_list:
            factor_quantile['{}_quantile'.format(k)] = \
                factor_data.groupby(grouper)[k] \
            .apply(quantile_calc, quantiles, _no_raise,None, None)
            # factor_quantile.name = '{}_quantile'.format(k)
        return factor_quantile.dropna()

        # print(1)

