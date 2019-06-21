# encoding:utf-8

import os
from ipyparallel import Client
from functools import partial
import pandas as pd

def _apply(func, *args, **kwargs):
    return partial(func, *args, **kwargs)()


def _get_selector(selector_name, selector_package_name):
    import importlib
    module = importlib.import_module("%s.%s" % (selector_package_name, selector_name))
    selector = getattr(module, selector_name)
    return selector


class Admin(object):
    PACKAGE_NAME = os.environ.get("FXDAYU_SELECTOR_PACKAGE_NAME", "selectors")

    def __init__(self, *all_selectors_name):
        self._all_selectors_name = all_selectors_name
        self._all_selectors_result = {}

    @staticmethod
    # MultiIndex格式的选股方案累加
    def combine_selector(selector_result_list):
        gather_result = pd.concat(selector_result_list, join="outer", axis=1).fillna(0).sum(axis=1)
        gather_result.index.names = ["date", "asset"]
        return gather_result

    @staticmethod
    # 选股方案取交集
    def Intersection_Strategy(selectors_result_dict,
                              rank=None,
                              rank_pct=None,
                              weight_dict=None):
        """
        对若干选股方案取交集
        :param selectors_result_dict: 若干选股器结果组成的字典(dict),形式为:
                             {"selector_name_1":selector_1,"selector_name_2":selector_2}
                           　每个选股器结果(selector_result)格式为一个MultiIndex Series，索引(index)为date(level 0)和asset(level 1),
                             包含一列结果值。(1:选出,0:不选,-1:做空)  形如:
                                        -----------------------------------
                                            date    |    asset   |
                                        -----------------------------------
                                                    |   AAPL     |   1
                                                    -----------------------
                                                    |   BA       |   1
                                                    -----------------------
                                        2014-01-01  |   CMG      |   1
                                                    -----------------------
                                                    |   DAL      |   0
                                                    -----------------------
                                                    |   LULU     |   -1
                                                    -----------------------
        :param rank:  选出得分排名前rank的股票(个数排名) 该参数在交集方案下默认为空
        :param rank_pct: 选出得分排名前rank_pct的股票(百分位排名) 该参数在交集方案下默认为空
        :param weight_dict: 各选股器权重所组成的dict。该参数在交集方案下默认为空
        :return: 取交集的策略结果: Strategy 对象,包含"strategy_name", "strategy_result", "weight_dict"三个属性。
                                 "strategy_name":组合的选股结果名称(str)
                                 "strategy_result":组合的选股结果(格式为一个MultiIndex Series，索引(index)为date(level 0)和asset(level 1),
                            　　　  　              包含一列结果值。(>0:选出,0:不选,<0:做空))
                                 "weight_dict":各选股器权重所组成的dict(dict/None)
        """
        from fxdayu_alphaman.selector.utility import Strategy

        gather_result = Admin.combine_selector(list(selectors_result_dict.values()))
        strategy_name = "+".join(list(selectors_result_dict.keys()))

        strategy = Strategy()
        strategy["strategy_name"] = strategy_name + "_Intersection"
        strategy["strategy_result"] = gather_result[gather_result >= gather_result.max() - 0.000000001]
        strategy["weight_dict"] = None

        return strategy

    @staticmethod
    # 选股方案取并集
    def Union_Strategy(selectors_result_dict,
                       rank=None,
                       rank_pct=None,
                       weight_dict=None):

        """
        对若干选股方案取并集
        :param selectors_result_dict: 若干选股器结果组成的字典(dict),形式为:
                             {"selector_name_1":selector_1,"selector_name_2:selector_2}
                           　每个选股器结果(selector_result)格式为一个MultiIndex Series，索引(index)为date(level 0)和asset(level 1),
                             包含一列结果值。(1:选出,0:不选,-1:做空)  形如:
                                        -----------------------------------
                                            date    |    asset   |
                                        -----------------------------------
                                                    |   AAPL     |   1
                                                    -----------------------
                                                    |   BA       |  1
                                                    -----------------------
                                        2014-01-01  |   CMG      |   1
                                                    -----------------------
                                                    |   DAL      |  0
                                                    -----------------------
                                                    |   LULU     |   -1
                                                    -----------------------
        :param rank:  选出得分排名前rank的股票(个数排名) 该参数在并集方案下默认为空
        :param rank_pct: 选出得分排名前rank_pct的股票(百分位排名) 该参数在并集方案下默认为空
        :param weight_dict: 各选股器权重所组成的dict。该参数在并集方案下默认为空
        :return: 取交集的策略结果: Strategy 对象,包含"strategy_name", "strategy_result", "weight_dict"三个属性。
                                 "strategy_name":组合的选股结果名称(str)
                                 "strategy_result":组合的选股结果(格式为一个MultiIndex Series，索引(index)为date(level 0)和asset(level 1),
                            　　　  　              包含一列结果值。(>0:选出,0:不选,<0:做空))
                                 "weight_dict":各选股器权重所组成的dict(dict/None)
        """
        from fxdayu_alphaman.selector.utility import Strategy

        gather_result = Admin.combine_selector(list(selectors_result_dict.values()))
        strategy_name = "+".join(list(selectors_result_dict.keys()))

        strategy = Strategy()
        strategy["strategy_name"] = strategy_name + "_Union"
        strategy["strategy_result"] = gather_result[gather_result > 0]
        strategy["weight_dict"] = None

        return strategy

    @staticmethod
    # 加权汇总排序选股
    def Rank_Strategy(selectors_result_dict,
                      rank=10,
                      rank_pct=None,
                      weight_dict=None):

        """
        各选股结果加权汇总后，取排名前rank/rank_pct且至少被某一个选股器选出过一次的股票。
        :param selectors_result_dict: 若干选股器结果组成的字典(dict),形式为:
                             {"selector_name_1":selector_1,"selector_name_2:selector_2}
                           　每个选股器结果(selector_result)格式为一个MultiIndex Series，索引(index)为date(level 0)和asset(level 1),
                             包含一列结果值。(1:选出,0:不选,-1:做空)  形如:
                                        -----------------------------------
                                            date    |    asset   |
                                        -----------------------------------
                                                    |   AAPL     |   1
                                                    -----------------------
                                                    |   BA       |  1
                                                    -----------------------
                                        2014-01-01  |   CMG      |   1
                                                    -----------------------
                                                    |   DAL      |  0
                                                    -----------------------
                                                    |   LULU     |   -1
                                                    -----------------------
        :param rank (optional):  选出得分排名前rank的股票(个数排名) (int)
        :param rank_pct (optional): 选出得分排名前rank_pct的股票(百分位排名) (float) ,与rank二选一。该参数有值的时候,rank参数失效。
        :param weight_dict: 各选股器权重所组成的dict。默认等权重。
                            字典键为选股器名称(str),包含了selectors_result_dict中的所有选股器;
                            字典值为float,代表对应选股器的给分权重。
                            形如:{'selector_name_1':1.0,'selector_name_2':2.0,...}
        :return: 加权汇总的策略结果: Strategy 对象,包含"strategy_name", "strategy_result", "weight_dict"三个属性。
                                  "strategy_name":组合的选股结果名称(str)
                                  "strategy_result":组合的选股结果(格式为一个MultiIndex Series，索引(index)为date(level 0)和asset(level 1),
                            　　　  　               包含一列结果值。(>0:选出,0:不选,<0:做空))
                                  "weight_dict":各选股器权重所组成的dict(dict/None)
        """

        from fxdayu_alphaman.selector.utility import Strategy

        def get_weighted_selector_result_list(selectors_result_dict,
                                              weight_dict=None):
            if weight_dict:
                weighted_selector_result_list = []
                for selector_name in selectors_result_dict:
                    weighted_selector_result_list.append(
                        selectors_result_dict[selector_name] * weight_dict[selector_name])
                return weighted_selector_result_list
            else:
                return list(selectors_result_dict.values())

        def get_strategy_result_by_rank(gather_result, rank):
            return gather_result[gather_result.groupby(level=0).
                   apply(lambda x:x.rank(method="min", ascending=False)<=rank)]

        def get_strategy_result_by_rank_pct(gather_result, rank_pct):
            return gather_result[gather_result.groupby(level=0).apply(lambda x:x>=x.quantile(1 - rank_pct))]

        # 对选股器做加权处理
        weighted_selector_result_list = get_weighted_selector_result_list(selectors_result_dict,
                                                                          weight_dict)
        # 所有选股器累加(累积打分)
        gather_result = Admin.combine_selector(weighted_selector_result_list)
        strategy_name = "+".join(list(selectors_result_dict.keys()))

        if rank_pct:  # 选股组合方案按权重【百分位排名】过滤 含并列
            gather_result = get_strategy_result_by_rank_pct(gather_result, rank_pct)
            name_tail = "_rank_pct_%s" % (rank_pct,)
        else:  # 选股组合方案结果按权重【排名】过滤 含并列
            gather_result = get_strategy_result_by_rank(gather_result, rank)
            name_tail = "_rank_%s" % (rank,)

        strategy = Strategy()
        strategy["strategy_name"] = strategy_name + name_tail
        strategy["strategy_result"] = gather_result[gather_result > 0]
        strategy["weight_dict"] = weight_dict

        return strategy

    @staticmethod
    # 计算选股（含多策略组合）策略的绩效表现
    def calculate_performance(strategy_name,
                              strategy_result,
                              start,
                              end,
                              periods=(1, 5, 10),
                              benchmark_return=None,
                              price=None,
                              price_high=None,
                              price_low=None,
                              ):

        """
        计算选股（含多策略组合）策略的绩效表现
        :param strategy_name: 策略名称 string
        :param strategy_result: 策略结果（选股结果 或组合策略结果）
                                格式为一个MultiIndex Series，索引(index)为date(level 0)和asset(level 1),
                            　　　  　包含一列结果值。(>0:选出,0:不选,<0:做空)  形如:
                                        -----------------------------------
                                            date    |    asset   |
                                        -----------------------------------
                                                    |   AAPL     |   1
                                                    -----------------------
                                                    |   BA       |  1
                                                    -----------------------
                                        2014-01-01  |   CMG      |   1
                                                    -----------------------
                                                    |   DAL      |  0
                                                    -----------------------
                                                    |   LULU     |   -1
                                                    -----------------------

        :param start: 回测起始时间 datetime
        :param end: 回测结束时间 datetime
        :param periods: 持有时间 tuple
        :param benchmark_return(optional): 基准收益 mulitiIndex.索引(index)为factor_quantile(level 0)和date(level 1),
                                           columns 为持有时间(与periods一一对应)。形如：
                                                         1         5         10
                factor_quantile date
                HS300           2013-01-01 15:00:00  0.000000  0.000000  0.000000
                                2013-01-02 15:00:00  0.000000  0.000000  0.000000
                                2013-01-03 15:00:00  0.000000  0.000000  0.000000
                                2013-01-04 15:00:00  0.004587 -0.016313  0.028137
                                2013-01-05 15:00:00  0.000000  0.000000  0.000000
                                2013-01-06 15:00:00  0.000000  0.000000  0.000000
                                2013-01-07 15:00:00 -0.004203  0.016459  0.029539
                                2013-01-08 15:00:00  0.000317  0.027929  0.028341
                                2013-01-09 15:00:00  0.001758  0.020173  0.032195
                                2013-01-10 15:00:00 -0.018707  0.008769  0.020620
                                .................................................

        :param price (optional):计算绩效时用到的的个股每日价格,通常为收盘价（close）。
                                索引（index)为datetime,columns为各股票代码。pandas dataframe类型,形如:
                                       sh600011  sh600015  sh600018  sh600021  sh600028
                datetime
                2014-10-08 15:00:00    18.743    17.639       NaN     7.463     9.872
                2014-10-09 15:00:00    18.834    17.556       NaN     7.536     9.909
                2014-10-10 15:00:00    18.410    17.597       NaN     7.580     9.835
                2014-10-13 15:00:00    18.047    17.515       NaN     7.536     9.685
                2014-10-14 15:00:00    18.773    17.494       NaN     7.433     9.704
                2014-10-15 15:00:00    18.561    17.597       NaN     7.477     9.704
                2014-10-16 15:00:00    18.501    17.659       NaN     7.448     9.685
                2014-10-17 15:00:00    18.349    17.535       NaN     7.272     9.611
                2014-10-20 15:00:00    18.319    17.618       NaN     7.360     9.629
                .....................................................................
        :param price_high (optional):计算绩效时用到的的个股每日最高价格,pandas dataframe类型,形同price
        :param price_low (optional):计算绩效时用到的的个股每日最低价格,pandas dataframe类型,形同price
        :return:该策略的绩效 Performance object
                包含"strategy_name","mean_return","key_performance_indicator",
                   "holding_return", "holding_distribution_features",
                   "upside_return","upside_distribution_features",
                   "downside_return","downside_distribution_features" 这些属性

        """

        from fxdayu_alphaman.selector.selector_analysis import get_stocklist_mean_return, \
            get_stocks_upside_return, \
            plot_distribution_features_table, \
            get_stocks_downside_return, get_stocks_holding_return
        from fxdayu_alphaman.selector.utility import Performance
        from fxdayu_data import DataAPI
        import pyfolio as pf
        import numpy as np
        import datetime

        def get_price_data(pool, start, end, max_window=10):
            data = DataAPI.candle(tuple(pool), "D",
                                  start=start - datetime.timedelta(days=max_window),
                                  end=end + datetime.timedelta(days=max_window))
            data = data.replace(to_replace=0, value=np.NaN)
            return data

        def deal_benchmark_return(benchmark_return, periods):
            if benchmark_return is None:
                benchmark_return = {}
                for period in periods:
                    benchmark_return[period] = None
            else:
                benchmark_return = benchmark_return.copy()
                benchmark_return.index = benchmark_return.index.droplevel(level=0)
            return benchmark_return

        # 判断是否结果为空,为空则返回空字典
        if len(strategy_result) == 0:
            return None

        pool = strategy_result.index.levels[1]

        if (price is None) or (price_high is None) or (price_low is None):
            price_data = get_price_data(pool.tolist(), start, end, max_window=max(periods))
            price = price_data.minor_xs("close")
            price_high = price_data.minor_xs("high")
            price_low = price_data.minor_xs("low")

        ################################################################################
        # 以下计算各绩效指标
        performance = Performance()

        performance["strategy_name"] = strategy_name

        # 持股周期平均收益
        performance["mean_return"] = get_stocklist_mean_return(strategy_result,
                                                               strategy_name,
                                                               start,
                                                               end,
                                                               price,
                                                               periods)

        # 计算关键性绩效指标
        mean_return = performance["mean_return"].copy()
        mean_return.index = mean_return.index.droplevel(level=0)
        benchmark_return = deal_benchmark_return(benchmark_return, periods)

        performance["key_performance_indicator"] = {}

        # 按周期分别计算
        for period in periods:
            performance["key_performance_indicator"]["period_%s" % (period,)] = pf.timeseries.perf_stats(
                mean_return[period],
                benchmark_return[period])

        performance["holding_return"] = get_stocks_holding_return(strategy_result,
                                                                  price,
                                                                  strategy_name,
                                                                  periods)

        performance["holding_distribution_features"] = plot_distribution_features_table(performance["holding_return"],
                                                                                        periods)

        performance["upside_return"] = get_stocks_upside_return(strategy_result,
                                                                price,
                                                                price_high,
                                                                strategy_name,
                                                                periods)

        performance["upside_distribution_features"] = plot_distribution_features_table(performance["upside_return"],
                                                                                       periods)

        performance["downside_return"] = get_stocks_downside_return(strategy_result,
                                                                    price,
                                                                    price_low,
                                                                    strategy_name,
                                                                    periods)

        performance["downside_distribution_features"] = plot_distribution_features_table(performance["downside_return"],
                                                                                         periods)
        return performance

    def rank_performance(self,
                         strategies_performance,
                         target_period=10,
                         target_indicator="Sharpe ratio",
                         ascending=False
                         ):
        """
        将若干Performance对象所组成的列表（strategies_performance）按指定持有期(target_period)下的指定指标(target_indicator)排序，默认为降序
        :param strategies_performance: 若干Performance对象所组成的列表（list）
               Performance object
               包含"strategy_name","mean_return","key_performance_indicator",
                   "holding_return", "holding_distribution_features",
                   "upside_return","upside_distribution_features",
                   "downside_return","downside_distribution_features" 这些属性
        :param target_period: 指定持有期(int)
        :param target_indicator: 指定用于排序的绩效指标(str)
                                 指标包含:
                                        Annual return
                                        Cumulative returns
                                        Annual volatility
                                        Sharpe ratio
                                        Calmar ratio
                                        Stability
                                        Max drawdown
                                        Omega ratio
                                        Sortino ratio
                                        Skew
                                        Kurtosis
                                        Tail ratio
                                        Daily value at risk
                                        Alpha
                                        Beta
        :param ascending: 是否升序（bool）。默认False(降序）
        :return: 排序后的Performance对象所组成的列表
        """

        return sorted(strategies_performance,
                      key=lambda x: x.key_performance_indicator["period_%s" % (target_period,)][target_indicator],
                      reverse=(ascending == False))

    # 批量计算strategies_result_dict中所有选股方案(含组合方案)的表现，并汇总成列表——list
    def show_strategies_performance(self,
                                    strategies_result_dict,
                                    start,
                                    end,
                                    periods=(1, 5, 10),
                                    benchmark_return=None,
                                    price=None,
                                    price_high=None,
                                    price_low=None,
                                    parallel=False):
        """
        批量计算strategies_result_dict中所有选股方案(含组合方案)的表现。
        :param strategies_result_dict: 若干选股策略结果组成的字典(dict),形式为:
                             {"strategy_name_1":strategy_1,"strategy_name_2:strategy_2}
                           　每个选股策略结果(选股结果 或组合策略结果)格式为一个MultiIndex Series，
                             索引(index)为date(level 0)和asset(level 1),包含一列结果值。(>0:选出,0:不选,<0:做空)  形如:
                                        -----------------------------------
                                            date    |    asset   |
                                        -----------------------------------
                                                    |   AAPL     |   1
                                                    -----------------------
                                                    |   BA       |  1
                                                    -----------------------
                                        2014-01-01  |   CMG      |   1
                                                    -----------------------
                                                    |   DAL      |  0
                                                    -----------------------
                                                    |   LULU     |   -1
                                                    -----------------------
        :param start: 回测起始时间 datetime
        :param end: 回测结束时间 datetime
        :param periods: 持有时间 tuple
        :param benchmark_return(optional): 基准收益 mulitiIndex.索引(index)为factor_quantile(level 0)和date(level 1),
                                           columns 为持有时间(与periods一一对应)。形如：
                                                         1         5         10
                factor_quantile date
                HS300           2013-01-01 15:00:00  0.000000  0.000000  0.000000
                                2013-01-02 15:00:00  0.000000  0.000000  0.000000
                                2013-01-03 15:00:00  0.000000  0.000000  0.000000
                                2013-01-04 15:00:00  0.004587 -0.016313  0.028137
                                2013-01-05 15:00:00  0.000000  0.000000  0.000000
                                2013-01-06 15:00:00  0.000000  0.000000  0.000000
                                2013-01-07 15:00:00 -0.004203  0.016459  0.029539
                                2013-01-08 15:00:00  0.000317  0.027929  0.028341
                                2013-01-09 15:00:00  0.001758  0.020173  0.032195
                                2013-01-10 15:00:00 -0.018707  0.008769  0.020620
                                .................................................

        :param price(optional):计算绩效时用到的的个股每日价格,通常为收盘价（close）。
                                 索引（index)为datetime,columns为各股票代码。pandas dataframe类型,形如:
                                       sh600011  sh600015  sh600018  sh600021  sh600028
                datetime
                2014-10-08 15:00:00    18.743    17.639       NaN     7.463     9.872
                2014-10-09 15:00:00    18.834    17.556       NaN     7.536     9.909
                2014-10-10 15:00:00    18.410    17.597       NaN     7.580     9.835
                2014-10-13 15:00:00    18.047    17.515       NaN     7.536     9.685
                2014-10-14 15:00:00    18.773    17.494       NaN     7.433     9.704
                2014-10-15 15:00:00    18.561    17.597       NaN     7.477     9.704
                2014-10-16 15:00:00    18.501    17.659       NaN     7.448     9.685
                2014-10-17 15:00:00    18.349    17.535       NaN     7.272     9.611
                2014-10-20 15:00:00    18.319    17.618       NaN     7.360     9.629
                .....................................................................
        :param price_high (optional):计算绩效时用到的的个股每日最高价格,pandas dataframe类型,形同price
        :param price_low (optional):计算绩效时用到的的个股每日最低价格,pandas dataframe类型,形同price
        :param parallel: 是否执行并行计算（bool） 默认不执行。 如需并行计算需要在jupyter notebook下启动工作脚本。
        :return:选股方案的表现 (Performance object)所组成的列表(list),
                列表里每个元素为选股方案的表现 (Performance object)
                包含"strategy_name","mean_return","key_performance_indicator",
                   "holding_return", "holding_distribution_features",
                   "upside_return","upside_distribution_features",
                   "downside_return","downside_distribution_features" 这些属性
        """

        if parallel:
            client = Client()
            lview = client.load_balanced_view()
            results = []
            for strategy_name in strategies_result_dict.keys():
                results.append(lview.apply_async(self.calculate_performance,
                                                 strategy_name,
                                                 strategies_result_dict[strategy_name],
                                                 start,
                                                 end,
                                                 periods=periods,
                                                 benchmark_return=benchmark_return,
                                                 price=price,
                                                 price_high=price_high,
                                                 price_low=price_low))
            lview.wait(results)
            strategies_performance = [result.get() for result in results if not (result.get() == None)]
            return strategies_performance
        else:
            strategies_performance = []
            for strategy_name in strategies_result_dict.keys():
                result = _apply(self.calculate_performance,
                                strategy_name,
                                strategies_result_dict[strategy_name],
                                start,
                                end,
                                periods=periods,
                                benchmark_return=benchmark_return,
                                price=price,
                                price_high=price_high,
                                price_low=price_low)
                if not (result == None):
                    strategies_performance.append(result)
            return strategies_performance

    # 枚举选股器的各种组合权重
    def enumerate_selectors_weight(self,
                                   func,
                                   weight_range_dict,
                                   selectors_result_dict = None,
                                   rank=10,
                                   rank_pct=None,
                                   parallel=False):
        """
        按指定的组合方法,枚举不同权重(不同打分),对若干选股器进行组合打分。
        :param func: 指定选股器的组合方法 选项有(admin.Intersection_Strategy/admin.Union_Strategy/admin.Rank_Strategy)
        :param weight_range_dict: 描述每个选股器的权重优化空间。键为选股器名称，值为range对象，表示优化空间的起始、终止、步长。
                                  如weight_range_dict = {"selector_name_1"：range(0,10,1),"selector_name_2":range(0,10,1)}
        :param selectors_result_dict: 若干选股器结果组成的字典(dict),形式为:
                                      {"selector_name_1":selector_1,"selector_name_2":selector_2}
        :param rank (optional):  选出得分排名前rank的股票(个数排名) (int)
        :param rank_pct (optional): 选出得分排名前rank_pct的股票(百分位排名) (float) ,与rank二选一。该参数有值的时候,rank参数失效。
        :param parallel: 是否执行并行计算（bool） 默认不执行。 如需并行计算需要在jupyter notebook下启动工作脚本。
        :return: 不同权重下的组合选股结果
                 格式为由Strategy 对象所组成的列表(list),
                 每个由Strategy 对象包含"strategy_name", "strategy_result", "weight_dict"三个属性。
                 "strategy_name":组合的选股结果名称(str)
                 "strategy_result":组合的选股结果(格式为一个MultiIndex Series，索引(index)为date(level 0)和asset(level 1),
            　　　  　              包含一列结果值。(>0:选出,0:不选,<0:做空))
                 "weight_dict":各选股器权重所组成的dict(dict/None)
        """

        from itertools import product

        if not selectors_result_dict:
            if not self._all_selectors_result:
                raise TypeError("There is no selector calculated.")
            else:
                selectors_result_dict = self._all_selectors_result # 默认组合包含了所有选股器

        if len(list(selectors_result_dict.keys())) < 2: #至少要有两个选股器
            raise TypeError("you must give more than 2 selectors.")

        parameter_dict = {}
        for selector_name in selectors_result_dict.keys():
            parameter_dict[selector_name] = weight_range_dict[selector_name]

        keys = list(parameter_dict.keys())

        if parallel:
            client = Client()
            lview = client.load_balanced_view()
            results = []
            for value in product(*parameter_dict.values()):
                weight_dict = dict(zip(keys, value))
                results.append(lview.apply_async(func,
                                                 selectors_result_dict,
                                                 rank,
                                                 rank_pct,
                                                 weight_dict))
            lview.wait(results)
            combination_results = [result.get() for result in results]
            return combination_results
        else:
            results = []
            for value in product(*parameter_dict.values()):
                weight_dict = dict(zip(keys, value))
                results.append(_apply(func,
                                      selectors_result_dict,
                                      rank,
                                      rank_pct,
                                      weight_dict))
            return results


    @staticmethod
    # 计算选股器指定时间段的选股结果
    def instantiate_selector_and_get_selector_result(selector_name,
                                                     pool,
                                                     start,
                                                     end,
                                                     Selector=None,
                                                     data=None,
                                                     data_config={"freq": "D", "api": "candle", "adjust": "after"},
                                                     para_dict=None,
                                                     ):

        """
        计算某个选股器指定时间段的选股结果(可支持直接用selector_name加载到对应因子的算法)
        :param selector_name: 选股器名称(str) 需确保传入的selector_name、选股器的类名、对应的module文件名一致(不含.后缀),选股器才能正确加载
        :param pool: 股票池范围（list),如：["000001.XSHE","600300.XSHG",......]
        :param start: 起始时间 (datetime)
        :param end: 结束时间 (datetime)
        :param Selector (optional): 选股器(selector.selector.Selector object),可选.可以输入一个设计好的Selector类来执行计算.
        :param data (optional): 计算选股结果需用到的数据,根据计算需求自行指定。(可选)
        :param data_config (optional): 在data参数为None的情况下(不传入自定义数据),
                                       可通过该参数调用fxdayu_data api 访问到数据 (dict),
                                       与data参数二选一。
        :param para_dict (optional): 外部指定选股器里所用到的参数集(dict),为空则不修改原有参数。 形如:{"fast":5,"slow":10}
        :return: selector_result: 选股器结果　格式为一个MultiIndex Series，索引(index)为date(level 0)和asset(level 1),
                            　　　 包含一列结果值。(1:选出,0:不选,-1:做空)  形如:
                                        -----------------------------------
                                            date    |    asset   |
                                        -----------------------------------
                                                    |   AAPL     |   1
                                                    -----------------------
                                                    |   BA       |  1
                                                    -----------------------
                                        2014-01-01  |   CMG      |   1
                                                    -----------------------
                                                    |   DAL      |  0
                                                    -----------------------
                                                    |   LULU     |   -1
                                                    -----------------------
        """
        # 实例化选股器
        if Selector is None:
            selector = _get_selector(selector_name, Admin.PACKAGE_NAME)()
        else:
            selector = Selector

        # 接收外部传入的参数
        if para_dict:
            for para in list(para_dict.keys()):
                setattr(selector, para, para_dict[para])

        # 选股结果获取
        selector_result = selector.selector_result(pool,
                                                   start,
                                                   end,
                                                   data=data,
                                                   data_config=data_config,
                                                   update=True
                                                   )

        return selector_result

    # 枚举不同参数下的选股结果
    def enumerate_parameter(self,
                            selector_name,
                            para_range_dict,
                            pool,
                            start,
                            end,
                            Selector=None,
                            data=None,
                            data_config={"freq": "D", "api": "candle", "adjust": "after"},
                            parallel=False):
        """
        # 枚举选股器的不同参数
        :param selector_name: 选股器名称（str） 需确保传入的selector_name、选股器的类名、对应的module文件名一致(不含.后缀),选股器才能正确加载
        :param para_range_dict: 描述了selector当中待优化参数的选择空间（dict）。键为参数名称，值为range对象，表示优化空间的起始、终止、步长。
               如：para_range_dict = {"fast"：range(0,10,1),"slow":range(0,10,1)}.
        :param pool: 股票池范围（list),如：["000001.XSHE","600300.XSHG",......]
        :param start: 起始时间 (datetime)
        :param end: 结束时间 (datetime)
        :param Selector (optional): 选股器(selector.selector.Selector object),可选.可以输入一个设计好的Selector类来执行计算.
        :param data (optional): 计算选股结果需用到的数据,根据计算需求自行指定。(可选)
        :param data_config (optional): 在data参数为None的情况下(不传入自定义数据),可通过该参数调用fxdayu_data api 访问到数据 (dict)
        :param parallel: 是否执行并行计算（bool） 默认不执行。 如需并行计算需要在jupyter notebook下启动工作脚本。
        :return: selector_result_by_para_list, para_dict_list
                 selector_result_by_para_list：不同参数下得到的选股结果所组成的list(list)
                 para_dict_list：不同参数集所组成的list（list），与selector_result_by_para_list一一对应。
                                 每个参数集格式为dict，形如:{"fast":5,"slow":10}
        """

        from itertools import product

        keys = list(para_range_dict.keys())

        if parallel:
            client = Client()
            lview = client.load_balanced_view()
            results = []
            para_dict_list = []
            for value in product(*para_range_dict.values()):
                para_dict = dict(zip(keys, value))
                para_dict_list.append(para_dict)
                results.append(lview.apply_async(self.instantiate_selector_and_get_selector_result,
                                                 selector_name,
                                                 pool,
                                                 start,
                                                 end,
                                                 Selector,
                                                 data,
                                                 data_config,
                                                 para_dict))

            lview.wait(results)
            diff_results = [result.get() for result in results]
            return diff_results, para_dict_list
        else:
            results = []
            para_dict_list = []
            for value in product(*para_range_dict.values()):
                para_dict = dict(zip(keys, value))
                para_dict_list.append(para_dict)
                results.append(_apply(self.instantiate_selector_and_get_selector_result,
                                      selector_name,
                                      pool,
                                      start,
                                      end,
                                      Selector,
                                      data,
                                      data_config,
                                      para_dict))
            return results, para_dict_list

    # 获取admin下所有选股器的选股结果
    def get_all_selectors_result(self,
                                 pool,
                                 start,
                                 end,
                                 all_Selectors_dict=None,
                                 all_selectors_data_dict=None,
                                 all_selectors_data_config_dict=None,
                                 all_selectors_para_dict=None,
                                 parallel=False,
                                 update=False):

        """
        计算admin下加载的所有选股器的选股器结果

        :param pool: 股票池范围（list),如：["000001.XSHE","600300.XSHG",......]
        :param start: 起始时间 (datetime)
        :param end: 结束时间 (datetime)
        :param all_Selectors_dict(optional):加载到admin下的所有Selector类(selector.selector.Selector object)构成的字典, 可选.
                                      可以输入一系列设计好的选股器类(与Admin._all_selectors_name一一对应)直接执行计算.
                                      形如:{"selector_name_1":Selector_1,"selector_name_2":Selector_2,...}
        :param all_selectors_data_dict （optional): 计算选股器需用到的自定义数据组成的字典（dict）,根据计算需求自行指定。
                                                    字典键名为所有载入的选股器的选股器名(admin._all_selectors_name),值为对应选股器所需的数据。
                                                    形如：{"selector_name_1":data_1,"selector_name_2":data_2,...}
        :param all_selectors_data_config_dict (optional):  在all_selectors_data_dict参数为None的情况下(不传入自定义数据),
                                                          可通过该参数调用fxdayu_data api 访问到数据 (dict).
                                                          与 all_selectors_data_dict 二选一（未指定数据通过fxdayu_data api获取）.
                                                          字典键名为所有载入的选股器的选股器名(admin._all_selectors_name),
                                                          值为对应选股器所需的数据api访问参数设置dict(data_config)。
                                                          形如：{"selector_name_1":data_config_1,"selector_name_2":data_config_2,...}
        :param all_selectors_para_dict (optional): 所有选股器外部指定参数集(dict)所构成的字典(dict),可选。为空则不修改选股器原有参数。
                                                   字典键名为所有载入的选股器的选股器名(admin._all_selectors_name),
                                                   值为对应选股器的指定参数集(dict)。
                                                   形如: {"selector_name_1":{"fast":5,"slow":10},"selector_name_2":{"fast":4,"slow":7},...}
        :param parallel: 是否执行并行计算（bool） 默认不执行。 如需并行计算需要在jupyter notebook下启动工作脚本。
        :param update: 是否更新已有记录值(bool)。默认为False——如果admin曾经计算过所有选股器结果,则不再重复计算。 True 则更新计算所加载的选股器结果。
        :return: all_selectors_result : admin下加载的所有选股器的选股器结果(dict)。
                                       字典键名为所有载入的选股器的选股器名(admin._all_selectors_name)
                                       值为 选股器结果(selector_result)　格式为一个MultiIndex Series，索引(index)为date(level 0)和asset(level 1),
                                       包含一列selector结果。
        """

        if self._all_selectors_result and not update:
            return self._all_selectors_result
        else:
            # data参数验证
            if not all_selectors_data_dict:
                all_selectors_data_dict = {}
                for selector_name in self._all_selectors_name:
                    all_selectors_data_dict[selector_name] = None
            if not all_selectors_data_config_dict:
                all_selectors_data_config_dict = {}
                for selector_name in self._all_selectors_name:
                    all_selectors_data_config_dict[selector_name] = {"freq": "D", "api": "candle", "adjust": "after"}
            if not all_selectors_para_dict:
                all_selectors_para_dict = {}
                for selector_name in self._all_selectors_name:
                    all_selectors_para_dict[selector_name] = None
            if not all_Selectors_dict:
                all_Selectors_dict = {}
                for selector_name in self._all_selectors_name:
                    all_Selectors_dict[selector_name] = None

            # 遍历选股结果
            self._all_selectors_result = {}
            if parallel:
                client = Client()
                lview = client.load_balanced_view()
                selectors_result = {}
                for selector_name in self._all_selectors_name:
                    selectors_result[selector_name] = lview.apply_async(
                        self.instantiate_selector_and_get_selector_result,
                        selector_name,
                        pool,
                        start,
                        end,
                        all_Selectors_dict[selector_name],
                        all_selectors_data_dict[selector_name],
                        all_selectors_data_config_dict[selector_name],
                        all_selectors_para_dict[selector_name])
                lview.wait(selectors_result.values())
                self._all_selectors_result = {name: result.get() for name, result in selectors_result.items()}
            else:
                for selector_name in self._all_selectors_name:
                    self._all_selectors_result[selector_name] = _apply(
                        self.instantiate_selector_and_get_selector_result,
                        selector_name,
                        pool,
                        start,
                        end,
                        all_Selectors_dict[selector_name],
                        all_selectors_data_dict[selector_name],
                        all_selectors_data_config_dict[selector_name],
                        all_selectors_para_dict[selector_name])
            return self._all_selectors_result
