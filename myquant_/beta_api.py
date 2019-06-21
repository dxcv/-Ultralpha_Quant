



import jqdatasdk
from Dataapi import *
from Dataview import *
import time
import datetime
import pandas as pd
from jqdatasdk import get_factor_values
from Factor_analyzer import *
import Performance

def DV_init():
    jqdatasdk.auth('15026415693', 'quiet2520')
    dataapi_beta = DataApi()
    asset = dataapi_beta.get_index_stocks('000300.XSHG')[0:100]
    analyzer = DataView(
        name='loader_test',
        jq_api=dataapi_beta,
        start_date='2015-04-01',
        end_date='2016-04-01',
        universe=asset,
        fields=['open', 'close'],
        factors=['beta', 'book_to_price_ratio', 'gross_profit_ttm']
    )

    analyzer._data_preprocessing()
    analyzer.save_dataview('save')
    return analyzer,analyzer.load_factors,analyzer.pre_data_

def FA_init(dataview):
    factor_analyzer = FactorAnalyzer(dataview)
    return factor_analyzer

def test_FA():

    ax,_,_=DV_init()
    FA_init(ax)

def pct_change_test():
    x = pd.DataFrame({
        'a': [1, 2, 6, 24, 120],
        'b': [120, 24, 6, 2, 1]
    })

    print(x.pct_change(1).shift(-1))

def test_load(panel):
    for k in panel.test_data_list:
        print(k)
        print('================')
        print( panel.__dict__[k], '\n')



if __name__ == "__main__":

    # jqdatasdk.auth('15026415693', 'quiet2520')
    # dataapi_beta = DataApi()

    ax,factor,data = DV_init()


    x = list(data.index.get_level_values('date'))
    y = list(factor.index.get_level_values('date'))
# y = [ type(datetime_) for datetime_ in x]
    print(min(x)-datetime.timedelta(days = 3))

    fx = FA_init(ax)
    # print(fx._cleaned_factor_data.columns)
    # print(fx._cleaned_factor_data.index)
    # print(fx._cleaned_factor_data['period_5'])
    Protfilio1 = Performance.factor_returns(fx.cleaned_factor_data,'beta')
    Protfilio2 = Performance.factor_returns(fx.cleaned_factor_data,'beta',False)
    booksize = 20000000

    print((booksize*Protfilio1['period_5']))

    # print((booksize*Protfilio2['period_5']).sum())


# print(factor.loc[data.index[0:5]])
# factor_copy = factor
# index_group = data.index[0:5]

# groupby = groupby.stack().loc[factor_copy.index]
# diff2 =  set(index_group)-set(factor_copy.index)
#
# diff3 = index_group.difference(factor_copy.index)

# if len(diff2) > 0:
#     print("Assets {} not in group mapping".format(
#         list(diff2)))
# factor_a = FactorAnalyzer(ax)

# print(data['close'])
# print(data['close'].shift(1))
# print(max(y))




# panel = DataView(jq_api=dataapi_beta,name='copycat')
# panel.load_dataview('save',name='loader_test')
# test_load(panel)

# print(analyzer.load_factors)
# print(analyzer.pre_data_)
# x = analyzer.load_factors

# # print(y.index)
# print(y.columns)


# analyzer.load_dataview('save')

# Func_test()


#
# x = jqdatasdk.get_factor_values(
#     securities=jqdatasdk.get_index_stocks('000300.XSHG'),
#     factors=['VOL5'],
#     start_date='2018-01-01',
#     end_date='2018-12-31')
#
# print(x)
# Func_test()

# print(analyzer.pre_data_['close'])

# jqdatasdk.get_price(security='000001.XSHE',start_date='2018-04-01',end_date='2015-05-01')