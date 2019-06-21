import Factor_analyzer
import Dataview
import pandas as pd
import numpy as np
import re


def factor_returns(factor_data, selected_factor,demeaned=True, group_adjust=False):
    """
    计算按因子值加权的投资组合的收益
    权重为去均值的因子除以其绝对值之和 (实现总杠杆率为1).

    参数
    ----------
    factor_data : pd.DataFrame - MultiIndex
        一个 DataFrame, index 为日期 (level 0) 和资产(level 1) 的 MultiIndex,
        values 包括因子的值, 各期因子远期收益, 因子分位数,
        因子分组(可选), 因子权重(可选)
    demeaned : bool
        因子分析是否基于一个多空组合? 如果是 True, 则计算权重时因子值需要去均值
    group_adjust : bool
        因子分析是否基于一个分组(行业)中性的组合?
        如果是 True, 则计算权重时因子值需要根据分组和日期去均值

    返回值
    -------
    returns : pd.DataFrame
        每期零风险暴露的多空组合收益
    """

    def to_weights(group, is_long_short):
        if is_long_short:
            demeaned_vals = group - group.mean()
            return demeaned_vals / demeaned_vals.abs().sum()
        else:
            return group / group.abs().sum()

    grouper = [factor_data.index.get_level_values('date')]
    if group_adjust:
        grouper.append('group')

    weights = factor_data.groupby(grouper)[selected_factor] \
        .apply(to_weights, demeaned)

    if group_adjust:
        weights = weights.groupby(level='date').apply(to_weights, False)

    weighted_returns = \
        factor_data[get_forward_returns_columns(factor_data.columns)] \
        .multiply(weights, axis=0)

    returns = weighted_returns.groupby(level='date').sum()

    return returns


def get_forward_returns_columns(columns):
    syntax = re.compile("^period_\\d+$")
    return columns[columns.astype('str').str.contains(syntax, regex=True)]