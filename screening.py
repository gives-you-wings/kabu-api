# ライブラリを読み込む
from yahooquery import Ticker
from yahooquery import Screener
import pandas as pd

ticker_num = '8267.T'
ticker_data = Ticker(ticker_num)
print(ticker_data.summary_detail)

company_metrics = [ticker_num]
# 損益計算書を取得する
# summary_detailを用いて1株当りの配当金,配当利回り,過去5年間の配当利回り平均,配当性向,時価総額を取得する
summary_detail_keys = ['dividendRate', 'dividendYield', 'fiveYearAvgDividendYield', 'payoutRatio', 'marketCap']
for summary_detail_key in summary_detail_keys:
    try:
        company_metrics.append(ticker_data.summary_detail[ticker_num][summary_detail_key])
    except Exception as e:
        print('証券コード:{},未取得の属性:{}'.format(ticker_num, summary_detail_key))
        # company_metrics.append(np.nan)
print(company_metrics)
# print(ticker_data.)
# [ticker_num]['BasicEPS']
income = ticker_data.income_statement(trailing=False)
# csv = income.to_csv('income.csv')
print(income[['asOfDate', 'DilutedEPS']])

cf = ticker_data.cash_flow(trailing=False)
print(cf[['asOfDate', 'NetIncome']])

bs = ticker_data.balance_sheet(trailing=False)
print(bs[['asOfDate', 'ShareIssued']])
