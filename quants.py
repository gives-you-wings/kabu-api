import json
from dotenv import load_dotenv
import requests
import db_connection
import my_logger
import re
import auth_quants
from concurrent.futures import ThreadPoolExecutor

load_dotenv()
logger = my_logger.MyLogger(__name__).logger
connection = db_connection.DatabaseConnection()


def fetch_stock_info():
    select_sql = '''select m.stock_code, m.company_name, s.per
                    from trade.stock.tbl_m_stock_info m
                    left join trade.stock.tbl_t_statement s on m.stock_code = s.stock_code
                    where s.per is null and m.sector_17_code <> '99'
                    and m.stock_code <> '49870'
                    and m.stock_code not like '%A%'
                    and m.stock_code not like '%5'
                    order by m.stock_code '''
    # select_sql = '''select m.stock_code
    #                 , m.company_name
    #                 , dp.adjustment_close
    #                 , s.forecast_dividend_per_share_annual
    #                 , s.dividend_yield
    #                 , s.per
    #                 , s.pbr
    #                 , s.roe
    #                 from stock.tbl_t_statement s
    #                 inner join stock.tbl_m_stock_info m on s.stock_code = m.stock_code
    #                 inner join stock.tbl_t_daily_price dp on s.stock_code = dp.stock_code
    #                 and dp."date" =  current_date -1
    #                 where s.forecast_dividend_per_share_annual is not null
    #                 and s.dividend_yield is not null
    #                 and s.dividend_yield >4
    #                 order by s.dividend_yield desc'''

    select_sql = '''select stock_code from stock.tbl_m_stock_info where stock_code = '99760' order by stock_code'''
    rows = connection.select_all(select_sql)

    logger.info(rows)
    with ThreadPoolExecutor(max_workers=16, thread_name_prefix="thread") as executor:
        for row in rows:
            executor.submit(update_statement, row['stock_code'])

    logger.info('fetch stock info done')


def update_statement(target_code):
    stock_info = call_quants_api(target_code)
    upsert_sql = '''INSERT INTO stock.tbl_t_statement(stock_code, net_sales, operating_profit, ordinary_profit,
         profit, earnings_per_share, total_assets, equity, equity_to_asset_ratio, book_value_per_share, result_payout_ratio_annual, 
         forecast_dividend_per_share_annual, forecast_payout_ratio_annual, forecast_earnings_per_share, issued_stock,
          per, pbr, dividend_yield, roe, roa, created_at)  VALUES (%(stock_code)s, %(net_sales)s, 
          %(operating_profit)s, %(ordinary_profit)s, %(profit)s, %(earnings_per_share)s, %(total_assets)s, %(equity)s, 
          %(equity_to_asset_ratio)s, %(book_value_per_share)s, %(result_payout_ratio_annual)s, 
          %(forecast_dividend_per_share_annual)s, %(forecast_payout_ratio_annual)s, %(forecast_earnings_per_share)s, 
          %(issued_stock)s, %(per)s, %(pbr)s, %(dividend_yield)s, %(roe)s, %(roa)s, now())
        on conflict(stock_code) do update set net_sales = %(net_sales)s, operating_profit = %(operating_profit)s, 
        ordinary_profit = %(ordinary_profit)s, profit = %(profit)s, earnings_per_share = %(earnings_per_share)s,
        total_assets = %(total_assets)s, equity = %(equity)s, equity_to_asset_ratio = %(equity_to_asset_ratio)s, 
        book_value_per_share = %(book_value_per_share)s, result_payout_ratio_annual = %(result_payout_ratio_annual)s, 
         forecast_dividend_per_share_annual = %(forecast_dividend_per_share_annual)s,
            forecast_payout_ratio_annual = %(forecast_payout_ratio_annual)s, forecast_earnings_per_share = 
            %(forecast_earnings_per_share)s, issued_stock = %(issued_stock)s, per = %(per)s, pbr = %(pbr)s, 
            dividend_yield = %(dividend_yield)s, roe = %(roe)s, roa = %(roa)s, updated_at = now()'''
    connection.execute(upsert_sql, stock_info)
    logger.info('upsert stock info done.')


def call_quants_api(stock_code):
    logger.info(stock_code)
    target_code = stock_code

    id_token_sql = '''select token from feed.tbl_t_token where env_type = 3'''
    id_token = connection.select_one(id_token_sql)

    api_url = 'https://api.jquants.com/v1/fins/statements'
    headers = {
        'Authorization': 'Bearer ' + id_token
    }

    listed_info_url = 'https://api.jquants.com/v1/listed/info'
    response = requests.get(listed_info_url, headers=headers, params={'code': target_code})
    status = response.status_code
    logger.info('status: %s', status)
    if status == 401 or status == 403:
        logger.info('refresh token')
        auth_quants.fetch_id_token()

    json_data = response.json()
    logger.info(json_data)
    if not json_data['info']:
        logger.info('no info')
        return
    listed_info = json_data['info'][0]
    # json log
    # logger.info(json.dumps(listed_info, indent=2, ensure_ascii=False))

    company_name = listed_info['CompanyName']

    # 現在の株価を取得
    daily_quotes_url = 'https://api.jquants.com/v1/prices/daily_quotes'
    response = requests.get(daily_quotes_url, headers=headers, params={'code': target_code})
    json_data = response.json()
    daily_quote = json_data['daily_quotes'][-1]
    # json log
    # logger.info(json.dumps(daily_quote, indent=2, ensure_ascii=False))
    close = daily_quote['AdjustmentClose']

    response = requests.get(api_url, headers=headers, params={'code': target_code})
    json_data = response.json()
    statements = json_data['statements']
    logger.info(json.dumps(statements, indent=2, ensure_ascii=False))

    eps = None
    f_eps = None
    fdpsa = None
    all_stock = None
    bps = None
    rpra = None
    nyfpra = None
    total_assets = None
    equity = None
    net_sales = None
    operating_profit = None
    ordinary_profit = None
    profit = None
    equity_to_asset_ratio = None

    for statement in statements:
        f_eps_4_log = statement['ForecastEarningsPerShare']
        # logger.info('f_eps_4_log: %s, date: %s, financialType: %s', f_eps_4_log, statement['DisclosedDate'],
        #             statement['TypeOfDocument'])
        ordinary_profit_4_log = statement['OrdinaryProfit']
        # logger.info('ordinary_profit_4_log: %s, date: %s, financialType: %s', ordinary_profit_4_log,
        #             statement['DisclosedDate'], statement['TypeOfDocument'])
        # 配当
        fdpsa_4_log = statement['ForecastDividendPerShareAnnual']
        logger.info('fdpsa_4_log: %s, date: %s, financialType: %s', fdpsa_4_log, statement['DisclosedDate'],
                    statement['TypeOfDocument'])
        # 配当性向
        # rpra_4_log = statement['ResultPayoutRatioAnnual']
        # logger.info('rpra_4_log: %s, date: %s, financialType: %s', rpra_4_log, statement['DisclosedDate'],
        #             statement['TypeOfDocument'])
        # nyfpra_4_log = statement['NextYearForecastPayoutRatioAnnual']
        # logger.info('nyfpra_4_log: %s, date: %s, financialType: %s', nyfpra_4_log, statement['DisclosedDate'],
        #             statement['TypeOfDocument'])
        # 配当予想は3Qに決まる？もしくは配当予想の修正
        if ('3QFinancialStatements' in statement['TypeOfDocument'] or
                'DividendForecastRevision' in statement['TypeOfDocument']):
            fdpsa = statement['ForecastDividendPerShareAnnual']
            # logger.info('fdpsa: %s, date: %s, financialType: %s', fdpsa, statement['DisclosedDate'],
            #             statement['TypeOfDocument'])
        # 予想配当はFYに決まる
        if ('FYFinancialStatements' in statement['TypeOfDocument'] and
                'NextYearForecastDividendPerShareAnnual' in statement):
            fdpsa = statement['NextYearForecastDividendPerShareAnnual']
        if (statement['TypeOfCurrentPeriod'] == 'FY'
                and 'FYFinancialStatements' in statement['TypeOfDocument']):
            bps = statement['BookValuePerShare'] if 'OrdinaryProfit' in statement else None
            rpra = statement['ResultPayoutRatioAnnual']
            nyfpra = statement['NextYearForecastPayoutRatioAnnual']
        #
        if ('FinancialStatements' in statement['TypeOfDocument'] or
                'EarningsPerShare' in statement['TypeOfDocument']):
            equity = statement['Equity']
            total_assets = statement['TotalAssets']
            if statement['EarningsPerShare'] != '':
                eps = statement['EarningsPerShare']
            if statement['ForecastEarningsPerShare'] != '':
                f_eps = statement['ForecastEarningsPerShare']
            # if statement['ForecastDividendPerShareAnnual'] != '':

            if statement['NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock'] != '':
                all_stock = statement['NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock']
            if statement['NetSales'] != '':
                net_sales = statement['NetSales']
            if statement['OperatingProfit'] != '':
                operating_profit = statement['OperatingProfit']
            if statement['OrdinaryProfit'] != '':
                ordinary_profit = statement['OrdinaryProfit']
            if statement['Profit'] != '':
                profit = statement['Profit']
            if statement['EquityToAssetRatio'] != '':
                equity_to_asset_ratio = statement['EquityToAssetRatio']

    # json log
    # logger.info(json.dumps(last_statement, indent=2, ensure_ascii=False))

    #
    # logger.info('銘柄コード: %s', target_code)
    # logger.info('会社名: %s', company_name)
    # logger.info('1株当たり四半期純利益: %s', eps)
    # logger.info('1株当たり当期純利益(EPS): %s', f_eps)
    # logger.info('1株当たり純資産(BPS): %s', bps)
    # logger.info('1株当たり配当予想: %s', fdpsa)
    # logger.info('純資産: %s', equity)
    # roe = float(f_eps) / float(bps) if f_eps != '' and bps != '' else 0
    # formatted_roe = "{:.2%}".format(roe)
    # logger.info('ROE: %s', formatted_roe)

    # formatted_all_stock = "{:,}".format(int(all_stock)) if all_stock != '' else ''

    # formatted_rpra = "{:.2%}".format(float(rpra)) if rpra != '' else ''
    # logger.info('配当性向(実績): %s', formatted_rpra)

    # formatted_nyfpra = "{:.2%}".format(float(nyfpra)) if nyfpra != '' else ''
    # logger.info('配当性向(予想): %s', formatted_nyfpra)
    #
    # logger.info('発行済株式数: %s', all_stock)
    # logger.info('株価: %s', close)
    # logger.info('PER: %s', round(close / float(f_eps), 2)) if f_eps != '' else ''
    # logger.info('PBR: %s', round(close / float(bps), 2)) if bps != '' else ''
    # logger.info('配当利回り: %s', round(float(fdpsa) / close * 100, 2)) if fdpsa != '' else ''

    # detail_info = {
    #     '銘柄コード': target_code,
    #     '会社名': company_name,
    #     '1株当たり四半期純利益': eps,
    #     '1株当たり当期純利益(EPS)': f_eps,
    #     '1株当たり純資産(BPS)': bps,
    #     '自己資本利益率(ROE)': formatted_roe,
    #     '1株当たり配当予想': fdpsa if fdpsa else '',
    #     '配当性向(実績)': formatted_rpra,
    #     '配当性向(予想)': formatted_nyfpra,
    #     '発行済株式数': formatted_all_stock,
    #     '株価': close,
    #     'PER': round(close / float(f_eps), 2) if f_eps and close else '',
    #     'PBR': round(close / float(bps), 2) if bps and close else '',
    #     '配当利回り': round(float(fdpsa) / close * 100, 2) if fdpsa and close else 0
    # }
    # logger.info('net_sales: %s', net_sales)
    # f_epsがなければepsを代用する
    if f_eps is None and eps is not None:
        f_eps = eps
    logger.debug('f_eps: %s', f_eps)
    if float(f_eps) != 0:
        logger.info('close: %s', close)
        per = round(close / float(f_eps), 2) if close else None
    else:
        per = 0
    logger.debug('per: %s', per)
    detail_info = {
        'stock_code': target_code,
        'net_sales': net_sales,  # net_salesの値を適切に設定する必要があります
        'operating_profit': operating_profit,
        'ordinary_profit': ordinary_profit,
        'profit': profit,  # profitの値を適切に設定する必要があります
        'earnings_per_share': eps,
        'total_assets': total_assets,  # total_assetsの値を適切に設定する必要があります
        'equity': equity,
        'equity_to_asset_ratio': equity_to_asset_ratio,  # equity_to_asset_ratioの値を適切に設定する必要があります
        'book_value_per_share': bps if bps != '' else None,
        'result_payout_ratio_annual': rpra if rpra != '' else None,
        'forecast_dividend_per_share_annual': fdpsa if fdpsa != '' else None,
        'forecast_payout_ratio_annual': nyfpra if nyfpra != '' else None,
        'forecast_earnings_per_share': f_eps,
        'issued_stock': all_stock,
        'per': per,
        'pbr': round(close / float(bps), 2) if bps is not None and bps != 0 and bps != '' and close else None,
        'dividend_yield': round(float(fdpsa) / close * 100, 2) if fdpsa and close else None,
        'roe': round(float(f_eps) / float(bps) * 100, 2) if f_eps and bps else None,
        'roa': round(float(profit) / float(total_assets) * 100, 2) if profit and total_assets else None
    }
    logger.info(json.dumps(detail_info, indent=2, ensure_ascii=False))
    # stock_info = json.dumps(detail_info, ensure_ascii=False)
    return detail_info


fetch_stock_info()
