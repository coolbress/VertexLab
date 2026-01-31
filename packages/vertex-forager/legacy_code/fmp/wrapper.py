import asyncio
import pandas as pd
from datetime import datetime 

from .fetcher import FMPFetcher

from .utils import generate_year_list, generate_date_ranges

"""
5/12 milestone 
- from, to date 를 반복하면서 가져올 때 인덱스가 중복이 생길 수 밖에 없음.. 왜냐하면 현재 인덱스 범위를 생성할 때 to 가 다음 범위의 from 으로 지정되게 되어 있거든..
- 그리고 페이지를 순회하면서 중복값이 생기는 경우도 있음 
- EOD 같은 경우 FMP 자체 데이터에 인덱스가 중복되는 경우가 있더라고..?
- 이 모든 부분들을 처리할려면 그냥 데이터 가져오고 전처리하는 과정에서 인덱스 중복을 없애주는게 제일 베스트인듯!! (개별로 처리하는 것보다..)
- 하나의 종목에 대해 순회하면서 풀 데이터를 가져와야 하는 경우는 중복값이 생길 수 있는 여지가 있으니깐 이 부분만 중복값을 제거해주기? 
- Wrapper 만들 때 데이터 보고 lv1_column, lv2_column 설정해줘야됨! 
- LegacyV1, Stable API 둘 중에 뭘 써야 하나? 일단 같은 데이터라면 Stable API 로 하는게 좋음!(향후 FMP 에서 LegacyV1 API 를 없애버릴 수도 있으니깐) 
- 하지만 한쪽에 없는 데이터 혹은 같은 데이터여도 더 많은 데이터를 제공하면 해당 API 를 사용하면 됨! 
"""

class FMPWrapper:
    """FMP API 사용자 친화적 래퍼 클래스"""
    def __init__(self, api_key=None):
        self.fetcher = FMPFetcher(api_key)
        
    # ----------------------------------------------------
    # ✅ <Ticker Listings & Meta Data
    # ----------------------------------------------------
    def get_symbols_list(self, exchange_list=['NASDAQ', 'NYSE', 'AMEX'], type_name='stock'):
        """Returns the list of available stock symbols."""
        url = f'https://financialmodelingprep.com/api/v3/stock/list?apikey={self.fetcher.api_key}'
        exclude_columns = ['price', 'exchange']
        data, error = asyncio.run(self.fetcher.router(base_url=url, exclude_columns=exclude_columns))
        if data is not None and not data.empty:
            filtered_df = data[
                (data['exchangeShortName'].isin(exchange_list)) &
                (data['type'] == type_name)
            ]
        else:
            filtered_df = data
        return filtered_df, error
    
    def get_tradable_listings(self, exchange_list=['NASDAQ', 'NYSE', 'AMEX'], type_name='stock'): 
        """Returns tradable listings for the specified exchanges and type."""
        url = f'https://financialmodelingprep.com/api/v3/available-traded/list?apikey={self.fetcher.api_key}'
        exclude_columns = ['price', 'exchange']
        data, error = asyncio.run(self.fetcher.router(base_url=url, exclude_columns=exclude_columns))
        if data is not None and not data.empty:
            filtered_df = data[
                (data['exchangeShortName'].isin(exchange_list)) &
                (data['type'] == type_name)
            ]
        else:
            filtered_df = data
        return filtered_df, error
    
    def get_delisted_listings(self, start_page=0): # stable 
        """Returns delisted company listings, paginated."""
        base_url = f'https://financialmodelingprep.com/stable/delisted-companies?page={{page}}&apikey={self.fetcher.api_key}'
        lv2_column = None 
        return asyncio.run(self.fetcher.router(base_url=base_url, lv2_column=lv2_column, page_start=start_page))
    
    def get_symbols_change_list(self, limit=6000): # stable 
        """Returns the list of symbol changes."""
        base_url = f'https://financialmodelingprep.com/stable/symbol-change?limit={limit}&apikey={self.fetcher.api_key}'
        data, error = asyncio.run(self.fetcher.router(base_url=base_url))
        return data, error
    
    # ----------------------------------------------------
    # ✅ <Company Information> - Stable API 
    # ----------------------------------------------------
    def get_company_profile(self, ticker_list): # stable 
        """Returns company profile information for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/profile?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        exclude_columns = ['price', 'marketCap', 'beta', 'lastDividend', 'ceo', 'exchangeFullName', 'range', 'change', 'changePercentage', 'volume', 'averageVolume', 'website', 'description', 'fullTimeEmployees', 'phone', 'address', 'city', 'state', 'zip', 'image', 'defaultImage',]
        lv1_column = None 
        lv2_column = None 
        return asyncio.run(self.fetcher.router(base_url=base_url, ticker_list=ticker_list, exclude_columns=exclude_columns, lv1_column=lv1_column, lv2_column=lv2_column))
    
    def get_stock_peers(self, ticker_list): # stable
        """Returns stock peer information for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/stock-peers?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'symbol'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
            keep_symbol=True # keep_symbol=True 로 설정하면 데이터 자체에 존재하는 symbol 컬럼을 유지한다는 뜻! 
        )) 
        
    def get_company_notes(self, ticker_list): # stable
        """Returns company notes for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/company-notes?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        exclude_columns = ['exchange']
        lv1_column = 'ticker'
        lv2_column = 'cik'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv1_column=lv1_column,
            lv2_column=lv2_column
        ))
        
    def get_historical_employee_count(self, ticker_list, limit=None): # stable 
        """Returns historical employee counts for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/historical-employee-count?symbol={{ticker}}&limit={limit}&apikey={self.fetcher.api_key}'
        exclude_columns = ["acceptanceTime", "source"]
        lv1_column = 'ticker'
        lv2_column = 'filingDate'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv1_column=lv1_column,
            lv2_column=lv2_column
        ))
        
    def get_historical_market_capital(self, ticker_list, start='1975-01-01', end=None, interval=10): # stable
        """Returns historical market capitalization for the given tickers."""
        ranges = generate_date_ranges(start=start, end=end, interval=interval, mode='year')
        ticker_range_dict = {ticker: ranges for ticker in ticker_list}
        base_url = f'https://financialmodelingprep.com/stable/historical-market-capitalization?symbol={{ticker}}&from={{from}}&to={{to}}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_range_dict=ticker_range_dict,
            lv1_column = lv1_column,
            lv2_column=lv2_column
        ))
        
    def get_current_market_capital(self, ticker_list): # stable
        """Returns current market capitalization for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/market-capitalization?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list
        ))
    
    def get_historical_executive_compensation(self, ticker_list): # stable 
        """Returns historical executive compensation for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/governance-executive-compensation?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        exclude_columns = ['link']
        lv1_column = 'ticker'
        lv2_column = 'filingDate'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv1_column=lv1_column,
            lv2_column=lv2_column
        ))
        
    def get_historical_compensation_benchmark(self): # stable 
        """Returns historical compensation benchmarks for all years."""
        base_url = f'https://financialmodelingprep.com/stable/executive-compensation-benchmark?year={{date}}&apikey={self.fetcher.api_key}'
        lv1_column = 'year'
        date_list = generate_year_list()
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            lv1_column=lv1_column,
            date_list=date_list
        ))
        
        
    def get_executives_info(self, ticker_list, active=None): # stable
        """Returns executive information for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/key-executives?symbol={{ticker}}&active={active}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'title'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column = lv1_column,
            lv2_column=lv2_column
        ))
        
    def get_current_share_float(self, ticker_list): # stable 
        """Returns current share float for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/shares-float?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        exclude_columns = ['source']
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns
        ))
    
    # ----------------------------------------------------
    # ✅ <Financial Statements>
    # ----------------------------------------------------
    def get_historical_financial_statements(self, ticker_list, statement_type='income-statement', period='annual', limit=400): # stable 
        """Returns historical financial statements for the given tickers."""
        base_url = f"https://financialmodelingprep.com/stable/{statement_type}?symbol={{ticker}}&limit={limit}&period={period}&apikey={self.fetcher.api_key}"
        exclude_columns = ["reportedCurrency", "acceptedDate"]
        lv1_column = 'ticker'
        lv2_column = 'filingDate'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
            exclude_columns=exclude_columns
        ))
        
    def get_historical_financial_statements_TTM(self, ticker_list, statement_type='income-statement', limit=400): # stable 
        """Returns historical financial statements for the given tickers."""
        base_url = f"https://financialmodelingprep.com/stable/{statement_type}-ttm?symbol={{ticker}}&limit={limit}&apikey={self.fetcher.api_key}"
        exclude_columns = ["reportedCurrency", "acceptedDate"]
        lv1_column = 'ticker'
        lv2_column = 'filingDate'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
            exclude_columns=exclude_columns
        ))
        

    def get_historical_key_metrics(self, ticker_list, period='annual', limit=400): # stable
        """Returns historical key metrics for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/key-metrics?symbol={{ticker}}&limit={limit}&period={period}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
        
    def get_historical_financial_ratios(self, ticker_list, period='annual', limit=400): # stable 
        """Returns historical financial ratios for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/ratios?symbol={{ticker}}&limit={limit}&period={period}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
    
    def get_current_ttm_key_metrics(self, ticker_list): # stable 
        """Returns trailing twelve months (TTM) key metrics for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/key-metrics-ttm?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
        ))
        
        
    def get_current_ttm_financial_ratios(self, ticker_list): # stable 
        """Returns trailing twelve months (TTM) financial ratios for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/ratios-ttm?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
        ))
        
    def get_current_financial_scores(self, ticker_list): # stable 
        """Returns current financial scores for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/financial-scores?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
        ))
    
    def get_historical_owner_earnings(self, ticker_list): # LegacyV1 
        """Returns historical owner earnings for the given tickers."""
        base_url = f'https://financialmodelingprep.com/api/v4/owner_earnings?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column   
        ))
        
    def get_historical_enterprise_values(self, ticker_list, period='annual', limit=400): # stable 
        """Returns historical enterprise values for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/enterprise-values?symbol={{ticker}}&limit={limit}&period={period}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))

    def get_historical_financial_growth(self, ticker_list, statement_type='income-statement', period='annual', limit=400): # stable 
        """
        Returns historical growth metrics for the given tickers.
        statement_type: 'income-statement' / 'balance-sheet-statement'/ 'cash-flow-statement'/ 'financial'
        """
        base_url = f'https://financialmodelingprep.com/stable/{statement_type}-growth?symbol={{ticker}}&limit={limit}&period={period}&apikey={self.fetcher.api_key}'
        exclude_columns = ['reportedCurrency']
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv1_column=lv1_column,
            lv2_column=lv2_column
        ))
        
    def get_historical_revenue_product_segmentation(self, ticker_list, period='annual'): # stable (Only Stable API)
        """Returns historical revenue product segmentation for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/revenue-product-segmentation?symbol={{ticker}}&period={period}&apikey={self.fetcher.api_key}'
        excldue_columns = ['reportedCurrency']
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=excldue_columns,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
        
    def get_historical_revenue_geographic_segmentation(self, ticker_list, period='annual'): # stable (Only Stable API)
        """Returns historical revenue geographic segmentation for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/revenue-geographic-segmentation?symbol={{ticker}}&period={period}&apikey={self.fetcher.api_key}'
        excldue_columns = ['reportedCurrency']
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=excldue_columns,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
        
    # ----------------------------------------------------
    # ✅ <Analyst Related Data>
    # ----------------------------------------------------
    def get_historical_stock_grade(self, ticker_list): # stable 
        """Returns historical grades for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/grades?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
        
    def get_historical_analyst_financial_estimates(self, ticker_list, period='annual', limit=500): # stable
        """Returns historical analyst financial estimates for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/analyst-estimates?symbol={{ticker}}&period={period}&limit={limit}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
        
    def get_historical_analyst_recommendation(self, ticker_list, limit=None): # stable (LegacyV1, Stable 둘다 2018부터밖에 데이터가 존재하지 않음)
        """Returns historical analyst recommendations for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/grades-historical?symbol={{ticker}}&limit={limit}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker' 
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
        
    def get_historical_analyst_ratings(self, ticker_list, limit=20000): # stable  
        """Returns historical analyst ratings for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/ratings-historical?symbol={{ticker}}&limit={limit}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker' 
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
        
    def get_current_price_target_summary(self, ticker_list): # stable 
        """Returns current price target summary for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/price-target-summary?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
        ))
        
    def get_current_price_target_consensus(self, ticker_list): # stable 
        """Returns current price target consensus for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/price-target-consensus?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
        ))
        
    def get_current_stock_grades_summary(self, ticker_list): # stable
        """Returns current stock grades summary for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/grades-consensus?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
        ))
        
    # ----------------------------------------------------
    # ✅ <ESG>
    # ----------------------------------------------------
    def get_historical_esg_score(self, ticker_list): # stable API
        """Returns historical ESG scores for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/esg-disclosures?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        exclude_columns = ['url']
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
    
    def get_historical_esg_rating(self, ticker_list): # Stable API
        """Returns historical ESG ratings for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/esg-ratings?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'year'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
        
    def get_historical_esg_benchmark(self):
        """Returns historical ESG Score benchmarks for all years."""
        base_url = f'https://financialmodelingprep.com/stable/esg-benchmark?year={{date}}&apikey={self.fetcher.api_key}'
        lv1_column = 'fiscalYear'
        lv2_column = 'sector'
        date_list = generate_year_list()
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
            date_list=date_list
        ))
        
    # ----------------------------------------------------
    # ✅ <Calendar> - Dividends, Splits, Earnings 
    # ----------------------------------------------------
    def get_historical_dividend(self, ticker_list, limit=500): # Stable API
        """Returns historical dividend information for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/dividends?symbol={{ticker}}&limit={limit}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,  
        ))
        
    def get_historical_actual_and_estimated_earnings(self, ticker_list, limit=500): # Stable API
        """Returns historical actual and estimated earnings for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/earnings?symbol={{ticker}}&limit={limit}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,  
        ))
        
            
    def get_historical_split(self, ticker_list, limit=500): # Stable API 
        """Returns historical stock split information for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/splits?symbol={{ticker}}&limit={limit}&apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))

    def get_historical_earning_surprises(self, ticker_list): # Legacy Only
        """Returns historical earning surprises for the given tickers."""
        base_url = f'https://financialmodelingprep.com/api/v3/earnings-surprises/{{ticker}}?apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
        
    # ----------------------------------------------------
    # ✅ <Charts>
    # ----------------------------------------------------
    def get_historical_EOD_price(self, ticker_list, to_date=datetime.today().strftime('%Y-%m-%d')): # LegacyV1
        """Returns historical end-of-day prices for the given tickers."""
        base_url = f'https://financialmodelingprep.com/api/v3/historical-price-full/{{ticker}}?from=1970-01-01&to={to_date}&apikey={self.fetcher.api_key}'
        exclude_columns = ['unadjustedVolume', 'change', 'changePercent', 'vwap', 'label', 'changeOverTime']
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
        
    def get_historical_1m_price(self, ticker_list, start, end): # LegacyV1  
        """Returns historical 1-minute prices for the given tickers."""
        # Validate range is not longer than 4 days
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)
        if (end_dt - start_dt).days > 4:
            raise ValueError("1min OHLCV API only supports up to 4 days range. Please reduce your date range.")
        ranges = generate_date_ranges(start=start, end=end, interval=1, mode='day')
        ticker_range_dict = {ticker: ranges for ticker in ticker_list}
        base_url = f'https://financialmodelingprep.com/api/v3/historical-chart/1min/{{ticker}}?from={{from}}&to={{to}}&apikey={self.fetcher.api_key}'
        return asyncio.run(self.fetcher.router(
            base_url=base_url, 
            ticker_range_dict=ticker_range_dict, 
        ))
        
    # ----------------------------------------------------
    # ✅ <ETF & Mutual Fund Holdings>
    # ----------------------------------------------------
    def get_etf_holder(self, ticker_list):
        """Returns ETF holder information for the given tickers."""
        base_url = f'https://financialmodelingprep.com/api/v3/etf-holder/{{ticker}}?apikey={self.fetcher.api_key}'
        exclude_columns = []
        lv2_column = 'updated'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv2_column=lv2_column
        ))

    def get_etf_meta_data(self, ticker_list):
        """Returns ETF meta data for the given tickers."""
        base_url = f'https://financialmodelingprep.com/api/v4/etf-info?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        exclude_columns = ['description', 'website']
        lv2_column = None
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv2_column=lv2_column
        ))

    def get_etf_sector_weighting(self, ticker_list):
        """Returns ETF sector weighting for the given tickers."""
        base_url = f'https://financialmodelingprep.com/api/v3/etf-sector-weightings/{{ticker}}?apikey={self.fetcher.api_key}'
        lv2_column = 'sector'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv2_column=lv2_column
        ))

    def get_etf_country_weighting(self, ticker_list):
        """Returns ETF country weighting for the given tickers."""
        base_url = f'https://financialmodelingprep.com/api/v3/etf-country-weightings/{{ticker}}?apikey={self.fetcher.api_key}'
        lv2_column = 'country'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv2_column=lv2_column
        ))

    def get_etf_stock_exposure(self, ticker_list):
        """Returns ETF stock exposure for the given tickers."""
        base_url = f'https://financialmodelingprep.com/api/v3/etf-stock-exposure/{{ticker}}?apikey={self.fetcher.api_key}'
        exclude_columns = ['assetExposure']
        lv2_column = 'etfSymbol'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv2_column=lv2_column
        ))
        
    def get_mutual_fund_holder(self, ticker_list):
        """Returns mutual fund holder information for the given tickers."""
        base_url = f'https://financialmodelingprep.com/api/v3/mutual-fund-holder/{{ticker}}?apikey={self.fetcher.api_key}'
        lv2_column = 'holder'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv2_column=lv2_column
        ))
    
    # ----------------------------------------------------
    # ✅ <13F> 
    # -----------------------------------------------------
    def get_historical_13F_asset_allocation(self): # Legacy API
        """Returns historical 13F asset allocation data."""
        # get 13F asset allocation date list 
        date_url = f'https://financialmodelingprep.com/api/v4/13f-asset-allocation-date?apikey={self.fetcher.api_key}'
        raw_date_list, _ = asyncio.run(self.fetcher.router(base_url=date_url))
        cleaned_date_list = raw_date_list['date'].dropna().tolist()

        # get 13F asset allocation data
        base_url = f'https://financialmodelingprep.com/api/v4/13f-asset-allocation?date={{date}}&apikey={self.fetcher.api_key}'
        lv1_column = 'date'
        lv2_column = 'industryTitle'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            date_list=cleaned_date_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
    
    def get_institutional_holders_list(self): # Legacy API 
        base_url = f'https://financialmodelingprep.com/api/v4/institutional-ownership/list?apikey={self.fetcher.api_key}'
        lv1_column = 'cik'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            lv1_column=lv1_column,
        ))
        
    def get_13F_positions_summary_by_ticker(self, ticker_list): # Stable API 
        base_url = f'https://financialmodelingprep.com/stable/institutional-ownership/symbol-positions-summary?symbol={{ticker}}&year={{date}}&quarter={{page}}&apikey={self.fetcher.api_key}'
        date_list = generate_year_list(start=1977)
        page_start = 1 
        lv1_column = 'ticker'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            date_list=date_list,
            page_start=page_start,
            lv1_column=lv1_column,
            lv2_column=lv2_column, 
        ))
        
    def get_stock_ownership_by_holders(self, ticker_list):
        pass 
    
    def get_institutional_portfolio_holdings_summary(self, cik_list): # Legacy API 
        """Returns institutional portfolio holdings summary for the given CIKs."""
        base_url = f'https://financialmodelingprep.com/api/v4/institutional-ownership/portfolio-holdings-summary?cik={{ticker}}&apikey={self.fetcher.api_key}'
        lv1_column = 'cik'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=cik_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
    
    def get_institutional_industry_ownership(self, cik_list): # Legacy API 
        """Returns institutional industry ownership for the given CIKs."""
        base_url = f'https://financialmodelingprep.com/stable/institutional-ownership/holder-industry-breakdown?cik={{ticker}}&year={{date}}&quarter={{page}}&apikey={self.fetcher.api_key}'
        date_list = generate_year_list(start=1977)
        page_start = 1
        lv1_column = 'cik'
        lv2_column = 'date'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=cik_list,
            date_list=date_list,
            page_start=page_start,
            lv1_column=lv1_column,
            lv2_column=lv2_column, 
        ))

        
    def get_13F_extracts_by_holders(self, cik_list): # Stable API
        base_url = f'https://financialmodelingprep.com/stable/institutional-ownership/extract?cik={{ticker}}&year={{date}}&quarter={{page}}&apikey={self.fetcher.api_key}'
        date_list = generate_year_list(start=1977)
        page_start = 1
        lv1_column = 'ticker'
        lv2_column = 'filingDate'
        exclude_columns = ['link', 'finalLink']
        keep_symbol = True 
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=cik_list,
            exclude_columns=exclude_columns,
            date_list=date_list,
            page_start=page_start,
            lv1_column=lv1_column,
            lv2_column=lv2_column, 
            keep_symbol=keep_symbol
        ))
    
    def get_historical_13F_shares_held(self, ticker_list):
        base_url = f'https://financialmodelingprep.com/api/v3/institutional-holder/{{ticker}}?apikey={self.fetcher.api_key}'
        lv1_column = 'ticker'
        lv2_column = 'dateReported'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
        
    # ----------------------------------------------------
    # ✅ <Insider Trading>
    # ----------------------------------------------------
    def get_historical_insider_by_symbol(self, ticker_list):
        """Returns historical insider trading data for the given tickers."""
        base_url = f'https://financialmodelingprep.com/api/v4/insider-roaster?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        exclude_columns = []
        lv2_column = 'transactionDate'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv2_column=lv2_column
        ))

    def get_historical_insider_trade_statistics(self, ticker_list):
        """Returns historical insider trade statistics for the given tickers."""
        base_url = 'https://financialmodelingprep.com/api/v4/insider-roaster-statistic?symbol={ticker}&apikey=dccfb74eef726f2021b85ab81565231c'
        exclude_columns = []
        lv2_column = 'year'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv2_column=lv2_column
        ))

    def get_historical_beneficial_ownership(self, ticker_list):
        """Returns historical beneficial ownership data for the given tickers."""
        base_url = f'https://financialmodelingprep.com/api/v4/insider/ownership/acquisition_of_beneficial_ownership?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        exclude_columns = ['acceptedDate', 'citizenshipOrPlaceOfOrganization', 'url']
        lv2_column = 'filingDate'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv2_column=lv2_column
        ))

    def get_historical_fail_to_deliver(self, ticker_list, page_start=0):
        """Returns historical fail-to-deliver data for the given tickers."""
        base_url = f'https://financialmodelingprep.com/api/v4/fail_to_deliver?symbol={{ticker}}&page={{page}}&apikey={self.fetcher.api_key}'
        exclude_columns = ['Name']
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            page_start=page_start
        ))
        
    # ----------------------------------------------------
    # ✅ <Senate & House trading>
    # ----------------------------------------------------
    def get_historical_senate_trading(self, ticker_list): # Stable API 
        """Returns senate trading data for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/senate-trades?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        exclude_columns = ['comment', 'link']
        lv1_column = 'ticker'
        lv2_column = 'disclosureDate'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))

    def get_historical_house_trading(self, ticker_list): # Stable API 
        """Returns house disclosure data for the given tickers."""
        base_url = f'https://financialmodelingprep.com/stable/house-trades?symbol={{ticker}}&apikey={self.fetcher.api_key}'
        exclude_columns = ['comment', 'link']
        lv1_column = 'ticker'
        lv2_column = 'disclosureDate'
        return asyncio.run(self.fetcher.router(
            base_url=base_url,
            ticker_list=ticker_list,
            exclude_columns=exclude_columns,
            lv1_column=lv1_column,
            lv2_column=lv2_column,
        ))
        
    # ----------------------------------------------------
    # ✅ <Index Constituents>
    # ----------------------------------------------------
    def get_current_constituents(self, index_name='sp500'):
        """Returns current constituents for the specified index."""
        url = f'https://financialmodelingprep.com/api/v3/{index_name}_constituent?apikey={self.fetcher.api_key}'
        exclude_columns = ['headQuarter']
        data, error = asyncio.run(self.fetcher.router(base_url=url, exclude_columns=exclude_columns))
        return data, error

    def get_historical_constituents(self, index_name='sp500'):
        """Returns historical constituents for the specified index."""
        url = f'https://financialmodelingprep.com/api/v3/historical/{index_name}_constituent?apikey={self.fetcher.api_key}'
        data, error = asyncio.run(self.fetcher.router(base_url=url))
        return data, error
    
if __name__ == '__main__':
    test = FMPWrapper()
    ticker_list = ['MSFT', 'O']
    data, error = test.get_company_profile(ticker_list=ticker_list)
    print(data)
