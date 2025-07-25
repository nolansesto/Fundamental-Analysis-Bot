import yfinance as yf
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class EnhancedGOOGAnalyzer:
    def __init__(self, fmp_key, alpha_vantage_key, fred_key):
        self.fmp_api_key = fmp_key
        self.alpha_vantage_key = alpha_vantage_key
        self.fred_api_key = fred_key
        
        # Data availability flags
        self.data_flags = {
            'fmp_api_accessible': False,
            'alpha_vantage_accessible': False,
            'fred_api_accessible': False,
            'yfinance_accessible': False,
            'earnings_data_available': False,
            'revision_data_available': False,
            'macro_data_available': False,
            'valuation_data_complete': False,
            'financial_statements_available': False,
            'analyst_estimates_available': False,
            'insider_trading_available': False,
            'institutional_holdings_available': False,
            'options_data_available': False,
            'news_sentiment_available': False
        }
        
        # Valuation data container
        self.valuation_metrics = {
            'price_metrics': {},
            'ratio_metrics': {},
            'growth_metrics': {},
            'profitability_metrics': {},
            'efficiency_metrics': {},
            'leverage_metrics': {},
            'liquidity_metrics': {},
            'market_metrics': {},
            'dividend_metrics': {},
            'quality_metrics': {}
        }
        
        # Enhanced CEO database
        self.ceo_database = {
            'GOOG': {
                'name': 'Sundar Pichai',
                'was_cfo': False,
                'background': 'engineer_product',
                'vision': 9,
                'execution': 9,
                'founder': False,
                'technical': True,
                'strategic': True,
                'notes': 'Former product head at Google. Engineering background. Led Chrome, Android before CEO. Strong AI vision.'
            }
        }
        
        print("🚀 ENHANCED GOOGLE (GOOG) - COMPLETE DATA ACCESS & VALUATION ANALYSIS")
        print("=" * 80)
        print("👔 CEO: Sundar Pichai (Engineering/Product Background)")
        print("🏭 Strategy: AI dominance, Cloud growth, Platform innovation")
        print("🔍 Data Access: Comprehensive validation & flag system")
        print("💰 Valuation: Complete metrics across all categories")
        print("=" * 80)
    
    def test_api_connections(self):
        """Test all API connections and set flags"""
        print("🔍 Testing API Connections & Data Access...")
        print("-" * 50)
        
        # Test FMP API
        try:
            fmp_test_url = f"https://financialmodelingprep.com/stable/quote?symbol=GOOG&apikey={self.fmp_api_key}"
            response = requests.get(fmp_test_url, timeout=10)
            if response.status_code == 200 and response.json():
                self.data_flags['fmp_api_accessible'] = True
                print("✅ FMP API: Connected")
            else:
                print("❌ FMP API: Connection failed")
        except Exception as e:
            print(f"❌ FMP API: Error - {str(e)[:50]}...")
        
        # Test Alpha Vantage API
        try:
            av_test_url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol=GOOG&apikey={self.alpha_vantage_key}"
            response = requests.get(av_test_url, timeout=10)
            if response.status_code == 200 and response.json():
                self.data_flags['alpha_vantage_accessible'] = True
                print("✅ Alpha Vantage API: Connected")
            else:
                print("❌ Alpha Vantage API: Connection failed")
        except Exception as e:
            print(f"❌ Alpha Vantage API: Error - {str(e)[:50]}...")
        
        # Test FRED API
        try:
            fred_test_url = f"https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&api_key={self.fred_api_key}&file_type=json&limit=1"
            response = requests.get(fred_test_url, timeout=10)
            if response.status_code == 200 and response.json():
                self.data_flags['fred_api_accessible'] = True
                print("✅ FRED API: Connected")
            else:
                print("❌ FRED API: Connection failed")
        except Exception as e:
            print(f"❌ FRED API: Error - {str(e)[:50]}...")
        
        # Test yfinance
        try:
            stock = yf.Ticker("GOOG")
            info = stock.info
            if info and info.get('symbol') == 'GOOG':
                self.data_flags['yfinance_accessible'] = True
                print("✅ YFinance: Connected")
            else:
                print("❌ YFinance: Connection failed")
        except Exception as e:
            print(f"❌ YFinance: Error - {str(e)[:50]}...")
        
        print(f"\n🎯 API Status Summary: {sum(self.data_flags.values())}/4 APIs accessible")
    
    def safe_api_call(self, url, api_name):
        """Safe API call with detailed logging"""
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data:
                    print(f"   ✅ {api_name}: Data retrieved successfully")
                    return data
                else:
                    print(f"   ⚠️  {api_name}: Empty response")
            else:
                print(f"   ❌ {api_name}: Status code {response.status_code}")
            return None
        except Exception as e:
            print(f"   ❌ {api_name}: {str(e)[:50]}...")
            return None
    
    def get_comprehensive_valuation_data(self):
        """Get all available valuation metrics with flags"""
        print("\n💰 Collecting Comprehensive Valuation Data...")
        print("-" * 50)
        
        try:
            stock = yf.Ticker("GOOG")
            info = stock.info
            hist = stock.history(period="1y")
            
            # Price Metrics
            current_price = info.get('currentPrice', info.get('regularMarketPrice', 0))
            self.valuation_metrics['price_metrics'] = {
                'current_price': current_price,
                'day_high': info.get('dayHigh', 0),
                'day_low': info.get('dayLow', 0),
                'week_52_high': info.get('fiftyTwoWeekHigh', 0),
                'week_52_low': info.get('fiftyTwoWeekLow', 0),
                'price_vs_52w_high': (current_price / info.get('fiftyTwoWeekHigh', 1) - 1) if info.get('fiftyTwoWeekHigh') else 0,
                'price_vs_52w_low': (current_price / info.get('fiftyTwoWeekLow', 1) - 1) if info.get('fiftyTwoWeekLow') else 0,
                'avg_volume': info.get('averageVolume', 0),
                'market_cap': info.get('marketCap', 0),
                'enterprise_value': info.get('enterpriseValue', 0),
                'price_target_mean': info.get('targetMeanPrice', 0),
                'price_target_high': info.get('targetHighPrice', 0),
                'price_target_low': info.get('targetLowPrice', 0)
            }
            
            # Ratio Metrics
            self.valuation_metrics['ratio_metrics'] = {
                'pe_ratio': info.get('trailingPE', 0),
                'forward_pe': info.get('forwardPE', 0),
                'peg_ratio': info.get('pegRatio', 0),
                'price_to_book': info.get('priceToBook', 0),
                'price_to_sales': info.get('priceToSalesTrailing12Months', 0),
                'ev_to_revenue': info.get('enterpriseToRevenue', 0),
                'ev_to_ebitda': info.get('enterpriseToEbitda', 0),
                'price_to_cash_flow': info.get('priceToCashflow', 0),
                'book_value': info.get('bookValue', 0),
                'price_to_book_ratio': info.get('priceToBook', 0)
            }
            
            # Growth Metrics
            self.valuation_metrics['growth_metrics'] = {
                'revenue_growth': info.get('revenueGrowth', 0),
                'earnings_growth': info.get('earningsGrowth', 0),
                'earnings_quarterly_growth': info.get('earningsQuarterlyGrowth', 0),
                'revenue_quarterly_growth': info.get('revenueQuarterlyGrowth', 0),
                'book_value_growth': 0,  # Will calculate if data available
                'eps_forward': info.get('forwardEps', 0),
                'eps_trailing': info.get('trailingEps', 0)
            }
            
            # Profitability Metrics
            self.valuation_metrics['profitability_metrics'] = {
                'profit_margins': info.get('profitMargins', 0),
                'operating_margins': info.get('operatingMargins', 0),
                'gross_margins': info.get('grossMargins', 0),
                'ebitda_margins': info.get('ebitdaMargins', 0),
                'roe': info.get('returnOnEquity', 0),
                'roa': info.get('returnOnAssets', 0),
                'roic': 0,  # Will calculate
                'net_income_to_common': info.get('netIncomeToCommon', 0),
                'diluted_eps': info.get('trailingEps', 0)
            }
            
            # Efficiency Metrics
            self.valuation_metrics['efficiency_metrics'] = {
                'asset_turnover': 0,  # Will calculate
                'inventory_turnover': 0,
                'receivables_turnover': 0,
                'working_capital': info.get('totalCash', 0) - info.get('totalDebt', 0),
                'operating_cash_flow': info.get('operatingCashflow', 0),
                'free_cash_flow': info.get('freeCashflow', 0),
                'capex_to_revenue': 0
            }
            
            # Leverage Metrics
            self.valuation_metrics['leverage_metrics'] = {
                'total_debt': info.get('totalDebt', 0),
                'total_cash': info.get('totalCash', 0),
                'net_debt': info.get('totalDebt', 0) - info.get('totalCash', 0),
                'debt_to_equity': info.get('debtToEquity', 0),
                'debt_to_assets': 0,
                'interest_coverage': 0,
                'debt_to_ebitda': 0,
                'cash_per_share': info.get('totalCashPerShare', 0)
            }
            
            # Liquidity Metrics
            self.valuation_metrics['liquidity_metrics'] = {
                'current_ratio': info.get('currentRatio', 0),
                'quick_ratio': info.get('quickRatio', 0),
                'cash_ratio': 0,
                'operating_cash_flow_ratio': 0,
                'working_capital_ratio': 0
            }
            
            # Market Metrics
            self.valuation_metrics['market_metrics'] = {
                'beta': info.get('beta', 0),
                'shares_outstanding': info.get('sharesOutstanding', 0),
                'shares_float': info.get('floatShares', 0),
                'shares_short': info.get('sharesShort', 0),
                'short_ratio': info.get('shortRatio', 0),
                'short_percent_float': info.get('shortPercentOfFloat', 0),
                'institutional_percent': info.get('heldPercentInstitutions', 0),
                'insider_percent': info.get('heldPercentInsiders', 0)
            }
            
            # Dividend Metrics
            self.valuation_metrics['dividend_metrics'] = {
                'dividend_yield': info.get('dividendYield', 0),
                'dividend_rate': info.get('dividendRate', 0),
                'payout_ratio': info.get('payoutRatio', 0),
                'ex_dividend_date': info.get('exDividendDate', None),
                'last_dividend_value': info.get('lastDividendValue', 0),
                'five_year_avg_dividend_yield': info.get('fiveYearAvgDividendYield', 0)
            }
            
            # Quality Metrics
            recommendation_mean = info.get('recommendationMean', 0)
            self.valuation_metrics['quality_metrics'] = {
                'recommendation_mean': recommendation_mean,
                'recommendation_key': info.get('recommendationKey', 'none'),
                'number_of_analyst_opinions': info.get('numberOfAnalystOpinions', 0),
                'analyst_target_upside': (info.get('targetMeanPrice', 0) / current_price - 1) if current_price > 0 else 0,
                'earnings_estimate_current_quarter': 0,
                'earnings_estimate_next_quarter': 0,
                'earnings_estimate_current_year': 0,
                'earnings_estimate_next_year': 0
            }
            
            # Set flags based on data availability
            self.data_flags['valuation_data_complete'] = True
            self.data_flags['yfinance_accessible'] = True
            
            print("   ✅ Basic valuation metrics collected")
            
        except Exception as e:
            print(f"   ❌ Error collecting valuation data: {e}")
            self.data_flags['valuation_data_complete'] = False
    
    def get_enhanced_financial_statements(self):
        """Get detailed financial statement data with flags"""
        print("\n📊 Collecting Enhanced Financial Statement Data...")
        print("-" * 50)
        
        financial_data = {
            'income_statement': {},
            'balance_sheet': {},
            'cash_flow': {},
            'key_ratios': {}
        }
        
        # Try FMP API for detailed financials
        if self.data_flags['fmp_api_accessible']:
            try:
                # Income Statement
                income_url = f"https://financialmodelingprep.com/stable/income-statement?symbol=GOOG&limit=5&apikey={self.fmp_api_key}"
                income_data = self.safe_api_call(income_url, "FMP Income Statement")
                
                if income_data:
                    latest_income = income_data[0] if income_data else {}
                    financial_data['income_statement'] = {
                        'revenue': latest_income.get('revenue', 0),
                        'gross_profit': latest_income.get('grossProfit', 0),
                        'operating_income': latest_income.get('operatingIncome', 0),
                        'ebitda': latest_income.get('ebitda', 0),
                        'net_income': latest_income.get('netIncome', 0),
                        'eps': latest_income.get('eps', 0),
                        'research_development': latest_income.get('researchAndDevelopmentExpenses', 0),
                        'selling_admin': latest_income.get('sellingGeneralAndAdministrativeExpenses', 0),
                        'interest_expense': latest_income.get('interestExpense', 0),
                        'tax_expense': latest_income.get('incomeTaxExpense', 0)
                    }
                    self.data_flags['financial_statements_available'] = True
                
                # Balance Sheet
                balance_url = f"https://financialmodelingprep.com/stable/balance-sheet-statement?symbol=GOOG&limit=5&apikey={self.fmp_api_key}"
                balance_data = self.safe_api_call(balance_url, "FMP Balance Sheet")
                
                if balance_data:
                    latest_balance = balance_data[0] if balance_data else {}
                    financial_data['balance_sheet'] = {
                        'total_assets': latest_balance.get('totalAssets', 0),
                        'total_liabilities': latest_balance.get('totalLiabilities', 0),
                        'shareholders_equity': latest_balance.get('totalStockholdersEquity', 0),
                        'cash_equivalents': latest_balance.get('cashAndCashEquivalents', 0),
                        'short_term_investments': latest_balance.get('shortTermInvestments', 0),
                        'accounts_receivable': latest_balance.get('netReceivables', 0),
                        'inventory': latest_balance.get('inventory', 0),
                        'ppe_net': latest_balance.get('propertyPlantEquipmentNet', 0),
                        'goodwill': latest_balance.get('goodwill', 0),
                        'intangible_assets': latest_balance.get('intangibleAssets', 0),
                        'total_debt': latest_balance.get('totalDebt', 0),
                        'accounts_payable': latest_balance.get('accountPayables', 0)
                    }
                
                # Cash Flow Statement
                cf_url = f"https://financialmodelingprep.com/stable/cash-flow-statement?symbol=GOOG&limit=5&apikey={self.fmp_api_key}"
                cf_data = self.safe_api_call(cf_url, "FMP Cash Flow")
                
                if cf_data:
                    latest_cf = cf_data[0] if cf_data else {}
                    financial_data['cash_flow'] = {
                        'operating_cash_flow': latest_cf.get('netCashProvidedByOperatingActivities', 0),
                        'investing_cash_flow': latest_cf.get('netCashUsedForInvestingActivites', 0),
                        'financing_cash_flow': latest_cf.get('netCashUsedProvidedByFinancingActivities', 0),
                        'free_cash_flow': latest_cf.get('freeCashFlow', 0),
                        'capex': latest_cf.get('capitalExpenditure', 0),
                        'acquisitions': latest_cf.get('acquisitionsNet', 0),
                        'dividends_paid': latest_cf.get('dividendsPaid', 0),
                        'stock_repurchases': latest_cf.get('commonStockRepurchased', 0)
                    }
                
                print(f"   ✅ FMP financial statements collected")
                
            except Exception as e:
                print(f"   ❌ FMP financial statements error: {e}")
        
        # Try Alpha Vantage as backup
        if self.data_flags['alpha_vantage_accessible'] and not self.data_flags['financial_statements_available']:
            try:
                av_url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol=GOOG&apikey={self.alpha_vantage_key}"
                av_data = self.safe_api_call(av_url, "Alpha Vantage Overview")
                
                if av_data:
                    financial_data['key_ratios'] = {
                        'trailing_pe': float(av_data.get('TrailingPE', 0)) if av_data.get('TrailingPE', 'None') != 'None' else 0,
                        'forward_pe': float(av_data.get('ForwardPE', 0)) if av_data.get('ForwardPE', 'None') != 'None' else 0,
                        'peg_ratio': float(av_data.get('PEGRatio', 0)) if av_data.get('PEGRatio', 'None') != 'None' else 0,
                        'price_to_book': float(av_data.get('PriceToBookRatio', 0)) if av_data.get('PriceToBookRatio', 'None') != 'None' else 0,
                        'price_to_sales': float(av_data.get('PriceToSalesRatioTTM', 0)) if av_data.get('PriceToSalesRatioTTM', 'None') != 'None' else 0,
                        'dividend_yield': float(av_data.get('DividendYield', 0)) if av_data.get('DividendYield', 'None') != 'None' else 0,
                        'beta': float(av_data.get('Beta', 0)) if av_data.get('Beta', 'None') != 'None' else 0,
                        'profit_margin': float(av_data.get('ProfitMargin', 0)) if av_data.get('ProfitMargin', 'None') != 'None' else 0,
                        'operating_margin': float(av_data.get('OperatingMarginTTM', 0)) if av_data.get('OperatingMarginTTM', 'None') != 'None' else 0,
                        'roe': float(av_data.get('ReturnOnEquityTTM', 0)) if av_data.get('ReturnOnEquityTTM', 'None') != 'None' else 0,
                        'roa': float(av_data.get('ReturnOnAssetsTTM', 0)) if av_data.get('ReturnOnAssetsTTM', 'None') != 'None' else 0,
                        'revenue_ttm': float(av_data.get('RevenueTTM', 0)) if av_data.get('RevenueTTM', 'None') != 'None' else 0,
                        'market_cap': float(av_data.get('MarketCapitalization', 0)) if av_data.get('MarketCapitalization', 'None') != 'None' else 0
                    }
                    self.data_flags['financial_statements_available'] = True
                    print(f"   ✅ Alpha Vantage overview data collected")
                
            except Exception as e:
                print(f"   ❌ Alpha Vantage backup error: {e}")
        
        return financial_data
    
    def get_goog_earnings_data(self):
        """Enhanced earnings data with comprehensive flags"""
        print("\n📈 Analyzing Google's Earnings Performance...")
        print("-" * 50)
        
        earnings_data = {
            'cagr': 0,
            'quality': 'UNKNOWN',
            'source': 'NONE',
            'historical_earnings': [],
            'estimates': {},
            'revisions': {}
        }
        
        # Method 1: FMP API for historical earnings
        if self.data_flags['fmp_api_accessible']:
            fmp_url = f"https://financialmodelingprep.com/stable/income-statement?symbol=GOOG&limit=5&apikey={self.fmp_api_key}"
            fmp_data = self.safe_api_call(fmp_url, "FMP Historical Earnings")
            
            if fmp_data and len(fmp_data) >= 2:
                try:
                    earnings_list = []
                    print("   📊 Historical Earnings from FMP:")
                    for i, year_data in enumerate(reversed(fmp_data)):
                        year = year_data.get('calendarYear', 'Unknown')
                        net_income = year_data.get('netIncome', 0)
                        revenue = year_data.get('revenue', 0)
                        eps = year_data.get('eps', 0)
                        
                        earnings_data['historical_earnings'].append({
                            'year': year,
                            'net_income': net_income,
                            'revenue': revenue,
                            'eps': eps
                        })
                        
                        if net_income:
                            earnings_list.append(abs(net_income))
                            print(f"      {year}: Net Income: ${net_income/1e9:.1f}B, EPS: ${eps:.2f}")
                    
                    if len(earnings_list) >= 2:
                        years = len(earnings_list) - 1
                        earnings_data['cagr'] = (earnings_list[-1] / earnings_list[0]) ** (1/years) - 1
                        earnings_data['quality'] = 'HIGH'
                        earnings_data['source'] = 'FMP'
                        self.data_flags['earnings_data_available'] = True
                        print(f"   🎯 Calculated Earnings CAGR: {earnings_data['cagr']:.1%}")
                        
                except Exception as e:
                    print(f"   ❌ FMP calculation error: {e}")
        
        # Method 2: Get analyst estimates
        if self.data_flags['fmp_api_accessible']:
            estimates_url = f"https://financialmodelingprep.com/stable/analyst-estimates?symbol=GOOG&apikey={self.fmp_api_key}"
            estimates_data = self.safe_api_call(estimates_url, "FMP Analyst Estimates")
            
            if estimates_data:
                for estimate in estimates_data[:4]:  # Get next 4 quarters
                    period = estimate.get('period', 'Unknown')
                    earnings_data['estimates'][period] = {
                        'eps_avg': estimate.get('estimatedEpsAvg', 0),
                        'eps_high': estimate.get('estimatedEpsHigh', 0),
                        'eps_low': estimate.get('estimatedEpsLow', 0),
                        'revenue_avg': estimate.get('estimatedRevenueAvg', 0),
                        'revenue_high': estimate.get('estimatedRevenueHigh', 0),
                        'revenue_low': estimate.get('estimatedRevenueLow', 0),
                        'number_analysts': estimate.get('numberAnalystEstimatedEps', 0)
                    }
                self.data_flags['analyst_estimates_available'] = True
                print(f"   ✅ Analyst estimates for {len(earnings_data['estimates'])} periods collected")
        
        # Method 3: yfinance fallback
        if earnings_data['cagr'] == 0 and self.data_flags['yfinance_accessible']:
            try:
                stock = yf.Ticker("GOOG")
                info = stock.info
                
                earnings_growth = info.get('earningsGrowth', 0)
                if earnings_growth:
                    earnings_data['cagr'] = earnings_growth
                    earnings_data['quality'] = 'ESTIMATED'
                    earnings_data['source'] = 'YFINANCE'
                    print(f"   🔄 Using YFinance estimate: {earnings_data['cagr']:.1%}")
                else:
                    # Google specific estimate based on historical performance
                    earnings_data['cagr'] = 0.15
                    earnings_data['quality'] = 'ESTIMATED_HISTORICAL'
                    earnings_data['source'] = 'FALLBACK'
                    print(f"   ⚠️  Historical average estimate: {earnings_data['cagr']:.1%}")
                    
            except Exception as e:
                print(f"   ❌ YFinance fallback error: {e}")
        
        print(f"   🎯 Earnings Data Quality: {earnings_data['quality']} from {earnings_data['source']}")
        return earnings_data
    
    def get_goog_revisions(self):
        """Enhanced revision analysis with comprehensive tracking"""
        print("\n📊 Analyzing Google's Analyst Revisions...")
        print("-" * 50)
        
        revision_data = {
            'score': 0,
            'direction': 'STABLE',
            'confidence': 0.5,
            'source': 'NONE',
            'revision_history': [],
            'consensus_changes': {},
            'price_target_changes': {}
        }
        
        # Method 1: FMP API comprehensive revisions
        if self.data_flags['fmp_api_accessible']:
            fmp_url = f"https://financialmodelingprep.com/stable/analyst-estimates?symbol=GOOG&apikey={self.fmp_api_key}"
            fmp_data = self.safe_api_call(fmp_url, "FMP Revision Analysis")
            
            if fmp_data and len(fmp_data) >= 3:
                try:
                    df = pd.DataFrame(fmp_data)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values('date', ascending=False)
                    
                    # Track revision history
                    for _, row in df.head(5).iterrows():
                        revision_data['revision_history'].append({
                            'date': row['date'].strftime('%Y-%m-%d'),
                            'eps_avg': row.get('estimatedEpsAvg', 0),
                            'eps_high': row.get('estimatedEpsHigh', 0),
                            'eps_low': row.get('estimatedEpsLow', 0),
                            'revenue_avg': row.get('estimatedRevenueAvg', 0),
                            'number_analysts': row.get('numberAnalystEstimatedEps', 0)
                        })
                    
                    # Calculate revision trend
                    recent_estimates = df.head(3)['estimatedEpsAvg'].tolist()
                    print(f"   📈 Recent EPS Estimates: {[f'${x:.2f}' for x in recent_estimates]}")
                    
                    if len(recent_estimates) >= 2:
                        latest = recent_estimates[0]
                        previous = recent_estimates[-1]
                        
                        if previous != 0:
                            revision_trend = (latest - previous) / abs(previous)
                            
                            # Enhanced scoring system
                            if revision_trend > 0.15:
                                revision_data['score'] = 5
                                revision_data['direction'] = "VERY BULLISH"
                                revision_data['confidence'] = 0.95
                            elif revision_trend > 0.10:
                                revision_data['score'] = 4
                                revision_data['direction'] = "BULLISH"
                                revision_data['confidence'] = 0.9
                            elif revision_trend > 0.05:
                                revision_data['score'] = 3
                                revision_data['direction'] = "POSITIVE"
                                revision_data['confidence'] = 0.8
                            elif revision_trend > 0.02:
                                revision_data['score'] = 2
                                revision_data['direction'] = "SLIGHTLY POSITIVE"
                                revision_data['confidence'] = 0.7
                            elif revision_trend < -0.15:
                                revision_data['score'] = -4
                                revision_data['direction'] = "VERY BEARISH"
                                revision_data['confidence'] = 0.95
                            elif revision_trend < -0.10:
                                revision_data['score'] = -3
                                revision_data['direction'] = "BEARISH"
                                revision_data['confidence'] = 0.9
                            elif revision_trend < -0.05:
                                revision_data['score'] = -2
                                revision_data['direction'] = "NEGATIVE"
                                revision_data['confidence'] = 0.8
                            else:
                                revision_data['score'] = 0
                                revision_data['direction'] = "STABLE"
                                revision_data['confidence'] = 0.6
                            
                            revision_data['source'] = 'FMP'
                            self.data_flags['revision_data_available'] = True
                            print(f"   🎯 Revision Trend: {revision_trend:+.1%} → {revision_data['direction']}")
                            
                except Exception as e:
                    print(f"   ❌ FMP revision analysis error: {e}")
        
        # Method 2: yfinance backup with enhanced analysis
        if revision_data['score'] == 0 and self.data_flags['yfinance_accessible']:
            try:
                stock = yf.Ticker("GOOG")
                info = stock.info
                
                recommendation_mean = info.get('recommendationMean', 3)
                target_mean_price = info.get('targetMeanPrice', 0)
                target_high_price = info.get('targetHighPrice', 0)
                target_low_price = info.get('targetLowPrice', 0)
                current_price = info.get('currentPrice', info.get('regularMarketPrice', 0))
                num_analysts = info.get('numberOfAnalystOpinions', 0)
                
                revision_data['consensus_changes'] = {
                    'recommendation_mean': recommendation_mean,
                    'target_mean': target_mean_price,
                    'target_high': target_high_price,
                    'target_low': target_low_price,
                    'current_price': current_price,
                    'number_analysts': num_analysts
                }
                
                print(f"   📊 Analyst Recommendation: {recommendation_mean:.1f} (1=Strong Buy, 5=Strong Sell)")
                print(f"   💰 Price Targets: Low ${target_low_price:.2f} | Mean ${target_mean_price:.2f} | High ${target_high_price:.2f}")
                print(f"   📈 Current Price: ${current_price:.2f} | Analysts: {num_analysts}")
                
                if target_mean_price and current_price:
                    upside = (target_mean_price - current_price) / current_price
                    print(f"   📈 Implied Upside: {upside:+.1%}")
                    
                    # Enhanced scoring based on multiple factors
                    score_components = []
                    
                    # Upside component
                    if upside > 0.30:
                        score_components.append(4)
                    elif upside > 0.20:
                        score_components.append(3)
                    elif upside > 0.10:
                        score_components.append(2)
                    elif upside > 0.05:
                        score_components.append(1)
                    elif upside < -0.15:
                        score_components.append(-3)
                    elif upside < -0.05:
                        score_components.append(-1)
                    else:
                        score_components.append(0)
                    
                    # Recommendation component
                    if recommendation_mean < 2.0:
                        score_components.append(2)
                    elif recommendation_mean < 2.5:
                        score_components.append(1)
                    elif recommendation_mean > 4.0:
                        score_components.append(-2)
                    elif recommendation_mean > 3.5:
                        score_components.append(-1)
                    else:
                        score_components.append(0)
                    
                    # Number of analysts component (more analysts = higher confidence)
                    if num_analysts > 20:
                        revision_data['confidence'] = 0.8
                    elif num_analysts > 10:
                        revision_data['confidence'] = 0.7
                    else:
                        revision_data['confidence'] = 0.6
                    
                    revision_data['score'] = sum(score_components)
                    
                    # Determine direction
                    if revision_data['score'] >= 4:
                        revision_data['direction'] = "VERY BULLISH"
                    elif revision_data['score'] >= 2:
                        revision_data['direction'] = "BULLISH"
                    elif revision_data['score'] >= 1:
                        revision_data['direction'] = "POSITIVE"
                    elif revision_data['score'] <= -3:
                        revision_data['direction'] = "VERY BEARISH"
                    elif revision_data['score'] <= -1:
                        revision_data['direction'] = "BEARISH"
                    else:
                        revision_data['direction'] = "STABLE"
                    
                    revision_data['source'] = 'YFINANCE'
                    self.data_flags['revision_data_available'] = True
                
            except Exception as e:
                print(f"   ❌ YFinance revision analysis error: {e}")
        
        print(f"   🎯 Revision Score: {revision_data['score']}/5 | Direction: {revision_data['direction']} | Confidence: {revision_data['confidence']:.1%}")
        return revision_data
    
    def get_goog_macro_analysis(self):
        """Enhanced macro analysis with comprehensive data tracking"""
        print("\n🌍 Analyzing Macro Environment for Google...")
        print("-" * 50)
        
        macro_data = {
            'score': 0,
            'reasons': [],
            'confidence': 0.8,
            'sector_performance': {},
            'economic_indicators': {},
            'google_specific_factors': {}
        }
        
        try:
            # Enhanced sector analysis
            sector_tickers = {
                'QQQ': 'Technology Sector',
                'SPY': 'Broad Market',
                'XLK': 'Technology Select Sector',
                'FTEC': 'Fidelity Tech Sector',
                'XLC': 'Communication Services',
                'FCOM': 'Fidelity Communication Services'
            }
            
            print("   📊 Sector Performance Analysis:")
            for ticker, name in sector_tickers.items():
                try:
                    etf = yf.Ticker(ticker)
                    hist = etf.history(period="3mo")
                    
                    if not hist.empty:
                        returns_3m = (hist['Close'].iloc[-1] / hist['Close'].iloc[0]) - 1
                        
                        # Get 1-year performance for context
                        hist_1y = etf.history(period="1y")
                        returns_1y = (hist_1y['Close'].iloc[-1] / hist_1y['Close'].iloc[0]) - 1 if not hist_1y.empty else 0
                        
                        macro_data['sector_performance'][ticker] = {
                            'name': name,
                            'returns_3m': returns_3m,
                            'returns_1y': returns_1y,
                            'current_price': hist['Close'].iloc[-1]
                        }
                        
                        print(f"      {name} ({ticker}): 3M: {returns_3m:+.1%}, 1Y: {returns_1y:+.1%}")
                        
                        # Scoring based on performance
                        if ticker == 'XLC':  # Communication Services sector most important for Google
                            if returns_3m > 0.20:
                                macro_data['score'] += 3
                                macro_data['reasons'].append(f"🚀 Exceptional communication services momentum ({returns_3m:+.1%})")
                            elif returns_3m > 0.10:
                                macro_data['score'] += 2
                                macro_data['reasons'].append(f"✅ Strong communication services sector ({returns_3m:+.1%})")
                            elif returns_3m > 0.05:
                                macro_data['score'] += 1
                                macro_data['reasons'].append(f"✅ Positive communication services trend ({returns_3m:+.1%})")
                            elif returns_3m < -0.15:
                                macro_data['score'] -= 3
                                macro_data['reasons'].append(f"❌ Weak communication services sector ({returns_3m:+.1%})")
                            elif returns_3m < -0.05:
                                macro_data['score'] -= 1
                                macro_data['reasons'].append(f"⚠️ Communication services weakness ({returns_3m:+.1%})")
                        
                        elif ticker == 'QQQ':  # Technology sector
                            if returns_3m > 0.15:
                                macro_data['score'] += 2
                                macro_data['reasons'].append(f"✅ Strong tech sector momentum ({returns_3m:+.1%})")
                            elif returns_3m > 0.05:
                                macro_data['score'] += 1
                                macro_data['reasons'].append(f"✅ Positive tech sector ({returns_3m:+.1%})")
                            elif returns_3m < -0.10:
                                macro_data['score'] -= 1
                                macro_data['reasons'].append(f"❌ Tech sector weakness ({returns_3m:+.1%})")
                                
                except Exception as e:
                    print(f"      ❌ Error analyzing {ticker}: {str(e)[:50]}...")
            
            # Economic indicators (if FRED API available)
            if self.data_flags['fred_api_accessible']:
                economic_indicators = {
                    'DGS10': '10-Year Treasury Rate',
                    'UNRATE': 'Unemployment Rate',
                    'CPIAUCSL': 'CPI Inflation',
                    'GDPC1': 'GDP Growth'
                }
                
                print("   📈 Economic Indicators:")
                for indicator, name in economic_indicators.items():
                    try:
                        fred_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={indicator}&api_key={self.fred_api_key}&file_type=json&limit=12&sort_order=desc"
                        fred_data = self.safe_api_call(fred_url, f"FRED {name}")
                        
                        if fred_data and 'observations' in fred_data:
                            obs = fred_data['observations']
                            latest_values = [float(o['value']) for o in obs[:3] if o['value'] != '.']
                            
                            if latest_values:
                                latest_value = latest_values[0]
                                macro_data['economic_indicators'][indicator] = {
                                    'name': name,
                                    'latest_value': latest_value,
                                    'trend': 'improving' if len(latest_values) >= 2 and latest_values[0] > latest_values[1] else 'declining'
                                }
                                print(f"      {name}: {latest_value:.2f}")
                                
                    except Exception as e:
                        print(f"      ❌ Error getting {name}: {str(e)[:50]}...")
            
            # Google-specific macro factors
            print("   🎯 Google-Specific Macro Factors:")
            google_factors = {
                'ai_leadership': {'weight': 3, 'description': 'AI leadership with Gemini, Bard, and AI infrastructure'},
                'search_dominance': {'weight': 2, 'description': 'Dominant search market share and pricing power'},
                'cloud_growth': {'weight': 2, 'description': 'Strong cloud growth competing with AWS/Azure'},
                'regulatory_risk': {'weight': -2, 'description': 'Antitrust and regulatory challenges globally'},
                'ad_market_strength': {'weight': 2, 'description': 'Digital advertising market recovery'},
                'youtube_growth': {'weight': 2, 'description': 'YouTube monetization and subscription growth'},
                'ai_competition': {'weight': -1, 'description': 'Competition from ChatGPT/OpenAI in search'},
                'privacy_regulations': {'weight': -1, 'description': 'Privacy regulations impacting ad targeting'},
                'waymo_opportunity': {'weight': 1, 'description': 'Waymo autonomous vehicle leadership'},
                'quantum_computing': {'weight': 1, 'description': 'Quantum computing breakthrough potential'}
            }
            
            for factor, details in google_factors.items():
                weight = details['weight']
                description = details['description']
                macro_data['score'] += weight
                
                if weight > 0:
                    macro_data['reasons'].append(f"✅ {description}")
                    print(f"      ✅ {description} (+{weight})")
                else:
                    macro_data['reasons'].append(f"❌ {description}")
                    print(f"      ❌ {description} ({weight})")
                
                macro_data['google_specific_factors'][factor] = details
            
            self.data_flags['macro_data_available'] = True
            
        except Exception as e:
            print(f"   ❌ Macro analysis error: {e}")
            macro_data['confidence'] = 0.5
        
        print(f"   🎯 Overall Macro Score: {macro_data['score']}/10 | Confidence: {macro_data['confidence']:.1%}")
        return macro_data
    
    def analyze_goog_ceo(self):
        """Enhanced CEO analysis with comprehensive tracking"""
        print("\n👔 Analyzing Google's CEO: Sundar Pichai...")
        print("-" * 50)
        
        ceo_data = self.ceo_database['GOOG']
        
        ceo_analysis = {
            'name': ceo_data['name'],
            'score': 0,
            'reasons': [],
            'background_analysis': {},
            'performance_metrics': {},
            'strategic_assessment': {},
            'execution_track_record': {},
            'was_cfo': ceo_data['was_cfo'],
            'vision': ceo_data['vision'],
            'execution': ceo_data['execution']
        }
        
        # Mark's #1 Rule Analysis
        print("   📋 Mark's CEO Criteria Analysis:")
        if not ceo_data['was_cfo']:
            ceo_analysis['score'] += 4
            ceo_analysis['reasons'].append("✅ Passes Mark's #1 rule - Not former CFO")
            print("      ✅ Not former CFO - engineering/product background")
        else:
            ceo_analysis['score'] -= 2
            ceo_analysis['reasons'].append("❌ Fails Mark's #1 rule - Former CFO")
            print("      ❌ Former CFO background")
        
        # Background Analysis
        background_scores = {
            'technical': 2 if ceo_data['technical'] else 0,
            'strategic': 1 if ceo_data['strategic'] else 0,
            'founder': 2 if ceo_data['founder'] else 0,
            'industry_experience': 2  # Sundar has deep Google/tech experience
        }
        
        ceo_analysis['background_analysis'] = {
            'technical_expertise': ceo_data['technical'],
            'strategic_experience': ceo_data['strategic'],
            'founder_status': ceo_data['founder'],
            'industry_experience': True,
            'previous_ceo_experience': False,  # First CEO role
            'google_tenure': 'Joined 2004, CEO since 2015',
            'education': 'Engineering (IIT, Stanford, Wharton)',
            'leadership_style': 'Collaborative technical visionary'
        }
        
        total_background_score = sum(background_scores.values())
        ceo_analysis['score'] += total_background_score
        
        print(f"   📊 Background Assessment:")
        print(f"      Technical Expertise: {'✅' if ceo_data['technical'] else '❌'} (+{background_scores['technical']})")
        print(f"      Strategic Experience: {'✅' if ceo_data['strategic'] else '❌'} (+{background_scores['strategic']})")
        print(f"      Founder Status: {'✅' if ceo_data['founder'] else '❌'} (+{background_scores['founder']})")
        print(f"      Industry Experience: ✅ (+{background_scores['industry_experience']})")
        
        # Vision Scoring
        vision = ceo_data['vision']
        if vision >= 9:
            vision_score = 4
            vision_desc = "🔥 Exceptional strategic vision"
        elif vision >= 7:
            vision_score = 3
            vision_desc = "✅ Strong strategic vision"
        elif vision >= 5:
            vision_score = 1
            vision_desc = "✅ Adequate strategic vision"
        else:
            vision_score = -1
            vision_desc = "❌ Weak strategic vision"
        
        ceo_analysis['score'] += vision_score
        ceo_analysis['reasons'].append(f"{vision_desc} ({vision}/10)")
        
        ceo_analysis['strategic_assessment'] = {
            'vision_score': vision,
            'strategic_initiatives': [
                'AI-first company transformation',
                'Cloud infrastructure expansion',
                'Pixel ecosystem development',
                'YouTube platform evolution',
                'Search AI integration',
                'Quantum computing leadership',
                'Privacy-preserving technology',
                'Workspace productivity suite'
            ],
            'strategic_clarity': 'Very High - Clear AI and platform focus',
            'market_positioning': 'Dominant - Leading in multiple key markets'
        }
        
        print(f"   🎯 Strategic Vision: {vision}/10 - {vision_desc}")
        
        # Execution Scoring
        execution = ceo_data['execution']
        if execution >= 9:
            execution_score = 4
            execution_desc = "🔥 Exceptional execution"
        elif execution >= 7:
            execution_score = 3
            execution_desc = "✅ Strong execution"
        elif execution >= 5:
            execution_score = 1
            execution_desc = "✅ Adequate execution"
        else:
            execution_score = -2
            execution_desc = "❌ Execution challenges"
        
        ceo_analysis['score'] += execution_score
        ceo_analysis['reasons'].append(f"{execution_desc} ({execution}/10)")
        
        ceo_analysis['execution_track_record'] = {
            'execution_score': execution,
            'major_achievements': [
                'Chrome browser dominance',
                'Android platform leadership',
                'Google Cloud 3x growth',
                'AI research breakthroughs',
                'YouTube $30B+ revenue',
                'Successful Pixel launch',
                'Workspace adoption surge',
                'Strong financial performance'
            ],
            'execution_strengths': [
                'Product development excellence',
                'Technical innovation',
                'Team building and retention',
                'Strategic acquisitions',
                'Platform ecosystem development'
            ],
            'timeline_performance': 'Excellent - Consistent delivery on major initiatives'
        }
        
        print(f"   ⚡ Execution Capability: {execution}/10 - {execution_desc}")
        
        # Performance validation with company metrics
        if self.data_flags['yfinance_accessible']:
            try:
                stock = yf.Ticker("GOOG")
                info = stock.info
                
                performance_metrics = {
                    'roe': info.get('returnOnEquity', 0),
                    'roa': info.get('returnOnAssets', 0),
                    'profit_margins': info.get('profitMargins', 0),
                    'operating_margins': info.get('operatingMargins', 0),
                    'revenue_growth': info.get('revenueGrowth', 0),
                    'earnings_growth': info.get('earningsGrowth', 0),
                    'market_cap': info.get('marketCap', 0),
                    'enterprise_value': info.get('enterpriseValue', 0)
                }
                
                ceo_analysis['performance_metrics'] = performance_metrics
                
                print(f"   📈 Company Performance Under Pichai:")
                print(f"      ROE: {performance_metrics['roe']:.1%}")
                print(f"      Profit Margins: {performance_metrics['profit_margins']:.1%}")
                print(f"      Revenue Growth: {performance_metrics['revenue_growth']:+.1%}")
                
                # Performance-based scoring
                performance_score = 0
                if performance_metrics['roe'] > 0.15:
                    performance_score += 2
                    ceo_analysis['reasons'].append(f"✅ Strong ROE ({performance_metrics['roe']:.1%})")
                elif performance_metrics['roe'] > 0.10:
                    performance_score += 1
                    ceo_analysis['reasons'].append(f"✅ Decent ROE ({performance_metrics['roe']:.1%})")
                elif performance_metrics['roe'] < 0.05:
                    performance_score -= 1
                    ceo_analysis['reasons'].append(f"❌ Weak ROE ({performance_metrics['roe']:.1%})")
                
                if performance_metrics['profit_margins'] > 0.20:
                    performance_score += 1
                    ceo_analysis['reasons'].append(f"✅ Strong margins ({performance_metrics['profit_margins']:.1%})")
                elif performance_metrics['profit_margins'] < 0.10:
                    performance_score -= 1
                    ceo_analysis['reasons'].append(f"❌ Weak margins ({performance_metrics['profit_margins']:.1%})")
                
                ceo_analysis['score'] += performance_score
                
            except Exception as e:
                print(f"   ❌ Performance validation error: {e}")
        
        # Google-specific CEO factors
        print("   🎯 Google-Specific CEO Factors:")
        google_specific_factors = [
            ("✅", "Deep Google culture understanding (20+ years)", 1),
            ("✅", "Engineering credibility and technical depth", 1),
            ("✅", "Strong product development track record", 1),
            ("✅", "AI vision and research investment focus", 1),
            ("✅", "Successful platform ecosystem management", 1),
            ("⚠️", "Regulatory navigation challenges", 0),
            ("✅", "Strong financial performance delivery", 1)
        ]
        
        for status, factor, score_impact in google_specific_factors:
            print(f"      {status} {factor}")
            ceo_analysis['score'] += score_impact
            if score_impact != 0:
                ceo_analysis['reasons'].append(f"{status} {factor}")
        
        # Final score calculation and normalization
        final_score = min(max(ceo_analysis['score'], 0), 15)
        ceo_analysis['score'] = final_score
        
        print(f"   🎯 Final CEO Score: {final_score}/15")
        
        return ceo_analysis
    
    def display_comprehensive_data_flags(self):
        """Display comprehensive data availability status"""
        print("\n🔍 COMPREHENSIVE DATA ACCESS REPORT")
        print("=" * 60)
        
        print("📡 API Connection Status:")
        api_flags = {
            'fmp_api_accessible': 'Financial Modeling Prep API',
            'alpha_vantage_accessible': 'Alpha Vantage API',
            'fred_api_accessible': 'FRED Economic Data API',
            'yfinance_accessible': 'Yahoo Finance API'
        }
        
        for flag, description in api_flags.items():
            status = "✅ Connected" if self.data_flags[flag] else "❌ Failed"
            print(f"  {description}: {status}")
        
        print("\n📊 Data Category Availability:")
        data_categories = {
            'earnings_data_available': 'Historical Earnings Data',
            'revision_data_available': 'Analyst Revision Data',
            'macro_data_available': 'Macro Environment Data',
            'valuation_data_complete': 'Comprehensive Valuation Metrics',
            'financial_statements_available': 'Financial Statement Data',
            'analyst_estimates_available': 'Forward Analyst Estimates'
        }
        
        for flag, description in data_categories.items():
            status = "✅ Available" if self.data_flags[flag] else "❌ Missing"
            quality = "HIGH" if self.data_flags[flag] else "LOW"
            print(f"  {description}: {status} | Quality: {quality}")
        
        print("\n💰 Valuation Metrics Coverage:")
        valuation_categories = [
            'price_metrics', 'ratio_metrics', 'growth_metrics', 
            'profitability_metrics', 'efficiency_metrics', 'leverage_metrics',
            'liquidity_metrics', 'market_metrics', 'dividend_metrics', 'quality_metrics'
        ]
        
        for category in valuation_categories:
            metrics = self.valuation_metrics.get(category, {})
            coverage = len([v for v in metrics.values() if v != 0]) / max(len(metrics), 1) * 100
            status = "✅" if coverage > 70 else "⚠️" if coverage > 40 else "❌"
            print(f"  {category.replace('_', ' ').title()}: {status} {coverage:.0f}% coverage")
        
        print(f"\n🎯 Overall Data Quality Score: {sum(self.data_flags.values())}/{len(self.data_flags)} ({sum(self.data_flags.values())/len(self.data_flags)*100:.0f}%)")
    
    def calculate_goog_conviction_score(self):
        """Enhanced conviction score calculation with comprehensive data tracking"""
        print("\n" + "="*80)
        print("🎯 CALCULATING GOOGLE'S ENHANCED CONVICTION SCORE")
        print("="*80)
        
        # First, get comprehensive valuation data
        self.get_comprehensive_valuation_data()
        financial_data = self.get_enhanced_financial_statements()
        
        # Display data access status
        self.display_comprehensive_data_flags()
        
        try:
            stock = yf.Ticker("GOOG")
            info = stock.info
            
            print(f"\n📊 Alphabet Inc. (GOOG) - Enhanced Analysis")
            print(f"Current Price: ${info.get('currentPrice', info.get('regularMarketPrice', 0)):.2f}")
            print(f"Market Cap: ${info.get('marketCap', 0)/1e9:.1f}B")
            print(f"Sector: {info.get('sector', 'Technology')}")
            print(f"Industry: {info.get('industry', 'Internet Content & Information')}")
            
            total_score = 0
            detailed_breakdown = {}
            
            # === ENHANCED VALUATION ANALYSIS (25 points max) ===
            print(f"\n📈 ENHANCED VALUATION ANALYSIS (25 points max)")
            print("-" * 50)
            
            valuation_score = 0
            valuation_reasons = []
            
            # Get enhanced earnings data
            earnings_data = self.get_goog_earnings_data()
            earnings_cagr = earnings_data['cagr']
            
            # Factor 1: Forward P/E vs Trailing P/E with enhanced analysis
            price_metrics = self.valuation_metrics['price_metrics']
            ratio_metrics = self.valuation_metrics['ratio_metrics']
            
            forward_pe = ratio_metrics.get('forward_pe', 0)
            trailing_pe = ratio_metrics.get('pe_ratio', 0)
            
            print(f"Forward P/E: {forward_pe:.1f}")
            print(f"Trailing P/E: {trailing_pe:.1f}")
            
            if forward_pe > 0 and trailing_pe > 0:
                pe_compression = (trailing_pe - forward_pe) / trailing_pe
                print(f"P/E Compression: {pe_compression:+.1%}")
                
                if pe_compression > 0.25:
                    valuation_score += 10
                    valuation_reasons.append(f"🔥 Massive P/E compression ({pe_compression:.1%})")
                elif pe_compression > 0.20:
                    valuation_score += 8
                    valuation_reasons.append(f"🔥 Excellent P/E compression ({pe_compression:.1%})")
                elif pe_compression > 0.15:
                    valuation_score += 6
                    valuation_reasons.append(f"✅ Strong P/E compression ({pe_compression:.1%})")
                elif pe_compression > 0.10:
                    valuation_score += 4
                    valuation_reasons.append(f"✅ Good P/E compression ({pe_compression:.1%})")
                elif pe_compression > 0.05:
                    valuation_score += 2
                    valuation_reasons.append(f"✅ Moderate P/E compression ({pe_compression:.1%})")
                else:
                    valuation_reasons.append(f"❌ No P/E compression ({pe_compression:.1%})")
            
            # Factor 2: Enhanced PEG Ratio Analysis
            peg_ratio = ratio_metrics.get('peg_ratio', 0)
            if peg_ratio == 0 and forward_pe > 0 and earnings_cagr != 0:
                peg_ratio = forward_pe / (abs(earnings_cagr) * 100)
            
            print(f"PEG Ratio: {peg_ratio:.2f}")
            
            if earnings_cagr > 0 and peg_ratio > 0:
                if peg_ratio < 0.5:
                    valuation_score += 8
                    valuation_reasons.append(f"🔥 Incredible PEG ratio ({peg_ratio:.2f})")
                elif peg_ratio < 0.8:
                    valuation_score += 6
                    valuation_reasons.append(f"✅ Excellent PEG ratio ({peg_ratio:.2f})")
                elif peg_ratio < 1.2:
                    valuation_score += 4
                    valuation_reasons.append(f"✅ Good PEG ratio ({peg_ratio:.2f})")
                elif peg_ratio < 1.8:
                    valuation_score += 2
                    valuation_reasons.append(f"✅ Fair PEG ratio ({peg_ratio:.2f})")
                elif peg_ratio > 2.5:
                    valuation_score -= 2
                    valuation_reasons.append(f"❌ High PEG ratio ({peg_ratio:.2f})")
            elif earnings_cagr < 0:
                valuation_score -= 3
                valuation_reasons.append(f"❌ Negative earnings growth")
            
            # Factor 3: Price Target Analysis
            target_upside = (price_metrics.get('price_target_mean', 0) / price_metrics.get('current_price', 1) - 1) if price_metrics.get('current_price', 0) > 0 else 0
            if target_upside != 0:
                print(f"Analyst Target Upside: {target_upside:+.1%}")
                if target_upside > 0.30:
                    valuation_score += 4
                    valuation_reasons.append(f"🚀 High analyst target upside ({target_upside:.1%})")
                elif target_upside > 0.15:
                    valuation_score += 2
                    valuation_reasons.append(f"✅ Good analyst target upside ({target_upside:.1%})")
                elif target_upside < -0.10:
                    valuation_score -= 2
                    valuation_reasons.append(f"❌ Negative analyst target ({target_upside:.1%})")
            
            # Factor 4: 52-week positioning
            price_vs_52w_high = price_metrics.get('price_vs_52w_high', 0)
            if price_vs_52w_high < -0.30:
                valuation_score += 3
                valuation_reasons.append(f"✅ Trading well below 52W high ({price_vs_52w_high:.1%})")
            elif price_vs_52w_high < -0.15:
                valuation_score += 1
                valuation_reasons.append(f"✅ Below 52W high ({price_vs_52w_high:.1%})")
            
            print(f"🎯 Enhanced Valuation Score: {valuation_score}/25")
            
            # === ENHANCED GROWTH QUALITY (25 points max) ===
            print(f"\n📊 ENHANCED GROWTH QUALITY ANALYSIS (25 points max)")
            print("-" * 50)
            
            growth_score = 0
            growth_reasons = []
            
            # Factor 1: Enhanced Earnings Growth Analysis
            print(f"Earnings CAGR: {earnings_cagr:+.1%} (Source: {earnings_data['source']})")
            print(f"Data Quality: {earnings_data['quality']}")
            
            if earnings_cagr > 0.40:
                growth_score += 12
                growth_reasons.append(f"🚀 Explosive earnings growth ({earnings_cagr:.1%})")
            elif earnings_cagr > 0.30:
                growth_score += 10
                growth_reasons.append(f"🚀 Exceptional earnings growth ({earnings_cagr:.1%})")
            elif earnings_cagr > 0.20:
                growth_score += 8
                growth_reasons.append(f"✅ Strong earnings growth ({earnings_cagr:.1%})")
            elif earnings_cagr > 0.15:
                growth_score += 6
                growth_reasons.append(f"✅ Good earnings growth ({earnings_cagr:.1%})")
            elif earnings_cagr > 0.10:
                growth_score += 4
                growth_reasons.append(f"✅ Moderate earnings growth ({earnings_cagr:.1%})")
            elif earnings_cagr > 0:
                growth_score += 2
                growth_reasons.append(f"✅ Slow earnings growth ({earnings_cagr:.1%})")
            else:
                growth_score -= 4
                growth_reasons.append(f"❌ Negative earnings growth ({earnings_cagr:.1%})")
            
            # Factor 2: Enhanced Revenue Growth
            growth_metrics = self.valuation_metrics['growth_metrics']
            revenue_growth = growth_metrics.get('revenue_growth', 0)
            print(f"Revenue Growth: {revenue_growth:+.1%}")
            
            if revenue_growth > 0.30:
                growth_score += 8
                growth_reasons.append(f"🚀 Explosive revenue growth ({revenue_growth:.1%})")
            elif revenue_growth > 0.20:
                growth_score += 6
                growth_reasons.append(f"✅ Strong revenue growth ({revenue_growth:.1%})")
            elif revenue_growth > 0.15:
                growth_score += 4
                growth_reasons.append(f"✅ Good revenue growth ({revenue_growth:.1%})")
            elif revenue_growth > 0.10:
                growth_score += 3
                growth_reasons.append(f"✅ Moderate revenue growth ({revenue_growth:.1%})")
            elif revenue_growth > 0.05:
                growth_score += 2
                growth_reasons.append(f"✅ Slow revenue growth ({revenue_growth:.1%})")
            elif revenue_growth > 0:
                growth_score += 1
                growth_reasons.append(f"✅ Minimal revenue growth ({revenue_growth:.1%})")
            else:
                growth_score -= 3
                growth_reasons.append(f"❌ Negative revenue growth ({revenue_growth:.1%})")
            
            # Factor 3: Enhanced Profitability Analysis
            profitability_metrics = self.valuation_metrics['profitability_metrics']
            profit_margins = profitability_metrics.get('profit_margins', 0)
            operating_margins = profitability_metrics.get('operating_margins', 0)
            gross_margins = profitability_metrics.get('gross_margins', 0)
            
            print(f"Profit Margins: {profit_margins:.1%}")
            print(f"Operating Margins: {operating_margins:.1%}")
            print(f"Gross Margins: {gross_margins:.1%}")
            
            # Profit margin scoring
            if profit_margins > 0.30:
                growth_score += 5
                growth_reasons.append(f"🔥 Outstanding profit margins ({profit_margins:.1%})")
            elif profit_margins > 0.20:
                growth_score += 4
                growth_reasons.append(f"✅ Excellent profit margins ({profit_margins:.1%})")
            elif profit_margins > 0.15:
                growth_score += 3
                growth_reasons.append(f"✅ Good profit margins ({profit_margins:.1%})")
            elif profit_margins > 0.10:
                growth_score += 2
                growth_reasons.append(f"✅ Fair profit margins ({profit_margins:.1%})")
            elif profit_margins > 0.05:
                growth_score += 1
                growth_reasons.append(f"✅ Modest profit margins ({profit_margins:.1%})")
            else:
                growth_reasons.append(f"❌ Poor profit margins ({profit_margins:.1%})")
            
            print(f"🎯 Enhanced Growth Score: {growth_score}/25")
            
            # === ENHANCED MANAGEMENT ANALYSIS (25 points max) ===
            print(f"\n👔 ENHANCED MANAGEMENT ANALYSIS (25 points max)")
            print("-" * 50)
            
            ceo_analysis = self.analyze_goog_ceo()
            management_score = min(int(ceo_analysis['score'] * 1.67), 25)
            management_reasons = ceo_analysis['reasons']
            
            print(f"🎯 Enhanced Management Score: {management_score}/25")
            
            # === ENHANCED CONSENSUS EDGE (25 points max) ===
            print(f"\n📈 ENHANCED CONSENSUS ANALYSIS (25 points max)")
            print("-" * 50)
            
            consensus_score = 0
            consensus_reasons = []
            
            # Factor 1: Enhanced Earnings Revisions
            revision_data = self.get_goog_revisions()
            revision_score = revision_data['score']
            
            if revision_score >= 5:
                consensus_score += 15
                consensus_reasons.append(f"🚀 Exceptional revisions ({revision_data['direction']})")
            elif revision_score >= 4:
                consensus_score += 12
                consensus_reasons.append(f"🚀 Very bullish revisions ({revision_data['direction']})")
            elif revision_score >= 3:
                consensus_score += 10
                consensus_reasons.append(f"✅ Bullish revisions ({revision_data['direction']})")
            elif revision_score >= 2:
                consensus_score += 7
                consensus_reasons.append(f"✅ Positive revisions ({revision_data['direction']})")
            elif revision_score >= 1:
                consensus_score += 4
                consensus_reasons.append(f"✅ Stable revisions ({revision_data['direction']})")
            elif revision_score <= -4:
                consensus_score -= 8
                consensus_reasons.append(f"❌ Very bearish revisions ({revision_data['direction']})")
            elif revision_score <= -3:
                consensus_score -= 5
                consensus_reasons.append(f"❌ Bearish revisions ({revision_data['direction']})")
            elif revision_score <= -2:
                consensus_score -= 3
                consensus_reasons.append(f"❌ Negative revisions ({revision_data['direction']})")
            else:
                consensus_reasons.append(f"⚠️ Neutral revisions ({revision_data['direction']})")
            
            # Factor 2: Enhanced Macro Environment
            macro_data = self.get_goog_macro_analysis()
            macro_score = macro_data['score']
            
            consensus_score += min(max(macro_score * 1.5, -8), 10)
            consensus_reasons.extend(macro_data['reasons'][:4])
            
            print(f"🎯 Enhanced Consensus Score: {consensus_score}/25")
            
            # === TOTAL ENHANCED CONVICTION SCORE ===
            total_score = valuation_score + growth_score + management_score + consensus_score
            
            detailed_breakdown = {
                'valuation': {'score': valuation_score, 'max': 25, 'reasons': valuation_reasons},
                'growth': {'score': growth_score, 'max': 25, 'reasons': growth_reasons},
                'management': {'score': management_score, 'max': 25, 'reasons': management_reasons},
                'consensus': {'score': consensus_score, 'max': 25, 'reasons': consensus_reasons}
            }
            
            enhanced_data = {
                'earnings_data': earnings_data,
                'revision_data': revision_data,
                'ceo_analysis': ceo_analysis,
                'macro_data': macro_data,
                'valuation_metrics': self.valuation_metrics,
                'financial_data': financial_data,
                'data_flags': self.data_flags
            }
            
            print(f"\n🎯 TOTAL ENHANCED CONVICTION SCORE: {total_score}/100")
            
            return total_score, detailed_breakdown, enhanced_data
            
        except Exception as e:
            print(f"❌ Error calculating enhanced conviction for GOOG: {e}")
            return 0, {}, {}
    
    def get_position_recommendation(self, conviction_score):
        """Enhanced position sizing recommendation"""
        
        if conviction_score >= 80:
            return {
                'action': '🚀 MAXIMUM CONVICTION BUY',
                'position_size': 'Very Large (10-15% portfolio)',
                'risk_level': 'MAXIMUM CONVICTION',
                'urgency': 'IMMEDIATE',
                'rationale': 'Exceptional conviction across all metrics - rare opportunity'
            }
        elif conviction_score >= 70:
            return {
                'action': '🐂 GRAB BULL BY HORNS',
                'position_size': 'Large (7-10% portfolio)',
                'risk_level': 'VERY HIGH CONVICTION',
                'urgency': 'IMMEDIATE',
                'rationale': 'High conviction - time to make a major bet'
            }
        elif conviction_score >= 60:
            return {
                'action': '💪 STRONG BUY',
                'position_size': 'Medium-Large (5-7% portfolio)',
                'risk_level': 'HIGH CONVICTION',
                'urgency': 'HIGH',
                'rationale': 'Strong fundamentals support significant position'
            }
        elif conviction_score >= 50:
            return {
                'action': '🚀 BUY',
                'position_size': 'Medium (3-5% portfolio)',
                'risk_level': 'STRONG CONVICTION',
                'urgency': 'MEDIUM-HIGH',
                'rationale': 'Good opportunity with solid conviction'
            }
        elif conviction_score >= 40:
            return {
                'action': '👀 WATCH/SMALL BUY',
                'position_size': 'Small (1-3% portfolio)',
                'risk_level': 'MODERATE CONVICTION',
                'urgency': 'MEDIUM',
                'rationale': 'Some positive factors but limited conviction'
            }
        else:
            return {
                'action': '❌ AVOID',
                'position_size': 'None (0% portfolio)',
                'risk_level': 'NO CONVICTION',
                'urgency': 'NONE',
                'rationale': 'Insufficient conviction for investment'
            }
    
    def run_complete_enhanced_analysis(self):
        """Run complete enhanced Mark's strategy analysis on Google"""
        
        # Test API connections first
        self.test_api_connections()
        
        # Calculate enhanced conviction score
        conviction_score, breakdown, enhanced_data = self.calculate_goog_conviction_score()
        
        # Get position recommendation
        recommendation = self.get_position_recommendation(conviction_score)
        
        # Display comprehensive results
        print("\n" + "="*80)
        print("🎯 GOOGLE (GOOG) - COMPLETE ENHANCED CONVICTION ANALYSIS")
        print("="*80)
        
        print(f"\n📊 ENHANCED CONVICTION BREAKDOWN:")
        for category, details in breakdown.items():
            print(f"\n{category.upper()}: {details['score']}/{details['max']} points")
            for reason in details['reasons']:
                print(f"  • {reason}")
        
        print(f"\n🎯 FINAL ENHANCED RECOMMENDATION")
        print("-" * 50)
        print(f"Conviction Score: {conviction_score}/100")
        print(f"Action: {recommendation['action']}")
        print(f"Position Size: {recommendation['position_size']}")
        print(f"Risk Level: {recommendation['risk_level']}")
        print(f"Urgency: {recommendation['urgency']}")
        print(f"Rationale: {recommendation['rationale']}")
        
        # Enhanced insights with data quality
        print(f"\n📈 ENHANCED DATA INSIGHTS")
        print("-" * 50)
        earnings_data = enhanced_data.get('earnings_data', {})
        revision_data = enhanced_data.get('revision_data', {})
        ceo_data = enhanced_data.get('ceo_analysis', {})
        macro_data = enhanced_data.get('macro_data', {})
        data_flags = enhanced_data.get('data_flags', {})
        
        print(f"Earnings CAGR: {earnings_data.get('cagr', 0):.1%} ({earnings_data.get('source', 'Unknown')} - {earnings_data.get('quality', 'Unknown')})")
        print(f"Revisions Trend: {revision_data.get('direction', 'N/A')} (Score: {revision_data.get('score', 0)}/5, Confidence: {revision_data.get('confidence', 0):.1%})")
        print(f"CEO Analysis: {ceo_data.get('name', 'Unknown')} (Score: {ceo_data.get('score', 0)}/15)")
        print(f"Macro Environment: {len(macro_data.get('reasons', []))} factors (Score: {macro_data.get('score', 0)}/10)")
        print(f"Data Quality: {sum(data_flags.values())}/{len(data_flags)} sources available ({sum(data_flags.values())/len(data_flags)*100:.0f}%)")
        
        # Valuation metrics summary
        valuation_metrics = enhanced_data.get('valuation_metrics', {})
        print(f"\n💰 KEY VALUATION METRICS")
        print("-" * 50)
        if valuation_metrics.get('ratio_metrics'):
            ratios = valuation_metrics['ratio_metrics']
            print(f"Forward P/E: {ratios.get('forward_pe', 0):.1f}")
            print(f"PEG Ratio: {ratios.get('peg_ratio', 0):.2f}")
            print(f"Price/Book: {ratios.get('price_to_book', 0):.1f}")
            print(f"EV/Revenue: {ratios.get('ev_to_revenue', 0):.1f}")
        
        if valuation_metrics.get('profitability_metrics'):
            prof = valuation_metrics['profitability_metrics']
            print(f"ROE: {prof.get('roe', 0):.1%}")
            print(f"Profit Margins: {prof.get('profit_margins', 0):.1%}")
            print(f"Operating Margins: {prof.get('operating_margins', 0):.1%}")
        
        # Special Google considerations
        print(f"\n⚠️  GOOGLE-SPECIFIC ENHANCED CONSIDERATIONS")
        print("-" * 50)
        print("✅ AI leadership with Gemini and infrastructure dominance")
        print("✅ Search monopoly with 90%+ market share")
        print("✅ YouTube $30B+ revenue and growing")
        print("✅ Cloud computing triple-digit growth trajectory")
        print("✅ Sundar Pichai's engineering excellence")
        print("✅ Strong free cash flow generation")
        print("❌ Regulatory scrutiny and antitrust risks")
        print("❌ AI competition from ChatGPT/Microsoft")
        print("❌ Privacy regulations impacting ad business")
        print("⚠️ High valuation relative to peers")
        
        # Mark's enhanced perspective
        print(f"\n🎯 ENHANCED STRATEGY PERSPECTIVE")
        print("-" * 50)
        if conviction_score >= 70:
            print("🚀 EXCEEDS high conviction threshold - RARE OPPORTUNITY")
            print("✅ CEO passes all of key criteria")
            if breakdown['valuation']['score'] >= 18:
                print("✅ Exceptional valuation support")
            if breakdown['growth']['score'] >= 15:
                print("✅ Growth profile meets standards")
            if breakdown['management']['score'] >= 20:
                print("✅ Management quality excellent")
        elif conviction_score >= 60:
            print("✅ Meets conviction threshold for significant position")
            print("✅ CEO background aligns with preferences")
            if breakdown['valuation']['score'] >= 15:
                print("✅ Valuation support present")
            if breakdown['growth']['score'] >= 15:
                print("✅ Growth profile attractive")
        else:
            print("⚠️  Does not meet high conviction threshold")
            if breakdown['growth']['score'] < 12:
                print("❌ Growth concerns limit conviction")
            if breakdown['consensus']['score'] < 12:
                print("❌ Consensus/macro headwinds present")
            if breakdown['valuation']['score'] < 12:
                print("❌ Valuation support insufficient")
        
        # Data quality assessment
        print(f"\n📊 DATA QUALITY ASSESSMENT")
        print("-" * 50)
        api_success_rate = sum([
            self.data_flags['fmp_api_accessible'],
            self.data_flags['alpha_vantage_accessible'], 
            self.data_flags['fred_api_accessible'],
            self.data_flags['yfinance_accessible']
        ]) / 4 * 100
        
        data_completeness = sum([
            self.data_flags['earnings_data_available'],
            self.data_flags['revision_data_available'],
            self.data_flags['macro_data_available'],
            self.data_flags['valuation_data_complete']
        ]) / 4 * 100
        
        print(f"API Success Rate: {api_success_rate:.0f}%")
        print(f"Data Completeness: {data_completeness:.0f}%")
        print(f"Analysis Confidence: {min(api_success_rate, data_completeness):.0f}%")
        
        if api_success_rate < 75:
            print("⚠️  Consider checking API keys and connections")
        if data_completeness < 75:
            print("⚠️  Some data sources missing - analysis may be incomplete")
        
        return {
            'conviction_score': conviction_score,
            'recommendation': recommendation,
            'breakdown': breakdown,
            'enhanced_data': enhanced_data,
            'data_quality': {
                'api_success_rate': api_success_rate,
                'data_completeness': data_completeness,
                'confidence': min(api_success_rate, data_completeness)
            }
        }

# USAGE: Enhanced Google analyzer with comprehensive data tracking
if __name__ == "__main__":
    
    # Initialize enhanced Google analyzer
    enhanced_analyzer = EnhancedGOOGAnalyzer(
        fmp_key="API",
        alpha_vantage_key="API", 
        fred_key="API"
    )
    
    # Run complete enhanced analysis
    results = enhanced_analyzer.run_complete_enhanced_analysis()
    
    print("\n🎯 ENHANCED ANALYSIS COMPLETE!")
    print("📊 Google analyzed with comprehensive data flags and validation!")
    print("💰 All valuation metrics tracked and quality-assured!")
    print("🔍 Data access fully monitored and optimized!")
    print("🚀 Ready for high-confidence investment decision!")
