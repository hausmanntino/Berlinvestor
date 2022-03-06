#!/usr/bin/env python
# coding: utf-8

# In[12]:


# loading mysql connector, mysql connector Error & pandas
import mysql.connector
from mysql.connector import Error
import pandas as pd
from pandas.io import sql
import sqlalchemy
import mysql.connector
from sqlalchemy import create_engine
import pymysql

import pandas as pd
import requests

# defining function to create server connection

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    
    return connection

# defining fuction to create database

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")
        
# defining function to create database connection

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
        
    return connection

# defining function to execute query

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successfull")
    except Error as err:
        print(f"Error: '{err}'")

###################################################################
# REPLACE HOST_NAME, USR_NAME & USER_PASSWORD BELOW IN CONNECTION #
###################################################################

connection = create_server_connection(HOST_NAME, USER_NAME, USER_PASSWORD) # <-- here

create_database_query = "CREATE DATABASE financial"
create_database(connection, create_database_query)

def load_data_to_database(filename, API_KEY):
    
    pd.options.display.max_rows = 99999
    
    tickerfile = pd.read_csv(filename)
    TickerSymbols = tickerfile['ticker']
    SeriestickerList = pd.Series(TickerSymbols)
    
    ###############################
    # Defining SQL ALCHEMY ENGINE #
    ###############################

    engine= create_engine("mysql+pymysql://{user}:{pw}@"HOST_NAME"/{db}"
                         .format(user = USER,
                                pw = USER_PASSWORD,
                                db = "financial"))

    
    # loading tickersymbols into company table
    TickerSymbols.to_sql('company', con = engine, if_exists = 'append', chunksize = 1000)
    
    #######################################
    # FINANCIAL MODEL PREP API Connection #
    #######################################

    
    for ticker in SeriestickerList:
        
        try:
    
            # financial statements (Profit & Loss, Balance Sheet, Cashflow Statement) Enterprise Value Components, Stock Quote & Companyprofile

            PLurl = 'https://financialmodelingprep.com/api/v3/income-statement/'+ticker+'?limit=120&apikey='+API_KEY
            CFurl = 'https://financialmodelingprep.com/api/v3/cash-flow-statement/'+ticker+'?limit=120&apikey='+API_KEY
            BLurl = 'https://financialmodelingprep.com/api/v3/balance-sheet-statement/'+ticker+'?limit=120&apikey='+API_KEY
            STurl = 'https://financialmodelingprep.com/api/v3/enterprise-values/'+ticker+'?limit=40&apikey='+API_KEY
            Profileurl = 'https://financialmodelingprep.com/api/v3/profile/'+ticker+'?apikey='+API_KEY
            Quoteurl = 'https://financialmodelingprep.com/api/v3/quote/'+ticker+'?apikey='+API_KEY

            # transforming Profit & Loss Statement API data into Pandas Dataframe
            PLreq = requests.get(PLurl)
            PLdata = PLreq.json()
            PL = pd.DataFrame(PLdata)

            # transforming Balance Sheet Statement API data into Pandas Dataframe
            BLreq = requests.get(BLurl)
            BLdata = BLreq.json()
            BL = pd.DataFrame(BLdata)

            # transforming Cashflow Statement API data into Pandas Dataframe
            CFreq = requests.get(CFurl)
            CFdata = CFreq.json()
            CF = pd.DataFrame(CFdata)

            # transforming Enterprise Value API data into Pandas Dataframe
            STreq = requests.get(STurl)
            STdata = STreq.json()
            ST = pd.DataFrame(STdata)

            # transforming Profile API data into Pandas Dataframe
            Profilereq = requests.get(Profileurl)
            Profiledata = Profilereq.json()
            Profile = pd.DataFrame(Profiledata)
            Profile = Profile.rename(columns={"range": "PriceRange"})

            # transforming Stock Quote API data into Pandas Dataframe
            Quotereq = requests.get(Quoteurl)
            Quotedata = Quotereq.json()
            Quote = pd.DataFrame(Quotedata)
            Quote = Quote.rename(columns={"change": "changeAbs"})

            # LOADING DATAFRAME TO MYSQL DATABASE TABLE

            PL.to_sql('profitloss', con = engine, if_exists = 'append', chunksize = 1000)
            BL.to_sql('balancesheet', con = engine, if_exists = 'append', chunksize = 1000)
            CF.to_sql('cashflow', con = engine, if_exists = 'append', chunksize = 1000)
            ST.to_sql('enterprisevalue', con = engine, if_exists = 'append', chunksize = 1000)
            Profile.to_sql('profile', con = engine, if_exists = 'append', chunksize = 1000)
            Quote.to_sql('stockquote', con = engine, if_exists = 'append', chunksize = 1000)
        except:
            continue

load_data_to_database('ticker.csv')

use_database = """
USE financial;
"""
modify_profitloss_table = """
ALTER TABLE profitloss
	MODIFY COLUMN date DATETIME,
	MODIFY COLUMN symbol VARCHAR(30),
	MODIFY COLUMN reportedCurrency VARCHAR(30),
	MODIFY COLUMN cik VARCHAR(30),
	MODIFY COLUMN fillingDate DATE,
	MODIFY COLUMN acceptedDate DATETIME,
	MODIFY COLUMN calendarYear INT,
	MODIFY COLUMN period VARCHAR(10),
	MODIFY COLUMN revenue BIGINT,
	MODIFY COLUMN costOfRevenue BIGINT,
	MODIFY COLUMN grossProfit BIGINT,
	MODIFY COLUMN grossProfitRatio DECIMAL(20,2),
	MODIFY COLUMN researchAndDevelopmentExpenses BIGINT,
	MODIFY COLUMN generalAndAdministrativeExpenses BIGINT,
	MODIFY COLUMN sellingAndMarketingExpenses BIGINT,
	MODIFY COLUMN sellingGeneralAndAdministrativeExpenses BIGINT,
	MODIFY COLUMN otherExpenses BIGINT,
	MODIFY COLUMN operatingExpenses BIGINT,
	MODIFY COLUMN costAndExpenses BIGINT,
	MODIFY COLUMN interestIncome BIGINT,
	MODIFY COLUMN interestExpense BIGINT,
	MODIFY COLUMN depreciationAndAmortization BIGINT,
	MODIFY COLUMN ebitda BIGINT,
	MODIFY COLUMN ebitdaratio DECIMAL(20,2),
	MODIFY COLUMN operatingIncome BIGINT,
	MODIFY COLUMN operatingIncomeRatio DECIMAL(20,2),
	MODIFY COLUMN totalOtherIncomeExpensesNet BIGINT,
	MODIFY COLUMN incomeBeforeTax BIGINT,
	MODIFY COLUMN incomeBeforeTaxRatio DECIMAL(20,2),
	MODIFY COLUMN incomeTaxExpense BIGINT,
	MODIFY COLUMN netIncome BIGINT,
	MODIFY COLUMN netIncomeRatio DECIMAL(20,2),
	MODIFY COLUMN eps DECIMAL(20,2),
	MODIFY COLUMN epsdiluted DECIMAL(20,2),
	MODIFY COLUMN weightedAverageShsOut BIGINT,
	MODIFY COLUMN weightedAverageShsOutDil BIGINT,
	MODIFY COLUMN link VARCHAR(100),
	MODIFY COLUMN finalLink VARCHAR(100);
"""

modify_balancesheet_table = """
ALTER TABLE balancesheet
    MODIFY COLUMN date DATETIME,
    MODIFY COLUMN symbol VARCHAR(30),
    MODIFY COLUMN reportedCurrency VARCHAR(30),
    MODIFY COLUMN cik VARCHAR(30),
    MODIFY COLUMN fillingDate DATE,
    MODIFY COLUMN acceptedDate DATETIME,
    MODIFY COLUMN calendarYear BIGINT,
    MODIFY COLUMN period VARCHAR(10),
    MODIFY COLUMN cashAndCashEquivalents BIGINT,
    MODIFY COLUMN shortTermInvestments BIGINT,
    MODIFY COLUMN cashAndShortTermInvestments BIGINT,
    MODIFY COLUMN netReceivables BIGINT,
    MODIFY COLUMN inventory BIGINT,
    MODIFY COLUMN otherCurrentAssets BIGINT,
    MODIFY COLUMN totalCurrentAssets BIGINT,
    MODIFY COLUMN propertyPlantEquipmentNet BIGINT,
    MODIFY COLUMN goodwill BIGINT,
    MODIFY COLUMN intangibleAssets BIGINT,
    MODIFY COLUMN goodwillAndIntangibleAssets BIGINT,
    MODIFY COLUMN longTermInvestments BIGINT,
    MODIFY COLUMN taxAssets BIGINT,
    MODIFY COLUMN otherNonCurrentAssets BIGINT,
    MODIFY COLUMN totalNonCurrentAssets BIGINT,
    MODIFY COLUMN otherAssets BIGINT,
    MODIFY COLUMN totalAssets BIGINT,
    MODIFY COLUMN accountPayables BIGINT,
    MODIFY COLUMN shortTermDebt BIGINT,
    MODIFY COLUMN taxPayables BIGINT,
    MODIFY COLUMN deferredRevenue BIGINT,
    MODIFY COLUMN otherCurrentLiabilities BIGINT,
    MODIFY COLUMN totalCurrentLiabilities BIGINT,
    MODIFY COLUMN longTermDebt BIGINT,
    MODIFY COLUMN deferredRevenueNonCurrent BIGINT,
    MODIFY COLUMN deferredTaxLiabilitiesNonCurrent BIGINT,
    MODIFY COLUMN otherNonCurrentLiabilities BIGINT,
    MODIFY COLUMN totalNonCurrentLiabilities BIGINT,
    MODIFY COLUMN otherLiabilities BIGINT,
    MODIFY COLUMN capitalLeaseObligations BIGINT,
    MODIFY COLUMN totalLiabilities BIGINT,
    MODIFY COLUMN preferredStock BIGINT,
    MODIFY COLUMN commonStock BIGINT,
    MODIFY COLUMN retainedEarnings BIGINT,
    MODIFY COLUMN accumulatedOtherComprehensiveIncomeLoss BIGINT,
    MODIFY COLUMN othertotalStockholdersEquity BIGINT,
    MODIFY COLUMN totalStockholdersEquity BIGINT,
    MODIFY COLUMN totalLiabilitiesAndStockholdersEquity BIGINT,
    MODIFY COLUMN minorityInterest BIGINT,
    MODIFY COLUMN totalEquity BIGINT,
    MODIFY COLUMN totalLiabilitiesAndTotalEquity BIGINT,
    MODIFY COLUMN totalInvestments BIGINT,
    MODIFY COLUMN totalDebt BIGINT,
    MODIFY COLUMN netDebt BIGINT,
    MODIFY COLUMN link VARCHAR(100),
    MODIFY COLUMN finalLink VARCHAR(100)
"""

modify_cashflow_table = """
ALTER TABLE cashflow
    MODIFY COLUMN date DATETIME,
    MODIFY COLUMN symbol VARCHAR(30),
    MODIFY COLUMN reportedCurrency VARCHAR(30),
    MODIFY COLUMN cik VARCHAR(30),
    MODIFY COLUMN fillingDate DATE,
    MODIFY COLUMN acceptedDate DATETIME,
    MODIFY COLUMN calendarYear BIGINT,
    MODIFY COLUMN period VARCHAR(10),
    MODIFY COLUMN netIncome BIGINT,
    MODIFY COLUMN depreciationAndAmortization BIGINT,
    MODIFY COLUMN deferredIncomeTax BIGINT,
    MODIFY COLUMN stockBasedCompensation BIGINT,
    MODIFY COLUMN changeInWorkingCapital BIGINT,
    MODIFY COLUMN accountsReceivables BIGINT,
    MODIFY COLUMN inventory BIGINT,
    MODIFY COLUMN accountsPayables BIGINT,
    MODIFY COLUMN otherWorkingCapital BIGINT,
    MODIFY COLUMN otherNonCashItems BIGINT,
    MODIFY COLUMN netCashProvidedByOperatingActivities BIGINT,
    MODIFY COLUMN investmentsInPropertyPlantAndEquipment BIGINT,
    MODIFY COLUMN acquisitionsNet BIGINT,
    MODIFY COLUMN purchasesOfInvestments BIGINT,
    MODIFY COLUMN salesMaturitiesOfInvestments BIGINT,
    MODIFY COLUMN otherInvestingActivites BIGINT,
    MODIFY COLUMN netCashUsedForInvestingActivites BIGINT,
    MODIFY COLUMN debtRepayment BIGINT,
    MODIFY COLUMN commonStockIssued BIGINT,
    MODIFY COLUMN commonStockRepurchased BIGINT,
    MODIFY COLUMN dividendsPaid BIGINT,
    MODIFY COLUMN otherFinancingActivites BIGINT,
    MODIFY COLUMN netCashUsedProvidedByFinancingActivities BIGINT,
    MODIFY COLUMN effectOfForexChangesOnCash BIGINT,
    MODIFY COLUMN netChangeInCash BIGINT,
    MODIFY COLUMN cashAtEndOfPeriod BIGINT,
    MODIFY COLUMN cashAtBeginningOfPeriod BIGINT,
    MODIFY COLUMN operatingCashFlow BIGINT,
    MODIFY COLUMN capitalExpenditure BIGINT,
    MODIFY COLUMN freeCashFlow BIGINT,
    MODIFY COLUMN link VARCHAR(100),
    MODIFY COLUMN finalLink VARCHAR(100);
"""

modify_enterprisevalue_table = """
ALTER TABLE enterprisevalue
    MODIFY COLUMN symbol VARCHAR(30),
    MODIFY COLUMN date DATETIME,
    MODIFY COLUMN stockPrice DECIMAL(20,2),
    MODIFY COLUMN numberOfShares BIGINT,
    MODIFY COLUMN marketCapitalization BIGINT,
    MODIFY COLUMN minusCashAndCashEquivalents BIGINT,
    MODIFY COLUMN addTotalDebt BIGINT,
    MODIFY COLUMN enterpriseValue BIGINT
"""

modify_stockquote_table = """
ALTER TABLE stockquote
    MODIFY COLUMN symbol VARCHAR(30),
    MODIFY COLUMN name VARCHAR(100),
    MODIFY COLUMN price DECIMAL(20,2),
    MODIFY COLUMN changesPercentage DECIMAL(20,2),
    MODIFY COLUMN changeAbs DECIMAL(20,2),
    MODIFY COLUMN dayLow DECIMAL(20,2),
    MODIFY COLUMN dayHigh DECIMAL(20,2),
    MODIFY COLUMN yearHigh DECIMAL(20,2),
    MODIFY COLUMN yearLow DECIMAL(20,2),
    MODIFY COLUMN marketCap BIGINT,
    MODIFY COLUMN priceAvg50 DECIMAL(20,2),
    MODIFY COLUMN priceAvg200 DECIMAL(20,2),
    MODIFY COLUMN volume BIGINT,
    MODIFY COLUMN avgVolume BIGINT,
    MODIFY COLUMN exchange VARCHAR(20),
    MODIFY COLUMN open DECIMAL(20,2),
    MODIFY COLUMN previousClose DECIMAL(20,2),
    MODIFY COLUMN eps DECIMAL(20,2),
    MODIFY COLUMN pe DECIMAL(20,2),
    MODIFY COLUMN earningsAnnouncement VARCHAR(30),
    MODIFY COLUMN sharesOutstanding BIGINT,
    MODIFY COLUMN timestamp BIGINT;
"""

modify_profile_table = """
ALTER TABLE profile
    MODIFY COLUMN symbol VARCHAR(30),
    MODIFY COLUMN price DECIMAL(20,2),
    MODIFY COLUMN beta DECIMAL(20,2),
    MODIFY COLUMN volAvg BIGINT,
    MODIFY COLUMN mktCap BIGINT,
    MODIFY COLUMN lastDiv DECIMAL(20,2),
    MODIFY COLUMN PriceRange VARCHAR(30),
    MODIFY COLUMN changes DECIMAL(20,2),
    MODIFY COLUMN companyName VARCHAR(100),
    MODIFY COLUMN currency VARCHAR(30),
    MODIFY COLUMN cik VARCHAR(30),
    MODIFY COLUMN isin VARCHAR(30),
    MODIFY COLUMN cusip VARCHAR(30),
    MODIFY COLUMN exchange VARCHAR(30),
    MODIFY COLUMN exchangeShortName VARCHAR(30),
    MODIFY COLUMN industry VARCHAR(30),
    MODIFY COLUMN website VARCHAR(30),
    MODIFY COLUMN description VARCHAR(2000),
    MODIFY COLUMN ceo VARCHAR(30),
    MODIFY COLUMN sector VARCHAR(30),
    MODIFY COLUMN country VARCHAR(30),
    MODIFY COLUMN fullTimeEmployees BIGINT,
    MODIFY COLUMN phone VARCHAR(30),
    MODIFY COLUMN address VARCHAR(30),
    MODIFY COLUMN city VARCHAR(30),
    MODIFY COLUMN state VARCHAR(30),
    MODIFY COLUMN zip VARCHAR(30),
    MODIFY COLUMN dcfDiff DECIMAL(20,2),
    MODIFY COLUMN dcf DECIMAL(20,2),
    MODIFY COLUMN image VARCHAR(100),
    MODIFY COLUMN ipoDate DATETIME,
    MODIFY COLUMN defaultImage VARCHAR(30),
    MODIFY COLUMN isEtf TINYINT,
    MODIFY COLUMN isActivelyTrading TINYINT,
    MODIFY COLUMN isAdr TINYINT,
    MODIFY COLUMN isFund TINYINT
"""

execute_query(connection, use_database)
execute_query(connection, modify_profitloss_table)
execute_query(connection, modify_balancesheet_table)
execute_query(connection, modify_cashflow_table)
execute_query(connection, modify_enterprisevalue_table)
execute_query(connection, modify_stockquote_table)
execute_query(connection, modify_profile_table)

