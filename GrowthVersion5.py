#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests

def GrowthV5(filename, API_key):
    
    
    pd.options.display.max_rows = 99999
    
    tickerfile = pd.read_csv(filename)
    TickerSymbols = tickerfile['ticker']
    SeriestickerList = pd.Series(TickerSymbols)
    
    GV5list = []

    for ticker in SeriestickerList:
        try:
            
            # Enterprise Value components
            STurl = 'https://financialmodelingprep.com/api/v3/enterprise-values/'+ticker+'?limit=40&apikey='+API_key
            
            # transforming Enterprise Value API data into Pandas Dataframe
            STreq = requests.get(STurl)
            STdata = STreq.json()
            ST = pd.DataFrame(STdata)
            
            if len(ST['stockPrice']) >= 5 :
        
                # financial statement (Profit & Loss)
                PLurl = 'https://financialmodelingprep.com/api/v3/income-statement/'+ticker+'?limit=120&apikey='+API_key
              
                # transforming Profit & Loss Statement API data into Pandas Dataframe
                PLreq = requests.get(PLurl)
                PLdata = PLreq.json()
                PL = pd.DataFrame(PLdata)

                # Assigning KPIs to ProfitLoss DataFrame
                PL['EBITDA in %'] = PL['ebitda'] / PL['revenue'] * 100
                PL['Net Profit in %'] = PL['netIncome'] / PL['revenue'] * 100

                # Calculating 5 Year Revneue CAGR and YoY Revenue Growth
                RevCAGR = ((PL['revenue'].iloc[0] / PL['revenue'].iloc[4])**(1/5)-1)*100
                RevYoY = ((PL['revenue'].iloc[0] / PL['revenue'].iloc[1])-1)*100

                # Calculating EBITDA-Expense CAGR and YoY EBITDAex YoY Growth
                PL['EBITDAex'] = PL['revenue'] - PL['ebitda'] 
                EBITDAexCAGR = ((PL['EBITDAex'].iloc[0] / PL['EBITDAex'].iloc[4])**(1/5)-1)*100
                EBITDAexYoY = ((PL['EBITDAex'].iloc[0] / PL['EBITDAex'].iloc[1])-1)*100

                DeltaEBITDAexRevCAGR = RevCAGR - EBITDAexCAGR
                DelatEBITDAexRevYoY = RevYoY - EBITDAexYoY
        
                if RevCAGR >= 50 and RevYoY > 0 and DeltaEBITDAexRevCAGR >= 10 and EBITDAexYoY > 0 and PL['ebitda'].iloc[0] < 0 and PL['ebitda'].iloc[1] < 0 and PL['ebitda'].iloc[4] < 0:
                    GV5list.append(ticker)
        except:
            continue
    dfGV5 = pd.Series(GV5list)
    dfGV5.to_csv('Screened_'+filename)

GrowthV5(CSV-File, API_key) # <- add CSV-File and API_key here
