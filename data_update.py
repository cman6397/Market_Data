import csv
import urllib
import pandas as pd
import numpy as np
import sys
import datetime
pd.core.common.is_list_like = pd.api.types.is_list_like
from pandas_datareader import data as pdr
from googlefinance.client import get_price_data, get_prices_data, get_prices_time_data
import fix_yahoo_finance as yf
import pyodbc 
import os
yf.pdr_override()

#pull yahoo finance historical data for all companies with positive Market Capital using exchange list. 
def fill_history(start_date,end_date,df,directory_prices,num_runs,cnxn,directory_ticker):
	count=0
	update_tickers(df,end_date,cnxn,directory_ticker)
	
	failed_downloads=[]
	success_downloads=[]

	for i,row in df.iterrows():
		if count % 100 == 0:
			print(count," Time: " + str(datetime.datetime.now()))
		count+=1
		symbol=i
		market_cap=row['MarketCap']
		nasdaq_price=row['LastSale']
		sector=row['Sector']
		try:
			query_yahoo(symbol,start_date,end_date,directory_prices)
			success_downloads.append(symbol)
		except Exception:
			failed_downloads.append(symbol)
				
	for x in range (0,num_runs):
		print("Total Failed Downloads: " + str(len(failed_downloads)) + " Total Success Downloads: " + str(len(success_downloads)) + " Time: " + str(datetime.datetime.now()))
		failed_downloads,success_downloads=fill_hist_help(start_date,end_date,failed_downloads,directory_prices,success_downloads)
		
	return (failed_downloads,success_downloads)

#Might try to add some kind of timeout function to this snipit  
def query_yahoo(symbol,start_date,end_date,directory):
	df_yahoo=pdr.get_data_yahoo(symbol,start_date,end_date,progress=False)
	df_yahoo['ticker']=symbol
	df_yahoo.to_csv(directory + '\\' + symbol +'.csv')
	

#for re-running failed downloads
def fill_hist_help(start_date,end_date,symbols,directory,success_downloads):
	failed_downloads=[]
	for symbol in symbols:
		try:
			query_yahoo(symbol,start_date,end_date,directory)
			success_downloads.append(symbol)
		except Exception:
			failed_downloads.append(symbol)
	return failed_downloads,success_downloads

#write yahoo stock historicall data csv files to one table in database
def write_to_sql(cnxn,directory):
	count=0
	cursor = cnxn.cursor()
	SQL_string=r"""Truncate table [stock_Data].[dbo].[Hist_Data]"""
	cursor.execute(SQL_string)
	cursor.close()
	
	for filename in os.listdir(directory):
		
		#just for general progress tricking
		count+=1
		if count % 100 == 0:
			print(str(count))

		if filename.endswith(".csv"): 
			try:
				ticker=filename[:-4]
				csv_path= "saved_data" + "\\" + filename
				cursor = cnxn.cursor()
	
				SQL_string= ("BULK INSERT [Stock_Data].[dbo].[Hist_Data] FROM '""" + 
				directory + "\\" + filename + """' WITH 
				(FIRSTROW=2,FIELDTERMINATOR = ',',ROWTERMINATOR = '\\n')""")
				
				cursor.execute(SQL_string)
				cnxn.commit()
				cursor.close()
			
			except Exception:
				print(filename[:-4])
				cursor.close()
			continue
		else:
			continue
	cnxn.close()
			


	#save marketcaps and update company master list in database
def update_tickers(df,end_date,cnxn,directory):
	df=df.where((pd.notnull(df)), None)
	df['Name']=df['Name'].str.replace(","," ")
	df['Sector']=df['Sector'].str.replace(","," ")
	df['Industry']=df['Industry'].str.replace(","," ")
	
	df[['Name','LastSale','MarketCap','ADR TSO','IPOyear','Sector','Industry']].to_csv(directory + r'\Tickers.csv')
	df['Date'] = end_date
	
	df[['MarketCap','Date']].to_csv(directory + r'\Tickers_Market_Cap.csv')
	
	cursor = cnxn.cursor()
	
	SQL_String= """Truncate Table Market_Cap_Staging"""
	
	cursor.execute(SQL_String)
	cnxn.commit()
	
	SQL_String = r"""BULK INSERT [Stock_Data].[dbo].[Market_Cap_Staging] 
	FROM""" + """ '""" + directory + """\Tickers_Market_Cap.csv' """ + """WITH (FIRSTROW=2,FIELDTERMINATOR = ',',ROWTERMINATOR = '\\n')"""
	
	cursor.execute(SQL_String)
	cnxn.commit()
	
	SQL_String="""Merge [Stock_Data].[dbo].[Market_Cap_Hist] as targ
	Using [Stock_Data].[dbo].[Market_Cap_Staging] as srce on targ.Ticker=srce.Ticker and targ.Date=srce.Date
	When Matched then Update Set targ.MarketCap=srce.MarketCap
	When Not Matched then Insert (Ticker,MarketCap,Date) Values (srce.Ticker,srce.MarketCap,srce.Date);"""

	cursor.execute(SQL_String)
	cnxn.commit()
	
	SQL_String= """Truncate Table Company_List_Staging"""

	cursor.execute(SQL_String)
	cnxn.commit()
	
	SQL_String = r"""BULK INSERT [Stock_Data].[dbo].[Company_List_Staging] 
	FROM""" + """ '""" + directory + """\Tickers.csv' """ + """WITH (FIRSTROW=2,FIELDTERMINATOR = ',',ROWTERMINATOR = '\\n')"""
	cursor.execute(SQL_String)
	cnxn.commit()

	SQL_String="""Merge [Stock_Data].[dbo].[Company_List] as targ
	Using [Stock_Data].[dbo].[Company_List_Staging] as srce on targ.Ticker=srce.Ticker
	When Matched then Update Set targ.LastSale=srce.lastSale,targ.MarketCap=srce.MarketCap,targ.ADR_TSO=srce.ADR_TSO
	When Not Matched then Insert (Ticker,Name,LastSale,MarketCap,ADR_TSO,IPOyear,Sector,Industry,Date_Added) Values (srce.Ticker,srce.Name,srce.LastSale,srce.MarketCap,srce.ADR_TSO,srce.IPOyear,srce.Sector,srce.Industry,GetDate());"""

	cursor.execute(SQL_String)
	cnxn.commit()
	cursor.close()
	cnxn.close()

def get_tickers():
	#Download lists of all companies in All US exchanges.  Catch Error if urls become outdated.  
	NASDAQ_url = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download'
	NYSE_url = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download'
	AMEX_url = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download'
	try:
		response = urllib.request.urlopen(NASDAQ_url)
		NASDAQ_df = pd.read_csv(response)
		
		response = urllib.request.urlopen(NYSE_url)
		NYSE_df = pd.read_csv(response)
		
		response = urllib.request.urlopen(AMEX_url)
		AMEX_df = pd.read_csv(response)
		
		df=NASDAQ_df
		df=df.append(NYSE_df)
		df=df.append(AMEX_df)

		df=df.drop_duplicates('Symbol')
		df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
		df=df.set_index('Symbol')
		df=df.sort_values(by=['MarketCap'],ascending=False)
		
		#trim white space
		
		
		#Only want companies with positive market cap
		df=df[df['MarketCap']>0]
		
	except Exception:
		print("Download Fail")

	return df


#Update historical data and add new stock data to database
if __name__ == '__main__': 
	df=get_tickers()
	cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
					  "Server=ADMIN-PC\SQLEXPRESS;"
					  "Database=Stock_Data;"
					  "Trusted_Connection=yes;")

	end_date=today=datetime.datetime.now()
	start_date= '2000-01-01'
	directory_prices = r'C:\Users\Admin\Documents\Market_Data\saved_data'
	directory_ticker=r'C:\Users\Admin\Documents\Market_Data'
	num_runs=10
	successful_downloads,failed_downloads=fill_history(start_date,end_date,df,directory_prices,num_runs,cnxn,directory_ticker)