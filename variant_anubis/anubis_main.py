"""__main__production__code . Have a break and enjoy the automation..... Processes the pulled data.
		-----------------cleaning, rinsing,washing and ironing-------------------  
"""

import os
import pandas as pd
import requests
import glob
import shutil


cwd=os.getcwd()
master_data = cwd + "/master_anubis_main.csv"
directory_to_bhav = cwd + "/BHAV_CPY/"
directory_to_oi = cwd + "/OI/"
directory_to_vol = cwd + "/VOLATILITY/"
directory_to_report = cwd + "/REPORT/"


def Csv_File_Creation_Report():
	only_file = [os.listdir(directory_to_bhav),os.listdir(directory_to_vol),os.listdir(directory_to_oi)]
	volatility = only_file[1][0]  #used as a reference for creating the report dir,***do not change*** must exist.

	def Csv_Calc():
			global data    ###literally no point in using global, can be modified

			data = pd.read_csv(directory_to_vol+only_file[1][0],usecols=[0,1,2,6,8,12])	   
			data = data.drop(data.index[[24,58,68,102,146,147,148,149,150,151,179]])

			no_of_stocks = data.shape[:1]
			Csv_File_Creation_Report.no_of_stocks = no_of_stocks    ###var to be used outside this  function
			
			name_of_stocks = data.ix[:,1]   
			name_of_stocks = name_of_stocks.reset_index()		###preventing unwanted contamination

			name_of_stocks = name_of_stocks.ix[:,1]   		###cleaned stock names ranging from index 0-208
			Csv_File_Creation_Report.name_of_stocks=name_of_stocks  ###var to be used outside the function

			range_of_stocks = range(0,220)				###221 is the index in Vol file
			list_of_stocks = (list(name_of_stocks))

			Csv_File_Creation_Report.list_of_stocks = list_of_stocks

			master_df = ['Date','Symbol','Underlying_close_price','Underlying_daily_volatility','Futures_close_price','Futures_daily_volatility','OI','OI_change','Futures_price_change','OI_fut&opt','OI_change_fut&opt']
			empty_ = pd.DataFrame(index=range_of_stocks,columns=master_df)    ###used for the intial col name creation of csv

			for i in range(0,209):
					empty_.to_csv(directory_to_report +'/{}.csv'.format(list_of_stocks[i]),sep='\t', encoding='utf-8')
	
	if os.path.exists(directory_to_report):				
		Csv_Calc()
		print "If part------->csv file creation report"										#debugger

	elif not os.path.exists(directory_to_report): 
		print "Directory not found, creating new one ===>Csv_Calc defn"     				#debugger
		os.mkdir(directory_to_report)
		Csv_Calc()		

def Data_Processing():

	only_file = [os.listdir(directory_to_bhav),os.listdir(directory_to_vol),os.listdir(directory_to_oi)]

	data = pd.read_csv(directory_to_vol+only_file[1][0],usecols=[0,1,2,6,8,12])	 ###acts as a main df for reading the content and storing
	data = data.drop(data.index[[24,58,68,102,146,147,148,149,150,151,179]])
	
	
	for i in range(0,3):
		if i==0:
			if os.path.exists(directory_to_bhav + only_file[i][0]): 			#debugger
				print "Bhav file exists---------->"
				print "processing {} the path".format(directory_to_bhav + only_file[i][0])
				#print (directory_to_bhav + only_file[i][0]) #last index reads a different .csv file in that dir, first index for the array no(dont change it).

				data_bhav  = pd.read_csv(directory_to_bhav + only_file[i][0],usecols=[1,10,12])
				data_bhav_cpy = data_bhav.copy()
				final_bhav = data_bhav_cpy.ix[42:668]		###original bhav contains that particular range of rows as desired one. If error found do check this range
				final_bhav = final_bhav.reset_index()		###final_bhav and the new_df refers to the same DataFrame changed the df for better understanding and preserving the original
				total_rows = final_bhav.shape[:1]
				final_bhav = final_bhav.ix[:,1:4]
				final_bhav["CONTRACTS_TOTAL"] = 0
				final_bhav["OI_TOTAL"] = 0

				def Cross_Calc():
					for i in range(0,627,3):
						final_bhav["CONTRACTS_TOTAL"].iloc[i] = (final_bhav.CONTRACTS.iloc[i] + final_bhav.CONTRACTS.iloc[i+1] + final_bhav.CONTRACTS.iloc[i+2])
						final_bhav["OI_TOTAL"].iloc[i] = (final_bhav.OPEN_INT.iloc[i] + final_bhav.OPEN_INT.iloc[i+1] + final_bhav.OPEN_INT.iloc[i+2])
						new_df = final_bhav.drop(final_bhav.columns[[0, 1,2]], axis=1)
						new_df = new_df[new_df>0]		
						new_df = new_df.dropna(axis=0,how='all')
						new_df = new_df.reset_index()
						global newton
						newton = new_df.ix[:,1:3]  #keep newton as a main df in the later part we can append the data to main_column
					
				Cross_Calc() 
				   #intiate a processing and retun a df with cleaned and needed data
			else:
				print "Bhav file doesn't exist"
				
		elif i==1:
			if os.path.exists(directory_to_vol + only_file[i][0]):
				print "vol file exists------------>"
				print "processing {} the path".format(directory_to_vol + only_file[i][0])
				data_temp = data
				data_temp = data_temp.reset_index()
				#print data_temp.head(60)

				data_temp.columns = ['index','DATE','SYMBOL','UNDERLYING_CLOSE_PRICE_A','A_VOLATILITY','FUT_CLOSE_PRICE_B','B_VOLATILITY']
				data_vol = data_temp
				data_vol = data_vol.loc[:,['UNDERLYING_CLOSE_PRICE_A','A_VOLATILITY','FUT_CLOSE_PRICE_B','B_VOLATILITY']]
				global newton_cpy         
				newton_cpy = newton             			###vale passing and chained merging of df from up else i==0
				newton_cpy = newton_cpy.join(data_vol)
			else:
				print "vol file doesn't exist"
		
		elif i==2:
			if os.path.exists(directory_to_oi+ only_file[i][0]):
				data_oi = pd.read_csv(directory_to_oi + only_file[i][0])
				print "Oi file exists------------->"
				print "Processing {} the path".format(directory_to_oi + only_file[i][0])
				
				data_oi.columns = ['DATE','ISIN','SCRIP_NAME','NSE_SYMBOL','MWPL','OI_FUT_OPT']
				data_oi = data_oi.sort_values(by=['NSE_SYMBOL'],ascending = [True])
				data_oi = data_oi.loc[:,['DATE','NSE_SYMBOL','MWPL','OI_FUT_OPT']]  #column of interest
				data_oi = data_oi.reset_index()
				data_oi = data_oi.loc[:,['DATE','NSE_SYMBOL','MWPL','OI_FUT_OPT']]
				data_oi = data_oi.join(newton_cpy)
				global master_df_main
				master_df_main = data_oi

				"""ADD COL NUMBER TO THE BELOW DF"""
				master_df_main['FUT_PRICE_CHANGE'] = 0
				master_df_main['OI_CHANGE'] = 0
				master_df_main['MWPL_CHANGE'] = 0
			else:
				print "Oi file doesn't exist"

def Change_Calculator():

	"""function which calculates the last 4 columns in the report """

	if not os.path.exists(master_data):
		print "Not existing------------>"
	else:
		zerbra_data = pd.read_csv(master_data)
		#print zerbra_data.ix[0,:]
		#print zerbra_data.head()
		print "File present------------>"
		print "Moving forward---------->"

		"""Calc part, flexible usuage for introducing new col in the report, modify segment flexibility and add a new col here"""

		master_df_main["FUT_PRICE_CHANGE"] = ((master_df_main.FUT_CLOSE_PRICE_B - zerbra_data.FUT_CLOSE_PRICE_B) / zerbra_data.FUT_CLOSE_PRICE_B) * 100
		master_df_main['OI_CHANGE'] = ((master_df_main.OI_TOTAL - zerbra_data.OI_TOTAL) / zerbra_data.OI_TOTAL) * 100 
		master_df_main['MWPL_CHANGE'] = 100-(((master_df_main.MWPL - master_df_main.OI_FUT_OPT) / master_df_main.MWPL) * 100)
		master_df_main['OI_FUT_OPT_CHANGE'] = ((master_df_main.OI_FUT_OPT - zerbra_data.OI_FUT_OPT) / zerbra_data.OI_FUT_OPT) * 100


def Writing_Processed_Values_new():      ###gets executed for the first time
	sorted_files = sorted(os.listdir(directory_to_report))
	
	for i in range(0,209):
		#if os.path.splitext(sorted_files[i])[0] == newton.STOCK_NAME.iloc[i])
		master_df_main.iloc[[i]].to_csv(directory_to_report + sorted_files[i])
		
			
def Writing_Processed_Values_old(): 	 ###gets executed if files are present and for the second time
	sorted_files = sorted(os.listdir(directory_to_report))

	for i in range(0,209):
		master_df_main.iloc[[i]].to_csv(directory_to_report + sorted_files[i],mode = 'a',header = False)
		

def Sweet_Honey(): 

	if not os.path.exists (directory_to_report):
		print "Dirctory to report does not exist.....Creating new one"
		Csv_File_Creation_Report()
		print "file creation complete"
		print "=======" * 10
		Data_Processing()
		print "=======" * 10
		print "processing the file and rewriting the csv"
		Writing_Processed_Values_new()
		print "=====End of the program=====" 
		#print master_df_main.shape
		master_df_main.to_csv(cwd + "/master_anubis_main.csv")
		#print master_df_main.tail(10)

	else:
		print "Directory Exists...Proceeding according to the flow"
		#Csv_File_Creation()
		print "file creation complete"
		print "=======" * 10
		Data_Processing()
		print "=======" * 10
		print "processing the file and rewriting the csv"
		print "migth take some for the cal part........"
		Change_Calculator()	
		print "+++++++Almost over++++++++++"
		Writing_Processed_Values_old()
		print "Everything has been pulled, cleaned and processed."
		print "=====End of the program=====" 
		print master_df_main.shape
		master_df_main.to_csv(cwd + "/master_anubis_main.csv")


def Bitch_Pls_Trigger():
	if os.path.exists(directory_to_bhav):
		Sweet_Honey()
	else:
		print "problem with main"

		
