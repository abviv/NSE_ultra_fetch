"""__main__production__code, bad shit hapens to good peoplem, contains some calc part feel free to dive in 

					--------------------eats the data and shits out a ton of useful stuff-----------------
"""

import os
import pandas as pd
import requests
import glob
import shutil

cwd=os.getcwd()
#print cwd
global knight

master_data = cwd + "/master_tdy_main.csv"
directory_to_bhav = cwd + "/bhav/"
directory_to_oi = cwd + "/oi/"
directory_to_vol = cwd + "/volatility/"
directory_to_report = cwd + "/report/"


def Csv_File_Creation_Report():
	only_file = [os.listdir(directory_to_bhav),os.listdir(directory_to_vol),os.listdir(directory_to_oi)] ###localized because of error while creating ****can be modified*** 

	###used as a reference for creating the report dir,***do not change var volatility*** must exist.
	volatility = only_file[1][0]     

	def Csv_Calc():
			global data  ###var data is localized so can be removed if wanted

			data = pd.read_csv(directory_to_vol+only_file[1][0],usecols=[0,1,2,6,8,12])	   
			data = data.drop(data.index[[24,58,68,102,146,147,148,149,150,151,179]])
			#print data
			no_of_stocks = data.shape[:1]
			Csv_File_Creation_Report.no_of_stocks = no_of_stocks
			name_of_stocks = data.ix[:,1]   
			name_of_stocks = name_of_stocks.reset_index()
			name_of_stocks = name_of_stocks.ix[:,1]   						###cleaned stock names ranging from index 0-208
			Csv_File_Creation_Report.name_of_stocks=name_of_stocks     		###to access the variable outside the function
			#print name_of_stocks


			range_of_stocks = range(0,220)
			list_of_stocks = (list(name_of_stocks))     					###contains the names of the stock 
			#print list_of_stocks

			Csv_File_Creation_Report.list_of_stocks = list_of_stocks        ###var was created to use outside of this CSsv_File_Creation_Report

			"""var master_df holds no use other than creating the initial model, can be removed but might cause error while creation of columns ****do not remove**** """
			
			master_df = ['Date','Symbol','Underlying_close_price','Underlying_daily_volatility','Futures_close_price','Futures_daily_volatility','OI','OI_change','Futures_price_change','OI_fut&opt','OI_change_fut&opt']
			
			###var empty_  is just a fancy shit dont be afraid

			empty_ = pd.DataFrame(index=range_of_stocks,columns=master_df)  
			
			#empty_.to_csv(directory_to_report +'/1_daily_calc.csv',sep='\t', encoding='utf-8')
			for i in range(0,209):
					empty_.to_csv(directory_to_report +'/{}.csv'.format(list_of_stocks[i]),sep='\t', encoding='utf-8')
	
	if os.path.exists(directory_to_report):				
		#shutil.rmtree(directory_to_report)			###delete a dir if there are contents
		#os.rmdir(directory_to_report)				###delete an empty dir
		#os.mkdir(directory_to_report)
		Csv_Calc()
		print "if part"																	#debugger

	elif not os.path.exists(directory_to_report): 
		print "Directory not found, creating new one ===>Csv_Calc defn"     			#debugger
		os.mkdir(directory_to_report)
		Csv_Calc()
		#print len(list_of_stocks)		



def Data_Processing():
	""" A lot of beautiful shit happens in this function, the crawler aka demon fetches the juice and drinks it here"""


	only_file = [os.listdir(directory_to_bhav),os.listdir(directory_to_vol),os.listdir(directory_to_oi)]  ###localized to remove the read error ***do not remove***

	data = pd.read_csv(directory_to_vol+only_file[1][0],usecols=[0,1,2,6,8,12])	   			###still regretting why i used this data
	data = data.drop(data.index[[24,58,68,102,146,147,148,149,150,151,179]])

	for i in range(0,3):
		if i==0:
			if os.path.exists(directory_to_bhav + only_file[i][0]): 						#debugger
				print "Bhav file exists---------->"
				#print (directory_to_bhav + only_file[i][0]) #last index reads a different .csv file in that dir, first index for the array no(dont change it).

				data_bhav  = pd.read_csv(directory_to_bhav + only_file[i][0],usecols=[1,10,12])
				data_bhav_cpy = data_bhav.copy()
				final_bhav = data_bhav_cpy.ix[42:668]			#original bhav contains that particular range of rows as desired one. If error found do check this range
				final_bhav = final_bhav.reset_index()			#final_bhav and the new_df refers to the same DataFrame changed the df for better understanding and preserving the original
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
						global newton   			### ***do not modify this global***

						###keep var newton as a main df in the later part we can append the data to main_column
						newton = new_df.ix[:,1:3]  

						#newton["STOCK_NAME"] = Csv_File_Creation.name_of_stocks
					#print (newton.head(20))										#debugger
					#print (newton.tail(10))										#debugger
				

				Cross_Calc()  #intiate a processing and retun a df with cleaned and needed data 
				   
			else:
				print "Bhav file not exists------->issue in Data_Processing() "		#debugger
				

		elif i==1:
			if os.path.exists(directory_to_vol + only_file[i][0]):
				#data_vol=pd.read_csv(directory_to_vol + only_file[i][0])
				print "vol file exists------------>"
				print "processing {} the path".format(directory_to_vol + only_file[i][0])
				data_temp = data
				data_temp = data_temp.reset_index()
				#print data_temp.head(60)

				data_temp.columns = ['index','DATE','SYMBOL','UNDERLYING_CLOSE_PRICE_A','A_VOLATILITY','FUT_CLOSE_PRICE_B','B_VOLATILITY']
				data_vol = data_temp
				data_vol = data_vol.loc[:,['UNDERLYING_CLOSE_PRICE_A','A_VOLATILITY','FUT_CLOSE_PRICE_B','B_VOLATILITY']]
				global newton_cpy
				newton_cpy = newton
				newton_cpy = newton_cpy.join(data_vol)

				#print newton_cpy.head(124)
				#print (directory_to_vol + only_file[i][0])
			else:
				print "vol file not exists-------->issue in Data_Processing()"
		

		elif i==2:
			if os.path.exists(directory_to_oi+ only_file[i][0]):
				data_oi = pd.read_csv(directory_to_oi + only_file[i][0])
				print "oi file exists------------->"
				print "processing {} the path".format(directory_to_oi + only_file[i][0])   
				
				data_oi.columns = ['DATE','ISIN','SCRIP_NAME','NSE_SYMBOL','MWPL','OI_FUT_OPT']
				data_oi = data_oi.sort_values(by=['NSE_SYMBOL'],ascending = [True])
				data_oi = data_oi.loc[:,['DATE','NSE_SYMBOL','MWPL','OI_FUT_OPT']]  #column of interest
				data_oi = data_oi.reset_index()
				data_oi = data_oi.loc[:,['DATE','NSE_SYMBOL','MWPL','OI_FUT_OPT']]
				
				#print data_oi.head()
				data_oi = data_oi.join(newton_cpy)
				global master_df_main
				master_df_main = data_oi

				###ADDING NEW COLUMNS CAN BE DONE LIKE THE BELOW FORMAT             ----------------------------->>>>segment flexibility
				master_df_main['FUT_PRICE_CHANGE'] = 0
				master_df_main['OI_CHANGE'] = 0
				master_df_main['MWPL_CHANGE'] = 0
				master_df_main['OI_FUT_OPT_CHANGE'] = 0
				
				#Writing_Processed_Values()
				#print master_df_main.head()
			else:
				print "oi file not exists-------->issue in Data_Processing()"


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
		

def Writing_Processed_Values_new():					
	"""will get executed for the first time this pgm is being run"""

	sorted_files = sorted(os.listdir(directory_to_report))
	
	for i in range(0,209):
		#if os.path.splitext(sorted_files[i])[0] == newton.STOCK_NAME.iloc[i])
		master_df_main.iloc[[i]].to_csv(directory_to_report + sorted_files[i])
		
			
def Writing_Processed_Values_old():
	"""will get executed for the rest of the time other than the first time"""

	sorted_files = sorted(os.listdir(directory_to_report))     ###re order the files in the list 

	for i in range(0,209):

		"""hard shit which writes data to each and every file in the report dir, the master_df_main df to the o/p .csv file"""
		master_df_main.iloc[[i]].to_csv(directory_to_report + sorted_files[i],mode = 'a',header = False)
		


def Sweet_Honey(): 

	if not os.path.exists (directory_to_report):
		#only_file = [os.listdir(directory_to_bhav),os.listdir(directory_to_vol),os.listdir(directory_to_oi)]
		print "Dirctory to report does not exist.....Creating new one"
		Csv_File_Creation_Report()
		print "file creation complete"
		print "=======" * 10
		Data_Processing()
		print "=======" * 10
		print "processing the file and rewriting the csv"
		Writing_Processed_Values_new()
		print "=====End of the program=====" 
		print master_df_main.shape
		master_df_main.to_csv(cwd + "/master_tdy_main.csv")   
		#print master_df_main.tail(10)


	else:
		#only_file = [os.listdir(directory_to_bhav),os.listdir(directory_to_vol),os.listdir(directory_to_oi)]
		print "Directory Exists...Proceeding according to the flow"
		print "file creation complete"
		print "=======" * 10
		
		#data = pd.read_csv(directory_to_vol+only_file[1][0],usecols=[0,1,2,6,8,12])	   
		#data = data.drop(data.index[[24,58,68,102,146,147,148,149,150,151,179]])

		Data_Processing()
		print "=======" * 10
		print "processing the file and rewriting the csv"

		""" ***Do not mess*** with the below fn and do not reorder the fun call.....i could write a story about this fn
			Change_Calculator fetches the previous day's data from master_tdy_main.csv just before its been rewritten by the Writing_Processed_Values_old
			Fn....... so the order of exection is damn wichtig!!!! 
			Mess with the below fn to see the full wrath"""

		Change_Calculator()	
		print "Processing....."				
		Writing_Processed_Values_old()
		print "++++++++++completed the data fetch and calc++++++++++"
		print "+++++Have a nice day+++++"
		print "=====End of the program=====" 
		print master_df_main.shape
		master_df_main.to_csv(cwd + "/master_tdy_main.csv")

def Bitch_Pls_Trigger():
	
	""" Funny fn name but i mean no offence to anyone. Triggering this fn will trigger a cascade functionality  """
	
	print "===============Take a break and enjoy for few minutes until the calc is done==================="

	if os.path.exists(directory_to_bhav):
		Sweet_Honey()
	else:
		print "problem with main"
		
