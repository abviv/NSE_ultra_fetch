"""__main__production__code   Princess Leia, i have no idea why i named it. Following code pulls the EOD data from NSE.
				------------------pulls  data------------------ 
"""

import datetime
import time
import schedule
import requests
import os, zipfile,sys,shutil
from os import listdir
from os.path import isfile, join
from datetime import datetime
from anubis_main import * 

file_path = os.getcwd()
directory_to_bhav = file_path + '/BHAV_CPY/'					
directory_to_oi = file_path +'/OI/' 				
directory_to_vol = file_path + '/VOLATILITY/'

day=datetime.now().date().strftime ("%d")         #.strftime ("%Y%m%d") ###for fetching the current date and strips the needed 
month=datetime.now().date().strftime ("%m")
print day
#day   = 2
#month = 06


list_of_months = ['unindex','JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
#print month
month_in_words = list_of_months[int(month)]
#print month_in_words


def Bhav_Cpy(month_var,day_var):
	month_temp = month_var            #str
	day_temp = day_var				  #int
	
	def Bhav_Cpy_mental():
			if (day_temp<=9):
				url='http://www.nseindia.com/content/historical/DERIVATIVES/2017/{}/fo{}{}2017bhav.csv.zip'
				
				r = requests.get(url.format(month_temp,day_temp,month_temp))	
				file_name="fo{}{}2017bhav.zip"
				modified_f_name=file_name.format(day_temp,month_temp)      #file name you want for the incoming to be saved as

				###os.path.join is used to merge the creating .zip files into the current /bhav_cpy dir
				with open(os.path.join(directory_to_bhav,modified_f_name), "wb") as code: 
						code.write(r.content)  

			else:
				url='http://www.nseindia.com/content/historical/DERIVATIVES/2017/{}/fo{}{}2017bhav.csv.zip'
				r = requests.get(url.format(month_temp,day_temp,month_temp))	
				file_name="fo{}{}2017bhav.zip"
				modified_f_name=file_name.format(day_temp,month_temp)
				with open(os.path.join(directory_to_bhav,modified_f_name), "wb") as code:
						code.write(r.content)			

	if not os.path.exists(directory_to_bhav):
		os.mkdir(directory_to_bhav)
		
		Bhav_Cpy_mental()
	
	else:
		print "ELse part"
		Bhav_Cpy_mental()


def Open_Interest(month_in_num_var,day_var):
	month_temp = month_in_num_var     #int
	day_temp = day_var				  #int
	
	def Open_Interest_mental():
			if (day_temp<=9):
				url='https://www.nseindia.com/archives/nsccl/mwpl/nseoi_{}{}2017.zip'
				
				r = requests.get(url.format(day_temp,month_temp))	
				file_name="nseoi_{}{}2017.zip"
				modified_f_name=file_name.format(day_temp,month_temp)

				###os.path.join is used to merge the creating .zip files into the current /bhav_cpy dir
				with open(os.path.join(directory_to_oi,modified_f_name), "wb") as code: 
						code.write(r.content)  

			else:
				url='https://www.nseindia.com/archives/nsccl/mwpl/nseoi_{}{}2017.zip'
				
				r = requests.get(url.format(day_temp,month_temp))	
				file_name="nseoi_{}{}2017.zip"
				modified_f_name=file_name.format(day_temp,month_temp)
				with open(os.path.join(directory_to_oi,modified_f_name), "wb") as code:
						code.write(r.content)

	if not os.path.exists(directory_to_oi):
		os.mkdir(directory_to_oi)
		
		Open_Interest_mental()
	
	else:
		print "ELse part"
		Open_Interest_mental()	


def Volatility(month_var,day_var):
	month_temp = month_var            #str
	day_temp = day_var				  #int
	
	def Volatility_mental():
			if (day_temp<=9):
				url='https://www.nseindia.com/archives/nsccl/volt/FOVOLT_{}{}2017.csv'
				
				r = requests.get(url.format(day_temp,month_temp))	
				file_name="fovolt_{}{}2017.csv"
				modified_f_name=file_name.format(day_temp,month_temp)

				###os.path.join is used to merge the creating .zip files into the current /bhav_cpy dir
				with open(os.path.join(directory_to_vol,modified_f_name), "wb") as code: 
						code.write(r.content)   

			else:
				url='https://www.nseindia.com/archives/nsccl/volt/FOVOLT_{}{}2017.csv'

				r = requests.get(url.format(day_temp,month_temp))	
				file_name="fovolt_{}{}2017.csv"
				modified_f_name=file_name.format(day_temp,month_temp)

				with open(os.path.join(directory_to_vol,modified_f_name), "wb") as code:
						code.write(r.content)			

	if not os.path.exists(directory_to_vol):
		os.mkdir(directory_to_vol)
		
		Volatility_mental()
	
	else:
		print "ELse part"
		Volatility_mental()



def Unzipper(unzip_directory):
	file_path=os.getcwd()
	###listdir is used to list the contents of a directory
	onlyfiles = os.listdir(unzip_directory)
	#print (unzip_directory)
	#print (onlyfiles)
	for i in onlyfiles:
	#if o.endswith(".zip"):
		path_to_zipfile= unzip_directory + "/{}".format(i)
		#print (path_to_zipfile)
		###refer zipfile doc for more info about the underlying function
		zip = zipfile.ZipFile(path_to_zipfile, 'r')
		zip.extractall(unzip_directory)
		zip.close()



def Full_Clean():
	directory =[directory_to_bhav,directory_to_vol,directory_to_oi]  		#directory_to_bhav,directory_to_vol,
	#print directory[0]
	for i in range(len(directory)):
	    test = os.listdir( directory[i] )
	    #print test
	    removable_file = (".zip",".xml")
	    for item in test:
	        #print item
	        if item.endswith(removable_file):
	            os.remove(os.path.join(directory[i], item ))



	            			###custom function for pulling particular date
def Groupping():									
	Bhav_Cpy(month_in_words,day)		#(month,day) ("str(first three letter of the month)",int)
	Unzipper (directory_to_bhav)
	Open_Interest(month,day)			#(month,day) (int,int)
	Unzipper(directory_to_oi)     
	Volatility(month,day)				#(month,day)  (int,int)
	Full_Clean()



							###contains the link defn to all the proper flow  ****do not modify **** 
def Godzilla():										
	if not os.path.exists(directory_to_bhav):
		Groupping()

	else:
		shutil.rmtree(directory_to_bhav)
		shutil.rmtree(directory_to_vol)
		shutil.rmtree(directory_to_oi)
		Groupping()	


schedule.every().day.at("19:30").do(Godzilla)
schedule.every().day.at("19:31").do(Bitch_Pls_Trigger)	###SCHEDULER CALLS ALL THIS FUNCTION AT THE PREDEFINED TIME https://schedule.readthedocs.io/en/stable/



if __name__ == '__main__':     ###DISABLE THIS TO STOP THE WHOLE PROGRAM
	#print file_path
	while True:
		schedule.run_pending()
		time.sleep(1)

			







