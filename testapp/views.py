from django.shortcuts import render
from django.http import HttpResponse
import csv
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
from datetime import datetime, timedelta
import json

import threading
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler 
# Create your views here.



path1="/home/zaid/ref1/Transaction_20180101101010.csv"
path2="/home/zaid/ref2/ProductReference.csv"
df1=pd.read_csv(path1)
df2=pd.read_csv(path2)
df1.columns = df1.columns.str.replace(' ', '')  #remove blank space from column name
df2.columns = df2.columns.str.replace(' ', '')



class mythread(threading.Thread):

    DIRECTORY_TO_WATCH = "/home/zaid/ref1"
    DIRECTORY_TO_WATCH1 = "/home/zaid/ref2"
        
    def run(self):
        self.observer = Observer()
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH1, recursive=True)
        self.observer.start()
        
    

class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            # Take any action here when a file is first created.
            newFilePath=event.src_path
            process_on_newFile(newFilePath)
            print ("Received created event path :- %s." % event.src_path)

def process_on_newFile(newFilePath):
    print ("inside funct@@@@@@@@@@@")
    print(newFilePath)
    newfile=pd.read_csv(newFilePath)
    newfile.columns=newfile.columns.str.replace(' ','')
    print(newfile.columns[0])
    if(newfile.columns[0] == "transactionId"):
        adding_to_dataframe(df1,newfile)
        
    else:
        adding_to_dataframe(df2,newfile)


def adding_to_dataframe(prevDataFrame,newfile):
    j=0
    for i in range(len(prevDataFrame),len(prevDataFrame)+len(newfile)):
            prevDataFrame.loc[i]=newfile.loc[j]
            j+=1
    print(prevDataFrame)




def details_on_id(request, data_id):
    print (df1)
    print (df1,data_id)
    temp=int(data_id)
    #j = df1.to_json(orient='records')
   # print (j)
    row=df1.loc[df1.transactionId == temp]
    prodId=int(row["productId"].values[0])
    row2=df2.loc[df2.productId ==prodId]
    #json_row=row.to_json(orient='records')
    #print (json_row)
    resp={row.columns[0]:row["transactionId"].values[0],row2.columns[1]:row2["productName"].values[0],row.columns[2]:row["transactionAmount"].values[0],row.columns[3]:row["transactionDatetime"].values[0]}
    
    return HttpResponse("%s" %resp)
    #return  HttpResponse("<h1>HEY! Welcome to Edureka! %s </h1>" % data_id)
#df1['transactionDatetime']=pd.to_datetime(df1['transactionDatetime'])//changing type of column to datetime


def transactionSummaryByProducts(request,days):
    
    df1['transactionDatetime']=pd.to_datetime(df1['transactionDatetime']) #changing type of column to datetime

    date_before_Ndays = datetime.now() - timedelta(days=int(days))

    response1 = df1.loc[df1.transactionDatetime >= date_before_Ndays].groupby("productId")["transactionAmount"].aggregate(['sum'])
    transaction_info=response1.reset_index() #to access groupby column

    #response1.set_index("productId", inplace = True)
    transaction_details=pd.merge(transaction_info,df2,on='productId',how='inner')
    jsonList=[]
    #print (len(transaction_details))
    for  i in range(0,len(transaction_details)):
        jsonList.append({"productName":transaction_details["productName"][i],"totalAmount":transaction_details["sum"][i]})
        i+=1
    #print (jsonList)
    output={"summary":jsonList}
    return HttpResponse("%s"%output)
   

   

def transactionSummaryByManufacturingCity(request,days):
    df1['transactionDatetime']=pd.to_datetime(df1['transactionDatetime']) #changing type of column to datetime

    date_before_Ndays = datetime.now() - timedelta(days=int(days))
    response = df1.loc[df1.transactionDatetime >= date_before_Ndays]
    #print (response)
    transaction_dtls=pd.merge(response,df2,on='productId',how='inner').groupby(["productManufacturingCity"])["transactionAmount"].aggregate(['sum']).reset_index()
    #print (transaction_dtls)
    jsonList=[]
    i=0
    
    for i in range(0,len(transaction_dtls)):
        jsonList.append({"cityName":transaction_dtls["productManufacturingCity"][i],"totalAmount":transaction_dtls["sum"][i]})

    #print (jsonList)
    output={"summary":jsonList}
    return HttpResponse("%s"%output)
    

thread1 = mythread()

thread1.start()

