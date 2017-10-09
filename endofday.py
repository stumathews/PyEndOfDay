#!/usr/bin/env python3
import urllib.parse, urllib.request, json, sys, getopt
import os
from sys import argv
import concurrent.futures
import time

start = time.time()
verbose = False
tickerData = {}
columns = []
options = {}
MAX_THREADS = 2
companyFile = "companylist.csv"
outputFilename = time.strftime("%Y-%m-%d-%H%M%S.csv")
firstColumns = ["Name", "Currency", "Ask", "Open", "PreviousClose", "PercentChange", "PriceBook", "Change", "DaysHigh", "DaysLow", "EarningsShare"]

def print_usage():
    print('''Usage: ./endofday.py <options>

Options:
 -o          : output file location - otherwise the default filename is used: year-month-day-time.csv
 -i          : companylist file location - otherwise the default filename is searched for: companylist.csv
 -t <num>    : Number of concurrent threads
 -v          : Be verbose
 -l <num>    : limit to x companies
 
 Examples:  ./endofpday.pl
            ./endofday.pl -i companies.csv -o output.csv          
 
 Notes: Python3 required.
        ''')
    exit(0)

try:
    opts, args = getopt.getopt(sys.argv[1:], "hi:o:t:v")
except getopt.GetoptError:
    print("python endofday.py -i <inputfile>")
    sys.exit(2)

for opt, arg in opts:
    options[opt] = arg
 
if('-h' in options):
    print_usage() 

if('-i' in options):
    companyFile = options['-i'];
if('-t' in options):
    MAX_THREADS = int(options['-t'])
if('-v' in options):
    verbose = True
    print("Verbose\n")

    
with open(companyFile) as f:
    content = f.readlines()
    
CompanyList = [x.strip() for x in content]
CompanyList = [urllib.parse.quote(company) for company in CompanyList]

def get_company_data(company):
    unencoded = urllib.parse.unquote(company)
    try:
        urlpath = f"https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.quotes%20where%20symbol%20in%20(%22{company}%22)&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callack=";
        with urllib.request.urlopen(urlpath) as url:
            data = json.loads(url.read().decode())
        if('query' in data and 'results' in data['query'] and 'quote' in data['query']['results'] and len(data['query']['results']['quote']) > 0 and data['query']['results']['quote'] != None):
            quote = (data['query']['results']['quote'][0])    
            print(f"%30s %7s %7s %7s\n" % (unencoded, quote["Ask"], quote["Open"], quote["Symbol"]))
            return quote
        else:
            print(f"bad data for '{unencoded}'") 
    except urllib.error.HTTPError as e:
        ResponseData = e.read().decode("utf8", 'ignore')
        print(f"error trying to get {unencoded} details\n")
        if(verbose is True):
            print(f"error trying to get {unencoded} details: {ResponseData}\n")
    except urllib.error.URLError as e: 
        ResponseData = e.read().decode("utf8", 'ignore')
        print(f"bad url: {url}\n")
        if(verbose is True):
            print(f"bad url: {ResponseData}\n")
    except KeyError:
        print(f"bad key: {ResponseData}\n")

with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_THREADS) as executor:
    for company, quote in zip(CompanyList, executor.map(get_company_data, CompanyList)):
        tickerData[company] = quote

if('-o' in options):
    outputFilename = options['-o']
    
print("writing results\n")
with open(outputFilename, 'w') as file:
    for company in tickerData.keys():
        quote = tickerData[company]
        if(quote == None):
            print("Quote was None\n")
            continue        
        if(len(columns) == 0): 
            print(columns)     
            columns = quote.keys()                 
            withoutPreferredColumns = [ x for x in columns if x not in firstColumns and x != None]
            withoutPreferredColumns = [ x for x in withoutPreferredColumns if x not in [None]]
            reorderedColumn = firstColumns + withoutPreferredColumns
            columns = reorderedColumn
            file.write(",".join([ str(x) for x in columns])+"\n") 
        else:    
            lineData = []
            for column in columns:
                if(quote[column] == None):
                    lineData.append("None")
                else:
                    lineData.append(quote[column])
                
            line = ",".join(lineData)
            print(f"{line}\n")
            file.write(f'{line}\n') 
            #
        
print("Job took %2d" %(time.time() - start)+" secs\n")
