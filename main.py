import asyncio
import sqlite3
import xmltodict
import requests
import time
import threading

queryies = list()

sqlite_connection = sqlite3.connect('/Users/kappa/PycharmProjects/Exchange_Site/Ex/exchange_info.db')
cursor = sqlite_connection.cursor()


def get_and_insert(xml,list):
    try:
        get = requests.get(xml)
        parsed = xmltodict.parse(get.text)
        data = parsed['rates']['item']
        for each in data:
            q = "INSERT INTO 'main'.'obmen' ('FROM', 'TO', 'Seller', 'Sellerweb', 'in', 'out', 'amount', " \
                "'created_at', 'minamount', 'maxamount', 'updated_at') VALUES ('{}', '{}', '{}', '{}', '{}', '{}', " \
                "'{}', '{}', '{}', '{}', '{}')".format(each['from'], each['to'], xml, xml, each['in'], each['out'],
                                                       each['amount'], '123', each['minamount'], each['maxamount'],
                                                       '123')
            list.append(q)

    except Exception as E:
        print(E)





url = {'https://cripta.cc/valuta.xml', 'https://coinstart.cc/valuta.xml', 'https://ex-bank.cc/assets/rates.xml',
       'http://papa-change.com/request-exportxml.xml', 'https://grambit.biz/request-exportxml.xml',
       'https://el-change.com/request-exportxml.xml', 'https://1obmen.net/request-exportxml.xml',
       'https://transfer24.pro/request-exportxml.xml', 'https://globalbits.org/request-exportxml.xml',
       'https://exline.pro/request-exportxml.xml', 'https://cryptomax.ru/request-exportxml.xml',
       'https://flashobmen.com/request-exportxml.xml', 'https://bitcoin-24.pro/request-exportxml.xml',
       'https://coincat.in/monitoring-export/estandarts.xml', 'https://ferma.cc/valuta.xml',
       'https://kassa.cc/valuta.xml'}



xmls = {'https://cripta.cc/valuta.xml', 'https://coinstart.cc/valuta.xml', 'https://ex-bank.cc/assets/rates.xml'}

threads = []
start_time = time.time()
counter = 0
for xml in url:
    counter += 1
    c = counter
    t = threading.Thread(target=get_and_insert,args=(xml,queryies))
    t.start()
    threads.append(t)

for t in threads:
    t.join()
for each in queryies:
    cursor.execute(each)
sqlite_connection.commit()

print(len(queryies))
print("--- %s seconds ---" % (time.time() - start_time))
