'''
@Descripttion: 捉取整个网络的所有页面链接下来！
@Author: BerryBC
@Version: 0.3.0
@Date: 2020-02-02 11:15:41
@LastEditors  : BerryBC
@LastEditTime : 2020-02-04 16:18:28
'''

from Lib.LMongoDB import claMongoDB
from Lib.LAddPage import claAddPage
from configobj import ConfigObj
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import threading
import time

strCfgPath = './cfg/dbCfg.ini'
objLinkDB = claMongoDB(strCfgPath, 'mongodb')
objAddPage = claAddPage(objLinkDB)
objParam = ConfigObj(strCfgPath)
intHowManyProxy = int(objParam['param']['HowManyProxy'])
intHowManyPageOneTime = int(objParam['param']['HowManyPageOneTime'])
intLessThenFail = int(objParam['param']['LessThenFail'])
intRequestTimeout = int(objParam['param']['RequestTimeout'])
intSemaphore = int(objParam['param']['Semaphore'])
intDeleteTime = int(objParam['param']['DeleteTime'])
intReusableRepeatTime = int(objParam['param']['ReusableRepeatTime'])
intNewRepeatTime = int(objParam['param']['NewRepeatTime'])
intDeleteRepeatTime = int(objParam['param']['DeleteRepeatTime'])
dictHeader = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.78 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'cache-control': 'no-cache',
    'Pragma': 'no-cache'}


def funMain():
    print('Program begin : '+time.strftime('%Y-%m-%d %H:%M:%S'))
    funSpyReusablePage()
    funSpyNewPage()
    funDeleteOldPage()
    # print(type(objLinkDB))
    # for dd in objLinkDB.LoadRandomLimit('proxydb',{"fail": {"$lte": 8}},20):
    #     print(dd)
    # print(objLinkDB.CheckOneExisit('proxydb',{'u':'106.85.133.109'}))


def funSpyReusablePage():
    print(' Reusable begin : '+time.strftime('%Y-%m-%d %H:%M:%S'))
    arrTarget = []
    curTarget = objLinkDB.LoadAllData('pagedb-Reuseable')
    for eleTarget in curTarget:
        arrTarget.append(eleTarget['url'])
    # print(arrTarget)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    semaphore = asyncio.Semaphore(intSemaphore)
    waittask = asyncio.gather(
        *([funSpyWeb(strWebSite, semaphore) for strWebSite in arrTarget]))
    loop.run_until_complete(waittask)
    loop.close()
    threading.Timer(60*intReusableRepeatTime, funSpyReusablePage).start()
    print(' Reusable end : '+time.strftime('%Y-%m-%d %H:%M:%S'))


def funSpyNewPage():
    print(' New begin : '+time.strftime('%Y-%m-%d %H:%M:%S'))
    arrTarget = []
    curTarget = objLinkDB.LoadRandomLimit(
        'pagedb-Crawled', {"ced": False}, intHowManyPageOneTime)
    for eleTarget in curTarget:
        objLinkDB.UpdateOneData(
            'pagedb-Crawled', {'_id': eleTarget['_id']}, {'ced': True})
        arrTarget.append(eleTarget['url'])
    # print(arrTarget)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    semaphore = asyncio.Semaphore(intSemaphore)
    waittask = asyncio.gather(
        *([funSpyWeb(strWebSite, semaphore) for strWebSite in arrTarget]))
    loop.run_until_complete(waittask)
    loop.close()
    threading.Timer(60*intNewRepeatTime, funSpyNewPage).start()
    print(' New end : '+time.strftime('%Y-%m-%d %H:%M:%S'))


def funDeleteOldPage():
    print(' Delete begin : '+time.strftime('%Y-%m-%d %H:%M:%S'))
    intNow = int(time.time()*1000)
    curDelete = objLinkDB.DeleteSome(
        'pagedb-Crawled', {'t': {'$lt': intNow-intDeleteTime}})
    print(' Delete URL Number : ' + str(curDelete.deleted_count))
    curDelete = objLinkDB.DeleteSome(
        'sampledb', {'t': {'$lt': (intNow-(intDeleteTime)*3)}})
    print(' Delete Content Number : ' + str(curDelete.deleted_count))
    # print(intNow)
    threading.Timer(60*intDeleteRepeatTime, funDeleteOldPage).start()
    print(' Delete end : '+time.strftime('%Y-%m-%d %H:%M:%S'))


async def funSpyWeb(eleWeb, inSemaphore):
    async with inSemaphore:
        bolRetry = True
        intTryTime = 0
        # arrProxy=[]
        # for eleProxy in objLinkDB.LoadRandomLimit('proxydb', {"fail": {"$lte": intLessThenFail}},intHowManyProxy):
        #     arrProxy.append(arrProxy)
        # print(type(arrProxy))
        arrProxy = objLinkDB.LoadRandomLimit(
            'proxydb', {"fail": {"$lte": intLessThenFail}}, intHowManyProxy)
        arrProxy = list(arrProxy)
        intProxyLen = len(arrProxy)
        # print(intProxyLen)
        try:
            async with aiohttp.ClientSession() as session:
                # 判断是否需要重试以及是否所有代理均已用完
                while (bolRetry and (intTryTime < intProxyLen)):
                    try:
                        # 导入代理
                        strProxyToSpy = "http://" + \
                            arrProxy[intTryTime]["u"] + \
                            ":"+arrProxy[intTryTime]["p"]
                        # print(strProxyToSpy)
                        # 异步请求网页内容
                        async with session.get(eleWeb, proxy=strProxyToSpy, timeout=intRequestTimeout, headers=dictHeader) as res:
                            if res.status == 200:
                                strhtml = await res.text()
                                soup = BeautifulSoup(strhtml, 'lxml')
                                aFromWeb = soup.select('a')
                                for eleA in aFromWeb:
                                    objAddPage.AddToDB(eleA.get('href'))
                                arrWebP=soup.select('p')
                                objAddPage.AddPContent(arrWebP)
                                # print(result)
                            bolRetry = False
                            # print("  After " + str(intTryTime) +
                            #       " time, success reach " + eleWeb)
                    except Exception as e:
                        intTryTime += 1
                        # print(" Get method error : " + str(e))
                        # print('    Fail ' + str(intTryTime) + ' time')
        except Exception as e:
            print(" Session error : " + str(e))


if __name__ == "__main__":
    funMain()
