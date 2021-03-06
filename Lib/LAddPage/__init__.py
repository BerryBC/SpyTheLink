'''
@Descripttion: 添加爬出来的页面 URL 进数据库
@Author: BerryBC
@Date: 2020-02-02 14:24:17
@LastEditors: BerryBC
@LastEditTime: 2020-04-28 22:38:56
'''
import time
from urllib.parse import urlparse


class claAddPage(object):

    def __init__(self, objMongoDB):
        self.objMongoDB = objMongoDB

    def AnEmptyPageEle(self):
        return {'url': '', 'd': 0, 'ced': False, 'jed': False, 't': int(time.time()*1000)}

    def AnEmptyContentEle(self):
        return {'ct': '', 'e': 0, 'cf': False, 'jed': False, 't': int(time.time()*1000)}

    def AddToDB(self, strHref, strInCurPageURL):
        # print(strHref)
        if not strHref is None:
            urlCurPageURL = urlparse(strInCurPageURL)
            strCurLoc = urlCurPageURL.scheme+'://'+urlCurPageURL.netloc
            strRealInsert = strHref
            if len(strHref) > 4:
                if strHref[:4] != 'http':
                    if strHref[1] == '/':
                        strRealInsert = urlCurPageURL.scheme+':'+strHref
                    elif strHref[0] == '/':
                        strRealInsert = strCurLoc+strHref
                else:
                    strRealInsert = strHref
            bolHttps = (
                'http://' in strRealInsert or 'https://' in strRealInsert)
            strCleanURL = self.CleanURL(strRealInsert)
            if bolHttps:
                if not self.objMongoDB.CheckOneExisit('pagedb-Crawled', {'url': strCleanURL}):
                    dictNewPage = self.AnEmptyPageEle()
                    intDepth = len(strCleanURL.split('/'))-3
                    dictNewPage['url'] = strCleanURL
                    dictNewPage['d'] = intDepth
                    self.objMongoDB.InsertOne('pagedb-Crawled', dictNewPage)
                    # print(strCleanURL)

    def AddPContent(self, arrTagP, strInCurURL, intEmo):
        # print('   成功爬了一个网站')
        strPContent = ''
        for eleP in arrTagP:
            strPContent += eleP.get_text().strip()+'\n'
        strPContent = strInCurURL+'\n'+strPContent
        dictNewContent = self.AnEmptyContentEle()
        dictNewContent['ct'] = strPContent
        dictNewContent['jed'] = True
        dictNewContent['e'] = intEmo
        if (len(strPContent)-len(strInCurURL)) > 20:
            # print('   成功爬了一个网站')
            self.objMongoDB.InsertOne('sampledb', dictNewContent)
        # else :
        #     print('   字数不够不保存')

    def CleanURL(self, strURL):
        strRealURL = strURL
        intBQ = strURL.find('?')
        if intBQ > 0:
            strRealURL = strURL[:intBQ]
        intBQ = strRealURL.find('#')
        if intBQ > 0:
            strRealURL = strRealURL[:intBQ]
        return strRealURL
