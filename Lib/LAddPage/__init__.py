'''
@Descripttion: 添加爬出来的页面 URL 进数据库
@Author: BerryBC
@Date: 2020-02-02 14:24:17
@LastEditors  : BerryBC
@LastEditTime : 2020-02-03 13:02:37
'''
import time


class claAddPage(object):

    def __init__(self, objMongoDB):
        self.objMongoDB = objMongoDB

    def AnEmptyPageEle(self):
        return {'url': '', 'd': 0, 'ced': False, 'jed': False, 't': int( time.time()*1000)}

    def AddToDB(self, strHref):
        # print(strHref)
        if not strHref is None:
            bolHttps = ('http://' in strHref or 'https://' in strHref)
            if bolHttps:
                if not self.objMongoDB.CheckOneExisit('pagedb-Crawled', {'url': strHref}):
                    dictNewPage = self.AnEmptyPageEle()
                    intDepth = len(strHref.split('/'))-3
                    dictNewPage['url'] = strHref
                    dictNewPage['d'] = intDepth
                    self.objMongoDB.InsertSome('pagedb-Crawled',[dictNewPage])
