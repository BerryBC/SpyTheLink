'''
@Descripttion: 添加爬出来的页面 URL 进数据库
@Author: BerryBC
@Date: 2020-02-02 14:24:17
@LastEditors  : BerryBC
@LastEditTime : 2020-02-04 16:18:39
'''
import time


class claAddPage(object):

    def __init__(self, objMongoDB):
        self.objMongoDB = objMongoDB

    def AnEmptyPageEle(self):
        return {'url': '', 'd': 0, 'ced': False, 'jed': False, 't': int(time.time()*1000)}

    def AnEmptyContentEle(self):
        return {'ct': '', 'e': 0, 'cf': False, 'jed': False, 't': int(time.time()*1000)}

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
                    self.objMongoDB.InsertOne('pagedb-Crawled', dictNewPage)

    def AddPContent(self, arrTagP):
        # print('   成功爬了一个网站')
        strPContent = ''
        for eleP in arrTagP:
            strPContent += eleP.get_text()+' '
        dictNewContent = self.AnEmptyContentEle()
        dictNewContent['ct']=strPContent
        if len(strPContent)>20:
            # print('   成功爬了一个网站')
            self.objMongoDB.InsertOne('sampledb', dictNewContent)
        # else :
        #     print('   字数不够不保存')