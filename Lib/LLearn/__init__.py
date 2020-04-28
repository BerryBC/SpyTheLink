'''
@Descripttion: 学习内容
@Author: BerryBC
@Date: 2020-04-27 22:29:02
@LastEditors: BerryBC
@LastEditTime: 2020-04-29 00:20:16
'''
import joblib
import jieba
import datetime

from dateutil import parser
from sklearn.svm import LinearSVC
from sklearn.ensemble import BaggingClassifier

class claLearn(object):

    def __init__(self, objMongoDB):
        self.objMongoDB = objMongoDB
        self.LoadLatestClf()

    def LoadLatestClf(self):
        self.objLatestClfCfg=self.objMongoDB.LoadOneBySort('clfdb', {},[('lt',-1)])
        self.clfLatestClf=BaggingClassifier(base_estimator=LinearSVC(random_state=0, tol=1e-05, max_iter=10000))
        self.clfLatestClf= joblib.load("../SpyDataWebAppAndAPI/ClfFile/"+self.objLatestClfCfg['clfFileName'])

    def JudContent(self,arrP):
        bolNotUseless=False
        intEmo=0
        strPContent = ''
        for eleP in arrP:
            strPContent += eleP.get_text().strip()+'\n'

        genSampleWord = jieba.cut(strPContent, cut_all=False)
        arrContentKW=[]
        # 对分词之后的每个关键字进行处理
        for eleKW in genSampleWord:
                # 如果前期没有该关键字即进行下一步
            if not eleKW in arrContentKW:
                arrContentKW.append(eleKW)
                
        arrKWToClf=[]
        for eleKW in self.objLatestClfCfg["kwlist"]:
            if eleKW in arrContentKW:
                arrKWToClf.append(True)
                bolNotUseless=True
            else:
                arrKWToClf.append(False)

        intTmpCount=0
        if bolNotUseless:
            print("有一个神奇的情绪产生了")
            print(self.clfLatestClf.predict([arrKWToClf]))
            intEmo=self.clfLatestClf.predict([arrKWToClf])[0]
            for eleKW in arrContentKW:
                intTmpCount+=1
                strNow = datetime.datetime.now().strftime("%Y/%m/%d")
                dateNow=parser.parse(strNow)
                objWordNum=self.objMongoDB.LoadOne('clfdb-kw', {'date': dateNow,'kw':eleKW,'e':intEmo})
                if objWordNum is None:
                    self.objMongoDB.InsertOne('clfdb-kw', {'date': dateNow,'kw':eleKW,'e':intEmo,'num':1})
                else:
                    self.objMongoDB.UpdateOneData('clfdb-kw', {'date': dateNow,'kw':eleKW,'e':intEmo}, {'num': objWordNum['num']+1})
            print("你更新了 " +intTmpCount+" 个词！")
        
        print("完事了")

        return intEmo