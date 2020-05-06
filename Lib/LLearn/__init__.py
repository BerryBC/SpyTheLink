'''
@Descripttion: 学习内容
@Author: BerryBC
@Date: 2020-04-27 22:29:02
@LastEditors: BerryBC
@LastEditTime: 2020-05-06 16:00:38
'''
import joblib
import jieba
import datetime
import time


import pandas as pd
import numpy as np

from dateutil import parser
from sklearn.svm import LinearSVC
from sklearn.ensemble import BaggingClassifier


class claLearn(object):

    def __init__(self, objMongoDB, strDirForClf):
        self.objMongoDB = objMongoDB
        self.strDirForClf = strDirForClf
        self.LoadLatestClf()

    def LoadLatestClf(self):
        self.objLatestClfCfg = self.objMongoDB.LoadOneBySort('clfdb', {}, [
                                                             ('lt', -1)])
        self.clfLatestClf = BaggingClassifier(
            base_estimator=LinearSVC(random_state=0, tol=1e-03, max_iter=1000))
        self.clfLatestClf = joblib.load(
            self.strDirForClf+self.objLatestClfCfg['clfFileName'])

    def JudContent(self, arrP, bolIsJustText):
        bolNotUseless = False
        intEmo = 0
        strPContent = ''
        if bolIsJustText:
            for eleP in arrP:
                strPContent += eleP.strip()+'\n'
        else:
            for eleP in arrP:
                strPContent += eleP.get_text().strip()+'\n'

        genSampleWord = jieba.cut(strPContent, cut_all=False)
        arrContentKW = []
        # 对分词之后的每个关键字进行处理
        for eleKW in genSampleWord:
                # 如果前期没有该关键字即进行下一步
            if not eleKW in arrContentKW:
                arrContentKW.append(eleKW)

        arrKWToClf = []
        for eleKW in self.objLatestClfCfg["kwlist"]:
            if eleKW in arrContentKW:
                arrKWToClf.append(True)
                bolNotUseless = True
            else:
                arrKWToClf.append(False)

        intTmpCount = 0
        if bolNotUseless:
            # print("有一个神奇的情绪产生了")
            intEmo = int(self.clfLatestClf.predict([arrKWToClf])[0])
        for eleKW in arrContentKW:
            intTmpCount += 1
            strNow = datetime.datetime.now().strftime("%Y/%m/%d")
            # dateNow = parser.parse(strNow)
            # objWordNum = self.objMongoDB.LoadOne(
            #     'clfdb-kw', {'date': dateNow, 'kw': eleKW, 'e': intEmo})
            objWordNum = self.objMongoDB.LoadOne(
                'clfdb-kw', {'kw': eleKW})
            if objWordNum is None:
                # self.objMongoDB.InsertOne(
                #     'clfdb-kw', {'date': dateNow, 'kw': eleKW, 'e': intEmo, 'num': 1})
                arrEmoNum = [0, 0, 0]
                arrEmoNum[intEmo+1] = 1
                self.objMongoDB.InsertOne(
                    'clfdb-kw', {'kw': eleKW, 'num': arrEmoNum})
            else:
                # self.objMongoDB.UpdateOneData(
                #     'clfdb-kw', {'date': dateNow, 'kw': eleKW, 'e': intEmo}, {'num': objWordNum['num']+1})
                arrEmoNum = objWordNum['num']
                arrEmoNum[intEmo+1] = arrEmoNum[intEmo+1]+1
                self.objMongoDB.UpdateOneData(
                    'clfdb-kw', {'kw': eleKW}, {'num': arrEmoNum})
            objWordNum.close()
        # print("你更新了 " + str( intTmpCount)+" 个词！")

        # print("完事了")

        return intEmo

    def CreatNewClf(self):
        # print('Start Load Sample')
        curPos = self.objMongoDB.LoadRandomLimit(
            'sampledb', {'cf': True, 'e': 1}, 150)
        curUseless = self.objMongoDB.LoadRandomLimit(
            'sampledb', {'cf': True, 'e': 0}, 50)
        curNeg = self.objMongoDB.LoadRandomLimit(
            'sampledb', {'cf': True, 'e': -1}, 150)

        curMLPos = self.objMongoDB.LoadRandomLimit(
            'sampledb', {'cf': False, 'jed': True, 'e': 1}, 150)
        curMLUseless = self.objMongoDB.LoadRandomLimit(
            'sampledb', {'cf': False, 'jed': True, 'e': 0}, 100)
        curMLNeg = self.objMongoDB.LoadRandomLimit(
            'sampledb', {'cf': False, 'jed': True, 'e': -1}, 150)

        # 初始化定义
        dictKW = {}
        arrSample = []
        # dictSampleOfNew={'kw':[],'e':1}
        intIndexNow = 0
        arrXPreTrain = []
        arrYPreTrain = []
        # 定义数据记录
        dictAllResult = {'intIndexNow': 0, 'arrXPreTrain': [], 'arrYPreTrain': [
        ], 'dictKW': {}, 'arrSample': [], 'arrColunms': []}

        # 分词信息
        def ToGrepSampleKW(curSamples, dictInAllResult):
            for eleSamples in curSamples:
                # 不设睡眠时间很容易 CPU 爆掉
                time.sleep(0.05)
                genSampleWord = jieba.cut(eleSamples['ct'], cut_all=False)
                dictSampleOfNew = {'kw': [], 'e': eleSamples['e']}
                # 对分词之后的每个关键字进行处理
                for eleKW in genSampleWord:
                        # 如果前期没有该关键字即进行下一步
                    if not eleKW in dictSampleOfNew['kw']:
                        dictSampleOfNew['kw'].append(eleKW)
                        # 如果所有样本都不存在此关键字即存在全局字典内
                        if not eleKW in dictInAllResult['dictKW'].keys():
                            dictInAllResult['dictKW'][eleKW] = dictInAllResult['intIndexNow']
                            dictInAllResult['intIndexNow'] += 1
                            dictInAllResult['arrColunms'].append(eleKW)
                dictInAllResult['arrSample'].append(dictSampleOfNew)
            return dictInAllResult

        # 做成一个矩阵，如果该样本含有该关键字，即对应位置为 1，否则为 0
        # 样本大致为
        #  	    我    爱    中国
        # 样本1	True  False True
        # 样本2	False True  True

        def ToArraySample(dictInAllResult):
            for dictEle in dictInAllResult['arrSample']:
                arrNewSample = [False for intX in range(
                    dictInAllResult['intIndexNow'])]
                for eleKWFI in dictEle['kw']:
                    arrNewSample[dictInAllResult['dictKW'][eleKWFI]] = True
                dictInAllResult['arrXPreTrain'].append(arrNewSample)
                dictInAllResult['arrYPreTrain'].append(dictEle['e'])
            return dictInAllResult

        # 处理信息
        # print('Split The Sample')
        dictAllResult = ToGrepSampleKW(curPos, dictAllResult)
        dictAllResult = ToGrepSampleKW(curUseless, dictAllResult)
        dictAllResult = ToGrepSampleKW(curNeg, dictAllResult)
        # print('Done Man Judgmented '+time.strftime('%Y-%m-%d %H:%M:%S'))
        dictAllResult = ToGrepSampleKW(curMLPos, dictAllResult)
        dictAllResult = ToGrepSampleKW(curMLUseless, dictAllResult)
        dictAllResult = ToGrepSampleKW(curMLNeg, dictAllResult)
        # print('Done Machine Judgmented '+time.strftime('%Y-%m-%d %H:%M:%S'))
        dictAllResult = ToArraySample(dictAllResult)
        # print('Done Arr '+time.strftime('%Y-%m-%d %H:%M:%S'))

        curPos.close()
        curUseless.close()
        curNeg.close()

        curMLPos.close()
        curMLUseless.close()
        curMLNeg.close()

        arrXPreTrain = dictAllResult['arrXPreTrain']
        arrYPreTrain = dictAllResult['arrYPreTrain']

        dfY = pd.DataFrame(arrYPreTrain)

        # DataFrame 中第几列
        def funGetEntropy(data_df, columns=None):
            time.sleep(0.01)
            if (columns is None):
                raise "the columns must be not empty!"
            # Information Entropy
            pe_value_array = data_df[columns].unique()
            ent = 0.0
            for x_value in pe_value_array:
                p = float(data_df[data_df[columns] ==
                                  x_value].shape[0]) / data_df.shape[0]
                logp = np.log2(p)
                ent -= p * logp
            return ent

        intLenOfXPreT = len(arrXPreTrain)
        arrKWForEntropy = []

        # 获得样本各关键字信息熵
        # print('Get Samples Entropy')
        for intI in range(dictAllResult['intIndexNow']):
            arrTmp = [arrXPreTrain[intJ][intI]
                      for intJ in range(intLenOfXPreT)]
            dfX = pd.DataFrame(arrTmp)
            if dfY[dfX[0]].shape[0] > 1:
                arrKWForEntropy.append(
                    [dictAllResult['arrColunms'][intI], funGetEntropy(dfY[dfX[0]], 0)])

        dfEntropy = pd.DataFrame(arrKWForEntropy, columns=['KW', 'IE'])
        # 分桶
        cutB = pd.cut(dfEntropy['IE'], 2, labels=['L', 'H'])
        dfEntropy['IEBin'] = cutB

        nparrKWWaitFor = dfEntropy[dfEntropy['IEBin'] == 'L']['KW'].values

        # 重新对低熵关键字在样本中排序
        # print('Redim the Keyword')
        arrForTrain = [[] for intI in range(len(arrXPreTrain))]
        for nparrEle in nparrKWWaitFor:
            intJ = dictAllResult['dictKW'][nparrEle]
            for intK in range(intLenOfXPreT):
                arrForTrain[intK].append(arrXPreTrain[intK][intJ])

        intKWWaitFor = len(nparrKWWaitFor)

        for intI in range(len(arrXPreTrain)):
            intSumKW = 0
            for intJ in range(intKWWaitFor):
                intSumKW = intSumKW or arrXPreTrain[intI][intJ]
            if intSumKW == False:
                arrYPreTrain[intI] = 0

        # 过滤无用的样本
        arrXForTrainReal = []
        arrYForTrainReal = []
        for intI in range(len(arrYPreTrain)):
            if(arrYPreTrain[intI] != 0):
                arrXForTrainReal.append(arrForTrain[intI])
                arrYForTrainReal.append(arrYPreTrain[intI])

        # 开始学习
        # print('Start Learn')
        clfBagging = BaggingClassifier(base_estimator=LinearSVC(
            random_state=0, tol=1e-03, max_iter=1000))
        clfBagging.fit(arrXForTrainReal, arrYForTrainReal)

        # 输出信息
        # print('Output Data')
        strFileName = 'svcclf'+datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        strSavePath = self.strDirForClf + strFileName
        joblib.dump(
            clfBagging, strSavePath)

        strNow = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        dateNow = parser.parse(strNow)
        dictData = {'clfFileName': strFileName,
                    'kwlist': nparrKWWaitFor.tolist(), 'lt': dateNow}
        self.objMongoDB.InsertOne('clfdb', dictData)
        clfBagging = None
        self.LoadLatestClf()
