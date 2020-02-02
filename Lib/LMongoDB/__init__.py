'''
@Descripttion: 链接 MongoDB 的库
@Author: BerryBC
@Date: 2020-02-02 11:21:44
@LastEditors  : BerryBC
@LastEditTime : 2020-02-03 00:19:39
'''

from configobj import ConfigObj
import pymongo


class claMongoDB(object):
    '''
    @name: __init__\n
    @msg: 初始化函数\n
    @param strCfgPath {string} 配置文件路径\n
    @param strDBField {string} 在配置文件中数据库配置\n
    '''

    def __init__(self, strCfgPath, strDBField):
        # strCfgPath will be '../../cfg/dbCfg.ini'
        # strDBField will be 'mongodb'
        self.objConfig = ConfigObj(strCfgPath)
        self.strDBPW = self.objConfig[strDBField]['passwork']
        self.strDBUser = self.objConfig[strDBField]['user']
        self.strPort = self.objConfig[strDBField]['port']
        self.strDBName = self.objConfig[strDBField]['database']
        self.strDBHost = self.objConfig[strDBField]['hosts']
        self.dbClient = pymongo.MongoClient(
            'mongodb://'+self.strDBHost+':'+self.strPort+'/')
        # dbMongo = self.dbClient[self.strDBName]
        # dbMongo.authenticate(self.strDBUser, self.strDBPW)

    def LoadRandomLimit(self, strTbCfgSet, dictFilter, intLimit):
        dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
        dbMongo.authenticate(
            self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
        return dbMongo[self.objConfig[strTbCfgSet]['table']].aggregate([{'$match': dictFilter}, {'$sample': {'size': intLimit}}])

    def InsertSome(self, strTbCfgSet, arrInsert):
        dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
        dbMongo.authenticate(
            self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
        dbMongo[self.objConfig[strTbCfgSet]['table']].insert_many(arrInsert)

    def CheckOneExisit(self, strTbCfgSet, dictFilter):
        dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
        dbMongo.authenticate(
            self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
        eleData = dbMongo[self.objConfig[strTbCfgSet]
                          ['table']].find_one(dictFilter)
        if eleData is None:
            return False
        else:
            return True

    def LoadAllData(self, strTbCfgSet):
        dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
        dbMongo.authenticate(
            self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
        return dbMongo[self.objConfig[strTbCfgSet]['table']].find()

    def UpdateOneData(self, strTbCfgSet, dictFilter, dictValue):
        dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
        dbMongo.authenticate(
            self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
        dbMongo[self.objConfig[strTbCfgSet]['table']].update_one(
            dictFilter, {"$set": dictValue})

    def DeleteSome(self, strTbCfgSet, dictFilter):
        dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
        dbMongo.authenticate(
            self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
        return dbMongo[self.objConfig[strTbCfgSet]['table']].delete_many(dictFilter)
