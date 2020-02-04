'''
@Descripttion: 链接 MongoDB 的库
@Author: BerryBC
@Date: 2020-02-02 11:21:44
@LastEditors  : BerryBC
@LastEditTime : 2020-02-05 00:31:10
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

    # def LoadRandomLimit(self, strTbCfgSet, dictFilter, intLimit):
    #     dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
    #     dbMongo.authenticate(
    #         self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
    #     return dbMongo[self.objConfig[strTbCfgSet]['table']].aggregate([{'$match': dictFilter}, {'$sample': {'size': intLimit}}])

    # def InsertSome(self, strTbCfgSet, arrInsert):
    #     dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
    #     dbMongo.authenticate(
    #         self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
    #     dbMongo[self.objConfig[strTbCfgSet]['table']].insert_many(arrInsert)

    # def CheckOneExisit(self, strTbCfgSet, dictFilter):
    #     dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
    #     dbMongo.authenticate(
    #         self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
    #     eleData = dbMongo[self.objConfig[strTbCfgSet]
    #                       ['table']].find_one(dictFilter)
    #     if eleData is None:
    #         return False
    #     else:
    #         return True

    # def LoadAllData(self, strTbCfgSet):
    #     dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
    #     dbMongo.authenticate(
    #         self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
    #     return dbMongo[self.objConfig[strTbCfgSet]['table']].find()

    # def UpdateOneData(self, strTbCfgSet, dictFilter, dictValue):
    #     dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
    #     dbMongo.authenticate(
    #         self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
    #     dbMongo[self.objConfig[strTbCfgSet]['table']].update_one(
    #         dictFilter, {"$set": dictValue})

    # def DeleteSome(self, strTbCfgSet, dictFilter):
    #     dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
    #     dbMongo.authenticate(
    #         self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
    #     return dbMongo[self.objConfig[strTbCfgSet]['table']].delete_many(dictFilter)

    # 这个是缩短的
    def GetTable(self, strTbCfgSet):
        dbMongo = self.dbClient[self.objConfig[strTbCfgSet]['database']]
        dbMongo.authenticate(
            self.objConfig[strTbCfgSet]['user'], self.objConfig[strTbCfgSet]['passwork'])
        return dbMongo[self.objConfig[strTbCfgSet]['table']]

    def LoadRandomLimit(self, strTbCfgSet, dictFilter, intLimit):
        return self.GetTable(strTbCfgSet).aggregate([{'$match': dictFilter}, {'$sample': {'size': intLimit}}])

    def InsertSome(self, strTbCfgSet, arrInsert):
        self.GetTable(strTbCfgSet).insert_many(arrInsert)

    def CheckOneExisit(self, strTbCfgSet, dictFilter):
        eleData = self.GetTable(strTbCfgSet).find_one(dictFilter)
        if eleData is None:
            return False
        else:
            return True

    def LoadAllData(self, strTbCfgSet):
        return self.GetTable(strTbCfgSet).find()

    def UpdateOneData(self, strTbCfgSet, dictFilter, dictValue):
        self.GetTable(strTbCfgSet).update_one(
            dictFilter, {"$set": dictValue})

    def DeleteSome(self, strTbCfgSet, dictFilter):
        return self.GetTable(strTbCfgSet).delete_many(dictFilter)

    def InsertOneWithID(self, strTbCfgSet, dictInsert):
        objIed=self.GetTable(strTbCfgSet).insert_one(dictInsert)
        return objIed.inserted_id

    def InsertOne(self, strTbCfgSet, dictInsert):
        self.GetTable(strTbCfgSet).insert_one(dictInsert)

    def LoadOne(self, strTbCfgSet, dictFilter):
        return self.GetTable(strTbCfgSet).find_one(dictInsert)

    def LoadSome(self, strTbCfgSet, dictFilter):
        return self.GetTable(strTbCfgSet).find(dictFilter)