import connectionsinfo
import mongoConfig
import logging
import urllib.parse
class ConnectionsManager(object):
    """Managing PG and Mongo connections info"""
    ROLE                    =   ""
    BATCHLIMIT              =   ""
    MONGO_DB                =   mongoConfig.MONGO_POOL
    MONGO_DB_R              =   mongoConfig.MONGO_POOL_R
    MONGO_RAW_COLL          =   connectionsinfo.MONGO_RAW_COLLECTION
    MONGO_TFIDF_COLL        =   connectionsinfo.MONGO_TFIDF_COLLECTION
    MONGO_SVD_COLL          =   connectionsinfo.MONGO_SVD_COLLECTION
    MONGO_KMEAN_COLL        =   connectionsinfo.MONGO_KMEAN_COLLECTION
    MONGO_KMEAN_DOCS_COLL   =   connectionsinfo.MONGO_KMEAN_DOCS_COLLECTION
    MONGO_SKILLS_CLUS_COLL  =   connectionsinfo.MONGO_SKILLS_CLUSTER
    MONGO_TITLES_CLUS_COLL  =   connectionsinfo.MONGO_TITLES_CLUSTER
    MONGO_TITLES_SKILS_COLL =   connectionsinfo.MONGO_TI_SK_CLUSTER
    MONGO_CSV_SKILLS_COLL   =   connectionsinfo.MONGO_CSV_SKILLS
    MONGO_CSV_EXPS_COLL     =   connectionsinfo.MONGO_CSV_EXPS
    MONGO_SKILLS_TEMP       =   connectionsinfo.MONGO_SKILLS_TEMP
    MONGO_EXPS_TEMP         =   connectionsinfo.MONGO_EXPS_TEMP

    def __init__(self):


        ####Data Processing Mongodb Config####
        self.mongodbHost        = mongoConfig.MONGO_HOST
        self.mongodbClient      = mongoConfig.MONGO_CLIENT
        self.mongodbPort        = mongoConfig.MONGO_PORT
        self.mongodbUserName    = mongoConfig.MONGO_USERNAME
        self.mongodbPassword    = mongoConfig.MONGO_PASSWORD
        self.mongodbPool        = mongoConfig.MONGO_POOL

        ####Web service Mongodb Config####
        self.mongodbHost_R        = mongoConfig.MONGO_HOST_R
        self.mongodbClient_R      = mongoConfig.MONGO_CLIENT_R
        self.mongodbPort_R        = mongoConfig.MONGO_PORT_R
        self.mongodbUserName_R    = mongoConfig.MONGO_USERNAME_R
        self.mongodbPassword_R    = mongoConfig.MONGO_PASSWORD_R
        self.mongodbPool_R        = mongoConfig.MONGO_POOL_R

    def mongodbConnectionInfo(self):
        from pymongo import MongoClient
        client      = self.mongodbClient
        host        = self.mongodbHost
        port        = self.mongodbPort
        pool        = self.mongodbPool
        user        = self.mongodbUserName
        pw          = self.mongodbPassword
        try:
            client      = MongoClient(client+'://'+user+':'+urllib.parse.quote(pw)+'@'+host+':'+port+'/'+pool)
        except Exception as e:
             print(e)
        return client
    def mongodbConnectionInfo_R(self):
        from pymongo import MongoClient
        client      = self.mongodbClient_R
        host        = self.mongodbHost_R
        port        = self.mongodbPort_R
        pool        = self.mongodbPool_R
        user        = self.mongodbUserName_R
        pw          = self.mongodbPassword_R
        try:
            client      = MongoClient(client+'://'+user+':'+urllib.parse.quote(pw)+'@'+host+':'+port+'/'+pool)
        except Exception as e:
             print(e)
        return client
connManager = ConnectionsManager()
