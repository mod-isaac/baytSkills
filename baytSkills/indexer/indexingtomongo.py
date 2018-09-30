import datapreparing
import pymongo
from pymongo import MongoClient
import sys
sys.path.append('/bayt/software/app/baytSkills/connections')
import mongoConfig
import connectionsmanager
import datapreprocessing

root_path = mongoConfig.ROOT_PATH

nlp_path    = root_path+'nlp'
sys.path.append(nlp_path)
ml_path = root_path+'ml'
sys.path.append(ml_path)
log_path =  root_path+'logs/skillsServiceLog.log'

from pymongo import MongoClient

import logging
logging.basicConfig(filename=log_path, level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger=logging.getLogger(__name__)

try:
    client                      = connectionsmanager.connManager.mongodbConnectionInfo()
    client_R                    = connectionsmanager.connManager.mongodbConnectionInfo_R()
    db                          = client[connectionsmanager.mongoConfig.MONGO_POOL]
    db_R                        = client_R[connectionsmanager.mongoConfig.MONGO_POOL_R]
    raw_collection              = db[connectionsmanager.connManager.MONGO_RAW_COLL]
    tfidf_collection            = db[connectionsmanager.connManager.MONGO_TFIDF_COLL]
    svd_collection              = db[connectionsmanager.connManager.MONGO_SVD_COLL]
    kmean_collection            = db[connectionsmanager.connManager.MONGO_KMEAN_COLL]
    kmean_docs_collection       = db[connectionsmanager.connManager.MONGO_KMEAN_DOCS_COLL]
    skills_clus_collection      = db[connectionsmanager.connManager.MONGO_SKILLS_CLUS_COLL]
    titles_clus_collection      = db[connectionsmanager.connManager.MONGO_TITLES_CLUS_COLL]
    tit_ski_clus_collection     = db[connectionsmanager.connManager.MONGO_TITLES_SKILS_COLL]
    tit_ski_clus_collection_R   = db_R[connectionsmanager.connManager.MONGO_TITLES_SKILS_COLL]
except Exception as e:
    logger.critical(str(e))


def insertRawSkillsBulck(bulkLimit=50):
    initBacth = 0
    dataList = datapreparing.rawDocumentsProvider()
    while (initBacth < len(dataList)):
        tempBulckList = []
        for rawsBatch in dataList[initBacth:bulkLimit+initBacth]:
            tempBulckList.append(rawsBatch)
        raw_collection.insert_many(tempBulckList)
        initBacth = initBacth + bulkLimit

def insertSkillsTFIDFBulck(bulkLimit=50):
    import tfidf
    initBacth = 0
    dataList = tfidf.vectorsTFIDF()
    while (initBacth < len(dataList)):
        tempBulckList = []
        for rawsBatch in dataList[initBacth:bulkLimit+initBacth]:
            tempBulckList.append(rawsBatch)
        tfidf_collection.insert_many(tempBulckList)
        initBacth = initBacth + bulkLimit

def insertSkillsSVDFBulck(bulkLimit=50):
    import lsa
    initBacth = 0
    dataList = lsa.SVDFetcher()
    while (initBacth < len(dataList)):
        tempBulckList = []
        for rawsBatch in dataList[initBacth:bulkLimit+initBacth]:
            tempBulckList.append(rawsBatch)
        svd_collection.insert_many(tempBulckList)
        initBacth = initBacth + bulkLimit

def insertSkillsKmeanBulck(bulkLimit=50):
    import kmean
    initBacth = 0
    dataList = kmean.KMeanFetcher()
    while (initBacth < len(dataList)):
        tempBulckList = []
        for rawsBatch in dataList[initBacth:bulkLimit+initBacth]:
            tempBulckList.append(rawsBatch)
        kmean_collection.insert_many(tempBulckList)
        initBacth = initBacth + bulkLimit

def insertSkillsKmeanDocsBulck(bulkLimit=50):
    import titlekmean
    initBacth = 0
    dataList = titlekmean.KMeanDocsFetcher()
    while (initBacth < len(dataList)):
        tempBulckList = []
        for rawsBatch in dataList[initBacth:bulkLimit+initBacth]:
            tempBulckList.append(rawsBatch)
        kmean_docs_collection.insert_many(tempBulckList)
        initBacth = initBacth + bulkLimit
    kmean_docs_collection.create_index([("cluster", pymongo.ASCENDING)])
def insertRawSkillsToClusters():
    SOURCE_LEN  = kmean_docs_collection.count()
    DIST_LEN    = 0
    from bson.objectid import ObjectId
    while (SOURCE_LEN > DIST_LEN):
        msg = "insertRawSkillsToClusters >>>", DIST_LEN
        logger.debug(msg)
        mongoIDS = datapreprocessing.getMongoIds(kmean_docs_collection,1)
        for id in mongoIDS:
            IdsCluster = list(kmean_docs_collection.find({"_id": ObjectId(id)},{"_id":False}))
            IdsList = IdsCluster[0]
            IdsList = IdsList['cluster']
            IdsList = str(IdsList).strip('[]')
            mongoBulk = datapreprocessing.getCVsClusterSkills(IdsList)
            skills_clus_collection.insert_one(mongoBulk)
        DIST_LEN    = skills_clus_collection.count()
#########################################################
def insertTitlesWithSkills():
    from bson.objectid import ObjectId
    import json
    SOURCE_LEN  = kmean_docs_collection.count()
    DIST_LEN    = 0
    while (SOURCE_LEN > DIST_LEN):
        msg = "insertTitlesWithSkills >>>", DIST_LEN
        print(msg)
        mongoIDS    = datapreprocessing.getMongoIds(kmean_docs_collection,2)
        for id in mongoIDS:
            try:
                IdsCluster = list(kmean_docs_collection.find({"_id": ObjectId(id)},{"_id":False}))
                IdsList = IdsCluster[0]
                IdsList = IdsList['cluster']
                IdsList = list(set(IdsList))
                for id in IdsList:
                    mongoBulk = datapreprocessing.getCVstitlesWithSkills(id)
                    tit_ski_clus_collection.insert_one(mongoBulk)
                    tit_ski_clus_collection_R.insert_one(mongoBulk)
            except Exception as e:
                pass
        DIST_LEN    = tit_ski_clus_collection.count()
        tit_ski_clus_collection.create_index([("skills", pymongo.TEXT),
                                            ("title", pymongo.TEXT)])
###########################################################
def cvsToMongo():
    import pandas as pd
    file = 'test_data2.csv'
    chunksize = 100
    j = 1
    for df in pd.read_csv(file, chunksize=chunksize, iterator=True, encoding='utf-8', header=None):
        x = df.iloc[:]
        ids = x[0]
        skills = x[1]
        u = zip(ids, skills)
        c = []
        j = 1
        for i in u:
            c.append({'index_id':j, 'doc_id':i[0], 'skill':i[1]})
            j = j + 1
        print(c)
def IndexProcessedData():
    import time
    start_time = time.time()
    try:
        cleanSkillColl          = db[connectionsmanager.connManager.MONGO_RAW_COLL]
        mongoThread = cleanSkillColl.count({"status": {"$ne":"yes"}})
        while (mongoThread > 0):
            insertSkillsKmeanDocsBulck()
            mongoThread = cleanSkillColl.count({"status": {"$ne":"yes"}})
    except Exception as e:
        logger.critical(str(e))

    try:
        insertRawSkillsToClusters()
    except Exception as e:
        logger.critical(str(e))

    try:
        insertTitlesWithSkills()
    except Exception as e:
        logger.critical(str(e))
