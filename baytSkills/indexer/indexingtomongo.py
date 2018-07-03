import datapreparing
import pymongo
from pymongo import MongoClient
import sys
sys.path.append('../connections')
sys.path.append('../ml')
sys.path.append('../nlp')
import connectionsmanager
import datapreprocessing
from pymongo import MongoClient

client                  = connectionsmanager.connManager.mongodbConnectionInfo()
db                      = client[connectionsmanager.connManager.MONGO_DB]
raw_collection          = db[connectionsmanager.connManager.MONGO_RAW_COLL]
tfidf_collection        = db[connectionsmanager.connManager.MONGO_TFIDF_COLL]
svd_collection          = db[connectionsmanager.connManager.MONGO_SVD_COLL]
kmean_collection        = db[connectionsmanager.connManager.MONGO_KMEAN_COLL]
kmean_docs_collection   = db[connectionsmanager.connManager.MONGO_KMEAN_DOCS_COLL]
skills_clus_collection  = db[connectionsmanager.connManager.MONGO_SKILLS_CLUS_COLL]
titles_clus_collection  = db[connectionsmanager.connManager.MONGO_TITLES_CLUS_COLL]
tit_ski_clus_collection = db[connectionsmanager.connManager.MONGO_TITLES_SKILS_COLL]


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

def insertRawSkillsToClusters():
    SOURCE_LEN  = kmean_docs_collection.count()
    DIST_LEN    = 0
    from bson.objectid import ObjectId
    while (SOURCE_LEN > DIST_LEN):
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
            except Exception as e:
                pass
        DIST_LEN    = tit_ski_clus_collection.count()
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
#insertRawSkillsBulck()
#insertSkillsTFIDFBulck()
#insertSkillsSVDFBulck()
#insertSkillsKmeanBulck()
import time
start_time = time.time()
insertSkillsKmeanDocsBulck()
print("--- %s seconds ---" % (time.time() - start_time))
insertRawSkillsToClusters()
print("--- %s seconds ---" % (time.time() - start_time))
insertTitlesWithSkills()
print("--- %s seconds ---" % (time.time() - start_time))
#insertRawSkillsToClusters()
#insertTitlesWithSkills()
