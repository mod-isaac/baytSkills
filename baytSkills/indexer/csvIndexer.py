## CREATE MONGO INDEXIS
import pandas as pd
import pymongo
from pymongo import MongoClient
import threading
from threading import Thread
from multiprocessing import Process
import time, sys
#sys.path.append('/bayt/software/app/baytSkills/connections')
sys.path.append('../connections')
import connectionsmanager
import connectionsinfo
import mongoConfig
root_path   = mongoConfig.ROOT_PATH
nlp_path    = root_path+'nlp'
sys.path.append(nlp_path)
log_path =  root_path+'logs/skillsServiceLog.log'

import os
from pathlib import Path

import logging
logging.basicConfig(filename=log_path, level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger=logging.getLogger(__name__)

import datacleaining
###########
home                    = str(Path.home())
client                  = connectionsmanager.connManager.mongodbConnectionInfo()
db                      = client[connectionsmanager.connManager.MONGO_DB]
skillsCollName          = connectionsinfo.MONGO_SKILLS_TEMP
experincesCollName      = connectionsinfo.MONGO_EXPS_TEMP
pandasChunkSize         = connectionsinfo.CSV_BATCH_LIMIT
CSVAggrigationLimit     = connectionsinfo.AGGRIGATION_LIMIT
skillsFileName          = '/'+connectionsinfo.SKILLS_CSV
experincesFileName      = '/'+connectionsinfo.EXPERINCE_CSV
joinedCSVFileName       = '/'+mongoConfig.JOINED_CSV
###########
skillsFiledName     = "skills"
experincesFiledName = "experinces"
def csvSplitter():
    try:
        os.remove(skillsFileName)
    except Exception as e:
         logger.critical(str(e))
    try:
        os.remove(experincesFileName)
    except Exception as e:
         logger.critical(str(e))


    df = pd.read_csv(joinedCSVFileName)
    df.columns = ['cv_id','skill_name','exp_title']

    skills_df = df[['cv_id','skill_name']]
    skills_df.to_csv('skills.csv', sep=',', encoding='utf-8',index = False)

    exp_df = df[['cv_id','exp_title']]
    exp_df.to_csv('experinces.csv', sep=',', encoding='utf-8',index = False)

    skills_df = pd.read_csv('skills.csv')
    skills_df.drop_duplicates(subset=None, inplace=True)
    skills_df.to_csv(skillsFileName, sep=',', encoding='utf-8',index = False)

    exp_df = pd.read_csv('experinces.csv')
    exp_df.drop_duplicates(subset=None, inplace=True)
    exp_df.to_csv(experincesFileName, sep=',', encoding='utf-8',index = False)

    os.remove('experinces.csv')
    os.remove('skills.csv')


def csvToMongo(fileName,collName,field):

    ##get out if Aggregation is done
    chunksize = pandasChunkSize
    file = fileName
    collection = db[collName]
    try:
        if collection.find({}).count() == 0 and collName in db.collection_names():
            collection.insert({"delete":"1"})
    except Exception as e:
         logger.critical(str(e))
    try:
        status = list (collection.find({},{"_id":False}))
        if status[0]['delete'] == '1':
            return
    except Exception as e:
         logger.critical(str(e))
    try:
        maxID = collection.find_one(sort=[("index_id", -1)])["index_id"]
    except Exception as e:
        maxID = 0
    df = pd.read_csv(file,encoding='utf-8', header=None,error_bad_lines=False)
    if len(df.index) == maxID + 1:
        msg = "Congrats ", fileName, " INSERTED to Mongo"
        logger.debug(msg)
        pass
    else:
        for df in pd.read_csv(file, chunksize=chunksize, iterator=True, encoding='utf-8', header=None,error_bad_lines=False):
            frameList = df.iloc[:]
            ids = frameList[0]
            skills = frameList[1]
            joinedDocs = zip(ids, skills)
            insertList = []
            for i in joinedDocs:
                if df.index[-1] > maxID:
                    maxID = maxID + 1
                    try:
                        id = int(i[0])
                        insertList.append({'index_id':maxID, 'doc_id':id, field:i[1]})
                    except Exception as e:
                         logger.critical(str(e))
            if len(insertList) > 0:
                try:
                    collection.insert_many(insertList)
                except Exception as e:
                     logger.critical(str(e))

def mongoRemoveByDocID(collName,delIDs):
    collection = db[collName]
    collection.delete_many({"doc_id":{"$in":delIDs}})
    msg = collName, ' >>>>>> ' , collection.find({}).count()
    logger.debug(msg)
    pass

def mongoAddBatch(collName,batch,Jbatch):
    collectionName = collName + "_agg"
    collection = db[collectionName]
    for i in batch:
        i.pop('_id', None)
    try:
        collection.insert_many(batch)
    except Exception as e:
         logger.critical(str(e))
    if collName == skillsCollName:
        JMongoList = []
        cleanCollName = collName + "_clean"
        collection = db[cleanCollName]
        for i in Jbatch:
            tempJMongoDict = {}
            try:
                tempJMongoDict['doc_id'] = i['id']
                tempJMongoDict['skills'] = datacleaining.textCleaner(i['skills'])
                JMongoList.append(tempJMongoDict)
            except Exception as e:
                 logger.critical(str(e))
        if len(JMongoList):
            try:
                collection.insert_many(JMongoList)
            except Exception as e:
                 logger.critical(str(e))

def parallelMongoOpes(collName,delIDs,addBatch,JaddBatch):
    mongoAddBatch(collName,addBatch,JaddBatch)
    collection          = db[collName]
    aggCollection       = db[collName+'_agg']
    cleanCollection     = db[collName+'_clean']
    collection.create_index([("id", pymongo.ASCENDING)])
    aggCollection.create_index([("id", pymongo.ASCENDING)])
    cleanCollection.create_index([("doc_id", pymongo.ASCENDING)])
    mongoRemoveByDocID(collName,delIDs)

def idsAggregator(collName,field):
    batchLimit = CSVAggrigationLimit
    collection = db[collName]
    idsList = []
    uniqueIds = []
    mainList = []
    JmainList = []
    interrupter = 0
    while (len(uniqueIds) < batchLimit):
        doc_ids = list(collection.find({"doc_id":{"$nin":uniqueIds}},{"_id":False,field:False,"index_id":False}).limit(batchLimit))
        interrupter = interrupter+1
        for id in doc_ids:
            try:
                idsList.append(id['doc_id'])
            except Exception as e:
                 logger.critical(str(e))
        uniqueIds = list(set(idsList))
        if len(uniqueIds) == batchLimit:
            interrupter = interrupter + 1
        if interrupter == 2:
            break
    fields =  list(collection.find({"doc_id":{"$in":uniqueIds}},{"_id":False,"index_id":False}))
    cleanFields = []
    for raw in fields:
        cleanFields.append(list(raw.values()))
    for id in uniqueIds:
        tempDict = {}
        tempList = []
        if collName == skillsCollName:
            ################
            JtempDict = {}
            JtempList = []
            JtempDict ['id'] = id
            ################
        for subField in cleanFields:
            try:
                subId = int(subField[0])
                subSkill = subField[1]
            except Exception as e:
                subId = subField[1]
                subSkill = subField[0]
            if subId == id:
                tempList.append(subSkill)
        if collName == skillsCollName:
            try:
                JtempDict[field] = ' '.join(tempList)
            except Exception as e:
                 logger.critical(str(e))
        tempDict[field] = tempList
        if len(tempList):
            tempDict['id'] = id
            mainList.append(tempDict)
            if collName == skillsCollName:
                JmainList.append(JtempDict)
    parallelMongoOpes(collName,uniqueIds,mainList,JmainList)

def insertingAggregated(collName,field):
    collection = db[collName]
    try:
        status = list (collection.find({},{"_id":False}))
        if status[0]['delete'] == '1':
            return
    except Exception as e:
        pass
    while (collection.find({}).count()):
        idsAggregator(collName,field)

def csvToMongoOps(filename,collection,field):
    csvToMongo(filename,collection,field)
    insertingAggregated(collection,field)

def indexCSVFormat():
    csvSplitter()
    Pros = []
    skills      =   Thread(target = csvToMongoOps, args=(skillsFileName,skillsCollName,skillsFiledName,))
    experinces  =   Thread(target = csvToMongoOps, args=(experincesFileName,experincesCollName,experincesFiledName,))
    Pros.append(skills)
    skills.start()
    Pros.append(experinces)
    experinces.start()
    for t in Pros:
        t.join()
