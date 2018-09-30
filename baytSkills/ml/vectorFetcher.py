import pymongo
from pymongo import MongoClient
import sys
sys.path.append('/bayt/software/app/baytSkills/connections')
import mongoConfig
root_path = mongoConfig.ROOT_PATH


nlp_path    = root_path+'nlp'
sys.path.append(nlp_path)

import datacleaining

import connectionsmanager
from pymongo import MongoClient

client              = connectionsmanager.connManager.mongodbConnectionInfo()
db                  = client[connectionsmanager.connManager.MONGO_DB]
raw_collection      = db[connectionsmanager.connManager.MONGO_RAW_COLL]


def getSkillsList(limit=20):
    skillsVectors = []
    if limit == "":
        skillsList = list(raw_collection.find({},{"_id":False,"doc_id": False}))
    else:
        skillsList = list(raw_collection.find({},{"_id":False,"doc_id": False}).limit(limit))
    for doc in skillsList:
        for key,val in doc.items():
            if len(val.strip(" ")):
                skillsVectors.append(val)
    return skillsVectors

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]

def getSkillsDocsList(limit=""):
    skillsVectors = []
    skillsVectorsDict = []
    dictList = {}
    u_ids = []
    if limit == "":
        skillsList = list(raw_collection.find({},{"_id":False,"skills": True}))
    else:
        skillsList = list(raw_collection.find({"status": {"$ne":"yes"}},{"_id":False,"doc_id": True,"skills": True}).limit(limit))
        for u_id in skillsList:
            u_ids.append(u_id['doc_id'])
        for DocId in u_ids:
            try:
                raw_collection.update_one({"doc_id":DocId},{"$set": {"status":"yes"}}, upsert=True)
            except Exception as e:
                print(e)
    for doc in skillsList:
        for key,val in doc.items():
            skillsVectors.append(val)
    i = iter(skillsVectors)
    skillsVectorsDict = dict(zip(i, i))
    for key,val in skillsVectorsDict.items():
        dictList[val] = key
    return dictList
