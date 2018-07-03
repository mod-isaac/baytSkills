import pymongo
from pymongo import MongoClient
import sys
sys.path.append('../connections')
sys.path.append('../nlp')
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
    if limit == "":
        skillsList = list(raw_collection.find({},{"_id":False,"skills": True}))
    else:
        skillsList = list(raw_collection.find({},{"_id":False,"doc_id": True,"skills": True}).limit(limit))
    for doc in skillsList:
        for key,val in doc.items():
            skillsVectors.append(val)
    i = iter(skillsVectors)
    skillsVectorsDict = dict(zip(i, i))
    for key,val in skillsVectorsDict.items():
        dictList[val] = key
    return dictList
