import pgqueries
from datacleaining import vectorCleaner
import sys
sys.path.append('/bayt/software/app/baytSkills/connections')
import connectionsmanager

role                    = connectionsmanager.connManager.ROLE
batchLimit              = connectionsmanager.connManager.BATCHLIMIT

client                  = connectionsmanager.connManager.mongodbConnectionInfo()
db                      = client[connectionsmanager.connManager.MONGO_DB]


def getDocsIDs():
    return pgqueries.getIds(role,batchLimit)

def getDocsInfo():
    infoList = []
    ids = getDocsIDs()
    for id in ids:
        infoList.append(pgqueries.experincesSelectorQuery(id))
    return infoList

def getCleanedVectors():
    cleanedBatch = {}
    for entity in getDocsInfo():
        for key,val in entity.items():
            cleanedBatch[key] = vectorCleaner(val)
    return cleanedBatch

def getMongoIds(collection,stat):
    from bson.objectid import ObjectId
    mongoIdsList = list(collection.find({"status":stat-1},{"_id":True}).limit(100))
    IdsList = []
    for id in mongoIdsList:
        IdsList.append(list(id.values())[0])
        mongo_id = str(id['_id'])
        collection.update_one({'_id':ObjectId(mongo_id)}, {"$set": {"status":stat}})
    return IdsList

def getCVsClusterSkills(idsCluster):
    dataDict = pgqueries.experincesClusterSelectorMongoQuery(idsCluster)
    dataDictlist = []
    for bulk in dataDict:
        for key,val in bulk.items():
            dataDictlist.append(val)
    flat_list = [item for sublist in dataDictlist for item in sublist]
    return {'cluster':list(set(flat_list))}

def getCVstitles(idsCluster):
        dataDict = pgqueries.titlesClusterSelectorMongoQuery(idsCluster)
        dataDictlist = []
        for bulk in dataDict:
            for key,val in bulk.items():
                dataDictlist.append(val)
        flat_list = [item for sublist in dataDictlist for item in sublist]
        return {'cluster':list(set(flat_list))}

def getCVstitlesWithSkills(id):
    titles_dataDict = pgqueries.titlesClusterSelectorSingleMongoQuery(id)
    titles_dataList = []
    for title in titles_dataDict:
        titles_dataList.append(title['experinces'])
    flat_titles_list = [item for sublist in titles_dataList for item in sublist]

    skills_dataDict = pgqueries.experincesClusterSelectorSingleMongoQuery(id)
    skills_dataList = []
    for skill in skills_dataDict:
        skills_dataList.append(skill['skills'])
    flat_skills_list = [item for sublist in skills_dataList for item in sublist]
    dataDict = {'title':list(set(flat_titles_list)) , 'skills':list(set(flat_skills_list)), 'status':0}
    return dataDict
