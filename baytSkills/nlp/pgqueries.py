import sys
sys.path.append('../connections')
import connectionsmanager

client                  = connectionsmanager.connManager.mongodbConnectionInfo()
db                      = client[connectionsmanager.connManager.MONGO_DB]
csv_skills_collection   = db[connectionsmanager.connManager.MONGO_CSV_SKILLS_COLL]
csv_titles_collection   = db[connectionsmanager.connManager.MONGO_CSV_EXPS_COLL]
def experincesClusterSelectorMongoQuery(ids):
    idsList = ids.split(",")
    intIdsList = []
    for id in idsList:
        intIdsList.append(int(id))
    skills =  list(csv_skills_collection.find({"id":{"$in":intIdsList}},{"_id":False,"id":False}))
    return skills


def titlesClusterSelectorMongoQuery(ids):
    idsList = ids.split(",")
    intIdsList = []
    for id in idsList:
        intIdsList.append(int(id))
    titles =  list(csv_titles_collection.find({"id":{"$in":intIdsList}},{"_id":False,"id":False}))
    return titles
#####################################################
def titlesClusterSelectorSingleMongoQuery(ids):
    titles =  list(csv_titles_collection.find({"id":ids},{"_id":False,"id":False}))
    return titles

def experincesClusterSelectorSingleMongoQuery(ids):
    skills =  list(csv_skills_collection.find({"id":ids},{"_id":False,"id":False}))
    return skills
######################################################
