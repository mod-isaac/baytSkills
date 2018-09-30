from pyramid.view import view_config
from pyramid.renderers import JSON
from wsgiref.simple_server import make_server
import wsgiref
from pyramid.config import Configurator
from pyramid.response import Response
import pymongo
import sys
from pymongo import MongoClient
#sys.path.append('/bayt/software/app/baytSkills/connections')
sys.path.append('../connections')
import connectionsmanager
import connectionsinfo
import mongoConfig
import logging
import os
import time

root_path =  mongoConfig.ROOT_PATH
log_path =  root_path+'logs/skillsServiceLogLive.log'
logging.basicConfig(filename=log_path, level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger=logging.getLogger(__name__)

client                  = connectionsmanager.connManager.mongodbConnectionInfo_R()
db                      = client[connectionsmanager.mongoConfig.MONGO_POOL_R]
skills_clus_collection  = db[connectionsmanager.connManager.MONGO_SKILLS_CLUS_COLL]
titles_clus_collection  = db[connectionsmanager.connManager.MONGO_TITLES_CLUS_COLL]
tit_ski_clus_collection = db[connectionsmanager.connManager.MONGO_TITLES_SKILS_COLL]
select_more             = connectionsinfo.MORE_THAN
pyr_host                = mongoConfig.PYRAMID_HOST
pyr_port                = mongoConfig.PYRAMID_PORT

def skillsMongoJSON(skill):
    skill = skill['skill']
    import collections
    import operator
    from collections import OrderedDict
    import json
    extractedSkills = []
    extractedList = []
    try:
        skillsList = list(tit_ski_clus_collection.find({"skills":skill},{"title":False,"_id":False,"status":False}))
    except Exception as e:
        print(e)
    for entity in skillsList:
        for key,val in entity.items():
            extractedSkills.append(val)
    for subList in extractedSkills:
        for i in subList:
            if i != skill:
                extractedList.append(i.lower().strip())
    x = dict(collections.Counter(extractedList))
    sorted_x = dict(sorted(x.items(), key=operator.itemgetter(1), reverse=True))
    #sorted_x = list(sorted_x.keys())
    print(sorted_x)
    cleanedSkills = []
    for key,val in sorted_x.items():
        if val > select_more:
            cleanedSkills.append({key:val})
    if len(cleanedSkills) == 0:
            for key,val in sorted_x.items():
                cleanedSkills.append({key:val})
    #return json.dumps(sorted_x[:select_more], ensure_ascii=False)
    return json.dumps(cleanedSkills, ensure_ascii=False)
####################
def skill2titlesMongoJSON(skill):
    skill = skill['skill']
    import collections
    import operator
    from collections import OrderedDict
    import json
    extractedSkills = []
    extractedList = []
    skillsList = list(tit_ski_clus_collection.find({"skills":skill},{"skills":False,"_id":False,"status":False}))
    for entity in skillsList:
        for key,val in entity.items():
            if val[0] != skill:
                extractedSkills.append(val[0].lower().strip())

    x = dict(collections.Counter(extractedSkills))
    sorted_x = dict(sorted(x.items(), key=operator.itemgetter(1), reverse=True))
    #sorted_x = list(sorted_x.keys())

    cleanedSkills = []
    for key,val in sorted_x.items():
        if val > select_more:
            cleanedSkills.append({key:val})
    if len(cleanedSkills) == 0:
            for key,val in sorted_x.items():
                cleanedSkills.append({key:val})
    #return json.dumps(sorted_x[:select_more], ensure_ascii=False)
    return json.dumps(cleanedSkills, ensure_ascii=False)
####################
def title2skillsMongoJSON(title):
    title = title['title']
    import collections
    import operator
    from collections import OrderedDict
    import json
    extractedTitles = []
    extractedList = []
    titlesList = list(tit_ski_clus_collection.find({"title":title},{"title":False,"_id":False,"status":False}))
    for entity in titlesList:
        for key,val in entity.items():
            if val[0] != title:
                extractedTitles.append(val[0].lower().strip())

    x = dict(collections.Counter(extractedTitles))
    sorted_x = dict(sorted(x.items(), key=operator.itemgetter(1), reverse=True))
    #sorted_x = list(sorted_x.keys())
    cleanedTitles = []
    for key,val in sorted_x.items():
        if val > select_more:
            cleanedTitles.append({key:val})
    if len(cleanedTitles) == 0:
        for key,val in sorted_x.items():
            cleanedTitles.append({key:val})
    #return json.dumps(sorted_x[:select_more], ensure_ascii=False)
    return json.dumps(cleanedTitles, ensure_ascii=False)
####################
def titlesMongoJSON(title):
    title = title['title']
    import collections
    import operator
    from collections import OrderedDict
    import json
    extractedTitles = []
    extractedList = []
    titlesList = list(tit_ski_clus_collection.find({"title":title},{"_id":False,"skills":False,"status":False}))
    for entity in titlesList:
        for key,val in entity.items():
            extractedTitles.append(val)
    for subList in extractedTitles:
        for i in subList:
            if i != title:
                extractedList.append(i.lower().strip())
    x = dict(collections.Counter(extractedList))
    sorted_x = dict(sorted(x.items(), key=operator.itemgetter(1), reverse=True))
    #sorted_x = list(sorted_x.keys())

    cleanedTitles = []
    for key,val in sorted_x.items():
        if val > select_more:
            cleanedTitles.append({key:val})
    if len(cleanedTitles) == 0:
        for key,val in sorted_x.items():
            cleanedTitles.append({key:val})
    #return json.dumps(sorted_x[:select_more], ensure_ascii=False)
    return json.dumps(cleanedTitles, ensure_ascii=False)

@view_config(renderer='json')
def skill2skill(request):

    logger.debug("skill2skill Logging")
    return Response(skillsMongoJSON(request.matchdict))


@view_config(renderer='json')
def title2title(request):
    try:
        logger.debug("title2titles Logging")
        return Response(titlesMongoJSON(request.matchdict))
    except Exception as e:
        logger.critical(str(e))

@view_config(renderer='json')
def skill2titles(request):
    try:
        logger.debug("skill2titles Logging")
        return Response(skill2titlesMongoJSON(request.matchdict))
    except Exception as e:
        logger.critical(str(e))

@view_config(renderer='json')
def title2skills(request):
    try:
        logger.debug("title2skills Logging")
        return Response(title2skillsMongoJSON(request.matchdict))
    except Exception as e:
        logger.critical(str(e))


if __name__ == '__main__':

    ##  REFRESHING CACHE AND DO CLEAN FOR NEXT RUN BY PROCESS SLEEP
    time.sleep(5)

    ##  START RUN NEW PYRAMID APP BY SAME KILLIED PORT
    with Configurator() as config:
        config.add_route('skills', 'skills/{skill}')
        config.add_view(skill2skill, route_name='skills',renderer='json')

        config.add_route('skilltotitles', 'skilltotitles/{skill}')
        config.add_view(skill2titles, route_name='skilltotitles',renderer='json')

        config.add_route('titletoskills', 'titletoskills/{title}')
        config.add_view(title2skills, route_name='titletoskills',renderer='json')

        config.add_route('titles', 'titles/{title}')
        config.add_view(title2title, route_name='titles',renderer='json')
    app = config.make_wsgi_app()
    server = make_server(pyr_host, pyr_port, app)
    server.serve_forever()
