from pyramid.view import view_config
from pyramid.renderers import JSON
from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.response import Response
import pymongo
import sys
from pymongo import MongoClient
sys.path.append('../connections')
import connectionsmanager
import connectionsinfo

client                  = connectionsmanager.connManager.mongodbConnectionInfo()
db                      = client[connectionsmanager.connManager.MONGO_DB]
svd_collection          = db[connectionsmanager.connManager.MONGO_SVD_COLL]
kmean_collection        = db[connectionsmanager.connManager.MONGO_KMEAN_COLL]
skills_clus_collection  = db[connectionsmanager.connManager.MONGO_SKILLS_CLUS_COLL]
titles_clus_collection  = db[connectionsmanager.connManager.MONGO_TITLES_CLUS_COLL]
tit_ski_clus_collection = db[connectionsmanager.connManager.MONGO_TITLES_SKILS_COLL]
select_more             = connectionsinfo.MORE_THAN
print(select_more)
def skillsMongoJSON(skill):
    skill = skill['skill']
    import collections
    import operator
    from collections import OrderedDict
    import json
    extractedSkills = []
    extractedList = []
    skillsList = list(skills_clus_collection.find({"cluster":skill},{"_id":False}))
    for entity in skillsList:
        for key,val in entity.items():
            extractedSkills.append(val)
    for subList in extractedSkills:
        for i in subList:
            if i != skill:
                extractedList.append(i)
    x = dict(collections.Counter(extractedList))
    sorted_x = dict(sorted(x.items(), key=operator.itemgetter(1), reverse=True))

    cleanedSkills = []
    for key,val in sorted_x.items():
        if val > select_more:
            cleanedSkills.append({key:val})
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
    skillsList = list(tit_ski_clus_collection.find({"skills":skill},{"skills":False,"_id":False}))
    for entity in skillsList:
        for key,val in entity.items():
            if val[0] != skill:
                extractedSkills.append(val[0])

    x = dict(collections.Counter(extractedSkills))
    sorted_x = dict(sorted(x.items(), key=operator.itemgetter(1), reverse=True))
    cleanedSkills = []
    for key,val in sorted_x.items():
        if val > select_more:
            cleanedSkills.append({key:val})
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
                extractedTitles.append(val[0])

    x = dict(collections.Counter(extractedTitles))
    sorted_x = dict(sorted(x.items(), key=operator.itemgetter(1), reverse=True))
    cleanedTitles = []
    for key,val in sorted_x.items():
        cleanedTitles.append({key:val})
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
    print(titlesList)
    for entity in titlesList:
        for key,val in entity.items():
            extractedTitles.append(val)
    for subList in extractedTitles:
        for i in subList:
            if i != title:
                extractedList.append(i)
    x = dict(collections.Counter(extractedList))
    sorted_x = dict(sorted(x.items(), key=operator.itemgetter(1), reverse=True))

    cleanedTitles = []
    for key,val in sorted_x.items():
        if val > select_more:
            cleanedTitles.append({key:val})
    return json.dumps(cleanedTitles, ensure_ascii=False)

@view_config(renderer='json')
def skill2skill(request):
    return Response(skillsMongoJSON(request.matchdict))

@view_config(renderer='json')
def title2title(request):
    return Response(titlesMongoJSON(request.matchdict))

@view_config(renderer='json')
def skill2titles(request):
    return Response(skill2titlesMongoJSON(request.matchdict))

@view_config(renderer='json')
def title2skills(request):
    return Response(title2skillsMongoJSON(request.matchdict))


def entry_point():
    config = Configurator()

    config.add_route('skills', 'skills/{skill}')
    config.add_view(skill2skill, route_name='skills',renderer='json')

    config.add_route('skilltotitles', 'skilltotitles/{skill}')
    config.add_view(skill2titles, route_name='skilltotitles',renderer='json')

    config.add_route('titletoskills', 'titletoskills/{title}')
    config.add_view(title2skills, route_name='titletoskills',renderer='json')

    config.add_route('titles', 'titles/{title}')
    config.add_view(title2title, route_name='titles',renderer='json')

    app = config.make_wsgi_app()
    server = make_server('0.0.0.0', 8001, app)
    server.serve_forever()

entry_point()
