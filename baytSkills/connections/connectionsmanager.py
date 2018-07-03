import connectionsinfo

class ConnectionsManager(object):
    """Managing PG and Mongo connections info"""
    ROLE                    =   ""
    BATCHLIMIT              =   ""
    MONGO_DB                =   connectionsinfo.MONGO_POOL
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
        ####Shpinx Config####
        self.shpinxHost     = connectionsinfo.SPHINX_HOST
        self.shpinxPort     = int(connectionsinfo.SPHINX_PORT)

        ####Mongodb Config####
        self.mongodbHost        = connectionsinfo.MONGO_HOST
        self.mongodbClient      = connectionsinfo.MONGO_CLIENT
        self.mongodbPort        = connectionsinfo.MONGO_PORT
        self.mongodbPool        = connectionsinfo.MONGO_POOL

        ####Postgres Config####
        self.pgName         = connectionsinfo.PG_CORE_DATABASE_NAME
        self.pgHost         = connectionsinfo.PG_CORE_HOST
        self.pgPort         = connectionsinfo.PG_CORE_PORT
        self.pgUsername     = connectionsinfo.PG_CORE_USERNAME
        self.pgPassword     = connectionsinfo.PG_CORE_PASSWORD

    def sphinxReadExe(self,stmt):
        import MySQLdb
        host    = self.shpinxHost
        port    = self.shpinxPort
        try:
            spx_db = MySQLdb.connect(host=host,port=port,charset='utf8')
            cur = spx_db.cursor()
            cur.execute(stmt)
            res = cur.fetchall()
            spx_db.close()
            return res
        except Exception as e:
            print('Failed to connect Shpinx\n', e)

    def pgCoreReadExe(self,stmt):
        import psycopg2
        name        =   self.pgName
        user        =   self.pgUsername
        host        =   self.pgHost
        password    =   self.pgPassword
        try:
            pg_db = psycopg2.connect(dbname=name, user=user, host=host, password=password)
            cur = pg_db.cursor()
            cur.execute(stmt)
            res = cur.fetchall()
            pg_db.close()
            return res
        except Exception as e:
            print('Failed to connect PGCore\n', e)

    def mongodbConnectionInfo(self):
        from pymongo import MongoClient
        client      = self.mongodbClient
        host        = self.mongodbHost
        port        = self.mongodbPort
        pool        = self.mongodbPool
        client      = MongoClient(client+'://'+host+':'+port+'/')
        return client

connManager = ConnectionsManager()
