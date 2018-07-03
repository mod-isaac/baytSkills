#************************ Bayt Skills Analysis Project*****************************#
#****************************MONGO_CONFIG******************************************#
####################################################################################
MONGO_CLIENT                  	=    'mongodb'
MONGO_HOST                    	=    'localhost'
MONGO_PORT                    	=    '27017'
####################################################################################
#****************************SHPINX_CONFIG*****************************************#
####################################################################################
SPHINX_USERNAME                 =   ''
SPHINX_PASSWORD                 =   ''
DATABASE                        =   'MySQLdb'
SPHINX_HOST                 	=   '192.168.6.30'
SPHINX_PORT			            =   '3391'
####################################################################################
#****************************POSTGRES_CONFIG***************************************#
####################################################################################
PG_CORE_DATABASE_NAME           =   'baytcore'
PG_CORE_HOST                    =   '192.168.6.23'
PG_CORE_PORT                    =   '5432'
PG_CORE_USERNAME                =   'baytpg'
PG_CORE_PASSWORD                =   'casi02'

BAYT_PG_DATABASE_NAME           =   'baytpg'
BAYT_PG_HOST                    =   '192.168.6.23'
BAYT_PG_PORT                    =   '5435'
BAYT_PG_USERNAME                =   'baytpg'
BAYT_PG_PASSWORD                =   'casi02'
################################Mongo I/O INFO####################################
MONGO_PORT                      =   '27017'
MONGO_CLIENT                    =   'mongodb'
MONGO_HOST                      =   'localhost'
MONGO_POOL                      =   'csv_data'
MONGO_RAW_COLLECTION            =   'csv_skills_clean'
MONGO_TFIDF_COLLECTION          =   'tfidf_skills'
MONGO_SVD_COLLECTION            =   'svd_skills'
MONGO_KMEAN_COLLECTION          =   'kmean_skills'
MONGO_KMEAN_DOCS_COLLECTION     =   'kmean_docs'
MONGO_SKILLS_CLUSTER            =   'skillsClusters'
MONGO_TITLES_CLUSTER            =   'titlesClusters'
MONGO_TI_SK_CLUSTER             =   'titlesWithSkills'
MONGO_CSV_SKILLS                =   'csv_skills_agg'
MONGO_CSV_EXPS                  =   'csv_experinces_agg'
MONGO_SKILLS_TEMP               =   'csv_skills'
MONGO_EXPS_TEMP                 =   'csv_experinces'
################################Data Info####################################
CSV_BATCH_LIMIT                 =   100
AGGRIGATION_LIMIT               =   100
KMEAN_NCLUSTERS                 =   2000
################################FILES Info###################################
SKILLS_CSV                      =   'allSkills.csv'
EXPERINCE_CSV                   =   'allTitles.csv'
MORE_THAN                       =   2
