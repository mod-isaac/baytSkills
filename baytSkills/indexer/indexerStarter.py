import csvIndexer
import indexingtomongo
import logging
import time, sys
sys.path.append('/bayt/software/app/baytSkills/connections')
import connectionsmanager
import connectionsinfo
import mongoConfig
root_path   = mongoConfig.ROOT_PATH
log_path =  root_path+'logs/skillsServiceLog.log'

logging.basicConfig(filename=log_path, level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger=logging.getLogger(__name__)

if __name__ == '__main__':
    try:
        #csvIndexer.indexCSVFormat()
        indexingtomongo.IndexProcessedData()
    except Exception as e:
        logger.critical(str(e))
