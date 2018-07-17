import csvIndexer
import indexingtomongo

if __name__ == '__main__':
    csvIndexer.indexCSVFormat()
    indexingtomongo.IndexProcessedData()
