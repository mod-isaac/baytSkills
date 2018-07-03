import sys
sys.path.append('../nlp')
import datapreprocessing

def deictToDocument(doc):
    """Convertng dictionary to mongo query"""
    # documentsBatch = datapreprocessing.getCleanedVectors()
    # return documentsBatch
    mongoDoc = {}
    for key,val in doc.items():
        mongoDoc['doc_id'] = key
        mongoDoc['doc_skills'] = val
    return mongoDoc

def rawDocumentsProvider():
    mongoBatch = []
    for key,val in datapreprocessing.getCleanedVectors().items():
        tempDect = {}
        tempDect[key] = val
        mongoBatch.append(deictToDocument(tempDect))
    return mongoBatch
