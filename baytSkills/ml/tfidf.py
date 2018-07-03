import vectorFetcher
from sklearn.feature_extraction.text import TfidfVectorizer

def vectorsTFIDF():
    corpus = vectorFetcher.getSkillsList()
    vectorizer = TfidfVectorizer(min_df=1)
    X = vectorizer.fit_transform(corpus)
    tfidf = vectorizer.idf_
    dataDict = (dict(zip(vectorizer.get_feature_names(),tfidf)))
    dictList = []
    for key,val in dataDict.items():
        temDict = {}
        if key.isnumeric() == False:
            temDict['skill'] = key
            temDict['tfidf']  = val
            dictList.append(temDict)
    return dictList
