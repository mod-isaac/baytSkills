import sys
import vectorFetcher
corpus = vectorFetcher.getSkillsList()

from sklearn.feature_extraction.text import TfidfVectorizer
vectorizer = TfidfVectorizer()
X = vectorizer.fit_transform(corpus)

from sklearn.decomposition import TruncatedSVD
n_components_len = int(len(corpus)/2)
lsa = TruncatedSVD(n_components=n_components_len,n_iter=10)
lsa.fit(X)

terms = vectorizer.get_feature_names()
def SVDFetcher():
    insertingClusters = {}
    dictList = []
    allDicte = []
    for i, comp in enumerate(lsa.components_):
        clusterName = ("cluster %d:" % i)
        tempTermsList = []
        termsInComp = zip(terms,comp)
        sortedterms = sorted(termsInComp, key=lambda x: x[1],reverse=True)[:10]
        for term in sortedterms:
            tempTermsList.append(term[0])
        insertingClusters[clusterName] = tempTermsList

    for key,val in insertingClusters.items():
        dictList.append({'cluster':val})
    return dictList
