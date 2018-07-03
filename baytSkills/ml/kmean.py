from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score
import vectorFetcher

documents = vectorFetcher.getSkillsList()

vectorizer = TfidfVectorizer(stop_words='english')
X = vectorizer.fit_transform(documents)
true_k = 5
model = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1)
model.fit(X)

order_centroids = model.cluster_centers_.argsort()[:, ::-1]
terms = vectorizer.get_feature_names()
def KMeanFetcher():
    insertingClusters = {}
    dictList = []
    allDicte = []
    for i in range(true_k):
        clusterName = ("cluster %d:" % i)
        tempTermsList = []
        for ind in order_centroids[i, :10]:
            tempTermsList.append(terms[ind])
        insertingClusters[clusterName] = tempTermsList
    for key,val in insertingClusters.items():
        dictList.append({'cluster':val})
    return dictList
