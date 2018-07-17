import collections
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import MiniBatchKMeans
import sys
import vectorFetcher
sys.path.append('../connections')
import connectionsinfo
import connectionsmanager

client                  = connectionsmanager.connManager.mongodbConnectionInfo()
db                      = client[connectionsmanager.connManager.MONGO_DB]
cleanSkillColl          = db[connectionsmanager.connManager.MONGO_CSV_SKILLS_COLL]
rawDocumentsLen         = cleanSkillColl.count()

rawDocuments = vectorFetcher.getSkillsDocsList(rawDocumentsLen)
#print(rawDocuments)
documents = list(rawDocuments.keys())
docsList  = list(rawDocuments.values())
try:
    testNum = int(docsList[0])
except Exception as e:
    documents = list(rawDocuments.values())
    docsList  = list(rawDocuments.keys())

nclusters   =   int(rawDocumentsLen*0.4)
def word_tokenizer(text):
        tokens = word_tokenize(text)
        return tokens

def cluster_sentences(documents, nb_of_clusters=5):
        tfidf_vectorizer = TfidfVectorizer(tokenizer=word_tokenizer,
                                        stop_words=stopwords.words('english'),
                                        max_df=0.9,
                                        min_df=0.1,
                                        lowercase=True)
        tfidf_matrix = tfidf_vectorizer.fit_transform(documents)
        #kmeans = KMeans(n_clusters=nb_of_clusters)
        kmeans = MiniBatchKMeans(n_clusters=nb_of_clusters,random_state=0, batch_size=100)
        kmeans.fit(tfidf_matrix)
        clusters = collections.defaultdict(list)
        for i, label in enumerate(kmeans.labels_):
                clusters[label].append(i)
        return dict(clusters)

def KMeanDocsFetcher():
    allDicte = []
    clusters = cluster_sentences(documents, nclusters)
    docsIDS = list(clusters.values())
    for cluster in docsIDS:
        idsCluster = []
        for id in cluster:
            idsCluster.append(docsList[id])
        allDicte.append({'cluster':idsCluster, 'status':0})
    return allDicte
