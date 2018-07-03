def termStemmer(term):
    from nltk.stem import PorterStemmer
    ps = PorterStemmer()
    return (ps.stem(term))

def removeSymboles(term):
    import re
    return (re.sub('\W+', ' ', term))

def isStopWord(term):
    from nltk.corpus import stopwords
    stopWord = False
    if term in stopwords.words('english'):
        stopWord = True
    return stopWord

def removeVectorDublicates(vector):
    return (list(set(vector)))

def vectorExpantion(vector):
    tempVector = []
    for item in vector:
        if len(str(item).split(',')) > 1:
            for i in str(item).split(','):
                tempVector.append(str(i).lower())
        else:
            tempVector.append(str(item).lower())
    return tempVector

def vectorCleaner(vector):
    restrictVector  = removeVectorDublicates(vectorExpantion(vector))
    cleanVector     = []
    for term in restrictVector:
        if isStopWord(term) == False:
                Cterm =  removeSymboles(term).strip()
                if Cterm.isnumeric() == False and Cterm !="":
                    cleanVector.append(Cterm)
    return cleanVector

def textCleaner(txt):
    import re
    txtList = []
    txtTokenz = txt.split()
    for token in txtTokenz:
        token = re.sub(' +',' ',removeSymboles(token.lower()))
        if token.isnumeric() == False and isStopWord(token) == False:
            txtList.append(token)
    #cleanText = removeVectorDublicates(txtList)
    return ' '.join(txtList)
