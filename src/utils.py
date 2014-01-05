'''
Created on Dec 21, 2013

@author: pavelo
'''


def connect_db_mongo():
    from pymongo import MongoClient
    return MongoClient()



def insert_stuff_mongodb(collection_name, list_dict, db_name="agent_famous"):
    connection = connect_db_mongo()
    db = connection[db_name]
    collection = db[collection_name]
    for l in list_dict:
        collection.update(l, l, True)
    
# TODO: change this for a proper mongodb handling (mongoexpress or wadevs) ORM
def insert_relations_dbpedia(connection, wiki_title, relation, arg_2, URI_rel):
    db = connection['agent_famous']
    collection = db['famous_relations']
    famous_doc = {"wiki_title":wiki_title,
                  'relation':relation,
                  'arg_2':arg_2,
                  'URI_rel':URI_rel}
    
    return  collection.insert(famous_doc)


def graph_histogram(list_values, plot_kind='bar', max=None):
    """
    Graph the histogram  (a plot) of the elements contained in list_values.
    More precisely, a plot of type plot_kind (bar, line, etc).
    """
    
    from pandas import Series, DataFrame
    import pprint
    import pylab
    import matplotlib
#     from ggplot import *
    
    matplotlib.rcParams.update({'font.size': 12})    
    s = Series(list_values)
    vc = s.value_counts()
    pprint.pprint(vc[:max])
    vc[:max].plot(kind=plot_kind)   
    pylab.show()
    return vc

def insert_docs(connection, famous_text, famous_name, wiki_title):
#     connection = connect_db_mongo()
    db = connection['agent_famous']
    collection = db['famous_wikitexts']
    famous_doc = {'famous_name':famous_name,
                  'famous_text':famous_text,
                  'wiki_title':wiki_title}
    
    return  collection.insert(famous_doc)


def query_mongo_distinct(db_name, collection_name, distinct_field, query_dict=None):
    """
    Wrapper over .find() mongoDB method.
    """
    connection = connect_db_mongo()
    db = connection[db_name]
    collection = db[collection_name]
    list_documents = []
    for f in collection.find(query_dict).distinct(distinct_field):
        list_documents.append(f)
    return list_documents    


def query_mongo(db_name, collection_name,
                query_dict=None, sorted=None, order=1):
    """
    Wrapper over .find() mongoDB method.
    """
    connection = connect_db_mongo()
    db = connection[db_name]
    collection = db[collection_name]
    list_documents = []
    result = collection.find(query_dict)
    if sorted:
        result = result.sort(sorted, order)
    for f in result:
        list_documents.append(f)
    return list_documents
    





if __name__ == '__main__':
#     insert_stuff_mongodb(collection_name="test", dict_stuff={"polo":3, "jeans": 5})
    pass
