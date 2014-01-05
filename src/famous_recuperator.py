'''
Created on Dec 21, 2013

@author: pavelo
'''
import sys

from useful_methods.files import import_text_lines
from utils import insert_docs, connect_db_mongo, insert_relations_dbpedia, \
    query_mongo, graph_histogram

def get_dbpedia_patterns():
    list_patterns = query_mongo(db_name="agent_famous",
                                collection_name="famous_patterns",
                                sorted="freq",
                                order=-1)
    POS_patterns = [pat["pattern"] for pat in list_patterns
                    if pat["freq"] > 2]
    print  len(POS_patterns)
    return POS_patterns
    

def import_persons_text():
    foo, names_ppl = import_text_lines("../input/famous_ppl.txt")
    return names_ppl

def find_frequent_dbpedia_relations():
    """
    Get the common (intersection) relations for all the relations found 
    in DBPedia.
    """    
    
    from collections import defaultdict
    list_dbp_info = query_mongo("agent_famous", "famous_relations")
    dict_relations = defaultdict(set)
    for r in list_dbp_info:
        dict_relations[r["wiki_title"]].update([r["relation"]])
    
    common_rels = set.intersection(*dict_relations.values())
    print
    print "Number of common relations:", len(common_rels)
    # There are no common relations :(
    # So we get most frequent relations  in order to then extract POS/Chunk tags
    import itertools
    all_relations = list(itertools.chain(*dict_relations.values()))
    
    counts = graph_histogram(all_relations, max=50)
    return counts
    
def get_relations_dbpedia(list_famous):
    
    """
    This function does:
    1. Get all relations (R) from DBPedia for each famous person (Arg1) 
        and its second argument (Arg2),
   
    """
    from pattern.web import DBPedia
    
    connection = connect_db_mongo()
    
    
    for f in list_famous:
        sparql_q = """SELECT distinct ?property, ?label_property, ?label_obj
                        WHERE {<http://dbpedia.org/resource/%s> ?property ?obj.              
                               ?property rdfs:label ?label_property.
                               ?obj rdfs:label ?label_obj.
                            FILTER regex(?property, "dbpedia"  )
                            FILTER (langMatches(lang(?label_property),"en"))
                            FILTER (langMatches(lang(?label_obj),"en"))
                            }
                        ORDER BY ?property""" % f["wiki_title"]
        for r in DBPedia().search(sparql_q, count=100):            
            print f['wiki_title'], r.property.name, r.label_property, r.label_obj
            insert_relations_dbpedia(connection, f["wiki_title"], r.label_property, r.label_obj, r.property.name)
    

def get_wikis(list_names):

    connection = connect_db_mongo()
    from pattern.web import Wikipedia
    w = Wikipedia()
    for idx, name in enumerate(list_names):
        try:
            print name, idx
            result = w.search(name)
#             print name, "| ", result.title
            insert_docs(connection, result.plaintext().strip('\n'),
                        name, result.title.replace(" ", "_"))
        except:
            print "Unexpected error:", sys.exc_info()[0], name
        

if __name__ == '__main__':
#     list_names = import_persons_text()
    # Get wikipedia pages for the names in list_names
#     get_wikis(list_names)
    # Get all the relations (properties) for the names in list_names, from DBpedia
#     get_relations_dbpedia(list_names)
    # Get and save the most frequent dbpedia relations
#     find_frequent_dbpedia_relations()
    # Get all the found patterns in the wiki text from DBPedia relations
    get_dbpedia_patterns()
    
    pass
