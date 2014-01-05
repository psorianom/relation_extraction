'''
Created on Dec 21, 2013

@author: pavelo
'''


from collections import defaultdict, Counter
import pprint

from pattern.en     import parsetree  # @UnresolvedImport

from utils import connect_db_mongo, query_mongo, \
    insert_relations_dbpedia, query_mongo_distinct, insert_stuff_mongodb, \
    graph_histogram
from pattern.search import search  # @UnresolvedImport
from famous_recuperator import find_frequent_dbpedia_relations, \
    get_dbpedia_patterns
from useful_methods.structures import sort_dict_value






def find_relations_from_DBPedia_patterns(list_famous, patterns):
    
    from pattern.en import Word  # @UnresolvedImport
    
    dict_rel_args = defaultdict(list)
    dict_rel_freq = defaultdict(int)
    list_all_relations = []   
    
    for doc in list_famous[:]:
        print "*"*50, doc["famous_name"], "*"*50
        parsed_tree = parsetree(doc['famous_text'])
#         parsed_tree = parsetree("Faust made a deal with the Devil,")
        for p in patterns:
            for m in search(p, parsed_tree):                
#                 print p, m.group(0)
                chunks = m.group(0, True)
                relation = [c.string for c in chunks if "VP" in c.pos]
                if not relation:
                    continue
                
                if type(chunks[0]) is Word:
                    arg1 = chunks[0].chunk.string 
                
                else:
                    arg1 = chunks[0].string
                if arg1 == "was":
                    pass
              
                arg2 = chunks[-1].string
                pass
                dict_rel_args[relation[0]].append((arg1, arg2))
                dict_rel_freq[relation[0]] += 1
                list_all_relations.append(relation[0])
    
    print "%d relations found.\n" % len(list_all_relations)
    counts = graph_histogram(list_all_relations, max=50)
    sorted_freq = zip(counts.index, counts.tolist())  # sort_dict_value(dict_rel_freq)  # @UnusedVariable
    print 
    
    # Print the top ten relations found, according to freq.
    # and print a random sample of the arguments
    from random import  sample
    for rel in sorted_freq[:10]:
        pass
        print "Relation", rel
        print 
        pprint.pprint(sample(dict_rel_args[rel[0]],
                             min(10, len(dict_rel_args[rel[0]]))))
        print "\n\n"
    pass

def find_relations_from_patterns(list_famous):
    
    

    """
    Patterns from Reverb paper V| VP | V Q* P
    """
    V = "VP RP? RB?"  # verb particle? adv?
    P = "IN|RP|TO"  # (prep | particle | inf. marker)
    W = "NN|JJ|RB|PR|DT"  # (noun | adj | adv | pron | det)
    PRP = ["he", "his", "she", "her"]  # a PRP. Make sure first argument is wether a  pronoun:  we are talking about the person
    NNP = "NNP"  # or wether a proper name 
    
    reverb1 = "{PRP|NNP} {%s} {NP}" % V 
    reverb2 = "{PRP|NNP} {%s %s} {NP}" % (V, P)
    reverb3 = "{PRP|NNP} {%s %s * %s} {NP}" % (V, W, P)
    patterns = [reverb1, reverb2, reverb3]

    dict_rel_args = defaultdict(list)
    dict_rel_freq = defaultdict(int)
    list_all_relations = []
    
    for doc in list_famous[:]:
        print "*"*50, doc["famous_name"], "*"*50
        parsed_tree = parsetree(doc['famous_text'])
#         parsed_tree = parsetree("Faust made a deal with the Devil,")
        for p in patterns:
            for m in search(p, parsed_tree):                
#                 print p, m.group(0)
                dict_rel_args[m.group(2).string].append((m.group(1).string, m.group(3).string))
                dict_rel_freq[m.group(2).string] += 1
                list_all_relations.append(m.group(2).string)
                
    print "%d relations found.\n" % len(list_all_relations)
    counts = graph_histogram(list_all_relations, max=50)
    sorted_freq = zip(counts.index, counts.tolist())  # sort_dict_value(dict_rel_freq)  # @UnusedVariable
    print 
    
    # Print the top ten relations found, according to freq.
    # and print a random sample of the arguments
    from random import  sample
    for rel in sorted_freq[:10]:
        
        print "Relation", rel
        print 
        pprint.pprint(sample(dict_rel_args[rel[0]], 10))
        print "\n\n"
    pass
    

def find_patterns_from_relations(list_wiki_info):
    
    dict_relation_pos = defaultdict(list)
    PRP_NNP = "{PRP|NNP}"
    
    for person in list_wiki_info[:]:
        if not person:
            continue
        ptree = parsetree(person['famous_text'], relations=True)
        
        dbpedia_relations = query_mongo("agent_famous", "famous_relations",
                                        {"wiki_title": person['wiki_title']})
        
#         print [(w["relation"], w['arg_2']) for w in dbpedia_relations]
        
        for relation in dbpedia_relations:

            
            arg2 = relation["arg_2"]
            for sentence in ptree:
                """
                First check in the sentence if there is a mention of the person:
                (if her name appears inside or if she/her (him/he).
                This is the first argument (Arg1) of the relation.
                """
#                 arg1_a = search(PRP_NNP, sentence)  # looks for he/she FIXED: too general
                # We then check for a he or she in the first middle of the sentence.
                arg1_a = [True for w in ["he", "she"] 
                          if w in [w.string.lower()
                                   for w in sentence[:len(sentence) / 2]]]
                arg1_b = [True for w in person['famous_name'].lower().split()
                          if w in sentence.string.lower()]
                if not arg1_a and not arg1_b:
                    continue
                """
                Now check if the second argument of the relation (Arg2) is in the sentence.
                """
                if not arg2 in sentence.string:
                    continue
                
                """
                If we get to this point, the arg2 and arg1 is in the sentence so we get the POS
                tags of each word in the phrase.
                """
                try:
                    # Get the index of arg2 in the sentence (where is it??)
                    index_arg2 = sentence.indexof(arg2, "chunk")
                    if not index_arg2:
                        index_arg2 = [i for i, w in enumerate(sentence.words) 
                                      if arg2.split()[0] in w.string][0]
                    else:
                        index_arg2 = index_arg2[0]
                    # Get the chunk (or phrase) were the arg2 is
                    chunk_arg2 = sentence.words[index_arg2].chunk
                    # Get the chunk (or phrase) were the relation is
                    chunk_relation = chunk_arg2.previous("VP")
                    # Get the chunk (or phrase) were the arg1 is
                    chunk_arg1 = chunk_relation.previous("NP")
                    word_arg1 = chunk_arg1.head
                    index_arg1 = word_arg1.index  # sentence.chunks.index(chunk_arg1)
                    pattern_found = " ".join([p.pos for p in sentence.words[index_arg1:index_arg2 + 1]
                                                                    if len(p.string) > 1 ])
#                     print pattern_found
                    
                    dict_relation_pos[relation["relation"]].append(pattern_found)                    
                    
                    verbose = False
                    if verbose:
                        
                        print "*"*50
                        print relation['relation'], relation["arg_2"]
                        print sentence.string
                        print [w.string for w in sentence.words[index_arg1:index_arg2 + 1]]
                        print "*"*50
                        print
                    break                
                except:
                    print "No ARG1 RELATION ARG2 relation found :("                        
                                 
    list_relations = [item for slist in dict_relation_pos.values() for item in slist]
    relations_histogrm = Counter(list_relations)
    insert_stuff_mongodb(collection_name="famous_patterns",
                         list_dict=[{"pattern":k, "freq":v} for k, v in relations_histogrm.items()])
    
    pass


def main():
    list_wiki_info = query_mongo("agent_famous", "famous_wikitexts")
#     list_dbp_info = query_mongo("agent_famous", "famous_relations")
    ######## 1.1 Relations from patterns. #########
    
    # Find relations with the pattern from ReVerb (first constraint)    
#     find_relations_from_patterns(list_wiki_info)


    ######## 1.2 PAtterns from relations #########

    # Find and save patterns from relations found through DBPedia
    find_patterns_from_relations(list_wiki_info)
    # Get the patterns found before, from mongodb
#     dbpedia_patterns = get_dbpedia_patterns()
    # Use the patterns found thorugh dbpedia and find new relations...
#     find_relations_from_DBPedia_patterns(list_wiki_info, dbpedia_patterns)
    pass
if __name__ == '__main__':
    main()
    pass
