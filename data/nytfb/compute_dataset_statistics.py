import json
import pandas as pd
import time

from tqdm import tqdm


def read_entities():
    rid = pd.read_csv('entity_ids.del', sep="\t", header=None)
    rid_lookup = rid.to_dict(orient='dict')[1]
    rev_rid = {val.replace("/", ".")[1:]: key for (key, val) in rid_lookup.items()}
    # print(rev_rid)
    return rev_rid


def read_relations():
    rid = pd.read_csv('relation_ids.del', sep="\t", header=None)
    rid_lookup = rid.to_dict(orient='dict')[1]
    rev_rid = {val: key for (key, val) in rid_lookup.items()}
    # print(rev_rid)
    return rev_rid


def read_entity_strings():
    rid = pd.read_csv('entity_strings.del', sep="\t", header=None)
    rid_lookup = rid.to_dict(orient='dict')[1]
    rev_rid = {val: key for (key, val) in rid_lookup.items()}
    # print(rev_rid)
    return rev_rid


def load_files(_fp):
    predicates = []
    entities = []
    sentences = []
    triples = []
    triple_idxs = []
    with open(_fp, 'rb') as f:
        data = json.load(f)
    entity_dict = read_entities()
    relation_dict = read_relations()
    entity_strings = read_entity_strings()
    num_fb_entities = list(entity_dict.values())[-1]
    orig_entity_names = list(entity_strings.keys())
    modified_entity_names = [x.lower().replace(' ', '_') for x in orig_entity_names]
    num_fb_relations = list(relation_dict.values())[-1]
    print('Originally had {} entities'.format(num_fb_entities))
    missing_entities = []
    for d in tqdm(data['data']):
        if d['rel'] not in predicates:
            predicates.append(d['rel'])
        if d['sub_id'] not in entities:
            entities.append(d['sub_id'])
        if d['obj_id'] not in entities:
            entities.append(d['obj_id'])
        if d['sent'] not in sentences:
            sentences.append(d['sent'])
        triples.append([d['sub_id'], d['obj_id'], d['rel']])
        try:
            triple_idxs.append([entity_dict[d['sub_id']], entity_dict[d['obj_id']], relation_dict[d['rel']]])
        except KeyError:
            try:
                entity_dict[d['sub_id']]
            except KeyError:
                num_fb_entities += 1
                if d['sub'] in modified_entity_names:
                    pass
                    #print("found potential match: {}".format(d['sub']))
                else:
                    missing_entities.append(d['sub'])
                entity_dict[d['sub_id']] = num_fb_entities
            try:
                entity_dict[d['obj_id']]
            except KeyError:
                num_fb_entities += 1
                if d['obj'] in modified_entity_names:
                    pass
                    #print("found potential match: {}".format(d['obj']))
                else:
                    missing_entities.append(d['obj'])
                entity_dict[d['obj_id']] = num_fb_entities
            try:
                relation_dict[d['rel']]
            except KeyError:
                num_fb_relations += 1
                relation_dict[d['rel']] = num_fb_relations
    print('Found {} predicates'.format(len(predicates)))
    print('Found {} entities'.format(len(entities)))
    print('Found {} sentences'.format(len(sentences)))
    print('Found {} triples'.format(len(triples)))
    print('Found {} unmatched entities'.format(len(list(set(missing_entities)))))
    return predicates, entities, sentences, triples


if __name__ == "__main__":
    print('===== Train =====')
    load_files('dataset_triples_train.json')
    print('===== Test =====')
    load_files('dataset_triples_test.json')

