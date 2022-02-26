from WikiApi import get_node_connections
import pandas as pd
from tqdm import tqdm
import csv
import sys

maxInt = sys.maxsize
try:
    csv.field_size_limit(maxInt)
except OverflowError:
    maxInt = int(maxInt/10)


def create_graph_batch_csv(path, add_ref=False):
    df = load_tsv_to_df(path)
    nodes, edges = extract_nodes_and_edges(df, add_ref=add_ref)
    write_to_csv(nodes, ['title'], 'data/csv_to_db/Article.csv')
    write_to_csv(edges, ['src_Article', 'dst_Article', 'weight', 'reference'], 'data/csv_to_db/MovedTo.csv')


def load_tsv_to_df(path):
    print('Loading tsv as dataframe...')
    graph_data_frame = pd.read_csv(path, sep='\t',
                                  names=['prev_id', 'curr_id', 'n', 'prev_title', 'curr_title', 'type'],
                                  dtype={'referrer_type': 'category', 'n': 'uint32'})
    graph_data_frame = graph_data_frame[['prev_title', 'curr_title', 'n']]
    print('Finished loading the dataframe')
    return graph_data_frame


def extract_nodes_and_edges(graph_df, add_ref=False):
    node_set = set()
    moved_to_edges_set = set()
    reference_dict = dict() if add_ref else False
    print('Starting to extract nodes and edges')
    df_size = len(graph_df.index)
    for i, row in graph_df.iterrows():
        print('\r', end='')
        print('Done {} %, Node count: {}, Edge count: {}'.format(round((i / df_size) * 100, 2), len(node_set),
                                                                 len(moved_to_edges_set)), end='')
        if add_ref:
            if row['prev_title'] not in reference_dict:
                reference_dict[row['prev_title']] = set()
                refrences = get_node_connections(row['prev_title'], 1)
                if refrences:
                    for key in refrences:
                        reference_dict[row['prev_title']].add(key)
        if add_ref:
            moved_to_edges_set.add((row['prev_title'], row['curr_title'], row['n'],
                                    row['curr_title'] in reference_dict[row['prev_title']]))
        else:
            moved_to_edges_set.add((row['prev_title'], row['curr_title'], row['n'], False))
        node_set.add(tuple([row['prev_title']]))
        node_set.add(tuple([row['curr_title']]))
    print('\n')
    return node_set, moved_to_edges_set


def write_to_csv(data, headers, csv_path):
    print('Writing data to {}'.format(csv_path))
    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for d in tqdm(data):
            writer.writerow(list(d))


if __name__ == '__main__':
    path_to_tsv = r'C:\Users\Rom Cohen\PycharmProjects\NeedleInADataHayStack-finals\data\2015_01_clickstream.tsv'
    create_graph_batch_csv(path_to_tsv, add_ref=False)

