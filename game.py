import networkx
import random
import pandas as pd
import random
import json
import time
import os
import glob


FORBIDEN_WORDS = ['other-google', 'other-empty', 'other-wikipedia', 'other', 'other-twitter', 'other-twitter',
                  'other-yahoo', 'other-yahoo', 'other-bing', 'other-bing', 'Main_Page', 'Main_Page', 'other-wikipedia',
                  'other-google', 'other-empty']

JSON_PATH = 'data/local_graphs'


class LocalGraph:

    def __init__(self, graph_depth, df=None, root=None):
        self.graph_data_frame = df
        self.graph_depth = graph_depth
        self.row_length = len(self.graph_data_frame.index)
        self.root = root if root is not None else self.get_random_node()
        self.graph = None
        self.load_graph()

    def get_random_node(self):
        rand_root = None
        while rand_root is None or rand_root in FORBIDEN_WORDS:
            rand_root = self.graph_data_frame.iloc[random.randint(0, self.row_length)]['prev_title']
        return rand_root

    def load_graph(self):
        self.load_graph_from_json(self.root, self.graph_depth)
        if not self.graph:
            self.init_graph(self.graph_depth, self.root)

    def init_graph(self, depth, root):
        if self.graph_data_frame is None:
            self.graph = False
        self.root = root
        self.graph_depth = depth
        traveled_in = set()
        traveled_in.add(root)
        self.graph = {root: self.rec_load_local_graph(depth, root, traveled_in)}
        self.save_graph_to_json()

    def load_graph_from_json(self, root, depth):
        file_name = 'graph_{}_{}.json'.format(root, depth)
        file_path = os.path.join(JSON_PATH, file_name)
        if os.path.isfile(file_path):
            with open(file_path, 'r') as reader:
                self.graph = json.loads(reader.read())
                self.root = root
                self.graph_depth = depth
        else:
            self.graph = None

    def save_graph_to_json(self):
        if not self.graph:
            return
        file_name = 'graph_{}_{}.json'.format(self.root, self.graph_depth)
        file_path = os.path.join(JSON_PATH, file_name)
        with open(file_path, 'w') as writer:
            writer.write(json.dumps(self.graph))

    def rec_load_local_graph(self, d, root, traveled_set):
        if d == 0:
            return None
        rows = self.graph_data_frame[self.graph_data_frame['prev_title'] == root]
        out_put = dict()
        for _, row in rows.iterrows():
            if row['curr_title'] not in traveled_set and row['curr_title'] not in FORBIDEN_WORDS:
                traveled_set.add(row['curr_title'])
                out_put[row['curr_title']] = self.rec_load_local_graph(d - 1, row['curr_title'], traveled_set)
        return out_put

    def get_graph(self):
        return self.graph


def get_path_from_node(a, b, graph):
    a_graph = rec_get_neighbors(graph, a)
    return get_path_from_root(b, a_graph)


def get_path_from_root(b, graph):
    if not graph:
        return []
    for node in graph:
        if node == b:
            return [node]
    for node in graph:
        result = get_path_from_root(b, graph[node])
        if len(result) != 0:
            return [node] + result
    return []


def get_neighbors(graph, src):
    neighbors = rec_get_neighbors(graph, src)
    if neighbors:
        return [node for node in neighbors]
    else:
        return False


def rec_get_neighbors(graph, src):
    for node in graph:
        if node == src:
            return graph[node]
    for node in graph:
        result = rec_get_neighbors(graph[node], src)
        if result:
            return result
    return False


class TheWikiGame:

    def __init__(self, graph, depth):
        self.game_graph = graph
        self.graph_depth = depth
        self.titles = list()
        self.init_titles(self.game_graph)
        self.graph_root = self.titles[0]

    def init_titles(self, graph):
        if not graph:
            return
        for node in graph:
            self.titles.append(node)
            self.init_titles(graph[node])

    def get_random_title(self):
        titles_len = len(self.titles)
        return self.titles[random.randint(0, titles_len)]

    def get_random_path(self):
        path_to = self.get_random_title()
        route = get_path_from_root(path_to, self.game_graph)
        return route


def load_json_from_tsv():
    tsv_path = r'C:\Users\Rom Cohen\PycharmProjects\NeedleInADataHayStack-finals\data\2015_01_clickstream.tsv'
    data_frame = pd.read_csv(tsv_path, sep='\t', names=['prev_id', 'curr_id', 'n', 'prev_title', 'curr_title', 'type'],
                             dtype={'referrer_type': 'category', 'n': 'uint32'})
    data_frame = data_frame[['prev_title', 'curr_title', 'n']]
    for j in range(5):
        for i in range(1, 4):
            start_time = time.time()
            temp = LocalGraph(i, data_frame)
            root = temp.root
            end_time = time.time()
            print('Finished with {} at {}'.format(root, str(round(float(str(end_time - start_time)), 2))))


if __name__ == '__main__':
    # with open('data/local_graphs/graph_List_of_fossil_sites_3.json', 'r') as reader:
    #     graph = json.loads(reader.read())
    #     game = TheWikiGame(graph, 3)
    #     print(game.get_random_path())
    load_json_from_tsv()


