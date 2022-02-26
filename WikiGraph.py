import pandas as pd
#import networkx as nx
import pickle
import pandas_dtype_efficiency as pd_eff
#import community
import matplotlib.cm as cm
import matplotlib.pyplot as plt
from networkx.algorithms.link_analysis.pagerank_alg import pagerank_numpy


FATHER = 'prev_title'
SON = 'curr_title'
WEIGHT = 'n'


class WikiGraphObject:
    def __init__(self, file_path=None, graph_path=None):
        if file_path is None and graph_path:
            self.init_graph(False, True, graph_path)
        else:
            self.file_path = file_path
            self.data_frame = None
            self.init_data_frame()
            #self.init_graph(False, False)

    def init_data_frame(self):
        self.data_frame = pd.read_csv(self.file_path, sep='\t',
                                             names=['prev_id', 'curr_id', 'n', 'prev_title', 'curr_title', 'type'],
                                      dtype={'referrer_type': 'category','n': 'uint32'})
        self.data_frame = self.data_frame[['prev_title', 'curr_title', 'n']]

    def init_graph(self, save, load, file_path = None):
        if load:
            with open(file_path) as f:
                self.graph = pickle.load(f)
        elif self.data_frame is not None:
            #self.graph = nx.from_pandas_edgelist(self.data_frame, SON, FATHER, [WEIGHT])
            # self.data_frame = None
            if save:
                with open('graph_netx.pickle', 'wb') as f:
                    pickle.dump(self.graph, f)

    def get_communities(self):
        return community.best_partition(self.graph)

    def plot_communities(self, partition):
        # TODO: visualize names of values.
        pos = nx.spring_layout(self.graph)
        # color the nodes according to their partition
        cmap = cm.get_cmap('viridis', max(partition.values()) + 1)
        nx.draw_networkx_nodes(self.graph, pos, partition.keys(), node_size=40,
                           cmap=cmap, node_color=list(partition.values()))
        nx.draw_networkx_edges(self.graph, pos, alpha=0.5)
        plt.show()

    def get_page_rank(self, alpha = 0.85):
        return pagerank_numpy(self.graph, alpha=alpha, weight='n')

    def add_communities_to_df(self, partition):
        communities_values = [partition.get(node) for node in self.data_frame[FATHER]]
        self.data_frame['community'] = communities_values