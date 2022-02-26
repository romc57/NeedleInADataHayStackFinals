import redis
from redisgraph import Node, Edge, Graph, Path


MOVE = 'MovedTo'


class GraphWrapper:

    def __init__(self, graph_name):
        self.r = redis.Redis(host='localhost', port=6379)
        self.redis_graph = Graph(graph_name, self.r)
        self.graph_name = graph_name

    def delete_graph(self):
        self.redis_graph.delete()

    def query_executor(self, query, params):
        try:
            return self.redis_graph.query(query, params).result_set
        except Exception as ex:
            print('Problem executing query:\n{}\nGot Exception:\n{}'.format(query, ex))

    def add_moved_to_relation(self, mother_title, daughter_title, weight, article_reference=False):
        graph_command = """MATCH (a:Article {title:'%s'}), (b:Article {title:'%s'}) 
        CREATE (a)-[:%s {weight:%s, reference:%s}]->(b)""" % (mother_title, daughter_title, MOVE, weight,
                                                              article_reference)
        self.r.execute_command("GRAPH.QUERY", self.graph_name, graph_command)

    def get_related(self, article_title, relation):
        params = {'title': article_title}
        query = """MATCH (a:Article {title:$title})-[r:""" + relation + """]->(d:Article)
                  RETURN d.title, r.weight, r.reference"""
        return self.query_executor(query, params)

    def get_all_moved_to(self, article_title):
        return self.get_related(article_title, MOVE)

    def get_rel(self, mother_title, daughter_title, rel):
        params = {'mother_title': mother_title, 'daughter_title': daughter_title}
        query = """MATCH (a:Article {title:$mother_title})-[r:""" + rel + """]->(d:Article {title:$daughter_title})
                      RETURN r.weight, r.reference"""
        return self.query_executor(query, params)

    def get_article_by_title(self, title):
        params = {'title': title}
        query = """MATCH (a: Article {title:$title}) RETURN a.title"""
        return self.query_executor(query, params)

    def node_exists(self, title):
        nodes = self.get_article_by_title(title)
        return len(nodes) != 0

    def check_if_moved(self, mother_title, daughter_title):
        edges = self.get_rel(mother_title, daughter_title, MOVE)
        return len(edges) != 0

    def run_page_rank(self, title, relation):
        page_rank = self.redis_graph.call_procedure("algo.pageRank", title, relation)
        return page_rank.result_set

    def run_BFS(self, source_node, max_level, reltion):
        bfs = self.redis_graph.call_procedure("algo.BFS", source_node, max_level, reltion)
        return bfs.result_set


if __name__ == '__main__':
    my_graph = GraphWrapper('WikiGraph')
    print(my_graph.get_all_moved_to('k'))
    # page_rank = my_graph.run_page_rank('article', MOTHER_REL)
    # bfs = my_graph.run_BFS('mother article second', '1',MOTHER_REL)
