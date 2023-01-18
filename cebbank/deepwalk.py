def deepwalk_walk(self, walk_length, start_node):

    walk = [start_node]

    while len(walk) < walk_length:
        cur = walk[-1]
        cur_nbrs = list(self.G.neighbors(cur))
        if len(cur_nbrs) > 0:
            walk.append(random.choice(cur_nbrs))
        else:
            break
    return walk

def _simulate_walks(self, nodes, num_walks, walk_length,):
    walks = []
    for _ in range(num_walks):
        random.shuffle(nodes)
        for v in nodes:
            walks.append(self.deepwalk_walk(walk_length=walk_length, start_node=v))
    return walks

results = Parallel(n_jobs=workers, verbose=verbose, )(
    delayed(self._simulate_walks)(nodes, num, walk_length) for num in
    partition_num(num_walks, workers))

walks = list(itertools.chain(*results))

from gensim.models import Word2Vec
w2v_model = Word2Vec(walks,sg=1,hs=1)

G = nx.read_edgelist('../data/wiki/Wiki_edgelist.txt',create_using=nx.DiGraph(),nodetype=None,data=[('weight',int)])

model = DeepWalk(G,walk_length=10,num_walks=80,workers=1)
model.train(window_size=5,iter=3)
embeddings = model.get_embeddings()

evaluate_embeddings(embeddings)
plot_embeddings(embeddings)
