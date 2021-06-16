import numpy as np
import torch

n_users = 1000
n_items = 500
n_follows = 3000
n_clicks = 5000
n_dislikes = 500
n_hetero_features = 10
n_user_classes = 5
n_max_clicks = 10

follow_src = np.random.randint(0, n_users, n_follows)
follow_dst = np.random.randint(0, n_users, n_follows)
click_src = np.random.randint(0, n_users, n_clicks)
click_dst = np.random.randint(0, n_items, n_clicks)
dislike_src = np.random.randint(0, n_users, n_dislikes)
dislike_dst = np.random.randint(0, n_items, n_dislikes)

hetero_graph = dgl.heterograph({
	('user', 'follow', 'user'): (follow_src, follow_dst),
	('user', 'followed-by', 'user'): (follow_dst, follow_src),
	('user', 'click', 'item'): (click_src, click_dst),
	('item', 'clicked-by', 'user'): (click_dst, click_src),
	('user', 'dislike', 'item'): (dislike_src, dislike_dst),
	('item', 'disliked-by', 'user'): (dislike_dst, dislike_src)})

hetero_graph.nodes['user'].data['feature'] = torch.randn(n_users, n_hetero_features)
hetero_graph.nodes['item'].data['feature'] = torch.randn(n_items, n_hetero_features)
hetero_graph.nodes['user'].data['label'] = torch.randint(0, n_user_classes, (n_users,))
hetero_graph.edges['click'].data['label'] = torch.randint(1, n_max_clicks, (n_clicks,)).float()
# randomly generate training masks on user nodes and click edges
hetero_graph.nodes['user'].data['train_mask'] = torch.zeros(n_users, dtype=torch.bool).bernoulli(0.6)
hetero_graph.edges['click'].data['train_mask'] = torch.zeros(n_clicks, dtype=torch.bool).bernoulli(0.6)


# Define a Heterograph Conv model
import dgl.nn as dglnn

class RGCN(nn.Module):
	def __init__(self, in_feats, hid_feats, out_feats, rel_names):
		super().__init__()

		self.conv1 = dglnn.HeteroGraphConv({
			rel: dglnn.GraphConv(in_feats, hid_feats)
			for rel in rel_names}, aggregate='sum')
		self.conv2 = dglnn.HeteroGraphConv({
			rel: dglnn.GraphConv(hid_feats, out_feats)
			for rel in rel_names}, aggregate='sum')

	def forward(self, graph, inputs):
		# inputs are features of nodes
		h = self.conv1(graph, inputs)
		h = {k: F.relu(v) for k, v in h.items()}
		h = self.conv2(graph, h)
		return h

model = RGCN(n_hetero_features, 20, n_user_classes, hetero_graph.etypes)
user_feats = hetero_graph.nodes['user'].data['feature']
item_feats = hetero_graph.nodes['item'].data['feature']
labels = hetero_graph.nodes['user'].data['label']
train_mask = hetero_graph.nodes['user'].data['train_mask']

node_features = {'user': user_feats, 'item': item_feats}
h_dict = model(hetero_graph, {'user': user_feats, 'item': item_feats})
h_user = h_dict['user']
h_item = h_dict['item']

opt = torch.optim.Adam(model.parameters())

for epoch in range(5):
	model.train()
	# forward propagation by using all nodes and extracting the user embeddings
	logits = model(hetero_graph, node_features)['user']
	# compute loss
	loss = F.cross_entropy(logits[train_mask], labels[train_mask])
	# Compute validation accuracy.  Omitted in this example.
	# backward propagation
	opt.zero_grad()
	loss.backward()
	opt.step()
	print(loss.item())

	# Save model if necessary.  Omitted in the example.
