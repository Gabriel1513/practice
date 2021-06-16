import networkx as nx
import matplotlib.pyplot as plt
import torch
import dgl

N=6#100个节点
DAMP=0.85#阻尼系数
K=10#迭代次数

g=dgl.DGLGraph()
g.add_nodes(6)

#添加边的关系
g.add_edges([1,1,1,2,2,4,0,5,5],[2,3,4,3,5,3,1,3,4])#1,2 1,3 1,4 2,3 2,5 4,3 ,0,1 5,3 5,4

nx.draw(g.to_networkx(),node_size=220,node_color=[[.88, .5, .156,]],with_labels=True)
plt.show()

#定义pagerank算法最重要的两个变量  pagerank值和边权重
g.ndata["pagerank"]=torch.ones(N)/N#节点的pagerank值
g.ndata["degree"]=g.out_degrees(g.nodes()).float()#初始分配  每个节点的所有出边权重一样

#定义消息函数  节点的pagerank值/节点的出度 所得到的值  即为要传递给目标节点的值
def pagerankMessageFunc(edges):
    return {"pv" :edges.src["pagerank"]/edges.src["degree"]}#边源节点的pr/deg

#d定义reduce函数  对邻居节点信息进行聚合
def pagerankReduceFunc(nodes):
    msgs=torch.sum(nodes.mailbox["pv"],dim=1)
    pv=(1-DAMP)/N+msgs*DAMP#pageRank的总公式  更新当前节点的pagerank值
    return {"pv":pv}

#注册以上两个函数
g.register_message_func(pagerankMessageFunc)
g.register_reduce_func(pagerankReduceFunc)

#实现pagerank迭代
def  pagerankIteration(g):
    for u,v in zip(*g.edges()):
        g.send((u,v))
    for v in g.nodes():
        g.recv(v)


pagerankIteration(g)
print(g.ndata["pagerank"])

import dgl.function as fn

def pagerank_builtin(g):
    g.ndata['pagerank'] = g.ndata['pagerank'] / g.ndata['degree']
    g.update_all(message_func=fn.copy_src(src='pagerank', out='m'),
                 reduce_func=fn.sum(msg='m',out='m_sum'))
    g.ndata['pagerank'] = (1 - DAMP) / N + DAMP * g.ndata['m_sum']

for i in range(5):
    pagerank_builtin(g)
    print(g.ndata["pagerank"])
