
# numpy
# [0,1)之间均匀分布的数 randn

r1 = np.random.rand()       # 默认是一个数
r2 = np.random.rand(2,5)    # 形状是2,5的

# 均值为0,方差为1 用法和rand一样

r3 = np.random.randn()
r4 = np.random.randn(2,5)

# randint的基本用法

# randint(low,high=None,size=None,dtype='I')
r8 = np.random.randint(10)      # 随机产生一个0-10的整数！！左闭右开
r9 = np.random.randint(low=5,high=10,size=(2,4))

# uniform基本用法 从指定范围内产生随机浮点数

# uniform(low=0.0,high=1.0,size=None)
r10 = np.random.uniform()
r11 = np.random.uniform(low=[1,2],high=[2,3],size=(2,2))    # 第一个数low[0]-high[0] 第二个数low[1]-high[1]

# 随机选择

r12 = np.random.choice(['正','反'],size=2)  # size是随机选择的次数

# 在原来的基础上打乱顺序

r13 = np.arange(1,5)
np.random.shuffle(r13)

#原文链接：https://blog.csdn.net/qq_43497702/article/details/98445998
