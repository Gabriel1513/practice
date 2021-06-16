
import pandas as pd
import numpy as np
import matplotlib as plt
import seaborn as sns
%matplotlib inline

data.info()
data.head()
data.shape
data.columns
data.index

#Filter
data.iloc[3,10]
train = data[data['Date']<'2015-01-01']


#Sort Value
df.sort_values('columnName', ascending=False)

#Drop Value
df.drop(columns=['columnName'])

#Plot

#各分组统计
sns.countplot(x='Survived',data=data)

#对比标签进行分组统计Visualization
sns.countplot(x='age',hue='Survived',data=data)

g=sns.factorplot(x='Sex',y='Age',data=data,kind='box')
g=sns.factorplot(x='Pclass',y='Age',data=data,kind='box')

Age0=data[(data['Survived']==0)&(data['Age'].notnull())]['Age'] #死亡乘客的 Age 数据
Age1=data[(data['Survived']==1)&(data['Age'].notnull())]['Age'] #生存乘客的 Age 数据
g=sns.kdeplot(Age0,legend=True,shade=True,color='r',label='NotSurvived') #死亡乘客年龄概率分布图， shade=True 设置阴影
g=sns.kdeplot(Age1,legend=True,shade=True,color='b',label='Survived') #生存乘客概率分布图


from statsmodels.stats.outliers_influence import variance_inflation_factor

VIF_list = [variance_inflation_factor(X_m, i) for i in range(X_m.shape[1])]

def vif(X, thres=10.0):
    col = list(range(X.shape[1]))
    dropped = True
    while dropped:
        dropped = False
        vif = [variance_inflation_factor(X.iloc[:,col].values, ix)
               for ix in range(X.iloc[:,col].shape[1])]

        maxvif = max(vif)
        maxix = vif.index(maxvif)
        if maxvif > thres:
            del col[maxix]
            print('delete=',X_train.columns[col[maxix]],'  ', 'vif=',maxvif )
            dropped = True
    print('Remain Variables:', list(X.columns[col]))
    print('VIF:', vif)
    return list(X.columns[col])  
