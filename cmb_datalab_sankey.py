import pandas as pd
import plotly
import chart_studio
import chart_studio.plotly as py
import time
import numpy as np
import collections
import teradatasql as tdsql
import teradataml as tdml

def flatten(x):
    result = []
    for el in x:
        if isinstance(x, collections.Iterable) and not isinstance(el, str):
            result.extend(flatten(el))
        else:
            result.append(el)
    return result


def genSankey(df, cat_cols=[], value_cols='', title='Sankey Diagram'):
    # maximum of 6 value cols -> 6 colors
    colorPalette = ['#4B8BBE', '#306998', '#FFE873', '#FFD43B', '#646464', '#F69824', '#CC2336']
    #colorPalette = ['#26235B', '#812878', '#A22160', '#CC2336', '#E4682A', '#F69824']

    labelList = []
    colorNumList = []
    for catCol in cat_cols:
        cat_value = flatten(df[catCol].values)
        labelListTemp = list(cat_value)
        colorNumList.append(len(labelListTemp))
        labelList = labelList + labelListTemp

    # remove duplicates from labelList
    labelList = list(dict.fromkeys(labelList))


    # define colors based on number of levels
    print('colorNumList:', colorNumList)
    colorList = []
    for idx, colorNum in enumerate(colorNumList):
        colorList = colorList + [colorPalette[idx]] * colorNum


    # transform df into a source-target pair
    for i in range(len(cat_cols) - 1):
        cat_str1 = ''.join(cat_cols[i])
        cat_str2 = ''.join(cat_cols[i + 1])

        if i == 0:
            sourceTargetDf = df[[cat_str1, cat_str2, value_cols]]
            #print("here:",flatten(df[cat_str1]))
            sourceTargetDf.columns = ['source', 'target', 'count']
        else:
            tempDf = df[[cat_str1, cat_str2, value_cols]]
            tempDf.columns = ['source', 'target', 'count']
            sourceTargetDf = pd.concat([sourceTargetDf, tempDf])
        sourceTargetDf = sourceTargetDf.groupby(['source', 'target']).agg({'count': 'sum'}).reset_index()
        sourceTargetDf['target'].replace('', np.nan, inplace=True)
        sourceTargetDf = sourceTargetDf.dropna(axis=0, inplace=False)

        #print('sourceTargetDf gouped:', sourceTargetDf)

    #time.sleep(5)
    # add index for source-target pair
    sourceTargetDf['sourceID'] = sourceTargetDf['source'].apply(lambda x: labelList.index(x))
    sourceTargetDf['targetID'] = sourceTargetDf['target'].apply(lambda x: labelList.index(x))

    # creating the sankey diagram
    data = dict(
        type='sankey',
        node=dict(
            pad=15,
            thickness=20,
            line=dict(
                color="black",
                width=0.5
            ),
            label=labelList,
            color=colorList
        ),
        link=dict(
            source=sourceTargetDf['sourceID'],
            target=sourceTargetDf['targetID'],
            value=sourceTargetDf['count']
        )
    )

    layout = dict(
        title=title,
        font=dict(
            size=10
        )
    )

    fig = dict(data=[data], layout=layout)
    return fig

con = tdsql.connect(host="192.168.100.162", user="td01", password="td01")
cus = con.cursor()

def sql_icld_cols(sql):
    cus.execute(sql)
    rst_no_cols = cus.fetchall()
    cols_des = cus.description

    col = []  # 创建一个空列表以存放列名
    for v in cols_des:
        col.append(v[0])  # 循环提取列名，并添加到col空列表
    dfsql = pd.DataFrame(rst_no_cols, columns=col)  # 将查询结果转换成DF结构，并给列重新赋值
    if dfsql.empty:
        return 'empty set'
    else:
        return dfsql

# Sankey 1 for all users
rst = sql_icld_cols("select 数据区类型, 数据区名称,ORG_NM,SUB_ORG_0,SUB_ORG_1,SUB_ORG_2,SUB_ORG_3,CountQuery \
    from cmb_datalab_20200409_sankey;")

fig = genSankey(rst, cat_cols=[['数据区类型'],['数据区名称'],['ORG_NM'], ['SUB_ORG_0'], ['SUB_ORG_1'], ['SUB_ORG_2'], ['SUB_ORG_3']],
                value_cols='CountQuery', title='Sankey Diagram-CMB_EDW全行用户查询量分布图(数据时间:2020.04.09)')
plotly.offline.plot(fig, filename='overview.html', validate=False)

# Sankey 2 for IT users
rst = sql_icld_cols("select 数据区类型, 数据区名称,ORG_NM,SUB_ORG_0,SUB_ORG_1,SUB_ORG_2,SUB_ORG_3,CountQuery \
    from cmb_datalab_20200409_sankey where ORG_NM = '信息技术部';")

fig = genSankey(rst, cat_cols=[['数据区类型'],['数据区名称'], ['SUB_ORG_1'], ['SUB_ORG_2'], ['SUB_ORG_3']],
                value_cols='CountQuery', title='Sankey Diagram-CMB_EDW信息技术部用户查询量分布图(数据时间:2020.04.09)')
plotly.offline.plot(fig, filename='it_dep.html', validate=False)

# Sankey 3 for Business users
rst = sql_icld_cols("select 数据区类型, 数据区名称,ORG_NM,SUB_ORG_0,SUB_ORG_1,SUB_ORG_2,SUB_ORG_3,CountQuery \
    from cmb_datalab_20200409_sankey where ORG_NM <> '信息技术部';")

fig = genSankey(rst, cat_cols=[['数据区类型'],['数据区名称'],['ORG_NM'], ['SUB_ORG_0'], ['SUB_ORG_1'], ['SUB_ORG_2'],
                               ['SUB_ORG_3']], value_cols='CountQuery',
                title='Sankey Diagram-CMB_EDW全行业务用户查询量分布图(数据时间:2020.04.09)')
plotly.offline.plot(fig, filename='b_dep.html', validate=False)
