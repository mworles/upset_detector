import pandas as pd
import numpy as np
import plotly.graph_objects as go

data_path = '../../data/results/'
data = pd.read_csv('../../data/results/bets_2017.csv')

x_values = data.index.values
y_values = data['net_cumulative'].values
point_labels = data['net_won'].astype(int).values
favorites = data['Favorite'].values
underdogs = data['Underdog'].values
predictions = np.where(data['prediction'] == 1, underdogs,
                       favorites)
probability = data['probability'].round(2).values
confidence = np.where(probability > .50, probability, 1- probability)
confidence = list(map(lambda x: "{}%".format(str(int(x*100))), confidence))
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=x_values,
    y=y_values,
    mode="lines+markers",
    customdata = list(zip(favorites, underdogs, predictions, confidence, 
                          point_labels)),
    hovertemplate = """Favorite:%{customdata[0]}
    <br>Underdog:%{customdata[1]}
    <br>Prediction:%{customdata[2]}
    <br>Confidence:%{customdata[3]}
    <br>Return:%{customdata[4]}""",
    name=''
))

fig.update_layout(title_text="Cumulative net for 2017 Tournament")

fig.show()
