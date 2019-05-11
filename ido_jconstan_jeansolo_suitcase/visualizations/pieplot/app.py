#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
from flask import render_template
import flask

df = pd.read_csv('property2.csv')
df = df.dropna(axis=0)


app = dash.Dash(__name__)
# app.config.suppress_callback_exceptions = True

# app.layout = html.Div([
#     dcc.Location(id='url', refresh=False),
#     html.Div(id='page-content')
# ])


# index_page = html.Div([
#     dcc.Link('Go to Page 1', href='../index.html'),
#     html.Br(),
#     dcc.Link('Go to Page 2', href='/page-2'),
# ])

app.layout = html.Div([
    html.Div([
        html.H1("Property Assessment and Bus Rider")], style={"textAlign": "center"}),
    dcc.Graph(id="my-graph"),
    html.Div([dcc.Slider(
        id='value-selected',
        min=3,
        max=11,
        value=8,
        marks={
            3: "Under 300k",
            4: "300k-400k",
            5: "400k-500k",
            6: "500k-600k",
            7: "600k-700k",
            8: "700k-800k",
            9: "800k-900k",
            10: "900k-1M",
            11: "Over 1M",
        },
    )
    ], style={
        'textAlign': "center", "margin": "30px", "padding": "10px","width":"65%","margin-left":"auto","margin-right":"auto"}),

], className="container")

@app.callback(
    dash.dependencies.Output("my-graph", "figure"),
    [dash.dependencies.Input("value-selected", "value")]
)

def update_graph(selected):
    return {
        "data": [go.Pie(
            labels=df["Bus"].unique().tolist(),
            values=df[df["Bucket"] == selected]["Value"].tolist(),
            marker={'colors': ['#EF963B',
                               '#C93277']},
            textinfo='label'

        )
        ],
        "layout": go.Layout(
            title=f"Percentage of Bus Riders",
            margin={"l": 300, "r": 300, },
            legend={"x": 1, "y": 0.7}
        )

    }

# @app.callback(dash.dependencies.Output('page-content', 'children'),
#               [dash.dependencies.Input('url', 'pathname')])

# def display_page(pathname):
#     if pathname == '/page-2':
#         return page_2_layout
#     else:
#         return "../index.html"

server = app.server # the Flask app

if __name__ == '__main__':
    app.run_server(debug=True)
