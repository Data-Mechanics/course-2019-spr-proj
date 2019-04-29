import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.plotly as py
import plotly.graph_objs as go
import plotly.graph_objs as graph_objs
import sd_material_ui
from dash.dependencies import Input, Output, State
import pymongo
from plotly import tools

import math


from ldisalvo_skeesara_vidyaap.helper.constants import TEAM_NAME, VOTING_DISTRICT_TOWNS_NAME, \
    DEMOGRAPHIC_DATA_DISTRICT_SENATE_NAME, DEMOGRAPHIC_SENATE_CORRELATIONS_NAME, DEMOGRAPHIC_HOUSE_CORRELATIONS_NAME
from dash.dependencies import Input, Output

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(TEAM_NAME, TEAM_NAME)

senateList = list(repo[VOTING_DISTRICT_TOWNS_NAME].find({"Type": "Senate"}, {"District":1}))
districts = []
for x in senateList:
    districts +=[x["District"]]
#print(districts)

print(districts[0])
race = ['White', 'Black or African American', 'American Indian and Alaska Native', 'Asian', 'Native Hawaiian and Other Pacific Islander', 'Hispanic or Latino']
initial = list(repo[DEMOGRAPHIC_DATA_DISTRICT_SENATE_NAME].find({"Senate District": districts[0]}, {"White alone, percent": 1,
                                                                 "Black or African American alone, percent":1,
                                                                 "American Indian and Alaska Native alone, percent":1,
                                                                 "Asian alone, percent":1,
                                                                 "Native Hawaiian and Other Pacific Islander alone, percent":1,
                                                                 "Hispanic or Latino, percent":1}))
initialValue = list(initial[0].values())[1:]

def getValues(district):
    val = list(repo[DEMOGRAPHIC_DATA_DISTRICT_SENATE_NAME].find({"Senate District": district}, {"White alone, percent": 1,
                                                                 "Black or African American alone, percent":1,
                                                                 "American Indian and Alaska Native alone, percent":1,
                                                                 "Asian alone, percent":1,
                                                                 "Native Hawaiian and Other Pacific Islander alone, percent":1,
                                                                 "Hispanic or Latino, percent":1}))
    finVal = list(val[0].values())[1:]
    return finVal

def getCorr():
    ans = []  # format: [[dem factors, dem values], [rep factors, rep values]]
    for x in range(2):
        party = list(repo[DEMOGRAPHIC_SENATE_CORRELATIONS_NAME].find())[x]

        temp = []
        dictList = []

        del party['_id']
        del party['Party']

        for key, value in party.items():
            if not math.isnan(value):
                temp = [key, value]
                dictList.append(temp)

        dictList.sort(key = lambda y: y[1], reverse=True)
        dictList = dictList[:5]  # taking top 5 metrics
        val = list(zip(*dictList))
        valList = map(list, val)
        ans += valList
    return ans


correlations = getCorr()
print(correlations)
corrDem = correlations[:2]
corrRep = correlations[2:]
DemLabel = [x.split(",")[0] for x in corrDem[0]]
DemValue = corrDem[1]
RepLabel = [x.split(",")[0] if "White alone" not in x else x for x in corrRep[0]]
RepValue = corrRep[1]

trace0 = go.Bar(x=DemLabel,
                y=DemValue,
                marker=dict(
                color='rgb(0,0,139)')
                    )

trace1 = go.Bar(x=RepLabel,
                y=RepValue,
                marker=dict(
        color='rgb(139,0,0)')
                    )

fig = tools.make_subplots(rows=1, cols=2, specs=[[{}, {}]], shared_xaxes=True,
                          shared_yaxes=False, vertical_spacing=0.5, horizontal_spacing=.1, subplot_titles=["Democratic Party", "Republican Party"])
fig.append_trace(trace0, 1, 1)
fig.append_trace(trace1, 1, 2)
fig['layout'].update(height=800, width=600, title='Strongest Political Party Predictors in Massachusetts', showlegend=False)
fig['layout']['xaxis1'].update(automargin=True, tickfont=dict(
            size=11,
            color='black'
        ), ticks='outside', tickangle=-45)
fig['layout']['xaxis2'].update(automargin=True, tickfont=dict(
            size=11,
            color='black'
        ), ticks='outside', tickangle=-45)


# yaxis = go.layout.YAxis(
#     title='Y-axis Title',
#     ticktext=['Very long label', 'long label', '3', 'label'],
#     tickvals=[1, 2, 3, 4],
#     tickmode='array',
#     automargin=True,

app = dash.Dash()

mapbox_access_token = "pk.eyJ1Ijoic2tlZXNhcmEiLCJhIjoiY2p1bXB5bGF6MHNsZTQzczh4djh1eDI3aCJ9.vTi1hnCqCO7txE_veUAaEg"

app.css.append_css({'external_url': 'https://codepen.io/plotly/pen/EQZeaW.css'})

spacer = html.Div(children=[], style=dict(height=20))

app.layout  = \
    html.Div([
        html.Div([
            dcc.Dropdown(
                id="District",
                options=[{
                    'label': i,
                    'value': i
                } for i in districts],
                value='All Districts',
                multi=False),

            dcc.Dropdown(
                id="Graph-type",
                options=[
                    {'label': 'Racial Breakdown', 'value': 'race'},
                    {'label': 'Strongest Political Party Predictors', 'value': 'corr'}
                ],
                multi=False,
                value="basic-demo",
            ),
            html.Button('Submit',
                id='button',
                style = dict(display='inline-block')),

            dcc.Graph(id='Correlation',
                      figure=fig,
                      )
        ], className='six columns'),

    ])
#
@app.callback(
    Output('District', 'disabled'),
    [Input('Graph-type', 'value')])
def disable_district(g_value):
    if g_value == "corr":
        return True
    return False


@app.callback(
Output('Correlation', 'figure'),
[Input('button', 'n_clicks')],
[State('District', 'value'), State('Graph-type', 'value')])
def update_output(n_clicks, d_value, g_value):
    if g_value == "corr":
        return fig

if __name__ == '__main__':
    app.run_server(debug=True)
