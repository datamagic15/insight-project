import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import mysql.connector
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import os


def generate_table(dataframe, max_rows=100):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

server = app.server
# gunicorn app:server 127.0.0.1:8000
# to run the app using gunicorn

app.layout = html.Div(children=[
    dcc.Graph(id='live-graph'),
    dcc.Interval(id='graph-update', interval=5000, n_intervals=0),
    html.H4(children='Latest Edgar Alerts'),
    html.Div(id='live-update-text',children=[]),
    dcc.Interval(
        id='interval-component',
        interval=5*1000, # in milliseconds
        n_intervals=0)
    #generate_table(df1)
])

@app.callback(
    [Output(component_id='live-graph', component_property = 'figure'),
    Output('live-update-text', 'children')],
    [Input('graph-update', 'n_intervals'),
    Input('interval-component', 'n_intervals')]
    )
def update_query(n,n_intervals):
    mydb = mysql.connector.connect(
        host=os.environ['MYSQL_HOST'],
        user = os.environ['MYSQL_USER'],
        passwd = os.environ['MYSQL_PWD'],
        database = os.environ['MYSQL_DB']
    )
    query1 = '''select c.ticker, c.name, a.stream_cik, a.stream_count, a.start_time, a.end_time, a.avg_cnt, a.big_cnt
     from edgar_alerts a join cik_ticker c on a.cik = c.cik
    where a.end_time = ( select max(end_time) from edgar_alerts ) order by a.stream_count desc limit 50'''

    df_upd = pd.read_sql(query1, con = mydb)

    html_table = generate_table(df_upd, 100)

    fig = go.Figure(data=[
        go.Bar(name='Streaing Count',x=df_upd.ticker, y=df_upd.stream_count),
        go.Bar(name='Historic Data', x=df_upd.ticker, y=df_upd.avg_cnt )
        ])
    
    fig.update_layout(
        barmode='group',
        height=700,
        width=1000,
        autosize=False
        )
    mydb.close()

    return (fig, html_table)


if __name__ == '__main__':
    app.run_server(debug=True)