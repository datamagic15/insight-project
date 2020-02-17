import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import mysql.connector
from dash.dependencies import Input, Output
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
    html.H4(children='Latest Edgar Alerts'),
    html.Div(id='live-update-text',children=[]),
    dcc.Interval(
        id='interval-component',
        interval=5*1000, # in milliseconds
        n_intervals=0)
    #generate_table(df1)
])

@app.callback(Output('live-update-text', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_query(n):
    mydb = mysql.connector.connect(
        host=os.environ['MYSQL_HOST'],
        user = os.environ['MYSQL_USER'],
        passwd = os.environ['MYSQL_PWD'],
        database = os.environ['MYSQL_DB']
    )
    query1 = '''select c.ticker, a.* from edgar_alerts a left outer join cik_ticker c on a.cik = c.cik
    where a.end_time = ( select max(end_time) from edgar_alerts ) order by a.stream_count desc'''

    df_upd = pd.read_sql(query1, con = mydb)

    html_table = generate_table(df_upd, 100)

    mydb.close()

    return html_table


if __name__ == '__main__':
    app.run_server(debug=True)