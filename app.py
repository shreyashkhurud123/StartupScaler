import os
from flask import Flask, Blueprint, jsonify
from influxdb_client import InfluxDBClient


app = Flask(__name__)


app.config['INFLUXDB_HOST'] = 'localhost'
app.config['INFLUXDB_PORT'] = 8086
app.config['INFLUXDB_USERNAME'] = 'admin'
app.config['INFLUXDB_PASSWORD'] = 'admin123'
app.config['INFLUXDB_DATABASE'] = 'bulk_data'


influx_client = InfluxDBClient(
    url='localhost',
    host=app.config['INFLUXDB_HOST'],
    port=app.config['INFLUXDB_PORT'],
    username=app.config['INFLUXDB_USERNAME'],
    password=app.config['INFLUXDB_PASSWORD'],
    database=app.config['INFLUXDB_DATABASE']
)


data_blueprint = Blueprint('data', __name__)


def retrieve_bulk_data():
    host = 'localhost'
    port = 8086
    token = 'KqGLGKpkeaeYLbQlDb9c8xUx6yJjC-D2STgNKE53JPCRkCG6E0WTdIxZEEIPJTOT1AzSfVPzbFE3yhyPUBlcGw=='
    org = 'StartupScaler'
    bucket = 'bulk_data'
    url = 'http://localhost:8086'

    client = InfluxDBClient(url=url, token=token, org=org)

    query = f'from(bucket: "{bucket}") |> range(start: -1d) |> group() |> keys()'

    result = client.query_api().query(org=org, query=query)

    serialized_data = []
    for table in result:
        for record in table.records:
            serialized_data.append(record.values)

    return serialized_data


@data_blueprint.route('/api/bulk-data', methods=['GET'])
def get_bulk_data():
    bulk_data = retrieve_bulk_data()
    return jsonify({'bulk_data': bulk_data})

if __name__ == '__main__':
    app.register_blueprint(data_blueprint)
    app.run(debug=True)
