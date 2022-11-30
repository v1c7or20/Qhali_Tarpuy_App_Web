from opentsdb import TSDBClient
import logging

logging.basicConfig(level=logging.DEBUG)
tsdb = TSDBClient('127.0.0.1')
tsdb.send('metric.Temps', 250, tag1='Temperature', tag2='Soil')
print(tsdb.statuses )
tsdb.close()
tsdb.wait()