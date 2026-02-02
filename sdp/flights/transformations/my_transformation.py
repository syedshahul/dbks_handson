from pyspark import pipelines as dp
# import and register the datasource
from pyspark_datasources import OpenSkyDataSource
spark.dataSource.register(OpenSkyDataSource)

@dp.table(name="ingest_flights")
def ingest_flights():
    return (
        spark.readStream
            .format("opensky")
            .load()
    )