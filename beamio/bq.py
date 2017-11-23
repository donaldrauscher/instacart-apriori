import apache_beam as beam

# creates BigQuery sink
def makeBigQuerySink(output, schema):
    from apache_beam.io.gcp.internal.clients import bigquery

    def makeTableFieldSchema(**kwargs):
        field = bigquery.TableFieldSchema()
        for k,v in kwargs.items():
            field.__setattr__(k,v)
        return field

    table_schema = bigquery.TableSchema()
    for k,v in schema.items():
        table_schema.fields.append(makeTableFieldSchema(**v))

    return beam.io.BigQuerySink(
        output,
        schema = table_schema,
        create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE)
