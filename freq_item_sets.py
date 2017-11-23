import apache_beam as beam

# normalizes an order stored in an array
def normalize(order):
    for p in order[0]:
        yield (p, 1)

# trim orders to items in a specified list
def trimOrders(order, items, min_length = 0):
    order_trimed = [o for o in order if o in items]
    if len(order_trimed) >= min_length:
        yield (order_trimed)

# create all combinations
def extractCombos(order, size):
    import itertools
    subsets = itertools.combinations(order, size)
    for s in subsets:
        yield (s, 1)

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

# builds and runs pipeline
def run(config):

    # initialize pipeline
    runner = "DataflowRunner"
    #runner = "DirectRunner"
    pipeline =  beam.Pipeline(runner = runner, argv = [
       "--job_name", "instacart-freq-item-sets",
       "--project", config['project'],
       "--staging_location", config['staging_location'],
       "--temp_location", config['temp_location']
    ])

    # get list of orders
    query = """
        SELECT order_id, ARRAY_AGG(product_id ORDER BY product_id) AS products
        FROM instacart.order_products__prior AS orders
        GROUP BY 1
    """
    orders = (pipeline
        | 'GetOrders' >> beam.io.Read(beam.io.BigQuerySource(query = query, use_standard_sql = True))
        | 'CleanOrders' >> beam.Map(lambda x: x['products']))

    # extract length 1 itemsets
    freq1 = (orders
        | 'Get1' >> beam.FlatMap(extractCombos, 1)
        | 'Count1' >> beam.CombinePerKey(sum)
        | 'Filter1' >> beam.Filter(lambda x: x[1] >= config['supp_cutoff']))

    freq1_items = (freq1
        | 'SelectFreq1' >> beam.Map(lambda x: x[0][0]))

    # extract length 2 itemsets
    freq2 = (orders
        | 'Trim2' >> beam.FlatMap(trimOrders, beam.pvalue.AsList(freq1_items), 2)
        | 'Get2' >> beam.FlatMap(extractCombos, 2)
        | 'Count2' >> beam.CombinePerKey(sum)
        | 'Filter2' >> beam.Filter(lambda x: x[1] >= config['supp_cutoff']))

    freq2_items = (freq2
        | 'NormalizeFreq2' >> beam.FlatMap(normalize)
        | 'CountFreq2' >> beam.CombinePerKey(sum)
        | 'FilterFreq2' >> beam.Filter(lambda x: x[1] >= 2)
        | 'SelectFreq2' >> beam.Map(lambda x: x[0]))

    # extract length 3 itemsets
    freq3 = (orders
        | 'Trim3' >> beam.FlatMap(trimOrders, beam.pvalue.AsList(freq2_items), 3)
        | 'Get3' >> beam.FlatMap(extractCombos, 3)
        | 'Count3' >> beam.CombinePerKey(sum)
        | 'Filter3' >> beam.Filter(lambda x: x[1] >= config['supp_cutoff']))

    # concatenate all item sets and export to BigQuery
    schema = {
        'products': {'name': 'products', 'type': 'integer', 'mode': 'repeated'},
        'frequency': {'name': 'frequency', 'type': 'integer', 'mode': 'required'}
    }

    all_freq = ((freq1, freq2, freq3)
        | 'Flatten' >> beam.Flatten()
        | 'ExportPrep' >> beam.Map(lambda x: {'products' : x[0], 'frequency' : x[1]})
        | 'Export' >> beam.io.Write(makeBigQuerySink("instacart.freq_item_sets", schema))
    )

    # run
    pipeline.run()


if __name__ == '__main__':
    import yaml
    with open("config.yaml", "r") as f:
        config = yaml.load(f)
    run(config)
