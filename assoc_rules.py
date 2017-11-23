import apache_beam as beam
from io.bq import makeBigQuerySink

# functions for building and formating association rules
def buildARuleNum(x):
    import itertools
    makeItemSetKey = lambda x: '_'.join([str(i) for i in x])
    items, freq = x
    if len(items) > 1:
        subsets = itertools.combinations(items, len(items)-1)
        for s in subsets:
            yield (makeItemSetKey(s), {'rhs':list(set(items) - set(s))[0], 'conf_num':freq})

def buildARuleDenom(x):
    makeItemSetKey = lambda x: '_'.join([str(i) for i in x])
    return (makeItemSetKey(x[0]), {'conf_denom':x[1]})

def formatARule(x):
    lhs, rhs, conf = x[0], x[1]['rhs'], float(x[1]['conf_num']) / float(x[1]['conf_denom'])
    lhs = tuple([int(i) for i in lhs.split('_')])
    return (lhs, rhs, conf)

# builds and runs pipeline
def run(config):

    # initialize pipeline
    #runner = "DataflowRunner"
    runner = "DirectRunner"
    pipeline =  beam.Pipeline(runner = runner, argv = [
       "--job_name", "instacart-assoc-rules",
       "--project", config['project'],
       "--staging_location", config['staging_location'],
       "--temp_location", config['temp_location']
    ])

    # get frequent item sets
    query = "SELECT products, frequency FROM instacart.freq_item_sets"
    freq_item_sets = (pipeline
        | 'GetFreqItemSets' >> beam.io.Read(beam.io.BigQuerySource(query = query, use_standard_sql = True))
        | 'ProcessFreqItemSets' >> beam.Map(lambda x: (x['products'], x['frequency'])))

    # build rules
    arules_num = freq_item_sets | 'MakeRulesNum' >> beam.FlatMap(buildARuleNum)
    arules_denom = freq_item_sets | 'MakeRulesDenom' >> beam.Map(buildARuleDenom)
    arules = ({'num' : arules_num, 'denom' : arules_denom}
        | 'Merge' >> beam.CoGroupByKey()
        | 'FilterMerge' >> beam.Filter(lambda x: len(x[1]['num']) > 0)
        | 'ProcessMerge' >> beam.Map(lambda x: (x[0], dict(x[1]['num'][0].items() + x[1]['denom'][0].items())))
        | 'FormatRules' >> beam.Map(formatARule)
        | 'FilterConf' >> beam.Filter(lambda x: x[2] > config['conf_cutoff']))

    # export
    schema = {
        'lhs': {'name': 'lhs', 'type': 'integer', 'mode': 'repeated'},
        'rhs': {'name': 'rhs', 'type': 'integer', 'mode': 'required'},
        'conf': {'name': 'conf', 'type': 'float', 'mode': 'required'}
    }

    arules = (arules
        | 'ExportPrep' >> beam.Map(lambda x: {'lhs':x[0], 'rhs':x[1], 'conf':x[2]})
        | 'Export' >> beam.io.Write(makeBigQuerySink("instacart.assoc_rules", schema)))

    # run
    pipeline.run()


if __name__ == '__main__':
    import yaml
    with open("config.yaml", "r") as f:
        config = yaml.load(f)
    run(config)
