from google.cloud import bigquery
import yaml, operator
import pandas as pd

# pull in config
with open("config.yaml", "r") as f:
    config = yaml.load(f)

# run query
client = bigquery.Client(config['project'])

query = """
    WITH rules AS (
      SELECT * EXCEPT(lhs) FROM (
        SELECT *, ROW_NUMBER() OVER () AS i FROM `instacart.assoc_rules`
      ), UNNEST(lhs) AS lhs2
    )
    SELECT i, STRING_AGG(lhs_name, ', ') AS lhs, ANY_VALUE(rhs_name) AS rhs, ANY_VALUE(supp) AS supp, ANY_VALUE(conf) AS conf
    FROM (
      SELECT rules.*, prod1.product_name AS lhs_name, prod2.product_name AS rhs_name
      FROM rules
      INNER JOIN `instacart.products` AS prod1 ON lhs2 = prod1.product_id
      INNER JOIN `instacart.products` AS prod2 ON rhs = prod2.product_id
    ) AS x GROUP BY 1 ORDER BY conf DESC
"""

query_job = client.query(query)
assert query_job.state == 'RUNNING'

# process results
def process_row(row):
    keys = sorted(row._xxx_field_to_index.items(), key=operator.itemgetter(1))
    keys = [k[0] for k in keys]
    values = row.values()
    return dict(zip(keys, values))

iterator = query_job.result()
rows = [process_row(row) for row in list(iterator)]
df = pd.DataFrame.from_dict(data = rows, orient = "columns")
df.to_csv('out/assoc_rules.csv', index = False)
