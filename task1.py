#Imports
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

#Functions for the pipeline
def filter_transactions(transaction):
    """
    Function that will filter out transactions with transaction amounts lower than 20
    and dates before 2010.
    """
    transaction_amount = float(transaction['transaction_amount'])
    return transaction_amount > 20 and not transaction['timestamp'].startswith('200')

def format_output(date_total):
    """
    Converts the tuples back into a dictionary-like string
    """
    return f"Date: {date_total[0]}, Total Amount: {date_total[1]}"

def run():
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        transactions = (
            p | 'ReadCSV' >> ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
              | 'ParseCSV' >> beam.Map(lambda line: dict(zip(['timestamp', 'origin', 'destination', 'transaction_amount'], line.split(','))))
              | 'FilterTransactions' >> beam.Filter(filter_transactions)
              | 'MapToTuple' >> beam.Map(lambda transaction: (transaction['timestamp'][:10], float(transaction['transaction_amount'])))
              | 'SumTotalByDate' >> beam.CombinePerKey(sum)
              | 'FormatOutput' >> beam.Map(format_output)
              | 'WriteToJSONLines' >> WriteToText('output/results', file_name_suffix='.jsonl.gz', shard_name_template='')
        )

if __name__ == '__main__':
    run()