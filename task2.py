#Imports
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import unittest
from io import StringIO
import sys
from unittest.mock import patch

#Same functions from task 1
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

#Composite Transform Here
class ProcessTransactions(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll | 'ParseCSV' >> beam.Map(lambda line: dict(zip(['timestamp', 'origin', 'destination', 'transaction_amount'], line.split(','))))
                  | 'FilterTransactions' >> beam.Filter(filter_transactions)
                  | 'MapToTuple' >> beam.Map(lambda transaction: (transaction['timestamp'][:10], float(transaction['transaction_amount'])))
                  | 'SumTotalByDate' >> beam.CombinePerKey(sum)
                  | 'FormatOutput' >> beam.Map(format_output)
        )

#Run a pipeline using this transform with a print statement to visually verify
def run_pipeline(input_data):
    with beam.Pipeline() as p:
        output = (
            p | 'ReadInput' >> beam.Create(input_data)
              | 'ProcessTransactions' >> ProcessTransactions()
              | 'PrintOutput' >> beam.Map(print)
        )

if __name__ == '__main__':
    # Sample input data
    input_data = [
        '2024-04-16,origin1,destination1,100',
        '2024-04-16,origin2,destination2,200',
        '2024-04-17,origin3,destination3,300',
        '2024-04-17,origin4,destination4,400',
        '2024-04-17,origin5,destination5,500',
        '2009-04-17,origin6,destination6,600'
        
    ]

    run_pipeline(input_data)

#Using apache beam unit test functionality
class TestPipeline(unittest.TestCase):
    @patch('sys.stdout', new_callable=StringIO)
    def test_run_pipeline(self, mock_stdout):
        # Sample input data
        input_data = [
            '2024-04-16,origin1,destination1,100',
            '2024-04-16,origin2,destination2,200',
            '2024-04-17,origin3,destination3,300',
            '2024-04-17,origin4,destination4,400',
            '2024-04-17,origin5,destination5,500',
            '2009-04-17,origin6,destination6,600'
        ]

        # Expected output
        expected_output = [
            'Date: 2024-04-16, Total Amount: 300.0\n',
            'Date: 2024-04-17, Total Amount: 1200.0\n'
        ]

        # Run pipeline
        run_pipeline(input_data)

        # Capture printed output
        printed_output = mock_stdout.getvalue()

        # Assert output matches expected output
        self.assertEqual(printed_output, ''.join(expected_output))

if __name__ == '__main__':
    unittest.main()

