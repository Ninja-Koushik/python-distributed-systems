"""
Unit tests for the Client.py script.

These tests use mock objects to simulate the gRPC server and ensure that the
client correctly handles both successful and failed job submissions.
"""
import unittest
from unittest.mock import patch, MagicMock, call
import sys
import argparse

# We need to import the client script to test its functions
sys.path.append('.')
from Client import submit_job, _parse_args
import job_queue_pb2

class TestClient(unittest.TestCase):
    """Test suite for the client's functionality."""

    @patch('Client.grpc.insecure_channel')
    def test_submit_job_success(self, mock_channel):
        """Test a successful job submission to the master."""
        # Setup mock gRPC channel and stub
        mock_stub = MagicMock()
        mock_channel.return_value.__enter__.return_value = mock_stub
        mock_stub.SendJob.return_value = job_queue_pb2.JobReply(message="Job received.")

        # Call the function with test data
        job_type = "test_type"
        payload = 123
        with patch('Client.uuid.uuid4', return_value='mock-uuid'):
            submit_job(job_type, payload)

        # Assertions to verify the correct behavior
        mock_channel.assert_called_once_with('localhost:50051')
        mock_stub.SendJob.assert_called_once()
        sent_job = mock_stub.SendJob.call_args[0][0]
        self.assertEqual(sent_job.job_type, job_type)
        self.assertEqual(sent_job.payload, payload)
        self.assertIn('mock-uuid', sent_job.job_id)

    @patch('Client.grpc.insecure_channel')
    def test_submit_job_rpc_error(self, mock_channel):
        """Test job submission when the master is unreachable."""
        # Setup mock to raise an RpcError on the stub call
        mock_stub = MagicMock()
        mock_channel.return_value.__enter__.return_value = mock_stub
        mock_error = grpc.RpcError()
        mock_error.details = lambda: "Connection refused"
        mock_stub.SendJob.side_effect = mock_error

        # Assert that the function call logs an error but does not raise an exception
        with self.assertLogs('Client', level='INFO') as cm:
            submit_job("test_type", 100)
            self.assertIn("Could not connect to or communicate with the master", cm.output[0])
            self.assertIn("Connection refused", cm.output[0])

    def test_parse_args_valid_input(self):
        """Test that command-line arguments are parsed correctly."""
        with patch('sys.argv', ['client.py', 'fibonacci', '50']):
            args = _parse_args()
            self.assertEqual(args.job_type, 'fibonacci')
            self.assertEqual(args.payload, 50)

    def test_parse_args_invalid_payload(self):
        """Test that an error is raised for non-integer payloads."""
        with patch('sys.argv', ['client.py', 'fibonacci', 'not-a-number']):
            with self.assertRaises(SystemExit):
                _parse_args()

    def test_parse_args_missing_arguments(self):
        """Test that an error is raised for missing arguments."""
        with patch('sys.argv', ['client.py', 'fibonacci']):
            with self.assertRaises(SystemExit):
                _parse_args()

if __name__ == '__main__':
    unittest.main()
