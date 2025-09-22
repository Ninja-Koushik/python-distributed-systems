"""
Unit tests for the Worker.py script.

These tests use mock objects to simulate the master's gRPC service and its
streaming behavior, ensuring that the worker can correctly process jobs and
handle various connection states.
"""
import unittest
from unittest.mock import patch, MagicMock
import sys
import threading
import time

# Append the current directory to the path to import the worker script
sys.path.append('.')
from Worker import fibonacci, _process_job, _send_heartbeats, run_worker
import job_queue_pb2

class TestWorker(unittest.TestCase):
    """Test suite for the worker node's functionality."""

    def test_fibonacci(self):
        """Test the core Fibonacci calculation function."""
        self.assertEqual(fibonacci(0), 0)
        self.assertEqual(fibonacci(1), 1)
        self.assertEqual(fibonacci(10), 55)
        self.assertEqual(fibonacci(20), 6765)

    def test_process_job_fibonacci(self):
        """Test that the worker correctly processes a Fibonacci job."""
        job = job_queue_pb2.JobRequest(job_type="fibonacci", payload=10)
        result = _process_job(job)
        self.assertEqual(result, "55")

    def test_process_job_unknown_type(self):
        """Test that the worker handles an unknown job type gracefully."""
        job = job_queue_pb2.JobRequest(job_type="unknown_type", payload=10)
        result = _process_job(job)
        self.assertEqual(result, "Error: Unknown job type")

    @patch('Worker.grpc.insecure_channel')
    def test_send_heartbeats(self, mock_channel):
        """Test that heartbeats are sent periodically and stop on event."""
        mock_stub = MagicMock()
        mock_channel.return_value.__enter__.return_value = mock_stub
        stop_event = threading.Event()
        
        # Start the heartbeat thread
        thread = threading.Thread(target=_send_heartbeats, args=(mock_stub, "test_worker", stop_event))
        thread.start()

        # Wait for a couple of heartbeats to be sent
        time.sleep(2)
        
        # Assert that heartbeats were called
        self.assertGreaterEqual(mock_stub.Heartbeat.call_count, 1)

        # Signal the thread to stop and wait for it to terminate
        stop_event.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())

    @patch('Worker.grpc.insecure_channel')
    def test_run_worker_job_processing_loop(self, mock_channel):
        """Test the main worker loop for processing jobs from a stream."""
        mock_stub = MagicMock()
        mock_channel.return_value.__enter__.return_value = mock_stub

        # Create a mock job stream that yields two jobs
        mock_jobs = [
            job_queue_pb2.JobRequest(job_id="job_1", job_type="fibonacci", payload=5),
            job_queue_pb2.JobRequest(job_id="job_2", job_type="fibonacci", payload=6)
        ]
        mock_jobs_stream = (job for job in mock_jobs)
        mock_stub.GetJobs.return_value = mock_jobs_stream
        
        # Mock the heartbeat thread to do nothing
        with patch('Worker.threading.Thread'):
            run_worker("test_worker")

        # Assertions to verify correct behavior
        mock_stub.GetJobs.assert_called_once()
        self.assertEqual(mock_stub.SendResult.call_count, 2)
        
        # Check the results sent back
        sent_result_1 = mock_stub.SendResult.call_args_list[0][0][0]
        self.assertEqual(sent_result_1.job_id, "job_1")
        self.assertEqual(sent_result_1.result, "5")

        sent_result_2 = mock_stub.SendResult.call_args_list[1][0][0]
        self.assertEqual(sent_result_2.job_id, "job_2")
        self.assertEqual(sent_result_2.result, "8")

    @patch('Worker.grpc.insecure_channel')
    def test_run_worker_connection_error(self, mock_channel):
        """Test that the worker handles a connection error gracefully."""
        mock_stub = MagicMock()
        mock_channel.return_value.__enter__.return_value = mock_stub
        mock_stub.GetJobs.side_effect = grpc.RpcError()
        
        # Mock the heartbeat thread to do nothing
        with patch('Worker.threading.Thread'):
            with self.assertLogs('Worker', level='ERROR') as cm:
                run_worker("test_worker")
                # Assert that the correct error message is logged
                self.assertIn("RPC error during job stream", cm.output[0])

if __name__ == '__main__':
    unittest.main()
