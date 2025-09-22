"""
Unit tests for the Master.py script.

These tests focus on the core logic of the master node, including job persistence,
concurrency, and worker health-check mechanisms, using mock objects for gRPC
interactions and controlling time to simulate real-world scenarios.
"""
import unittest
from unittest.mock import MagicMock, patch, mock_open
import json
import os
import time
import queue
import threading
import sys

# Append the current directory to the path to import the master script
sys.path.append('.')
from Master import JobQueueServicer, JOBS_FILE, HEARTBEAT_TIMEOUT
import job_queue_pb2

class TestMaster(unittest.TestCase):
    """Test suite for the master node's functionality."""

    def setUp(self):
        """Setup for each test, ensuring a clean state."""
        # Create a mock for the gRPC context
        self.mock_context = MagicMock()
        self.mock_context.is_active.return_value = True

        # Clean up the jobs file if it exists from a previous run
        if os.path.exists(JOBS_FILE):
            os.remove(JOBS_FILE)

    def tearDown(self):
        """Cleanup after each test."""
        if os.path.exists(JOBS_FILE):
            os.remove(JOBS_FILE)

    def test_initial_job_loading_from_empty_file(self):
        """Test that the master initializes with sample jobs if no file exists."""
        servicer = JobQueueServicer()
        self.assertGreater(servicer.pending_jobs.qsize(), 0)
        self.assertTrue(os.path.exists(JOBS_FILE))

    def test_job_persistence(self):
        """Test that jobs are saved to and loaded from a file correctly."""
        # First, create a servicer to save some jobs
        servicer_to_save = JobQueueServicer()
        servicer_to_save.pending_jobs.put(job_queue_pb2.JobRequest(job_id="test_job_1", payload=1))
        servicer_to_save.pending_jobs.put(job_queue_pb2.JobRequest(job_id="test_job_2", payload=2))
        servicer_to_save._save_jobs()

        # Now, create a new servicer and check if it loads the jobs
        servicer_to_load = JobQueueServicer()
        self.assertEqual(servicer_to_load.pending_jobs.qsize(), 102) # Initial 100 + the 2 we added
        
        # Check if the jobs are present
        found_test_jobs = 0
        while not servicer_to_load.pending_jobs.empty():
            job = servicer_to_load.pending_jobs.get()
            if job.job_id in ["test_job_1", "test_job_2"]:
                found_test_jobs += 1
        self.assertEqual(found_test_jobs, 2)

    def test_send_job_rpc(self):
        """Test that a new job is correctly added to the queue via RPC."""
        servicer = JobQueueServicer()
        initial_count = servicer.pending_jobs.qsize()
        
        request = job_queue_pb2.JobRequest(job_id="new_job", job_type="test", payload=10)
        response = servicer.SendJob(request, self.mock_context)
        
        self.assertEqual(response.message, "Job new_job received.")
        self.assertEqual(servicer.pending_jobs.qsize(), initial_count + 1)
        
        # Verify the job was persisted
        servicer_reloaded = JobQueueServicer()
        found = False
        while not servicer_reloaded.pending_jobs.empty():
            job = servicer_reloaded.pending_jobs.get()
            if job.job_id == "new_job":
                found = True
                break
        self.assertTrue(found)

    def test_get_jobs_rpc(self):
        """Test that the master streams jobs to a worker."""
        servicer = JobQueueServicer()
        request = job_queue_pb2.WorkerInfo(worker_id="test_worker")
        
        # Get one job from the stream
        jobs_stream = servicer.GetJobs(request, self.mock_context)
        first_job = next(jobs_stream)
        
        self.assertIsInstance(first_job, job_queue_pb2.JobRequest)
        self.assertEqual(servicer.jobs_in_progress[first_job.job_id].job_id, first_job.job_id)

    def test_send_result_rpc(self):
        """Test that the master correctly handles a job result."""
        servicer = JobQueueServicer()
        test_job_id = "test_job_123"
        servicer.jobs_in_progress[test_job_id] = job_queue_pb2.JobRequest(job_id=test_job_id, payload=1)

        request = job_queue_pb2.JobResult(
            job_id=test_job_id,
            worker_id="test_worker",
            result="55"
        )
        response = servicer.SendResult(request, self.mock_context)

        self.assertTrue(response.success)
        self.assertEqual(servicer.completed_jobs[test_job_id], "55")
        self.assertNotIn(test_job_id, servicer.jobs_in_progress)

    def test_dead_worker_requeues_job(self):
        """Test that a job is re-queued when a worker times out."""
        servicer = JobQueueServicer()
        worker_id = "dead_worker"
        test_job = job_queue_pb2.JobRequest(job_id="test_dead_job", job_type="test", payload=100)
        
        # Simulate a job being in progress for the dead worker
        with servicer.lock:
            servicer.jobs_in_progress[test_job.job_id] = test_job
            servicer.workers_last_heartbeat[worker_id] = time.time() - HEARTBEAT_TIMEOUT - 1

        # Use an event to control the background thread loop
        stop_event = threading.Event()
        def controlled_loop():
            # Run the check once
            servicer.check_dead_workers_loop()
            stop_event.set()

        thread = threading.Thread(target=controlled_loop)
        thread.start()
        stop_event.wait(timeout=10) # Wait for the check to complete
        thread.join()

        # Assert that the job was re-queued and removed from jobs_in_progress
        self.assertEqual(servicer.pending_jobs.qsize(), 101) # 100 initial + 1 requeued
        self.assertNotIn(test_job.job_id, servicer.jobs_in_progress)
        self.assertNotIn(worker_id, servicer.workers_last_heartbeat)

if __name__ == '__main__':
    unittest.main()
