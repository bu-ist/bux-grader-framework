import csv
import multiprocessing
import random
import time

from bux_grader_framework import Grader
from bux_grader_framework.util import make_hashkey

from .xqueue import XQueueStub


class GraderTestRunner(object):
    def __init__(self, settings_module, count=50, submissions_per_second=10,
                 simulation=False, resultfile='results.csv'):
        self.count = count
        self.submissions_per_second = submissions_per_second
        self.simulation = simulation
        self.resultfile = resultfile

        # Time to wait between submississions
        self.submission_delay = 1.0 / submissions_per_second

        self.xqueue = XQueueStub(simulation=self.simulation)
        self.grader = TestGrader(self.xqueue, settings_module)

    def run(self):
        """ Run load tests """

        # TODO: Generate a run number based on current time

        # Stats
        print "Starting grader load tests"
        if self.simulation:
            print "Simulation mode: ON"
        else:
            print "Simulation mode: OFF"
            print "Submission rate: %d/s" % self.submissions_per_second

        # Start the grader process
        print "Starting grader..."
        self.grader.start()

        # Sleep for a few seconds to give the grader time to initialize
        print "Waiting 5 seconds for grader to initialize..."
        time.sleep(5)

        time_start = time.time()

        # Generate test submissions
        print "Generating %d submissions..." % self.count
        for submission in self.generate_submissions():
            self.xqueue.submit(submission)

            # Simulate random delays between submissions
            if self.simulation:
                time.sleep(random.uniform(0.1, 0.5))
            else:
                time.sleep(self.submission_delay)

        print "Waiting for all submissions to be handled..."
        self.xqueue.submissions.join()
        self.xqueue.submissions.close()

        time_stop = time.time()
        elapsed_time = time_stop - time_start
        print "%d submissions handled in %0.3f seconds" % (self.count,
                                                           elapsed_time)

        # Gather results
        print "Generating results..."
        self.generate_results()
        self.xqueue.results.close()

        # Terminate grader process
        print "Stopping grader..."
        self.grader.stop()
        self.grader.join()

        print "Load tests completed."

    def generate_submissions(self):
        """ Generates ``self.count`` submissions """
        submit_num = 1
        while submit_num <= self.count:
            student_response, grader_payload = self.get_submission()
            submission = self.create_submission(submit_num, student_response,
                                                grader_payload)
            yield submission
            submit_num += 1

    def create_submission(self, submit_id, student_response, grader_payload):
        """ Creats a properly formatted submission dict """
        pull_time = time.time()
        pullkey = make_hashkey(str(pull_time)+str(submit_id))

        return {
            "xqueue_header": {
                "submission_id": submit_id,
                "submission_key": pullkey
            },
            "xqueue_body": {
                "student_response": student_response,
                "grader_payload": grader_payload,
                "submission_info": {
                    "submission_time": pull_time
                }
            },
            "xqueue_files": {}
        }

    def get_submission(self):
        """ Subclasses must implement """
        pass

    def generate_results(self):
        """ Logs results to CSV """
        print "Creating log file: %s" % self.resultfile
        with open(self.resultfile, 'w') as f:
            writer = csv.writer(f)
            for result in self.xqueue.get_results():
                writer.writerow(result)


class TestGrader(multiprocessing.Process):
    """ A thin wrapper around bux_grader_framework.Grader.

    Wraps it in a ``multiprocessing.Process``.
    Patches ``xqueue`` method to return an :class:`XQueueStub`.
    Adds a stop event.

    """
    def __init__(self, xqueue, settings_module):
        super(TestGrader, self).__init__()

        self.grader = Grader()

        # A little monkey patching...
        self.grader.xqueue = lambda: xqueue

        self.grader.config_from_module(settings_module)

    def run(self):
        print "Grader started"
        self.grader.run()
        print "Grader exited"

    def stop(self):
        self.grader.stop()
