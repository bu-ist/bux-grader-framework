import csv
import multiprocessing
import time

from bux_grader_framework import Grader
from bux_grader_framework.util import make_hashkey

from .xqueue import XQueueStub


class GraderTestRunner(object):
    def __init__(self, settings_module, count=50, resultfile='results.csv'):
        self.count = count
        self.resultfile = resultfile

        self.xqueue = XQueueStub()
        self.grader = TestGrader(self.xqueue, settings_module)

    def run(self):
        """ Run load tests """
        # Start the grader process
        self.grader.start()

        time_start = time.time()

        # Generate test submissions
        for submission in self.generate_submissions():
            self.xqueue.submit(submission)

        print "Waiting for all submissions to be handled..."
        self.xqueue.submissions.join()
        self.xqueue.submissions.close()

        time_stop = time.time()
        elapsed_time = time_stop - time_start
        print "%d submissions handled in %0.3f seconds" % (self.count, elapsed_time)

        # Gather results
        self.generate_results_log()
        self.xqueue.results.close()

        # Terminate grader process
        print "Stopping TestGrader..."
        self.grader.stop()
        print "Waiting for TestGrader to finish..."
        self.grader.join()
        print "TestGrader stopped, exiting"

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

    def generate_results_log(self):
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
        print "Sending stop event to Grader"
        self.grader.stop()
