import csv
import datetime
import multiprocessing
import random
import time

from bux_grader_framework import Grader
from bux_grader_framework.util import make_hashkey

from .xqueue import XQueueStub


class GraderTestRunner(object):
    def __init__(self, settings_module, count=50, submissions_per_second=10,
                 simulation=False, resultfile='results.csv'):
        self.settings_module = settings_module
        self.count = count
        self.submission_rates = [int(rate) for rate in str(submissions_per_second).split(",")]
        self.simulation = simulation
        self.resultfile = resultfile

    def run(self):
        """ Run load tests """

        # Prefix result CSV with datestamp
        self.run_time = datetime.datetime.now()

        print "Starting load tests"
        print "Run count: %d" % len(self.submission_rates)
        print "Run time: %s" % self.run_time.isoformat()
        print

        for idx, rate in enumerate(self.submission_rates):
            run = idx + 1
            print "======================= Run #%d =============== " % run
            self.run_test(rate, self.count)
            print "======================= Run #%d completed ===== " % run
            self.reset()
            print

            if run < len(self.submission_rates):
                print "... Sleeping 60 seconds between runs  z z zzz ..."
                print
                time.sleep(60)

        print "Load tests completed!"

    def run_test(self, submit_rate, count):
        """ A single load test run """
        self.xqueue = XQueueStub(count, submit_rate, self.simulation)
        self.grader = TestGrader(self.xqueue, self.settings_module)

        # Stats
        submit_delay = 1.0 / submit_rate
        print "Submission count: %d" % count
        print "Submission rate: %d/s" % submit_rate
        print

        # Start the grader process
        print "Starting grader..."
        self.grader.start()

        # Sleep for a few seconds to give the grader time to initialize
        print "Waiting 10 seconds for grader to initialize..."
        print
        time.sleep(10)

        # Generate test submissions
        time_start = time.time()
        print "Generating %d submissions..." % count
        print
        for submission in self.generate_submissions():
            self.xqueue.submit(submission)

            # Simulate random delays between submissions
            if self.simulation:
                time.sleep(random.uniform(0.1, 0.5))
            else:
                time.sleep(submit_delay)

        print "Waiting for all submissions to be handled..."
        self.xqueue.submissions.join()
        self.xqueue.submissions.close()

        time_stop = time.time()
        elapsed_time = time_stop - time_start
        print
        print "%d submissions handled in %0.3f seconds" % (count,
                                                           elapsed_time)

        print "Generating results..."
        self.generate_results(submit_rate)
        self.xqueue.results.close()

        print "Waiting 10 seconds for grader to finish..."
        time.sleep(10)

        print "Stopping grader..."
        self.grader.stop()
        self.grader.join()

    def reset(self):
        self.grader = None
        self.xqueue = None

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

        # The LMS generates the ``submission_time`` when a submission is
        # created as a formatted string:
        #
        #   from datetime import datetime
        #   datetime.strftime(datetime.now(UTC), '%Y%m%d%H%M%S')
        #
        # We use timestamps here instead for higher resolution. We use it to
        # log the delay between creation and reception in the ``XQueueWorker``.

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
                "student_info": {
                    "submission_time": pull_time
                }
            },
            "xqueue_files": {}
        }

    def get_submission(self):
        """ Subclasses must implement """
        pass

    def generate_results(self, rate):
        """ Logs results to CSV """
        outfile = "%s.%s.%s" % (self.run_time.strftime("%Y-%m-%d-%H%M%s"),
                                rate, self.resultfile)

        print "Creating log file: %s" % outfile
        with open(outfile, 'w') as f:
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
