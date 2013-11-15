#!/usr/bin/env python
#
# Copyright (c) 2013 prezi.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
# Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
import calendar
import heapq
import logging
import math
import re
import sys
import uuid


class Event(object):
    def __init__(self, data_dict, legacy):
        if legacy:  # Legacy logs need their data converted to a UNIX timestamp
            fields = ['year', 'month', 'day', 'hour', 'minute', 'second']
            self.timestamp = calendar.timegm(map(lambda k: int(data_dict[k]), fields))
        else:
            self.timestamp = int(data_dict['timestamp'])

    def __cmp__(self, other):
        return cmp(self.timestamp, other.timestamp)  # Required for sorting in heap queues


class Job(Event):
    def __init__(self, data_dict, legacy):
        super(Job, self).__init__(data_dict, legacy)
        self.category = data_dict['category']  # The category e.g. default, url, export
        self.elapsed = float(data_dict['elapsed'])  # Service time of job
        self.guid = data_dict['guid']  # Logging only


class Command(Event):
    def __init__(self, data_dict, legacy):
        super(Command, self).__init__(data_dict, legacy)
        self.category = data_dict['category']  # The category e.g. default, url, export
        self.cmd = data_dict['cmd']  # Command e.g. launch, terminate


class Machine(object):
    BILLING_UNIT = 3600  # VMs are billed by the hour (3600 seconds)
    MACHINE_INACTIVE = 120  # Boot time is 2 minutes (120 seconds)

    def __init__(self, booted, world):
        self.active_from = booted + self.MACHINE_INACTIVE  # Time at which we are done booting
        self.busy_till = 0  # Time at which we finish processing current job
        self.world = world  # The state of the world
        self.terminated = False  # True if the machine has been shut down
        self.guid = str(uuid.uuid1())  # Unique string to identify the machine (logging only)

    @property
    def running_since(self):
        return self.active_from - self.MACHINE_INACTIVE  # Time the machine started booting

    def till_billing(self, now):
        return abs((now - self.running_since) % -self.BILLING_UNIT)  # Number of seconds left till we have to pay again

    def is_active(self, now):
        return now >= self.active_from and self.busy_till <= now  # Whether the machine is/will be available at this point in time


class WithLog(object):  # This is just for logging
    log = logging.getLogger('prezi.com')

    def info(self, *args):
        self.log.info(*args)


class State(WithLog):
    MAX_QUEUE_TIME = 5  # This is the maximum time the job can queue while incurring penalties
    MAX_PENALTY_TIME = 120  # The maximum queuing time, after this the system is disqualified

    def __init__(self, legacy):
        self.legacy = legacy  # What kind of log we are parsing
        self.time = 0  # The current time
        self.billed = 0  # The machine hours billed
        self.penalty = 0  # The penalty incurred
        self.overwait = False  # True if we went beyond be the maximum queue time
        # These are heap queues that will store the machines and jobs
        if self.legacy:
            self.categories = ['general', 'export', 'url']
        else:
            self.categories = ['default', 'export', 'url']
        self.jobs = dict((category, []) for category in self.categories)
        self.machines = dict((category, []) for category in self.categories)

    @property
    def now(self):
        return self.time

    @now.setter  # The time is set every time a new line is read from the input
    def now(self, value):
        self.time = value

    def receive(self, event):  # Process a job/command from the input file
        self.now = event.timestamp  # Update the curren time
        if isinstance(event, Job):  # We got a new job
            heapq.heappush(self.jobs[event.category], event)  # Add the job to the queue
            self.process_event(event)  # Process the jobs in this category
        elif isinstance(event, Command):
            if event.cmd == 'launch':
                self.launch(Machine(event.timestamp, self), event.category)
            elif event.cmd == 'terminate':
                closest = heapq.nsmallest(1, self.machines[event.category])
                if closest:
                    self.terminate(closest[0], event.category)

    def launch(self, machine, category):
        heapq.heappush(self.machines[category], machine)
        self.info('launch %d %d %s' % (machine.running_since, machine.busy_till, machine.guid))

    def terminate(self, machine, category):
        machine.terminated = True
        bill = self.bill(machine, category)
        self.info('terminate %d %d %d %s' % (machine.running_since, machine.active_from, bill, machine.guid))

    def process_event(self, job):
        category = job.category
        self.info('job_retrieved %d %s' % (job.timestamp, job.guid))
        machine_queue = []
        # Prioritize the machines closest to their billing cycle
        # If there isn't enough time to complete the job, de-prioritize (machines with most time left go first)
        for machine in self.machines[category]:
            priority = (machine.till_billing(job.timestamp) - job.elapsed) % -3600
            heapq.heappush(machine_queue, (priority, machine))
        while machine_queue:
            _, machine = heapq.heappop(machine_queue)
            # Make sure the machine is active i.e. isn't starting up/busy the next 5 seconds
            if machine.is_active(job.timestamp + self.MAX_QUEUE_TIME):
                machine.busy_till = max(machine.busy_till, job.timestamp) + job.elapsed
                self.info('job_executed_till %d %s %s' % (machine.busy_till, job.guid, machine.guid))
                break
        else:  # This means that there is no machine that can complete the job within 5 seconds
            machine_queue = []
            # Prioritize the machines by when they become available (incurring minimum penalty)
            for machine in self.machines[category]:
                heapq.heappush(machine_queue, (max(machine.busy_till, machine.active_from), machine))
            _, machine = heapq.heappop(machine_queue)
            if max(machine.busy_till, machine.active_from) - job.timestamp <= self.MAX_PENALTY_TIME:
                machine.busy_till = max(machine.busy_till, machine.active_from) + job.elapsed
                self.penalize(max(machine.busy_till, machine.active_from) - job.timestamp)
                self.info('job_executed_till_with_penalty %d %s %s' % (machine.busy_till, job.guid, machine.guid))
            else:
                self.info('no_machine_for %d %s' % (job.timestamp, job.guid))
                self.overwait = True

    def penalize(self, waiting_time):
        self.penalty += (waiting_time - 5) / float(40)

    def bill(self, machine, category):
        """ Computes the cost of a single virtual machine. """
        self.machines[category].remove(machine)
        when_stops = max(self.now, machine.busy_till)
        billing_start = machine.running_since
        billing_end = when_stops + machine.till_billing(when_stops)
        bill = int(math.ceil(float(billing_end - billing_start) / machine.BILLING_UNIT))
        self.billed += bill
        return bill

    def evaluate(self):
        for category in self.categories:
            while self.machines[category]:
                machine = self.machines[category][0]
                self.terminate(machine, category)
        return self.billed + self.penalty


def read_events(fd, legacy):
    if legacy:
        common = r'^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}) (?P<hour>\d\d):(?P<minute>\d\d):(?P<second>\d\d) '
        cmd_re = re.compile(common + r'(?P<cmd>[^ ]+) (?P<category>\w+)')
        job_re = re.compile(common + r'(?P<guid>[^ ]+) (?P<category>\w+) (?P<elapsed>\d+\.\d+)')
    else:
        common = r'^(?P<timestamp>\d{10}) '
        cmd_re = re.compile(common + r'(?P<cmd>[^ ]+) (?P<category>\w+)')
        job_re = re.compile(common + r'(?P<elapsed>\d+\.\d+) (?P<guid>[^ ]+) (?P<category>\w+)')
    while True:
        line = fd.readline()
        if not line:
            break
        m = job_re.match(line)
        if m:
            yield Job(m.groupdict(), legacy)
            continue
        m = cmd_re.match(line)
        if m:
            yield Command(m.groupdict(), legacy)


def set_logger():
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


def main(file, debug=False, legacy=False):
    if debug:
        set_logger()
    state = State(legacy)
    with open(file) as fd:
        for event in read_events(fd, legacy):
            state.receive(event)
            if state.overwait:
                break
    print state.evaluate()
