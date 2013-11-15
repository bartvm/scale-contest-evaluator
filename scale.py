import argparse
import heapq
import logging
import random
import scipy
from scipy import stats
import sys
import time
import uuid

### Basic settings and parameters
CATEGORIES = ['url', 'default', 'export']
MAX_QUEUE_TIME = 5
MAX_PENALTY_TIME = 120
BILLING_UNIT = 3600
MACHINE_INACTIVE = 120


### Base classes
class Event(object):
    def __init__(self, data_dict):
        self.timestamp = int(data_dict['timestamp'])

    def __cmp__(self, other):
        return cmp(self.timestamp, other.timestamp)


class Job(Event):
    def __init__(self, data_dict):
        super(Job, self).__init__(data_dict)
        self.category = data_dict['category']
        self.duration = float(data_dict['duration'])
        self.guid = data_dict['guid']


class Command(Event):
    def __init__(self, data_dict):
        super(Command, self).__init__(data_dict)
        self.category = data_dict['category']
        self.cmd = data_dict['cmd']


### The algorithm
class Scale(object):
    """The actual algorithm
    """
    pass


### Simulating input
class Simulation(object):
    """Simulates activity with fixed arrival rates, randomly drawing service
    times from a data set. Behaves like an iterator, returning Job objects
    at each call.
    """
    def __init__(self, arrival_rates):
        """Initiate this iterator.

        Takes a dictionary of arrival rates, {'url': 2.1, ...} and a dictionary
        of lists with service time samples: {'url': [1.232, 2.1232, ...], ...}
        """
        self.now = int(time.time()) - random.randint(0, 7 * 24 * 60 * 60)
        self.arrival_rates = arrival_rates

    def load_service_distribution(self, input):
        self.service_times = dict((category, []) for category in CATEGORIES)
        for job in parse_input(input):
            self.service_times[job.category].append(job.duration)

    def __iter__(self):
        return self

    def next(self):
        for category in CATEGORIES:
            number_of_arrivals = scipy.stats.poisson.rvs(
                self.arrival_rates[category]
            )
            for i in range(number_of_arrivals):
                service_time = random.choice(self.service_times[category])
                guid = str(uuid.uuid1())
                data_dict = dict(zip(('timestamp', 'duration', 'guid', 'category'),
                                     (self.now, service_time, guid, category)))
                return Job(data_dict)
            self.now += 1


### Scoring and measurement
class Evaluator(object):
    def __init__(self):
        self.now = 0
        self.billed = 0
        self.penalty = 0
        self.overwait = False
        self.jobs = dict((category, []) for category in CATEGORIES)
        self.machines = dict((category, []) for category in CATEGORIES)
        self.statistics = Statistics(self)

    def receive(self, event):
        if self.now != event.timestamp:
            self.statistics.step()
            self.jobs = dict((category, []) for category in CATEGORIES)
            self.now = event.timestamp
        if isinstance(event, Job):
            heapq.heappush(self.jobs[event.category], event)
            self.process_event(event)
        elif isinstance(event, Command):
            if event.cmd == 'launch':
                self.launch(Machine(event.timestamp, self), event.category)
            elif event.cmd == 'terminate':
                closest = heapq.nsmallest(1, self.machines[event.category])
                if closest:
                    self.terminate(closest[0], event.category)

    def launch(self, machine, category):
        heapq.heappush(self.machines[category], machine)

    def terminate(self, machine, category):
        machine.terminated = True
        self.bill(machine, category)

    def process_event(self, job):
        # Find machine closest to billing cycle with enough time to run job
        machine_priority = []
        for machine in self.machines[job.category]:
            priority = (machine.till_billing(max(machine.available_from,
                        job.timestamp)) - job.duration) % -3600
            heapq.heappush(machine_priority, (priority, machine))
        while machine_priority:
            _, machine = heapq.heappop(machine_priority)
            if machine.is_available(job.timestamp + MAX_QUEUE_TIME):
                machine.busy_till = max(machine.available_from,
                                        job.timestamp) + job.duration
                break
        # Minimize penalty by finding first machine available
        else:
            machine = min(self.machines[job.category],
                          key=lambda machine: machine.available_from)
            machine.busy_till = machine.available_from + job.duration
            self.penalize(machine.available_from - job.timestamp)
            if not machine.available_from - job.timestamp < MAX_PENALTY_TIME:
                self.overwait = True

    def penalize(self, waiting_time):
        self.penalty += (waiting_time - 5) / float(40)

    def bill(self, machine, category):
        self.machines[category].remove(machine)
        when_stops = max(self.now, machine.busy_till)
        self.billed += (when_stops - machine.running_since) / BILLING_UNIT + 1

    def evaluate(self):
        for category in self.categories:
            while self.machines[category]:
                machine = self.machines[category][0]
                self.terminate(machine, category)
        self.statistics.plot_waiting_time()
        if self.overwait:
            return 0
        else:
            return self.billed + self.penalty


class Machine(object):
    def __init__(self, booted, world):
        self.active_from = booted + MACHINE_INACTIVE
        self.busy_till = 0
        self.world = world
        self.terminated = False
        self.guid = str(uuid.uuid1())

    @property
    def running_since(self):
        return self.active_from - MACHINE_INACTIVE

    def till_billing(self, now):
        return abs((now - self.running_since) % -BILLING_UNIT)

    def is_available(self, now):
        return now >= self.active_from and self.busy_till <= now

    def available_from(self):
        return max(self.active_from, self.busy_till)


### Statistics
class Statistics(object):
    def __init__(self, world):
        self.world = world


### Program logic
def parse(f):
    while True:
        line = f.readline()
        if not line:
            break
        data_dict = dict(zip(('timestamp', 'duration', 'guid', 'category'),
                             line.split()))
        yield Job(data_dict)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Prezi Scale contest algorithm and simulation'
    )
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Turn on debugging')
    return parser.parse_known_args()


def set_logger():
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
