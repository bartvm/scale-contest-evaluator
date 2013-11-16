import argparse
import heapq
import logging
from matplotlib import pyplot as plt
import numpy
import random
import scipy
from scipy import stats
import sys
import thread
import time
import uuid

### Basic settings and parameters
CATEGORIES = ['url', 'default', 'export']
MAX_QUEUE_TIME = 5
MAX_PENALTY_TIME = 120
BILLING_UNIT = 3600
MACHINE_INACTIVE = 120


### Allow classes to log
class WithLog(object):  # This is just for logging
    log = logging.getLogger('scale')

    def info(self, *args):
        self.log.info(*args)


### Base classes
class Event(object):
    """Represents either a job or a command.
    """
    def __init__(self, data_dict):
        self.timestamp = int(data_dict['timestamp'])

    def __cmp__(self, other):
        """Compare events by their timespan so that they can be sorted.
        """
        return cmp(self.timestamp, other.timestamp)


class Job(Event):
    def __init__(self, data_dict):
        super(Job, self).__init__(data_dict)
        self.category = data_dict['category']
        self.duration = float(data_dict['duration'])
        self.guid = data_dict['guid']

    def __str__(self):
        return ' '.join(map(str, [self.timestamp, self.duration,
                                  self.guid, self.category]))


class Command(Event):
    def __init__(self, data_dict):
        super(Command, self).__init__(data_dict)
        self.category = data_dict['category']
        self.cmd = data_dict['cmd']

    def __str__(self):
        return ' '.join(map(str, [self.timestamp, self.cmd, self.category]))


### The algorithm
class Scale(object):
    """The actual algorithm

    TODO: Implement everything.

    TODO: The algorithm needs to keep track of the jobs, and estimate activity
    using a moving average (should probably use deque and numpy). This can
    be combined with the extracted 24-hour dynamics to forecast activity.

    TODO: This needs to be used then to estimate the number of machines
    required half an hour from now. To get this estimate we need to use
    approximations (see Wikipedia M/G/k queue) or we need to use KDE to
    estimate the waiting time distribution for specific arrival rates
    and servers.
    """
    def __init__(self):
        self.now = 0

    def startup(self, event):
        """This gets called only by the very first event.

        TODO: Maybe look at the day (Saturday, Sunday, holiday?) and determine
        the number of servers we're going to start with.
        """
        starting_time = event.timestamp - MACHINE_INACTIVE
        for category in CATEGORIES:
            data_dict = dict(zip(('timestamp', 'category', 'cmd'),
                                 (starting_time, category, 'launch')))
            self.events.extend([Command(data_dict)] * 40)

    def receive(self, event):
        """Receives an event and does algorithmic magic.

        TODO: Everything (see class description)
        """
        self.events = [event]
        if not self.now:
            self.startup(event)
        self.now = event.timestamp
        return sorted(self.events)


### Scoring and measurement
class Evaluator(object):
    """Simulate the result of a set of events and commands. Returns the final
    number of machine hours used plus the penalty incurred.

    TODO: Might need to test it again to see if it does
    exactly what it should do.
    """
    def __init__(self):
        self.now = 0
        self.billed = 0
        self.penalty = 0
        self.overwait = False
        self.jobs = dict((category, []) for category in CATEGORIES)
        self.machines = dict((category, []) for category in CATEGORIES)
        self.statistics = Statistics(self)

    def receive(self, event):
        """Processes each event and also updates the time and
        asks for statistics.
        """
        if self.now != event.timestamp:
            self.now = event.timestamp
            if self.now and isinstance(event, Job):
                self.statistics.step()
                self.jobs = dict((category, []) for category in CATEGORIES)
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
        """Shuts the machine down by removing it from the list and asking
        for the bill.
        """
        self.machines[category].remove(machine)
        self.bill(machine)

    def process_event(self, job):
        """Sends each event to a virtual machine. First tries to find a machine
        close to the billing cycle, otherwise the machine that is available
        earliest.
        """
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
                job.waiting_time = max(machine.available_from,
                                       job.timestamp) - job.timestamp
                break
        # Minimize penalty by finding first machine available
        else:
            machine = min(self.machines[job.category],
                          key=lambda machine: machine.available_from)
            machine.busy_till = machine.available_from + job.duration
            job.waiting_time = machine.available_from - job.timestamp
            self.penalize(job.waiting_time)
            if not machine.available_from - job.timestamp < MAX_PENALTY_TIME:
                self.overwait = True

    def penalize(self, waiting_time):
        self.penalty += (waiting_time - 5) / float(40)

    def bill(self, machine):
        """Charges the machine hours and adds it to the bill.
        """
        when_stops = max(self.now, machine.busy_till)
        self.billed += (when_stops - machine.running_since) / BILLING_UNIT + 1

    def evaluate(self):
        """Shuts down all the machines that are still one and adds up the
        bill and the penalties. If a process had to wait more than 2 minutes
        the score is 0.
        """
        for category in CATEGORIES:
            while self.machines[category]:
                machine = self.machines[category][0]
                self.terminate(machine, category)
        if self.overwait:
            return 0
        else:
            return self.billed + self.penalty


class Machine(object):
    """Represents a virtual machine.
    """
    def __init__(self, booted, world):
        self.active_from = booted + MACHINE_INACTIVE
        self.busy_till = 0
        self.world = world

    @property
    def running_since(self):
        return self.active_from - MACHINE_INACTIVE

    @property
    def available_from(self):
        return max(self.active_from, self.busy_till)

    def till_billing(self, now):
        return abs((now - self.running_since) % -BILLING_UNIT)

    def is_available(self, now):
        return now >= self.active_from and self.busy_till <= now


### Statistics
class Statistics(WithLog):
    UPDATE_INTERVAL = 300
    """Displays statistics every second about the state of the model.

    TODO: Plot penalties, waiting time distributions, activity, etc.
    """
    def __init__(self, world):
        self.world = world
        self.beginning = self.world.now
        self.figure, self.axes = plt.subplots(2, 2)
        self.axes = self.axes.flatten()
        self.arrivals = dict((category, self.axes[0].plot([], [])[0])
                             for category in CATEGORIES)

    def add_point(self, line, new_data):
        line.set_xdata(numpy.append(line.get_xdata(), new_data[0]))
        line.set_ydata(numpy.append(line.get_ydata(), new_data[1]))
        self.update_plots()

    def update_plots(self):
        for axis in self.axes:
            axis.relim()
            axis.autoscale_view()
        if not self.world.now % self.UPDATE_INTERVAL:
            plt.draw()

    def step(self):
        for category in CATEGORIES:
            self.add_point(self.arrivals[category],
                           (self.world.now, len(self.world.jobs[category])))


### Simulating input
class Simulation(object):
    """Simulates activity with fixed arrival rates, randomly drawing service
    times from a data set. Behaves like an iterator, returning Job objects
    at each call.
    """
    def __init__(self, time_frame, arrival_rates):
        """Initiate this iterator.

        Takes a dictionary of arrival rates, {'url': 2.1, ...}.
        """
        self.now = int(time.time()) - random.randint(0, 7 * 24 * 60 * 60)
        self.start = self.now
        self.end = self.start + time_frame
        self.arrival_rates = arrival_rates

    def load_service_distribution(self, f):
        """Loads service time distributions from a log file.
        """
        self.service_times = dict((category, []) for category in CATEGORIES)
        with open(f) as f:
            for job in parse(f):
                self.service_times[job.category].append(job.duration)

    def __iter__(self):
        return self

    def generator(self):
        """Generator that creates random jobs.
        """
        while self.now < self.end:
            jobs = []
            for category in CATEGORIES:
                number_of_arrivals = scipy.stats.poisson.rvs(
                    self.arrival_rates[category]
                )
                for i in range(number_of_arrivals):
                    service_time = random.choice(self.service_times[category])
                    guid = str(uuid.uuid1())
                    data_dict = dict(zip(('timestamp', 'duration', 'guid',
                                          'category'), (self.now, service_time,
                                                        guid, category)))
                    jobs.append(Job(data_dict))
            random.shuffle(jobs)
            for job in jobs:
                yield job
            self.now += 1


### Program logic
def main(file=None, debug=False):
    args, rest = parse_arguments()
    if file:
        rest = [file]
    if args.debug or debug:
        set_logger()
    with open(rest[0]) if rest else sys.stdin as f:
        scale = Scale()
        evaluator = Evaluator()
        jobs = parse(f)
        L = []
        thread.start_new_thread(input_thread, (L,))
        for job in jobs:
            for event in scale.receive(job):
                evaluator.receive(event)
            if L:
                break
        print 'Final score: ' + str(evaluator.evaluate()) + \
              '\nPlease press enter.'


def input_thread(L):
    raw_input('Press enter to interrupt...\n')
    L.append(None)


def parse(f):
    """Parses a file or input and returns jobs.
    """
    f.readline()
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

if __name__ == '__main__':
    main()
