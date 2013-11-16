import argparse
import collections
import heapq
import itertools
import logging
import matplotlib
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

### Plotting settings
lines = {
    'linewidth': 1.0,
    'antialiased': True
}
matplotlib.rc('lines', **lines)

patch = {
    'linewidth': 0.5,
    'facecolor': '#348ABD',
    'edgecolor': '#eeeeee',
    'antialiased': True
}
matplotlib.rc('patch', **patch)

font = {
    'family': 'monospace',
    'size': 10,
    'monospace': 'Andale Mono, Nimbus Mono L, Courier New, ' +
                 'Courier, Fixed, Terminal, monospace'
}
matplotlib.rc('font', **font)

axes = {
    'facecolor': '#eeeeee',
    'edgecolor': '#bcbcbc',
    'linewidth': 1,
    'grid': True,
    'titlesize': 'x-large',
    'labelsize': 'large',
    'labelcolor': '#555555',
    'axisbelow': True,
    'color_cycle': ['348ABD', '#7A68A6', '#A60628', '#467821',
                    '#CF4457', '#188487', '#E24A33']
}
matplotlib.rc('axes', **axes)

figure = {
    'figsize': '14, 10',
    'facecolor': '0.85',
    'edgecolor': '0.5',
    'subplot.left': '0.05',
    'subplot.right': '0.95',
    'subplot.top': '0.95',
    'subplot.bottom': '0.05'
}
matplotlib.rc('figure', **figure)


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
        self.waiting_time = 0

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

    TODO: Everything, basically.

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
        """TODO: This is where the algorithm should store the past data it
        needs to do its magic
        """
        self.now = 0
        self.events = []

    def startup(self, event):
        """This gets called only by the very first event and makes sure that
        servers are started up by the time the first jobs come in.

        TODO: Maybe look at the day (Saturday, Sunday, holiday?) and determine
        the number of servers we're going to start with.
        """
        startup_time = event.timestamp - MACHINE_INACTIVE
        startup_number = {
            'url': 5,
            'default': 40,
            'export': 40
        }
        for category in CATEGORIES:
            data_dict = dict(zip(('timestamp', 'category', 'cmd'),
                                 (startup_time, category, 'launch')))
            self.events.extend([Command(data_dict)] * startup_number[category])

    def receive(self, event):
        """Receives an event and does algorithmic magic.

        Adds the machine commands (launch, terminate) to the self.events list.

        TODO: Everything (this is where the algorithm should do its work)
        """
        self.events = [event]
        if not self.now:
            self.startup(event)
        self.now = event.timestamp

        # This is where the algorithm should go

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
        """Launches a new machine.
        """
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
                job.waiting_time = max(machine.available_from,
                                       job.timestamp) - job.timestamp
                machine.busy_till = max(machine.available_from,
                                        job.timestamp) + job.duration
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
        """Calculates the penalty incurred.
        """
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
    UPDATE_INTERVAL = 900
    """Displays statistics every second about the state of the model.

    TODO: Code needs to be cleaned up a bit so that the axes of all the plots
    are sensible, and so that different plots can easily be moved around.

    TODO: Labels for the graphs need to show the time (in hours) + day

    TODO: Plot penalties incurred, machine hours billed, etc.
    """
    def __init__(self, world):
        self.world = world
        self.beginning = 0
        plt.ion()
        self.figure, self.axes = plt.subplots(2, 2)
        self.axes = self.axes.flatten()
        self.arrivals = dict((category,
                              collections.deque(maxlen=self.UPDATE_INTERVAL))
                             for category in CATEGORIES)
        self.waiting_times = dict((category, collections.deque())
                                  for category in CATEGORIES)
        self.durations = dict((category, collections.deque())
                              for category in CATEGORIES)
        self.arrival_plots = dict((category, self.axes[0].plot([], [])[0])
                                  for category in CATEGORIES)
        self.machine_plots = dict((category, self.axes[1].plot([], [])[0])
                                  for category in CATEGORIES)
        self.waiting_time_plots = dict((category, self.axes[2].plot([], [])[0])
                                       for category in CATEGORIES)

    def add_point(self, line, new_data):
        line.set_xdata(numpy.append(line.get_xdata(), new_data[0]))
        line.set_ydata(numpy.append(line.get_ydata(), new_data[1]))

    def update_plots(self):
        for axis in self.axes:
            axis.relim()
            axis.autoscale_view()
        self.axes[3].set_xlim([0, 120])
        self.axes[3].set_ylim([0, 1000])
        plt.draw()

    def step(self):
        if not self.beginning:
            self.beginning = self.world.now
        # Every second
        for category in CATEGORIES:
            self.arrivals[category].append(
                len(self.world.jobs[category])
            )
            for job in self.world.jobs[category]:
                self.waiting_times[category].append(job.waiting_time)
            for job in self.world.jobs[category]:
                self.durations[category].append(job.duration)
        # Every update interval
        if not (self.world.now - self.beginning) % self.UPDATE_INTERVAL \
           and (self.world.now - self.beginning):
            for category in CATEGORIES:
                self.add_point(self.arrival_plots[category],
                               (self.world.now,
                                numpy.mean(self.arrivals[category])))
                self.add_point(self.machine_plots[category],
                               (self.world.now,
                                len(self.world.machines[category])))
                self.add_point(self.waiting_time_plots[category],
                               (self.world.now,
                                numpy.mean(self.waiting_times[category])))
            self.axes[3].clear()
            self.axes[3].hist(list(itertools.chain(*self.waiting_times.itervalues())), range=(0, 120), bins=120)
            for queue in self.waiting_times.itervalues():
                queue.clear()
            for queue in self.durations.itervalues():
                queue.clear()
            self.update_plots()


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
    """Runs the algorithm and passes it to the simulation.

    TODO: Easily turn graphs on and off

    TODO: Easily run the algorithm on simulated data.

    TODO: Easily save recorde data to files/pass scores to GA.
    """
    args, rest = parse_arguments()
    if file:
        rest = [file]
    if args.debug or debug:
        set_logger()
    with open(rest[0]) if rest else sys.stdin as f:
        scale = Scale()
        evaluator = Evaluator()
        jobs = parse(f)
        abort = []
        thread.start_new_thread(input_thread, (abort,))
        for job in jobs:
            for event in scale.receive(job):
                evaluator.receive(event)
            if abort:
                break
        print 'Final score: ' + str(evaluator.evaluate()) + \
              '\nPlease press enter.'


def input_thread(abort):
    """This thread runs separately in order to respond to any keypress.
    """
    raw_input('Press enter to interrupt...\n')
    abort.append(None)


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
