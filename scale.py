import collections
import heapq
import itertools
import logging
import matplotlib
from matplotlib import pyplot as plt
import numpy
import random
import scipy.stats
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
# These are just settings that make the plots pretty.
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
    'size': 10
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
    'edgecolor': '0.5',
    'subplot.left': '0.05',
    'subplot.right': '0.95',
    'subplot.top': '0.95',
    'subplot.bottom': '0.05'
}
matplotlib.rc('figure', **figure)


### Logging
class WithLog(object):
    """This is a logging class. To allow a class to log information for
    debugging purposes, subclass this class so e.g. Evaluator(WithLog) and
    then use e.g. self.info("This is a debugging message") to log information
    for debugging purposes.

    Attributes:
        log: The logging process with the name 'scale'
    """
    log = logging.getLogger('scale')

    def info(self, *args):
        """This function logs the information using this format:
        http://docs.python.org/2/library/logging.html#logging.debug
        """
        self.log.info(*args)


### Base classes
class Event(object):
    """This is the base class of an event (jobs and commands).

    Attributes:
        timestamp: The time of this event in the form a UNIX timestamp, which
            is a 10-digit number. Accurate to 1 second.
    """
    def __init__(self, data_dict):
        self.timestamp = int(data_dict['timestamp'])

    def __cmp__(self, other):
        """Compare events by their timestamp. This means that we can easily
        call sort() to sort a list of Events.

        Returns:
            <0 if smaller than the other, >0 if bigger
        """
        return cmp(self.timestamp, other.timestamp)


class Job(Event):
    """A single Job that needs to be processed. This is a subclass of the Event
    class, so it also has a timestamp and can be sorted.

    Attributes:
        category: The category of the job, so url, default or export
        duration: The service time of this job e.g. 2.73 seconds
        guid: A unique identifier of the job
            e.g. 9ea24acd-cee3-4248-bf43-fb33e7e9ac4b, not used
        waiting_time: The time this job had to wait before being allocated,
            set to 0 initially but is changed during the simulation
    """
    def __init__(self, data_dict):
        """Initiates a Job instance from the data given.

        Args:
            data_dict: A dictionary with the properties of this job as strings
                e.g. {'timestamp': '1378072800', 'duration': '2.1', ...}
        """
        super(Job, self).__init__(data_dict)
        self.category = data_dict['category']
        self.duration = float(data_dict['duration'])
        self.guid = data_dict['guid']
        self.waiting_time = 0

    def __str__(self):
        """The string representation of the job, so if the job gets printed
        using e.g. print Job().

        Returns:
            A string that represents the job the same way as the log so e.g.
                '1378072800 2.133 9ea24acd-cee3-4248-bf43-fb33e7e9ac4b url'
        """
        return ' '.join(map(str, [self.timestamp, self.duration,
                                  self.guid, self.category]))


class Command(Event):
    """A command to launch or terminate a machine in a certain category. This
    is a subclass of the Event class, so it has a timestamp and can be sorted.

    Attributes:
        category: The category in which a machine should be launched/terminated
        cmd: The command to send, so either 'launch' or 'terminate'
    """
    def __init__(self, data_dict):
        super(Command, self).__init__(data_dict)
        self.category = data_dict['category']
        self.cmd = data_dict['cmd']

    def __str__(self):
        """The string representation of the command as Prezi wants it.

        Returns:
            A string representing the command in the Prezi way so e.g.
                '1378072800 launch url'
        """
        return ' '.join(map(str, [self.timestamp, self.cmd, self.category]))


### Algorithms (process jobs and determine launch/terminate commands)
class Fixed(object):
    """This class is an 'algorithm' purely for testing: it starts a fixed
    number of computers in the beginning, and does nothing else.

    Attributes:
        now: The current time, based on the last job that came in
        events: A list of events that are to be sent back at this timestep
        startup_amount: The number of servers to start for each category
            in the form of a dictionary: {'url': 5, 'default': 10, ...}
    """
    def __init__(self, startup_amount):
        """Initiates the class.
        """
        self.now = 0
        self.events = []
        self.startup_amount = startup_amount

    def startup(self, job):
        """This function starts a given number of machines in the beginning.

        Args:
            job: A Job instance of the very first job
        """
        startup_time = job.timestamp - MACHINE_INACTIVE
        for category in CATEGORIES:
            data_dict = dict(zip(('timestamp', 'category', 'cmd'),
                                 (startup_time, category, 'launch')))
            self.events.extend(
                [Command(data_dict)] * self.startup_amount[category]
            )

    def receive(self, job):
        """This function simply returns the event input, except at the first
        step, when it boots a fixed number of machines.

        Args:
            job: A Job instance of the last job to come in (must be in order)

        Returns:
            A sorted list of Event instances (Jobs and Events)
        """
        self.events = [job]
        if not self.now:
            self.startup(job)
        self.now = job.timestamp
        return sorted(self.events)


class Scale(object):
    """The actual algorithm

    TODO: Everything, basically.

    TODO: The algorithm needs to keep track of the jobs, and estimate activity
    using a moving average (should probably use deque and numpy). This can
    be combined with the extracted 24-hour dynamics to forecast activity.

    TODO: This can then be used to estimate the number of machines
    required half an hour from now. To get this estimate we need to use
    approximations (see Wikipedia M/G/k queue) or we need to use KDE to
    estimate the waiting time distribution for specific arrival rates
    and servers.

    Attributes:
        now: The current time, based on the last job that came in
        events: A list of events that are to be sent back at this timestep
        startup_amount: The number of servers to start for each category
            in the form of a dictionary: {'url': 5, 'default': 10, ...}
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

        TODO: Look at the time and the day (Saturday, Sunday, holiday?)
        and determine the number of servers we're going to start with.

        Args:
            job: A Job instance of the very first job
        """
        startup_time = event.timestamp - MACHINE_INACTIVE
        startup_amount = {
            'url': 5,
            'default': 40,
            'export': 40
        }
        for category in CATEGORIES:
            data_dict = dict(zip(('timestamp', 'category', 'cmd'),
                                 (startup_time, category, 'launch')))
            self.events.extend([Command(data_dict)] * startup_amount[category])

    def receive(self, event):
        """Receives an event and does algorithmic magic.

        TODO: Everything (this is where the algorithm should do its work)

        NOTE: If the algorithm decides to launch or terminate a VM at a later
        point in time, it should be stored temporarily and only output when
        a job arrives with the same or a later timestamp.

        Args:
            job: A Job instance of the last job to come in (must be in order)

        Returns:
            A sorted list of Event instances (Jobs and Events)
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

    Attributes:
        now: The current time as a UNIX timestamp (in seconds)
        billed: The number of machine hours charged
        penalty: The amount of penalty incurred
        overwait: A boolean indicating if a job has had to wait more than
            a minute, after which the algorithm is disqualified.
        jobs: A dictionary with lists of jobs that need to be completed
            at this step in time (cleared every second)
        machines: A dictionary with a heap que of machines that are
            currently active.
        scheduling: The scheduling algorithm that should be used:
            'prezi' allocates jobs to the machine closest to billing
            'uniform' allocates jobs to a machine at random
            'fifo' allocates each job to the next machine available
        statistics: A Statistics class that saves data and produces plots
    """
    def __init__(self, scheduling='prezi'):
        """Initiates the class with prezi as the default scheduling algorithm.

        Args:
            scheduling: One of the three scheduling
                algorithms (see class description)
        """
        self.now = 0
        self.billed = 0
        self.penalty = 0
        self.overwait = False
        self.jobs = dict((category, []) for category in CATEGORIES)
        self.machines = dict((category, []) for category in CATEGORIES)

        scheduling_algorithms = ['prezi', 'fifo', 'uniform']
        if not scheduling in scheduling_algorithms:
            raise ValueError('Scheduling algorithm must be one of ' +
                             str(scheduling_algorithms))
        self.scheduling = scheduling

        self._observers = []

    def bind_to(self, callback):
        """This allows other classes to bind itself as a dependant, and updates
        them every time the time stamp changes (observer pattern)

        Args:
            callback: The argument-less function that should be called at
                the end of every time step e.g. Statistics.step()
        """
        self._observers.append(callback)

    def receive(self, event):
        """Processes each event and also updates the time and
        asks for statistics at each new timestamp.

        Args:
            event: An Event instance (either Job or Command)
        """
        if self.now != event.timestamp:
            self.now = event.timestamp
            for callback in self._observers:
                callback()
            self.jobs = dict((category, []) for category in CATEGORIES)
        if isinstance(event, Job):
            heapq.heappush(self.jobs[event.category], event)
            self.process_event(event)
        elif isinstance(event, Command):
            if event.cmd == 'launch':
                self.launch(Machine(event.timestamp), event.category)
            elif event.cmd == 'terminate':
                closest = heapq.nsmallest(1, self.machines[event.category])
                if closest:
                    self.terminate(closest[0], event.category)

    def launch(self, machine, category):
        """Launches a new machine.

        Args:
            machine: A Machine instance
            category: The category to which to add this machine
        """
        heapq.heappush(self.machines[category], machine)

    def terminate(self, machine, category):
        """Shuts the machine down by removing it from the list and asking
        for the bill.

        Args:
            machine: A Machine instance
            category: The category from which to remove this machine
        """
        self.machines[category].remove(machine)
        self.bill(machine)

    def process_event(self, job):
        """Sends each event to a VM according to a specific scheduling
        algorithm. The busy time of that VM is then updated and the waiting
        time of the job calculated.

        Args:
            job: A Job instance that needs to be processed
        """
        if self.scheduling == 'uniform':
            machine = random.choice(self.machines[job.category])
            job.waiting_time = \
                max(machine.available_from, job.timestamp) - job.timestamp
            machine.busy_till = \
                max(machine.available_from, job.timestamp) + job.duration
        elif self.scheduling == 'fifo':
            machine = min(self.machines[job.category],
                          key=lambda machine: machine.available_from)
            job.waiting_time = \
                max(machine.available_from, job.timestamp) - job.timestamp
            machine.busy_till = \
                max(machine.available_from, job.timestamp) + job.duration
        else:
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
                if not (machine.available_from - job.timestamp
                        < MAX_PENALTY_TIME):
                    self.overwait = True

    def penalize(self, waiting_time):
        """Calculates the penalty incurred.

        Args:
            waiting_time: The waiting time of this job
        """
        assert waiting_time >= 5, "No penalty for %r seconds" % waiting_time
        self.penalty += (waiting_time - 5) / float(40)

    def bill(self, machine):
        """Charges the machine hours and adds it to the bill.

        Args:
            machine: The machine that is being shut down and billed.
        """
        when_stops = max(self.now, machine.busy_till)
        self.billed += (when_stops - machine.running_since) / BILLING_UNIT + 1

    def evaluate(self):
        """Shuts down all the machines that are still one and adds up the
        bill and the penalties. If a process had to wait more than 2 minutes
        the score is 0.

        Returns:
            0 if one of the jobs had to wait more than 2 minutes
            otherwise returns the sum of the bill and penalty
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

    Attributes:
        active_from: The time at which the machine has completed booting
            up and is ready to process jobs.
        busy_till: The time at which the machine finishes processing all the
            the jobs that have been allocated to it.
    """
    def __init__(self, booted):
        """Initiates the instance and sets the active_from time
        """
        self.active_from = booted + MACHINE_INACTIVE
        self.busy_till = 0

    @property
    def running_since(self):
        """Returns:
            The time from which this machine has been running, mostly
            for billing purposes.
        """
        return self.active_from - MACHINE_INACTIVE

    @property
    def available_from(self):
        """Returns:
            The first moment at which this machine can take on a next job,
            which is after it finished booting, and completed all its jobs.
        """
        return max(self.active_from, self.busy_till)

    def till_billing(self, now):
        """Args:
            now: A timestamp

        Returns:
            The amount of time remaining till the machine gets billed again at
            the given time 'now'
        """
        return abs((now - self.running_since) % -BILLING_UNIT)

    def is_available(self, now):
        """Returns:
            Whether the machine is available to process a new job at the given
            moment 'job' i.e. has it finished booting and no other jobs
        """
        return now >= self.active_from and self.busy_till <= now


### Statistics
class Statistics(WithLog):
    """Displays statistics every second about the state of the model.

    TODO: Code needs to be cleaned up a bit so that the axes of all the plots
    are sensible, and so that different plots can easily be moved around.

    TODO: Labels for the graphs need to show the time (in hours) + day

    TODO: Plot penalties incurred, machine hours billed, etc.

    Attributes:
        UPDATE_INTERVAL: The interval at which to update the plot. This is also
            the interval over which averages will be taken.
        world: An Evaluator instance that is processing the Events
        beginning: The timestamp at which the simulation began
        figure: The pyplot figure
        axes: A list of 4 axes with the different plots
        arrivals: A dictionary of queues which store the number of arrivals
            per second throughout the past update cycle, so that the mean
            arrival rate can be calculated
        arrival_plots: pyplot lines of the average arrival rate for each
            plotting cycle, to which we add a new point each cycle
        waiting_times: A dictionary of queues which store the waiting times
            from the past update cycle, so that the mean waiting time and
            waiting time distribution can be plotted
        waiting_time_plots: pyplot lines of the average waiting time for
            each plotting cycle, to which we add a new point each cycle
        machine_plots: pyplot lines of the number of active machines at
            the end of each update cycle
    """
    UPDATE_INTERVAL = 900

    def __init__(self, world):
        """Initializes the variables in which we will be storing measurements.
        Also turns on pyplot's interactive mode.

        WARNING: pyplot's interactive mode might have issues on Windows. Be
        sure to use IPython and use %pylab before trying to plot anything.

        Args:
            world: An Evaluator instance for which information needs to be
                measured and plotted.
        """
        plt.ion()
        self.world = world
        self.world.bind_to(self.step)
        self.beginning = 0
        self.figure, self.axes = plt.subplots(2, 2)
        self.suptitle = plt.suptitle(
            'Schudeling algorithm: %s, Algorithm failed: %s'
            % (self.world.scheduling, self.world.overwait)
        )
        self.axes = self.axes.flatten()
        self.arrivals = dict((category, collections.deque())
                             for category in CATEGORIES)
        self.arrival_plots = dict((category, self.axes[0].plot([], [])[0])
                                  for category in CATEGORIES)
        self.waiting_times = dict((category, collections.deque())
                                  for category in CATEGORIES)
        self.waiting_time_plots = dict((category, self.axes[2].plot([], [])[0])
                                       for category in CATEGORIES)
        self.machine_plots = dict((category, self.axes[1].plot([], [])[0])
                                  for category in CATEGORIES)
        self.next_update = 0

    def add_point(self, line, new_data):
        """Adds a single point to a pyplot line.

        Args:
            line: A pyplot line
            new_data: An (x, y)/[x, y] list or tuple with a new point to add
                to this line
        """
        line.set_xdata(numpy.append(line.get_xdata(), new_data[0]))
        line.set_ydata(numpy.append(line.get_ydata(), new_data[1]))

    def update_plots(self):
        """Rescales the axes, sets limits, and then calls draw()
        to display the new plots.
        """
        for axis in self.axes:
            axis.relim()
            axis.autoscale_view()
        self.axes[3].set_xlim([0, 60])
        self.axes[3].set_ylim([0, 1000])
        self.axes[3].plot([5, 5], [0, 1000], c='#A60628', ls=':')
        plt.draw()

    def update(self):
        """This function gets called once each update cycle and adds
        points to the plots, creates a new histogram for the waiting times
        and then clears the data that has been saved over the past cycle.

        Then calls update_plots() to display all these changes.
        """
        for category in CATEGORIES:
            self.add_point(self.arrival_plots[category],
                           (self.world.now,
                            numpy.sum(self.arrivals[category]) /
                            float(self.UPDATE_INTERVAL)))
            self.add_point(self.machine_plots[category],
                           (self.world.now,
                            len(self.world.machines[category])))
            self.add_point(self.waiting_time_plots[category],
                           (self.world.now,
                            numpy.mean(self.waiting_times[category])))
        self.axes[3].clear()
        self.axes[3].hist(
            list(itertools.chain(*self.waiting_times.itervalues())),
            range=(0, 60), bins=60
        )
        for queue in self.waiting_times.itervalues():
            queue.clear()
        for queue in self.arrivals.itervalues():
            queue.clear()
        self.update_plots()
        if self.world.overwait:
            self.suptitle.set_text(
                'Schudeling algorithm: %s, Algorithm failed: %s'
                % (self.world.scheduling, self.world.overwait)
            )

    def step(self):
        """This function gets called at every new time step. At this point
        we look at the Evaluator's .machines and .jobs attributes to look
        at the jobs that have been processed during this second, and the
        machines that are active. Information such as waiting times,
        and the number of jobs that arrived are saved every time step.

        Also calls the update() function every UPDATE_INTERVAL steps.
        """
        if not self.beginning:
            self.beginning = self.world.now + MACHINE_INACTIVE
            self.next_update = self.beginning + self.UPDATE_INTERVAL
        # Every second
        for category in CATEGORIES:
            self.arrivals[category].append(
                len(self.world.jobs[category])
            )
            for job in self.world.jobs[category]:
                self.waiting_times[category].append(job.waiting_time)
        # Every update interval
        if self.world.now >= self.next_update:
            self.next_update = self.next_update + self.UPDATE_INTERVAL
            self.update()


### Simulating input
class Simulation(object):
    """Simulates activity with fixed arrival rates, randomly drawing service
    times from a data set. Behaves like an iterator, returning Job objects
    at each call.

    Attributes:
        now: A random time taken between now and a week ago (so it can be
            any day of the week or time)
        start: The time at which the simulation started
        end: The time at which the simulation will end
        arrival_rates: The fixed arrival rates for each category (see __init())
    """
    def __init__(self, time_frame, arrival_rates):
        """Initiate this iterator.

        Args:
            time_frame: The length of the simulation in seconds
            arrival_rates: A dictionary of fixed arrival rates so e.g.
                {'url': 2.1, ...}.
        """
        self.now = int(time.time()) - random.randint(0, 7 * 24 * 60 * 60)
        self.start = self.now
        self.end = self.start + time_frame
        self.arrival_rates = arrival_rates

    def load_service_distribution(self, file):
        """Loads a log file and saves all the service times.

        TODO: Maybe allowe a list of file names so that service times can
        be drawn from a larger sample set.

        Args:
            file: A file name of the log file from which we load the
                service_time distribution
        """
        self.service_times = dict((category, []) for category in CATEGORIES)
        with open(file) as f:
            for job in parse(f):
                self.service_times[job.category].append(job.duration)

    def generator(self):
        """Generator that creates random jobs. The number of jobs is Poisson
        distributed according to the values in self.arrival_rates. The service
        time of each job is randomly chosen from all the service times
        in a log file.

        Returns:
            Yields a Job instance at every step.
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
def main(file, debug=False, allow_abort=True):
    """This a function that takes a log file and reads it, sends it to the
    Scale algorithm to process it, and takes the output of the algorithm
    and sends it to the Evaluator the calculate the score.

    TODO: Easily turn graphs on and off

    TODO: Easily run the algorithm on simulated data.

    TODO: Easily save recorde data to files/pass scores to GA.

    Args:
        file: The name of the log file to read
        debug: Turns on debugging
    """
    if debug:
        set_logger()
    with open(file) as f:
        scale = Scale()
        evaluator = Evaluator()
        Statistics(evaluator)
        jobs = parse(f)
        abort = []
        if allow_abort:
            thread.start_new_thread(input_thread, (abort,))
        for job in jobs:
            for event in scale.receive(job):
                evaluator.receive(event)
            if abort:
                break
        print 'Complete. Please press enter.'
        return evaluator.evaluate()


def input_thread(abort):
    """This method can be run as a separate thread, waiting for any key
    to be pressed.

    Args:
        abort: An empty list (evaluating to False) to which we append a value
            so that it evaluates to True when someone pressed enter.
    """
    raw_input('Press enter to abort.\n')
    abort.append(None)


def parse(f):
    """A generator that parses a file and returns Job instances.

    Args:
        f: A file object that can be parsed

    Returns:
        A Job instance for each line.
    """
    f.readline()
    while True:
        line = f.readline()
        if not line:
            break
        data_dict = dict(zip(('timestamp', 'duration', 'guid', 'category'),
                             line.split()))
        yield Job(data_dict)


def set_logger():
    """Turns on logging.
    """
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
