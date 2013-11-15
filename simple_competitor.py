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
import sys
import re
# Head ends here
# ----------------------------------------------------------------------
# | START OF CONTESTANT CODE (edit the following part)                 |
# ----------------------------------------------------------------------


def on_startup(startup_time, number_of_jobs=None):
    """ This is called after the first event has been read. """
    # The following example would launch 100 VMs for each queue.
    for i in range(40):
        for queue in ['default', 'url', 'export']:
            server_command(startup_time, 'launch', queue)
    # server_command(startup_time, 'launch', 'default')
    # server_command(startup_time + 1, 'terminate', 'default')


def on_shutdown(shutdown_time):
    """ This is called after the last event has been read. """
    # The following example would terminate 100 VMs for each queue.
    for i in range(40):
        for queue in ['default', 'url', 'export']:
            server_command(shutdown_time, 'terminate', queue)
    # pass


def on_event(current_event):
    """ This is called on each event read (including the first and last events).
    Add your super-sophisticated scaling algorithm here. """
    pass


# ----------------------------------------------------------------------
# | END OF CONTESTANT CODE (you probably don't need to edit from here) |
# ----------------------------------------------------------------------
# Tail starts here
def parse_line(line, legacy):
    if line:
        if legacy:
            fields = dict(zip(('date', 'time', 'queue', 'duration'), line.split()))
            datetime_re = re.compile(r'^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}) (?P<hour>\d\d):(?P<minute>\d\d):(?P<second>\d\d)')
            datetime_fields = datetime_re.match(fields['date'] + ' ' + fields['time']).groupdict()
            return dict(fields.items() + datetime_fields.items())
        else:
            return dict(zip(('start', 'duration', 'id', 'queue'), line.split()))


def server_command(timestamp, command, queue):
    sys.stdout.write('%s %s %s\n' % (timestamp, command, queue))


def main(legacy=False):
    with sys.stdin as f:
        if not legacy:
            number_of_jobs = int(f.readline().strip())
        # The first line of input is the number of jobs
        # which will follow
        line = f.readline()
        first_event = last_event = parse_line(line, legacy)
        # It takes two minutes for a virtual machine to boot,
        # so we run on_startup() 2 * 60 = 120 seconds before
        # the first conversion job arrives so we have machines
        # ready to process jobs by that point.
        on_startup(int(first_event['start']) - 120)
        while line:
            last_event = parse_line(line, legacy)
            # React to the new job by optionally
            # launching or terminating instances.
            on_event(last_event)
            # Write each line to STDOUT after it has been acted on.
            sys.stdout.write(line)
            line = f.readline()
        on_shutdown(last_event['start'])

if __name__ == '__main__':
    main()
