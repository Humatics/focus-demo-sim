"""
This module implements logic used to query historic InfluxDB data over a certain date range
"""

import datetime
from dateutil.parser import parse
import pytz

class DateTimeHandler(Exception):


    def __init__(self, range, start=None, duration=None, step=None):
        """Initialize DateTimeHandler object

        :param start: start time string of query [%Y-%m-%dT%H:%M:%S.%fZ]
        :param duration: duration string of query where query space is between start and start + duration
                        in the format of [%H:%M:%S]
        :param range: number of seconds to offset start value to calculate stop value in query [seconds]
        :param step: number of seconds to increment start value of query [seconds]

        If any arguments evaluate to None, class will use real-time queries
        Sets initial values

        For historic queries:
        Sets end_duration of as the initial start time + duration to determine when a loop should occur
        """
        if not(int(range) > 0):
            raise ValueError("Range must be a non-negative, non-zero number")
        self._range = int(range)

        # if any of start, duration, or step is none, assume real-time query
        self._historic_query = not((start is None) or (duration is None) or (step is None))
        if(self._historic_query):

            if not(int(step) > 0):
                raise ValueError("Step must be a non-negative, non-zero number")
            if not(int(range) >= int(step)):
                raise ValueError("Range must be equal or larger than step")

            self.start = start
            self._stop = self._start + datetime.timedelta(0, self._range)

            self.duration = duration

            # at least one value must be greater than 0
            if not(self.duration.hour > 0 or self.duration.minute > 0 or self.duration.second > 0):
                raise ValueError("Duration must be larger than 0 or was parsed incorrectly")

            self._duration_flag = 0

            self._end_duration = self._start + datetime.timedelta(hours=self._duration.hour,
                                                                    minutes=self._duration.minute,
                                                                    seconds=self._duration.second)
        else:
            self._stop = datetime.datetime.now(tz=pytz.timezone('UTC'))
            if not(step):
                step = 1

        self._step = int(step)


    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, value):
        """Takes in a string value of start time and converts and stores internally the datetime value"""
        self._start = parse(value)

    @property
    def duration(self):
        return self._duration

    @duration.setter
    def duration(self, value):
        """Takes in a string value of duration and converts and stores internally the datetime value"""
        self._duration = parse(value)

    @property
    def range(self):
        return self._range

    @range.setter
    def range(self, value):
        #Handle offsetting the end query time when range is updated, relative to the previous range
        self._stop = self._stop + datetime.timedelta(0, value - self._range)
        self._range = int(value)

    def get_query_start_stop(self):
        if(self._historic_query):
            stop = self._stop
        else:
            stop  = datetime.datetime.now(tz=pytz.timezone('UTC'))

        #compute start of query relative to the stop time - range
        start = stop - datetime.timedelta(0, self._range)

        #return start and stop values as strings
        return start.strftime('%Y-%m-%dT%H:%M:%S.%fZ'), stop.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


    def step_time(self):
        """Increments the stop time by the step value
        If the next step would cause duration to be exceeded, a sticky flag sets the return value to 1
        Calling this function when real-time queries are used simply returns 0"""
        if(self._historic_query):
            if self._stop > (self._end_duration):
                self._stop = self._start + datetime.timedelta(0, self._range)

                self._duration_flag = 1
                return self._duration_flag

            #step after checking if duration is busted, otherwise the last iteration is missed
            self._stop += datetime.timedelta(0, self._step)
            return self._duration_flag

        return 0
