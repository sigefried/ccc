from const import *

class Flight(object):

    def __init__(self, fields):
        self.rawData = fields
        self.flightDate = fields[FLIGHT_DATE_COL]
        self.destination = fields[DEST_COL]
        self.source = fields[ORIGIN_COL]
        self.flightNumber = fields[UNIQUECARRIER_COL]
        self.flightDepTime = fields[DEPTIME_COL]
        self.flightArrDelay = fields[DEPTIME_COL]

    def __str__(self):
        return ','.join(self.rawData)