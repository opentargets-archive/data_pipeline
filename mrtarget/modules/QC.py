'''Runs quality control queries on the database'''
import logging
import csv
import os.path
from numbers import Number

class QCMetrics(object):
    def __init__(self):
        self.metrics = dict()
        self._logger = logging.getLogger(__name__+".QCMetrics")

    """
    Update the metrics stored here with a dictionary of additional information
    """
    def update(self, other):
        self.metrics.update(other)

    """
    Write the stored metrics out to the provided filename.

    If the file exists, the metrics are added to the previous ones.

    If the file does not exist, it is created.
    """
    def write_out(self, filename):
        #make a copy of the current metrics to avoid side effects
        metrics = dict(self.metrics)

        #ensure the directory for the log file exists
        qcdir = os.path.dirname(os.path.abspath(filename))
        if not os.path.isdir(qcdir):
            self._logger.debug("log directory %s does not exist, creating", qcdir)
            os.makedirs(qcdir)


        #if the file exists, read it and add the contents to the metrics
        if os.path.isfile(filename):
            with open(filename, 'rb') as csvfile:
                csvreader = csv.reader(csvfile, delimiter='\t')
                for row in csvreader:
                    if row[0] not in metrics:
                        metrics[row[0]] = row[1:]
        
        with open(filename, 'wb') as csvfile:

            #write out
            csvwriter = csv.writer(csvfile, delimiter='\t')
            for metric in sorted(metrics):
                value = metrics[metric]
                if isinstance(value, basestring):
                    #its a string, wrap in a tuple
                    value = (value,)
                elif isinstance(value, Number):
                    #its a number, wrap in a tuple
                    value = (value,)
                else:
                    #convert to a tuple
                    value = tuple(value)

                row = (metric,)+value

                self._logger.debug("Writing to row %s", row)

                csvwriter.writerow(row)

    """
    Compare the metrics in this object with some of the same metrics from the provided file.

    Produces new metrics, which are not automatically added to this object
    """
    def compare_with(self, filename):
        #read old file
        with open(filename, 'rb') as csvfile:
            csvreader = csv.reader(csvfile, delimiter='\t')
            for row in csvreader:
                old_metric = row[0]
                old_value = row[1:]
                #if the metric exists
                if old_metric in self.metrics:
                    new_value = self.metrics[old_metric]
                    #work out what type it is
                    #new value is a number and old value is a single thing
                    if isinstance(new_value, Number) and len(old_value) == 1:
                        #single number
                        old_value_number = None
                        #convert old value to an int or a float if possible
                        try:
                            old_value_number = int(old_value[0])
                        except ValueError:
                            try:
                                old_value_number = float(old_value[0])
                            except ValueError:
                                old_value_number = None


                        #dont work out differences of differences
                        if not old_metric.endswith(".difference"):
                            #should have converted numbers at this point
                            if old_value_number is not None:
                                metric_diff = old_metric+".difference"
                                diff = new_value-old_value_number
                                self.metrics[metric_diff] = diff
                            else:
                                #ignore if we cant turn it into a number
                                pass
                    else:
                        #list/set/similar?

                        #dont work out differences of differences
                        if not old_metric.endswith(".added") and not old_metric.endswith(".removed"):

                            added = []
                            removed = []
                            for thing in old_value:
                                if thing not in new_value:
                                    removed.append(thing)
                            for thing in new_value:
                                if thing not in old_value:
                                    added.append(thing)
                            if len(added):
                                self.metrics[old_metric+".added"] = added
                            if len(removed):
                                self.metrics[old_metric+".removed"] = removed



