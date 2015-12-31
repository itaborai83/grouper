import math

#from aggregates import *

class RowExpr(object):
    """ Represents an Expression that returns a value given a dict"""

    def __init__(self, label):
        self._label = label

    def label(self):
        """ return the label for an expression """
        return self._label

    def value(self, row):
        """ Given a dict, it returns a value to be aggregated """
        raise NotImplementedError
    
class KeyExpr(RowExpr):
    """ Given a key, returns its value """

    def __init__(self, field):
        self.field = field
        super().__init__(field) 

    def value(self, row):
        return row[self.field]

    def as_(self, label):
        self._label = label
        return self

class AggregateList(object):
    """ Holds a list of aggregates to be used by a Grouper object"""

    def __init__(self,  *aggregates):
        self.aggregates = aggregates

    def reset(self):
        for aggregate in self.aggregates:
            aggregate.reset()

    def update(self, row):
        for aggregate in self.aggregates:
            aggregate.update(row)
    
    def value(self):
        aggregated = { aggr.label(): aggr.result() for aggr in self.aggregates }
        return aggregated


class Aggregate(object):
    """ Computation of an aggregate operation for a given grouping """

    def __init__(self, expr):
        if type(expr) == str:
            expr = KeyExpr(expr)
        expr_label = expr.label()
        label = "{}_{}".format(self.name(), expr.label())
        self.expr = expr
        self._label = label
        self.reset()
    
    def as_(self, label):
        self._label = label
        return self
        
    def label(self):
        return self._label

    def name(self):
        return type(self).__name__.lower()

    def reset(self):
        raise NotImplementedError
    
    def update(self, row):
        raise NotImplementedError

    def result(self):
        raise NotImplementedError
 
class Avg(Aggregate):

    def reset(self):
        self.sum = 0
        self.count = 0
    
    def update(self, row):
        self.sum += self.expr.value(row)
        self.count += 1

    def result(self):
        return self.sum / self.count

   
class Sum(Aggregate):

    def reset(self):
        self.value = 0
    
    def update(self, row):
        self.value += self.expr.value(row)

    def result(self):
        return self.value

class Min(Aggregate):

    def reset(self):
        self.value = None
    
    def update(self, row):
        value = self.expr.value(row)
        if self.value is None:
            self.value = value
        elif value < self.value:
            self.value = value

    def result(self):
        return self.value

class Max(Aggregate):

    def reset(self):
        self.value = None
    
    def update(self, row):
        value = self.expr.value(row)
        if self.value is None:
            self.value = value
        elif value > self.value:
            self.value = value

    def result(self):
        return self.value

class Count(Aggregate):

    def reset(self):
        self.value = 0
    
    def update(self, row):
        self.value += 1

    def result(self):
        return self.value

class Array(Aggregate):

    def reset(self):
        self.value = []
    
    def update(self, row):
        value = self.expr.value(row)
        self.value.append(value)

    def result(self):
        return self.value

class Concat(Aggregate):

    def __init__(self, expr, separator=""):
        self.separator = separator
        super().__init__(expr)
        
    def reset(self):
        self.value = []
    
    def update(self, row):
        value = self.expr.value(row)
        self.value.append(value)

    def result(self):
        return self.separator.join(map(str, self.value))

class Stddev(Aggregate):

    def reset(self):
        self.sum = 0.0
        self.sumsq = 0.0
        self.count = 0
    
    def update(self, row):
        value = self.expr.value(row)
        self.sum += value
        self.sumsq += value * value
        self.count += 1

    def result(self):
        if self.count < 2:
            return 0.0
        else:
            mean = self.sum / self.count
            return math.sqrt((self.sumsq - self.sum * mean) / (self.count - 1))

class Distinct(Aggregate):

    def reset(self):
        self.seen = set()

    def update(self, row):
        value = self.expr.value(row)
        self.seen.add(value)

    def result(self):
        return len(self.seen)


class Grouper(object):
    
    # PUBLIC
    # ------

    def __init__(self, *fields, map=None, unmap=None, where=None, having=None):
        identity_f = lambda row: row
        true_f = lambda row: True
        fields = list(fields)
        group_fields = []
        aggregates = []
        l = len(fields)
        for i in range(l):
            field = fields[i]
            if isinstance(field, str):
                field = KeyExpr(field)
                fields[i] = field
            if isinstance(field, RowExpr):
                group_fields.append(field)
            if isinstance(field, Aggregate):
                aggregates.append(field)
            if not isinstance(field, (RowExpr, Aggregate)):
                msg = "Invalid field {}".format(field)
                raise ValueError(msg)
        self.group_fields = group_fields
        self.aggregates = AggregateList(*aggregates)
        self.map = (map if map else identity_f)
        self.unmap = (unmap if unmap else identity_f)
        self.where = (where if where else true_f)
        self.having = (having if having else true_f)
        self.keys = None

    def run(self, rows, copy_rows=True):
        """ Groups and aggregates rows according to the objects parameters 

        """
        if copy_rows:
            rows = rows[:]
        self.sort(rows)
        self.aggregates.reset()
        curr_keys, self.keys, result = None, None, []
        for row in rows:
            row = self.map(row)
            if not self.where(row):
                continue

            curr_keys = self.grouping_values_for(row)
            if curr_keys != self.keys:
                if self.keys is not None:
                    output = self.output()
                    if self.having(output):
                        result.append(self.output())
                    self.aggregates.reset()
            self.keys = curr_keys
            self.aggregates.update(row)

        if curr_keys is not None:
            self.keys = curr_keys
            output = self.output()
            if self.having(output):
                result.append(self.output())
            self.aggregates.reset()

        return result
    
    # PRIVATE
    # -------

    def grouping_values_for(self, row):
        """ return the grouping values for a given row """
        return [ expr.value(row) for expr in self.group_fields ] 
    
    def output(self):
        """ return the output row for the current grouping """
        output = {}
        key_values = { expr.label(): key for expr, key in zip(self.group_fields, self.keys) } 
        aggregated_values = self.aggregates.value() 
        output.update(key_values)
        output.update(aggregated_values)
        return self.unmap(output)
        
    def sort(self, rows):
        """ sort rows by their grouping values """
        def func(row):
            return self.grouping_values_for(self.map(row))
        rows.sort(key=func)
    

