import unittest
import math

from grouper import *

class TestAggregate(unittest.TestCase):
    
    def test_it_aliases(self):
        s = Sum("A").as_("FOO")
        self.assertEqual(s.label(), "FOO")

    def test_it_sums(self):
        s = Sum("A")
        s.update({"A": 1})
        s.update({"A": 2})
        s.update({"A": 3})
        self.assertEqual(s.result(), 6)
    
    def test_it_averages(self):
        a = Avg("A")
        a.update({"A": 1})
        a.update({"A": 2})
        a.update({"A": 3})
        self.assertEqual(a.result(), 2.0)
    
    def test_it_finds_the_min(self):
        m = Min("A")
        m.update({"A": 1})
        m.update({"A": 2})
        m.update({"A": 3})
        self.assertEqual(m.result(),  1)
    
    def test_it_finds_the_max(self):
        m = Max("A")
        m.update({"A": 1})
        m.update({"A": 2})
        m.update({"A": 3})
        self.assertEqual(m.result(),  3)

    def test_it_counts(self):
        c = Count("A")
        c.update({"A": 1})
        c.update({"A": 2})
        c.update({"A": 3})
        self.assertEqual(c.result(), 3)

    def test_it_stores_values_in_an_array(self):
        a = Array("A")
        a.update({"A": 1})
        a.update({"A": 2})
        a.update({"A": 3})
        self.assertEqual(a.result(), [1, 2, 3])
    
    def test_it_concatenates_strings(self):
        c = Concat("A", ", ")
        c.update({"A": 1})
        c.update({"A": 2})
        c.update({"A": 3})
        self.assertEqual(c.result(), "1, 2, 3")

    def test_it_calcs_stddev(self):
        s = Stddev("A")
        s.update({"A": 1})
        s.update({"A": 2})
        s.update({"A": 3})
        s.update({"A": 4})
        self.assertAlmostEqual(s.result(), 1.2909944487358056, 8)

    def test_it_calcs_distinct_values(self):
        d = Distinct("A")
        d.update({"A": 1})
        d.update({"A": 1})
        d.update({"A": 2})
        d.update({"A": 3})
        self.assertEqual(d.result(), 3)

class TestRowExpr(unittest.TestCase):

    def test_it_returns_a_value_for_a_row(self):
        f = KeyExpr("field")
        row = { "field" : "value" }
        self.assertEqual(f.value(row), "value") 
    
    def test_it_labels(self):
        f = KeyExpr("field")
        self.assertEqual(f.label(), "field") 

    def test_it_aliases(self):
        f = KeyExpr("field").as_("FOO")
        self.assertEqual(f.label(), "FOO") 
    
class TestAggregateList(unittest.TestCase):

    def test_it_aggregates_values(self):
        group = AggregateList(Sum("C"), Count("C"))
        group.update({"A": 0, "B": 0, "C": 1})
        group.update({"A": 0, "B": 0, "C": 2})
        group.update({"A": 0, "B": 0, "C": 3})
        group.update({"A": 0, "B": 0, "C": 4})
        group.update({"A": 0, "B": 0, "C": 5})
        expected = { 
            "sum_C"   : 15,
            "count_C" : 5
        }
        self.assertEqual(expected, group.value())

class TestGrouper(unittest.TestCase):
   
    def test_it_returns_the_group_for_a_row(self):
        aggregates = None
        group_fields = ["A", "B"]
        group = Grouper("A", "B", Count("D"))
        result = group.grouping_values_for({
            "A": 1, "B": 2, "C": 3, "D": "FOO"
        })
        self.assertEqual(result, [1, 2])
    
    def test_it_sorts(self):
        grouper = Grouper("A", "B", Sum("C"))
        rows = [
            { "A": 3, "B": 1, "C": 9999 },
            { "A": 3, "B": 2, "C": 9999 },
            { "A": 2, "B": 2, "C": 9999 },
            { "A": 2, "B": 1, "C": 9999 },
            { "A": 1, "B": 2, "C": 9999 },
            { "A": 1, "B": 1, "C": 9999 },
        ]
        expected = [
            { "A": 1, "B": 1, "C": 9999 },
            { "A": 1, "B": 2, "C": 9999 },
            { "A": 2, "B": 1, "C": 9999 },
            { "A": 2, "B": 2, "C": 9999 },
            { "A": 3, "B": 1, "C": 9999 },
            { "A": 3, "B": 2, "C": 9999 },
        ]
        grouper.sort(rows)
        self.assertEqual(rows, expected)
    
    def test_it_returns_nothing_when_summing_nothing(self):
        grouper = Grouper("A", Sum("B"))
        result = grouper.run([])
        self.assertEqual(result, [])
    
    def test_it_aggregates(self):
        grouper = Grouper("A", Sum("B"))
        rows = [
            { "A": 3, "B": 1, "C": 9999 },
            { "A": 3, "B": 2, "C": 9999 },
            { "A": 2, "B": 2, "C": 9999 },
            { "A": 2, "B": 2, "C": 9999 },
            { "A": 1, "B": 2, "C": 9999 },
            { "A": 1, "B": 3, "C": 9999 },
        ]
        expected = [
            { "A": 1, "sum_B": 5 },
            { "A": 2, "sum_B": 4 },
            { "A": 3, "sum_B": 3 },
        ]
        output = grouper.run(rows)
        self.assertEqual(output, expected)

    def test_it_filters_input_rows(self):
        def f(row):
            return not (row['A'] == 3 and row['B'] == 2)

        grouper = Grouper("A", Sum("B"), where=f)
        rows = [
            { "A": 3, "B": 1, "C": 9999 },
            { "A": 3, "B": 2, "C": 9999 },
            { "A": 2, "B": 2, "C": 9999 },
            { "A": 2, "B": 2, "C": 9999 },
            { "A": 1, "B": 2, "C": 9999 },
            { "A": 1, "B": 3, "C": 9999 },
        ]
        expected = [
            { "A": 1, "sum_B": 5 },
            { "A": 2, "sum_B": 4 },
            { "A": 3, "sum_B": 1 },
        ]
        output = grouper.run(rows)
        self.assertEqual(output, expected)

    def test_it_filters_output_rows(self):
        def f(row):
            return row['sum_B'] > 3

        grouper = Grouper(
            "A", 
            Sum("B"), 
            having=f
        )
        rows = [
            { "A": 3, "B": 1, "C": 9999 },
            { "A": 3, "B": 2, "C": 9999 },
            { "A": 2, "B": 2, "C": 9999 },
            { "A": 2, "B": 2, "C": 9999 },
            { "A": 1, "B": 2, "C": 9999 },
            { "A": 1, "B": 3, "C": 9999 },
        ]
        expected = [
            { "A": 1, "sum_B": 5 },
            { "A": 2, "sum_B": 4 },
        ]
        output = grouper.run(rows)
        self.assertEqual(output, expected)

    def test_it_maps_inputs(self):
        def todict(row):
            return { "A": row[0], "B": row[1], "C": row[2] }
        grouper = Grouper("A", Sum("B"), map=todict)
        rows = [
            # A, B, C
            [ 3, 1, 9999 ],
            [ 3, 2, 9999 ],
            [ 2, 2, 9999 ],
            [ 2, 2, 9999 ],
            [ 1, 2, 9999 ],
            [ 1, 3, 9999 ],
        ]
        expected = [
            { "A": 1, "sum_B": 5 },
            { "A": 2, "sum_B": 4 },
            { "A": 3, "sum_B": 3 },
        ]
        output = grouper.run(rows)
        self.assertEqual(output, expected)

    def test_it_demaps_outputs(self):

        def fromdict(row):
            return [ row["A"], row["sum_B"] ]

        grouper = Grouper("A", Sum("B"),  unmap=fromdict)
        rows = [
            { "A": 3, "B": 1, "C": 9999 },
            { "A": 3, "B": 2, "C": 9999 },
            { "A": 2, "B": 2, "C": 9999 },
            { "A": 2, "B": 2, "C": 9999 },
            { "A": 1, "B": 2, "C": 9999 },
            { "A": 1, "B": 3, "C": 9999 },
        ]
        expected = [
            [ 1, 5 ],
            [ 2, 4 ],
            [ 3, 3 ],
        ]
        output = grouper.run(rows)
        self.assertEqual(output, expected)

    def test_it_aggregates_without_grouping_fields(self):
        result = Grouper(Sum("A")).run([
            { "A": 1 }, 
            { "A": 2 }, 
            { "A": 3 }, 
            { "A": 4 }
        ])
        self.assertEqual(result, [ { "sum_A": 10 } ])

if __name__ == '__main__':
    unittest.main()
