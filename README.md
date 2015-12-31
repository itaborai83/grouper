# grouper

Simple GROUP BY like functionality for data munging in Python.

Grouper's design was inspired by SQL but it is not limited by it. It is primarily aimed to do quick and dirty data aggregation and analysis.

It works out of the box with lists of dicts, but it can be extended to work with other inputs and outputs.

Grouper is fully unit tested.

Here is a simple example of how to use it.

```python
import grouper as g
  
rows = [
  { "Country": "Brazil", "Region": "Southeast",  "State": "Sao Paulo", "City": "Sao Paulo", "Population": 11895893 },
  { "Country": "Brazil", "Region": "Southeast",  "State": "Rio de Janeiro",   "City": "Rio de Janeiro", "Population": 6453682 },
  { "Country": "Brazil", "Region": "Northeast",  "State": "Bahia", "City": "Salvador", "Population": 2902927 },  
  { "Country": "Brazil", "Region": "Midwest", "State": "Distrito Federal", "City": "Brasilia", "Population": 2852372 } ,
  { "Country": "Brazil", "Region": "Northeast",  "State": "Ceara",  "City": "Fortaleza", "Population": 2571896 },
]

f = lambda row: row["Region"] != "Midwest"   

result = g.Grouper(
    "Country",
    "Region",
    g.Sum("Population").as_("Total Pop."),
    g.Array("City").as_("Cities"),
    where=f
).run(rows)

import pprint
pprint.pprint(result)

""" it outputs:

[{'Cities': ['Salvador', 'Fortaleza'],
  'Country': 'Brazil',
  'Region': 'Northeast',
  'Total Pop.': 5474823},
 {'Cities': ['Sao Paulo', 'Rio de Janeiro'],
  'Country': 'Brazil',
  'Region': 'Southeast',
  'Total Pop.': 18349575}]
"""
```

## Grouping Sorting Behaviour

Grouping is usually done through common item access on the dict contained in every row. In order to specify by wich fields the grouping is to be done, simply add the names of the groups as shown below

```python
>>> g.Grouper("Region").run(rows)
[{'Region': 'Midwest'}, {'Region': 'Northeast'}, {'Region': 'Southeast'}]
```
When given a string, it will implicity create a `KeyExpr` object as follows `KeyExpr(field)`.

If you need to change the ouput dictionary key, you need to manually instantiate this object and call the `as_` method as shown below

```python
>>> g.Grouper(
...   g.KeyExpr("Region").as_("Regiao")
... ).run(rows)
[{'Regiao': 'Midwest'}, {'Regiao': 'Northeast'}, {'Regiao': 'Southeast'}]
```

If you need, you can also implement custom grouping logic by subclassing `KeyExpr` or its parent `RowExpr` and using it directly.

## Aggregation operations

As of now, the following aggregation operations are supported.

 * `Avg(field)` - Computes the average of the observed values in a given grouping.
 * `Stddev(field)` - Computes the standard deviation of the observed values in a given grouping.
 * `Sum(field)` - Sums the average of the observed values in a given grouping.
 * `Min(field)` - Returns the minimal value of the observed values in a given grouping.
 * `Max(field)` - Returns the maximal value of the observed values in a given grouping.
 * `Counts(field)` - Counts the observed values in a given grouping.
 * `Distinct(field)` - Counts the distinct number of values for a given field in a given grouping.
 * `Array(field)` - Returns a list of the observed values in a given grouping.
 * `Concat(field, separator)` - Concatenates the observed values in a given grouping .
 
field can be either a simple string, as shown above, or a subclass of `RowExpr`.
 
The name of the output field will default to `"<name of the operation in lowercase>_<name of the field>`" as shown below.

```python
>>> g.Grouper("Country", g.Array("City")).run(rows)
[{'array_City': ['Sao Paulo', 'Rio de Janeiro', 'Salvador', 'Brasilia', 'Fortaleza'], 'Country': 'Brazil'}]
```

It can also be changed called the `as_` method.

```python
>>> g.Grouper(
... "Country",
... g.Array("City").as_("Cities")
... ).run(rows)
[{'Cities': ['Sao Paulo', 'Rio de Janeiro', 'Salvador', 'Brasilia', 'Fortaleza'], 'Country': 'Brazil'}]
```

You can create custom aggregation operations by subclassing `Aggregate`.
## Filtering

You can filter out rows before anda after the aggregation takes place by specifying the keyword arguments `where` and `having` in the Grouper constructor.

```javascript
>>> really_big_cities = lambda row: row['Population'] > 3000000
>>> coastal_regions = lambda row: row[ "Region" ] in ("Southeast", "Northeast")
>>> result = g.Grouper(
... "Country",
... "Region",
... "State",
... g.Sum("Population").as_("Total Pop."),
... where=really_big_cities,
... having=coastal_regions
... ).run(rows)
>>> import pprint
>>> pprint.pprint(result)
[{'Country': 'Brazil',
  'Region': 'Southeast',
  'State': 'Rio de Janeiro',
  'Total Pop.': 6453682},
 {'Country': 'Brazil',
  'Region': 'Southeast',
  'State': 'Sao Paulo',
  'Total Pop.': 11895893}]
```

## Dealing with unexpected inputs and outputs

By default Grouper will deal with lists of dicts. If your input is different than that, you can specify the keyword argument `map` to Grouper constructor. For mapping the output dict to something else, you can use the `unmap` keyword argument. 

`map` can accept anything, but it must return a dictionary.

`unmap` must accept a dictionary and it can return anything.

```python
>>> rows = [["foo", 1, 2], ["foo", 2, 1], ["bar", 10, 20], ["bar", 30, -5]]
>>> todict = lambda point: { "type": point[0], "x": point[1], "y": point[2] }
>>> fromdict = lambda point: [ point["type"], point["min_x"], point["max_y"] ]
>>> g.Grouper(
... "type",
... g.Min("x"),
... g.Max("y"),
... map=todict,
... unmap=fromdict
... ).run(rows)
[['bar', 10, 20], ['foo', 1, 2]]
```
