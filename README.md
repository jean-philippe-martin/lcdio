# Lowest Common Denominator I/O

"Everything is a list of dictionaries!"

LCDIO lets you read through the records in the same way for 
a variety of file formats:

- csv
- tsv
- json
- jsonl
- parquet
- field-separated text
- SQLite
- toml
- yaml


## Usage

For files with named columns: The file is a list of dictionaries (key is the column name).

```python
import lcdio

file = lcdio.open('testdata/planets.parquet')
for row in file:
  print(f'Planet {row["name"]} is {row["distance"]} light-seconds away from the sun.')
```

For files without named columns: The file is a list of dictionaries (key is the column number).

```python
import lcdio

file = lcdio.open('testdata/planets.csv')
for row in file:
  print(f'Planet {row[0]} is {row[1]} light-seconds away from the sun.')
```

### Additional features

The rows returned by LCDIO are dict-like, but they have a few other features:

- You can read multiple columns at a time (with slicing):

```python
import lcdio

file = lcdio.open('testdata/planets.csv')
for row in file:
  print(f'The first two columns are {row[0:2]}')
```

This includes all the slicing syntax, such as skipping every other item (`row[::2]`), skipping the first one (`row[1:]`), skipping the last one (`row[:-1]`) etc.

- You can use column index (and slices) even when the columns are named:

```python
import lcdio

file = lcdio.open('testdata/planets.parquet')
for row in file:
  print(f'Planet {row[0]}')
```

- If the row contains an array, you can access it by adding an argument:

```python
>>> row['days_in_office']
['monday', 'wednesday']
>>> row['days_in_office', 0]
'monday'
```

- If the row contains a JSON object, you can access members by adding arguments:

```python
>>> row[0]
{'age': 30, 'secrets': {'password': 'foo', 'closet': '2 skeletons'}}
>>> row[0, 'secrets', 'password']
'foo'
```


### Philosophy

This is not about making the most performant or the most featureful reader for these formats.

What this is about is making the library that is the easiest to use for reading a bunch of formats.
You don't need to remember the names of all the specific libraries to import. You don't need to remember
each of their syntax. 
You only need to remember one thing: "everything is a list of dictionaries."


## Other options

the `open` method has a `mode` argument to tell it what format to read the file as (if it can't be guessed from the file extension, say), and a `has_header` argument to flag whether the `csv` has a header row or not.


