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

```
import lcdio

file = lcdio.open('testdata/planets.parquet')
for row in file:
  print(f'Planet {row["name"]} is {row["distance"]} light-seconds away from the sun.')

```

For files without named columns: The file is a list of dictionaries (key is the column number).

```
import lcdio

file = lcdio.open('testdata/planets.csv')
for row in file:
  print(f'Planet {row[0]} is {row[1]} light-seconds away from the sun.')

```


### Philosophy

This is not about making the more performant or the most featureful reader for these formats, no.

What this is about is making the library that is the easiest to use for reading a bunch of formats.
You don't need to remember the names of all the specific libraries to import. You don't need to remember
each of their syntax. 
You only need to remember one thing: "everything is a list of dictionaries."


## Other options

the `open` method has a `mode` argument to tell it what format to read the file as (if it can't be guessed from the file extension, say), and a `has_header` argument to flag whether the `csv` has a header row or not.


