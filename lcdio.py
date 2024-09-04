"""Lowest Common Denominator I/O

The simple and consistent way to read data files.
"Everything is a list of dictionaries!"

Example:

import lcdio
data = lcdio.open('testdata/names.csv', has_header = True)
for row in data:
  print(row['name'])
"""
import builtins
from pathlib import Path

def open(filename_or_file, mode=None, has_header=False):
    """Open the file for reading.

    You can pass either the name of the file, or a file-like object.
    If you pass a file-like object then you have to indicate the file type
    with the 'mode' argument. You can also specify it with a filename if
    you don't want the library to guess based on the file extension.

    For files with an optional header row, use the 'has_header' argument
    to indicate whether it's present.

    >>> reader = open(['1,2,3', '4,5,6'], mode='csv')
    >>> list(reader)[0][1]
    '2'
    """
    if mode is None:
        # Try to guess the mode from the file name extension
        if not isinstance(filename_or_file, str):
            raise "When passing a file-like object, you must specify a mode."
        filename = filename_or_file.lower()
        if filename.endswith('.csv'):
            mode = 'csv'
        elif filename.endswith('.tsv'):
            mode = 'tsv'
        elif filename.endswith('.parquet'):
            mode = 'parquet'
        elif filename.endswith('.toml'):
            mode = 'toml'
        elif filename.endswith('.json'):
            mode = 'json'
        elif filename.endswith('.jsonl') or filename.endswith('.ndjson'):
            mode = 'jsonl'
        elif filename.endswith('.yaml'):
            mode = 'yaml'
        elif filename.endswith('.db'):
            mode = 'sqlite'
        else:
            raise "Unable to guess the file type for: " + filename_or_file

    if mode == 'csv':
        return CsvLcd(filename_or_file, has_header=has_header)
    elif mode == 'tsv':
        return CsvLcd(filename_or_file, delimiter="\t", has_header=has_header)
    elif mode == 'parquet':
        return ParquetLcd(filename_or_file)
    elif mode == 'toml':
        return TomlLcd(filename_or_file)
    elif mode == 'json':
        return JsonLcd(filename_or_file)
    elif mode == 'jsonl':
        return JsonlLcd(filename_or_file)
    elif mode == 'yaml':
        return YamlLcd(filename_or_file)
    elif mode == 'sqlite':
        return SqliteLcd(filename_or_file)
    raise "Unknown file type: " + str(mode)


class LcdRecord:
    """A 'universal' record: you can access it as record[0]
    or record['column_name'] if the data has column names.

    >>> record = LcdRecord(['Bob', 30], keys=['name','age'])
    >>> record['name']
    'Bob'
    >>> record[1]
    30

    You can also use the slice notation, like you would in a list.

    >>> record = LcdRecord(['Bob', 'Joe', 'Guy'])
    >>> record[0:2]
    ['Bob', 'Joe']

    You can also use the `keys`, `values`, and `items` methods, like you would
    in a dict.

    >>> record = LcdRecord(['Bob', 30], keys=['name','age'])
    >>> record.keys()
    ['name', 'age']
    >>> record.values()[0]
    'Bob'
    >>> list(record.items())[0]
    ('name', 'Bob')
    >>> record['name']
    'Bob'
    >>> record = LcdRecord(['Bob', {'age': 30, 'secrets': {'password': 'foo', 'closet': '2 skeletons'}}],
    ...   keys=['name', 'data'])
    >>> record['data']
    {'age': 30, 'secrets': {'password': 'foo', 'closet': '2 skeletons'}}

    If a record doesn't have keys, you can still use integers to access the values.

    >>> record = LcdRecord(['Bob', 30])
    >>> record[0]
    'Bob'

    One quirk of LcdRecord is we allow the list of values to sometimes be longer than
    the list of keys. This can happen in for example a malformed CSV file where a row
    has more values than the file header gives names for.

    A secret power of LcdRecord is that if the record contains dicts, 
    then you can access them as if they were additional dimensions!

    >>> record = LcdRecord(['Bob', {'age': 30, 'secrets': {'password': 'foo', 'closet': '2 skeletons'}}],
    ...   keys=['name', 'data'])
    >>> record['data', 'secrets', 'password']
    'foo'

    This also works with nested arrays:

    >>> record = LcdRecord([ [['cos', '-sin'], ['sin', 'cos']] ])
    >>> record[0,0,1]
    '-sin'


    """
    def __init__(self, values, keys=None):
        self.vals = values
        self.has_header = (keys is not None)
        self.headers = keys
        self.asdict = {}
        if keys:
            self.asdict = dict(zip(keys,values))

    def keys(self):
        """The list of column names"""
        if self.headers:
            return self.headers
        return range(len(self.vals))

    def values(self):
        """The values for this record, in order"""
        return self.vals
    
    def items(self):
        """List of (key,value)."""
        return zip(self.keys(), self.values())

    def __getitem__(self, indices):
        """Access one or more elements in the record by index, name, or slice."""
        if not isinstance(indices, tuple):
            indices = (indices,)
        element = self._get_single_item(indices[0])
        for index in indices[1:]:
            element = element[index]
            if element is None:
                return None
        return element

    def _get_single_item(self, single_index):
        if isinstance(single_index, int):
            return self.vals[single_index]
        if isinstance(single_index, slice):
            return self.vals[single_index]
        if isinstance(single_index, str):
            if not self.has_header:
                raise "no header present, please use integer index for access"
            return self.asdict[single_index]
        raise "index must be int or string"

    def __str__(self):
        """return a string that provides a human-readable representation of the object."""
        return f"LcdRecord({str(self.vals)}, headers:{self.headers})"



class CsvLcd:
    """Least common denominator reader for CSV data"""
    def __init__(self, filename_or_file, delimiter=",", has_header=False):
        import csv
        self.headers = None
        if isinstance(filename_or_file, str):
            self.file = builtins.open(filename_or_file, "r", newline="")
        else:
            self.file = filename_or_file
        self.reader = csv.reader(self.file, delimiter=delimiter)
        self.has_header = has_header
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reader = None
        self.file.close()

    def __iter__(self):
        self.iterator = iter(self.reader)
        if self.has_header:
            self.headers = next(self.iterator)
        return self

    def __next__(self):
        return LcdRecord(next(self.iterator), keys=self.headers)


class ParquetLcd:
    def __init__(self, filename):
        import pyarrow.parquet as pq
        self.table = pq.read_table(filename)
        self.headers = [self.table.field(c).name for c in range(len(self.table.columns))]
        self.line = 0;

    def __iter__(self):
        self.line = 0
        self.maxline = len(self.table.columns[0])
        return self

    def __next__(self):
        if self.line>=self.maxline:
            raise StopIteration
        row=[str(col[self.line]) for col in self.table.columns]
        self.line += 1
        return LcdRecord(row, keys=self.headers)

    def __len__(self):
        return len(self.table.columns[0])


class TomlLcd:
    """A TOML file will contain a single record, with the TOML data in it."""
    def __init__(self, filename):
        import toml
        self.file = [toml.load(filename)]

    def __iter__(self):
        self.iterator = iter(self.file)
        return self

    def __next__(self):
        nxt = next(self.iterator)
        return LcdRecord(list(nxt.values()), keys=list(nxt.keys()))

    def __len__(self):
        return 1


class JsonLcd:
    """A JSON file can contain an array or a map.
    If it's an array, here you can iterate through the elements inside
    the array. If it's a map, as you iterate you will only get one thing:
    the map itself."""
    def __init__(self, filename_or_file):
        import json
        file = filename_or_file
        if isinstance(filename_or_file, str):
            file = builtins.open(filename_or_file, "r", newline="")
        self.data=json.load(file);
        if isinstance(self.data, dict):
            self.data=[self.data]

    def __iter__(self):
        self.iterator = iter(self.data)
        return self

    def __next__(self):
        nxt = next(self.iterator)
        if isinstance(nxt, dict):
            return LcdRecord(list(nxt.values()), keys=list(nxt.keys()))
        return LcdRecord(nxt, keys=[])

    def __len__(self):
        return len(self.data)


class JsonlLcd:
    """JSONL is a format where we parse every line individually,
    and every line is JSON. You get one record per line."""
    def __init__(self, filename_or_file):
        import json
        self.file = filename_or_file
        if isinstance(filename_or_file, str):
            self.file = builtins.open(filename_or_file, "r", newline="")

    def __iter__(self):
        self.iterator = iter(self.file)
        return self

    def __next__(self):
        line = next(self.iterator)
        nxt = json.opens(line)
        if isinstance(nxt, dict):
            return LcdRecord(list(nxt.values()), keys=list(nxt.keys()))
        return LcdRecord(nxt, keys=[])


class YamlLcd:
    """Loading the YAML, each record is a YAML document
    (since YAML can contain multiple documents in one file),
    and each document will be a map or array depending on what's in the file.
    The order of elements is maintained, so if in the document the first key
    is a name, then when you access the returned record with [0] you will get
    the name.

    >>> first_doc = list(open('testdata/names.yaml'))[0]
    >>> first_doc.keys()[0]
    'name'

    """
    def __init__(self, filename_or_file):
        from ruamel.yaml import YAML
        self.file = filename_or_file
        if isinstance(filename_or_file, str):
            self.file = builtins.open(filename_or_file, "r", newline="")
        yaml=YAML(typ="safe");
        self.data = yaml.load_all(self.file)

    def __iter__(self):
        self.iterator = iter(self.data)
        return self

    def __next__(self):
        nxt = next(self.iterator)
        return LcdRecord(list(nxt.values()), keys=list(nxt.keys()))

class SqliteLcd:
    def __init__(self, filename_or_pathlike, table=None):
        import sqlite3
        my_file = Path(filename_or_pathlike)
        if not my_file.is_file():
            raise "File " + filename_or_pathlike + " does not exist"
        con = sqlite3.connect(filename_or_pathlike)
        
        if table:
            self.table = table
        else:
            # Find the first table
            cur = con.cursor()
            res = cur.execute("SELECT name FROM sqlite_master")
            res = res.fetchone()
            cur.close()
            if not res:
                raise "Database " + filename_or_pathlike + " contains no table"
            self.table = res[0]
        self.cur = con.cursor()

    def __iter__(self):
        self.iterator = self.cur.execute("SELECT * FROM " + self.table)
        return self

    def __next__(self):
        nxt = next(self.iterator)
        keys = [x[0] for x in self.cur.description]
        return LcdRecord(nxt, keys=keys)




def import_everything():
    """lcdio will import what is needed based on the type of file
    when specified. But you have the option to call this to import
    everything we might need ahead of time."""
    import csv
    import pyarrow.parquet as pq
    import toml
    import json
    from ruamel.yaml import YAML
    import sqlite3


def selftest():
    testdata = 'testdata/'
    print('csv')
    x=open(testdata + 'names.csv', has_header=True)
    for l in x:
        # Make sure we can access by name, index, or slice without crashing
        foo = l['name']
        foo = l[1]
        foo = l[0:]
    print('tsv')
    x=open(testdata + 'names.tsv', has_header=True)
    for l in x:
        # Make sure we can access by name, index, or slice without crashing
        foo = l['name']
        foo = l[1]
        foo = l[0:]
    print('parquet')
    x=open(testdata + 'delta_byte_array.parquet')
    count = 0
    for l in x:
        (f'{l["c_salutation"]} {l["c_first_name"]} {l["c_last_name"]}')
        count += 1
    assert 1000 == count
    print('toml')
    x=open(testdata + 'implicit-and-explicit-before.toml')
    for l in x:
        l
        l["a"]
        l["a","better"]
    print('json')
    x=open(testdata + 'names.json')
    for l in x:
        (l['name'], l['age'])
        assert l['name'] == l[0]
    print('yaml')
    x=open(testdata + 'names.yaml')
    for l in x:
        (l['name'], l['age'])
        assert l['name'] == l[0]
    print('yaml, array')   
    x=open(testdata + 'names_arr.yaml')
    for doc in x:
        for l in doc['people']:
            (l['name'], l['age'])
    print('sqlite')
    x = open(testdata + 'chinook.db')
    for record in x:
        print(record)
        break
    import_everything()
    return True

if __name__ == "__main__":
    import doctest
    import sys
    result = doctest.testmod()
    if result.failed!=0:
        print('Self-test failed at doctest step.')
        sys.exit(1)
    print('Doctests passed: ' + str(result))
    ok = selftest()
    if not ok:
        print('Self-test failed.')
        sys.exit(1)
    print('All self-tests passed.')
