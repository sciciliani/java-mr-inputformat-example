# Mapreduce Java InputFormat Class Example

## Explanation

Mapreduce default behavior is to treat every line (\n) as a record.
If you are planning to use multi-line file format (like json) you can implement an InputFormat reader to detail the begining and end of every record.

Your reader will receive the file as a buffer (byte by byte) and you would have to return the key and value.

## Example case

This example will process the following record format and return a CSV file.

```
{
    "date": "16-11-02 19:14:1478128457",
    "type": "C",
    "id": 0,
    "user": 5554
}
{
    "date": "16-11-02 19:14:1478128457",
    "type": "A",
    "id": 3,
    "user": 1234
}
```

## Additional considerations

* The JSON file is *HUGE* (TBs!). Otherwise a simple Python script would be enough to pre-process the file.
* There is no reduce operation. We just want to transform the file as it is.

## Usage

`hadoop jar JsonMapReduce.jar [path-to-json-file] [path-to-csv-result-dir]`


