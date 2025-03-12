# node-streaming

This project demonstrates the bug with a program that is trying to stream data but is exiting before
all the data has been processed.

The input is a file with millions of account numbers. Sample data can be generated via the following command. The command
generates ~12,000,000 account numbers for input

```shell
$ seq 200000000 212000000 > account_numbers.txt
```

The script can be run with the following command

```shell
$ BATCH_SIZE=100 node ./backfill-contract-events.js ./account-numbers.txt data.sql backfill-contract-events.log
```

To debug, add the `--inspect-brk` flag to `node`

The design of the script is

1. Read chars of data from the input file
2. Convert those chars into lines of account numbers by splitting on a newline. The last element of the array is
   considered incomplete (until the stream is flushed) and is left for subsequent processing. Lines are pushed through
   the pipeline.
3. Lines are grouped into batches according to the value of the `BATCH_SIZE` env var (eg: 100). Batches are pushed
   through the pipeline for processing.
4. When a batch is transformed, it is to be printed. For simplicity, the script just prints the input account numbers as
   transformation is not the issue, it is handling backpressure between the streams.

All the streams handle the return value from `push` that is, if `false` is returned, then they wait for an event before
continuing processing.

When running the script, it will exit with a 0 exit code, yet looking in the `data.sql` file, there will only be a
subset of the input data (eg: 3,300 lines).

For some reason, the script is exiting before all the data is successfully processed. If all the data was processed then
the amount of lines in the output would match the input.

Running the following command should yield no differences

```shell
$ diff -q account_numbers.txt data.sql
```
