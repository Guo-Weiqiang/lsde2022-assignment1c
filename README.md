# Assignment 1c – Spark

See https://event.cwi.nl/lsde/2022/dbcloud.shtml

## Testing the solutions

To test solutions, run the following commands. Note that the execution may take minutes per query, therefore you may want to limit the number of lines in `queries-test.csv`.

```bash
time python3 reorg.py /opt/lsde/dataset-sf100-csvs/
time python3 cruncher.py /opt/lsde/dataset-sf100-csvs/ queries-test.csv out.csv
```

Compare the results with the expected output using:

```bash
diff queries-test-output-sf100-all.csv out.csv
```

The submission system will perform the same operations (with different queries).

## Jupyter notebook

There is a Jupyter notebook available (`assignment-1c.ipynb`) to run interactive experiments in your browser. To run it, issue:

```bash
jupyter notebook
```

And use the resulting URL on 127.0.0.1 (`http://127.0.0.1:8888?token=...`). This URL will also work in the host operating system due to the port forwarding set up in the virtual machines.

:warning: Be aware that the submission system only uses the `reorg.py` and `cruncher.py` files.

## Data set

This assignment only uses the SF100 data set, which is stored in CSV format (before reorg).


#
A piece of advice: for this assignment, it’s recommended to avoid the DATE type in DuckDB’s schema. Timezone differences between your development machine and the leaderboard machine can cause differences in the results — see this thread for details: https://lsde2022.slack.com/archives/C03Q3SDPC3D/p1664209357234319
e.g. 2022-09-26 00:00 CEST is 2022-09-25 22:00 UTC
