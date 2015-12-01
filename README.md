# mapr-metrics-report
A Report Tool for MapR which queries the metrics database

The idea is to look at MapR jobs run over the past 24 hours, give a
quick summary of each, and call out any hosts that showed unusually slow
or fast performance.  I'm a SysAdmin so I want to get a sense for
problem hosts.  I think I can expand this a bit more to provide some
feedback to the devs, like tuning the number of map slots, &c.

Sample output snippet:

```
JOB: job_201510281311_0649 com.example.NameOfJob

TOTAL TIME:                                  13m29s
MAP AVERAGE:                                    55s
               c24-mtv-04-27.example.com       6m7s <-- SLOW HOST!
               c24-mtv-04-39.example.com      6m23s <-- SLOW HOST!
               c24-mtv-04-36.example.com       7m9s <-- SLOW HOST!
                     mapr-04.example.com      9m40s <-- SLOW HOST!
               c24-mtv-04-40.example.com      9m41s <-- SLOW HOST!
REDUCE AVERAGE:                               1m33s
```

## Set Up MapR Metrics Database

See: http://doc.mapr.com/display/MapR/Setting+up+the+MapR+Metrics+Database

## Hack Metrics Database to Fix Job Names

See: http://answers.mapr.com/answers/167354/view.html

## Use the Script

I run this locally on my mapr metrics host, which allows root without a
password.  Modify this line to suit:

`my $dbh = DBI->connect("DBI:mysql:database=metrics", "root");`

I put it in a nightly cron, like so:

```
@daily  /usr/bin/perl /home/djh/mapr-metrics-report/metrics-report.pl
```
