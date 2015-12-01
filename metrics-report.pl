#!/usr/bin/perl

use strict;
use warnings;
use DBI;

use Time::Duration;

my $dbh = DBI->connect("DBI:mysql:database=metrics", "root");

# Get a list of job_ids
my @jobs;
my $sth = $dbh->prepare('select JOB_ID,(TIME_FINISHED-TIME_STARTED),JOB_NAME from JOB where CREATED > date_sub(now(), interval 1 day) and time_finished is not NULL'); 
$sth->execute();
while( my @row = $sth->fetchrow_array() ) {
	push @jobs, { Job_ID => $row[0], Duration_MS => $row[1], Job_Name => $row[2] };
}

my $avg = 0;
my $std = 0;
my $stm = 1.96; # "standard multiplier"

foreach my $job_ref (@jobs) {
	my $job = $job_ref->{Job_ID};
	my $job_name = $job_ref->{Job_Name};
	my $duration_ms = $job_ref->{Duration_MS};

	# See: http://answers.mapr.com/answers/167354/view.html
	$job_name =~ s/^\[\w+\/\w+\]//; # Trim [blahblah/blahblah]

	printf "\nJOB: %s %s\n", $job, $job_name;
	printf "%-40s %10s\n", "TOTAL TIME:", concise(duration($duration_ms/1000));

	$sth = $dbh->prepare("select JOB_ID,AVG(TIME_FINISHED - TIME_STARTED) as 'MS',STD(TIME_FINISHED - TIME_STARTED) as 'STD' from TASK_ATTEMPT where JOB_ID = '$job' and TYPE = 'MAP' and STATUS='SUCCEEDED' group by JOB_ID");
	$sth->execute();
	while( my $ref = $sth->fetchrow_hashref() ) {
		$avg = $ref->{'MS'};
		$std = $ref->{'STD'};
		printf "%-40s %10s\n", "MAP AVERAGE:", concise(duration($avg/1000));
	}

	my $rs = $dbh->selectall_arrayref("select JOB_ID,HOST,AVG(TIME_FINISHED - TIME_STARTED) as 'MS' from TASK_ATTEMPT where JOB_ID = '$job' and TYPE = 'MAP' and STATUS='SUCCEEDED' group by HOST order by MS");

	foreach my $row (@$rs) {
		if( @$row[2] and (@$row[2] < ($avg - $std * $stm)) ) {
			printf "%40s %10s <-- FAST HOST!\n", @$row[1], concise(duration(@$row[2]/1000));
		}
		if( @$row[2] and (@$row[2] > ($avg + $std * $stm)) ) {
			printf "%40s %10s <-- SLOW HOST!\n", @$row[1], concise(duration(@$row[2]/1000));
		}
	}

	$sth = $dbh->prepare("select JOB_ID,AVG(TIME_FINISHED - TIME_STARTED) as 'MS',STD(TIME_FINISHED - TIME_STARTED) as 'STD' from TASK_ATTEMPT where JOB_ID = '$job' and TYPE = 'REDUCE' and STATUS='SUCCEEDED' group by JOB_ID");
	$sth->execute();
	while( my $ref = $sth->fetchrow_hashref() ) {
		$avg = $ref->{'MS'};
		$std = $ref->{'STD'};
		printf "%-40s %10s\n", "REDUCE AVERAGE:", concise(duration($avg/1000));
	}

	$rs = $dbh->selectall_arrayref("select JOB_ID,HOST,AVG(TIME_FINISHED - TIME_STARTED) as 'MS' from TASK_ATTEMPT where JOB_ID = '$job' and TYPE = 'REDUCE' and STATUS='SUCCEEDED' group by HOST order by MS");

	foreach my $row (@$rs) {
		if( @$row[2] and (@$row[2] < ($avg - $std * $stm)) ) {
			printf "%40s %10s <-- FAST HOST!\n", @$row[1], concise(duration(@$row[2]/1000));
		}
		if( @$row[2] and (@$row[2] > ($avg + $std * $stm)) ) {
			printf "%40s %10s <-- SLOW HOST!\n", @$row[1], concise(duration(@$row[2]/1000));
		}
	}

}

$sth->finish();

$dbh->disconnect();
