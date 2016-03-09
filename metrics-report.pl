#!/usr/bin/perl

use strict;
use warnings;
use DBI;

use Time::Duration;
use POSIX qw(strftime);

use Getopt::Std;

our($opt_i);
getopts('i:');
my $interval = $opt_i || '1 day';

my $dbh = DBI->connect("DBI:mysql:database=metrics", "root");

# Get a list of job_ids
my @jobs;
my $sth = $dbh->prepare("select JOB_ID,(TIME_FINISHED-TIME_STARTED),JOB_NAME,TIME_SUBMITTED,TIME_STARTED,TIME_FINISHED from JOB where CREATED > date_sub(now(), interval $interval) and time_finished is not NULL order by (TIME_FINISHED - TIME_STARTED) DESC"); 
$sth->execute();
while( my @row = $sth->fetchrow_array() ) {
	push @jobs, {
		Job_ID => $row[0],
		Duration_MS => $row[1],
		Job_Name => $row[2],
		Time_Submitted => $row[3],
		Time_Started => $row[4],
		Time_Finished => $row[5],
	};

}

my $avg = 0;
my $std = 0;
my $stm = 1.96; # "standard multiplier"

foreach my $job_ref (@jobs) {
	my $job = $job_ref->{Job_ID};
	my $job_name = $job_ref->{Job_Name};
	my $duration_ms = $job_ref->{Duration_MS};
	my $time_submitted = $job_ref->{Time_Submitted};
	my $time_started = $job_ref->{Time_Started};
	my $time_finished = $job_ref->{Time_Finished};
	my $maps_rack_local = 0;
	my $maps_total_launched = 0;
	my $reduces_total_launched = 0;

	# See: http://answers.mapr.com/answers/167354/view.html
	$job_name =~ s/^\[\w+\/\w+\]//; # Trim [blahblah/blahblah]

	printf "\nJOB: %s %s\n", $job, $job_name;
	printf "%-40s %10s  %5s\n", "SUBMITTED AT:",
		strftime("%Y-%m-%d", localtime($time_submitted/1000)),
		strftime("%H:%M", localtime($time_submitted/1000));
	printf "%-40s %10s  %5s %5s\n", "TOTAL TIME:",
		concise(duration($duration_ms/1000)),
		strftime("%H:%M", localtime($time_started/1000)),
		strftime("%H:%M", localtime($time_finished/1000));

	$maps_rack_local = $dbh->selectrow_array("select ATTR_VALUE from JOB_ATTRIBUTES where JOB_ID='$job' and ATTR_NAME = 'RACK_LOCAL_MAPS'") || 0;
	$maps_total_launched = $dbh->selectrow_array("select ATTR_VALUE from JOB_ATTRIBUTES where JOB_ID='$job' and ATTR_NAME = 'TOTAL_LAUNCHED_MAPS'") || 0;
	$reduces_total_launched = $dbh->selectrow_array("select ATTR_VALUE from JOB_ATTRIBUTES where JOB_ID='$job' and ATTR_NAME = 'TOTAL_LAUNCHED_REDUCES'") || 0;

	$sth = $dbh->prepare("select JOB_ID,AVG(TIME_FINISHED - TIME_STARTED) as 'MS',STD(TIME_FINISHED - TIME_STARTED) as 'STD' from TASK_ATTEMPT where JOB_ID = '$job' and TYPE = 'MAP' and STATUS='SUCCEEDED' group by JOB_ID");
	$sth->execute();
	while( my $ref = $sth->fetchrow_hashref() ) {
		$avg = $ref->{'MS'};
		$std = $ref->{'STD'};
#		printf "%-40s %10s  %d/%d local/launched\n", "MAP AVERAGE:",
#			concise(duration($avg/1000)), $maps_rack_local, $maps_total_launched;
		printf "%-40s %10s %6d launched\n", "MAP AVERAGE:",
			concise(duration($avg/1000)), $maps_rack_local, $maps_total_launched;
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
		printf "%-40s %10s %6d launched\n", "REDUCE AVERAGE:",
			concise(duration($avg/1000)), $reduces_total_launched;
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
