#!/usr/bin/perl

use strict;
use Test::Simple tests => 1;

my $qname = "testq";

# launch sqm
my $sqm_pid;
if ($sqm_pid == fork) {
    # parent
} else {
    # launch queue manager

    my $PERLLIB = join(":", @INC);
    exec "PERLLIB=$PERLLIB ./sqm $qname";
}

# launch worker
my $worker_pid;
if ($worker_pid == fork) {
    # parent
} else {
    # launch worker

    use Spread::Queue::Worker;
    my $worker = new Spread::Queue::Worker($qname);
    $worker->callbacks(
		       myfunc => \&worker_myfunc,
		      );
    $worker->run;
    exit;
}

sleep 3; # wait for the others to start

# this is the sender

use Spread::Queue::Sender;
my $sender = new Spread::Queue::Sender($qname);
my $remote_method = "myfunc";
my $data = { name => 'value' };
$sender->submit($remote_method, $data);
my $response = $sender->receive;

ok($response->{response} eq "I heard you!", 'end-to-end');

sleep 3;

######################################################################

kill 15, $worker_pid;
kill 15, $sqm_pid;

exit;

######################################################################

sub worker_myfunc {
    my ($worker, $originator, $input) = @_;

    sleep 1;

    my $output = {
		  response => "I heard you!",
		 };
    $worker->respond($originator, $output);
    return $output;
}
