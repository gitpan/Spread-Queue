package Spread::Queue::Manager;

require 5.005_03;
use strict;
use vars qw($VERSION);
$VERSION = '0.2';

use Spread::Session;
use Data::Serializer;

use Spread::Queue::ManagedWorker;
use Spread::Queue::FIFO;

use Log::Channel;

BEGIN {
    my $qmlog = new Log::Channel;
    sub qmlog { $qmlog->(@_) }
}


my %Worker;

sub new {
    my $proto = shift;
    my $class = ref ($proto) || $proto;

    my $queue = shift;

    my $self  = {};
    bless ($self, $class);

    $self->{QUEUE} = $queue;
    $self->{WQNAME} = "WQ_$queue";
    $self->{MQNAME} = "MQ_$queue";

    $self->{MQ} = new Spread::Queue::FIFO($self->{MQNAME});
    $self->{WQ} = new Spread::Queue::FIFO($self->{WQNAME});

    $self->{SESSION} = new Spread::Session;
    $self->{SESSION}->callbacks(
				message => \&message_callback,
				timeout => \&timeout_callback,
			       );
    $self->{SESSION}->subscribe($self->{MQNAME});
    $self->{SESSION}->subscribe($self->{WQNAME});

    $self->{SERIALIZER} = new Data::Serializer(serializer => 'Data::Denter');

    return $self;
}

sub run {
    my ($self) = shift;

    for (;;) {
	$self->{SESSION}->receive(2, $self);
    }
}


sub message_callback {
    my ($sender, $groups, $message, $self) = @_;

    if (grep { $_ eq $self->{MQNAME} } @$groups) {
	$self->handle_message($sender, $message);
    } elsif (grep { $_ eq $self->{WQNAME} } @$groups) {
	$self->handle_worker($sender, $message);
    }
}


sub handle_message {
    my ($self, $sender, $message) = @_;

    my $available_worker = $self->{WQ}->dequeue;
    if ($available_worker && $self->dispatchable()) {
	$self->dispatch($available_worker, {
					    originator => $sender,
					    body => $message
					   });
    } else {
	qmlog "ENQUEUE MESSAGE FROM $sender\n";
	$self->{MQ}->enqueue({
			      originator => $sender,
			      body => $message
			     });
    }
}


sub handle_worker {
    my ($self, $sender, $message) = @_;

    my $data = $self->{SERIALIZER}->deserialize($message);
    my $status = $data->{status};

    my $worker = $Worker{$sender};
    if (!$worker) {
	$worker = new Spread::Queue::ManagedWorker($sender);
	$Worker{$sender} = $worker;
    }

    if ($status eq 'ready') {
	$self->worker_ready($worker);
    } elsif ($status eq 'working') {
	$self->worker_working($worker);
    } elsif ($status eq 'terminate') {
	$self->worker_terminated($worker);
    } else {
	qmlog "**** INVALID STATUS FROM WORKER $sender ***\n";
    }
}


sub worker_ready {
    my ($self, $worker) = @_;

    my $pending_message = $self->{MQ}->dequeue;
    if ($pending_message) {
	$self->dispatch($worker, $pending_message);
    } else {
	if ($worker->is_ready) {
	    qmlog "WORKER ", $worker->private, " ALREADY PENDING\n";
	} else {
	    qmlog "WORKER ", $worker->private, " IS PENDING\n";
	    $self->{WQ}->enqueue($worker);
	}
	$worker->ready;
    }
}


sub worker_working {
    my ($self, $worker) = @_;

    if ($worker->is_working) {
	qmlog "WORKER ", $worker->private, " ACKNOWLEDGED\n";
	$worker->acknowledged;
    } else {
	qmlog "WHAT THE HECK IS ", $worker->private, " DOING???\n";
    }
}


sub worker_terminated {
    my ($self, $worker) = @_;

    qmlog "WORKER ", $worker->private, " TERMINATED\n";
    $worker->terminated;
    $self->dispatchable();
}


sub dispatch {
    my ($self, $worker, $message) = @_;

    qmlog "DISPATCH MESSAGE FROM $message->{originator} TO ", $worker->private, "\n";

    $self->{SESSION}->publish($worker->private,
			      $self->{SERIALIZER}->serialize($message));

    $worker->working;
}


sub timeout_callback {
    my ($self) = shift;

    # scrub workers from the front of the queue
    # who haven't signalled readiness lately

    foreach my $worker ($self->{WQ}->all) {
	if ($worker->is_talking) {
	    # leader looks OK
	    return;
	}
	my $worker = $self->{WQ}->dequeue;
	qmlog "REMOVED SILENT PENDING WORKER ", $worker->private, "\n";
    }
}

sub dispatchable {
    my ($self) = shift;

    # scrub worker from the front of the queue
    # who haven't signalled readiness lately

    foreach my $worker ($self->{WQ}->all) {
	if ($worker->is_talking) {
	    # leader looks OK
	    return 1;
	}
	my $worker = $self->{WQ}->dequeue;
	qmlog "REMOVED SILENT PENDING WORKER ", $worker->private, "\n";
    }
    return;
}

1;
