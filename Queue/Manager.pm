package Spread::Queue::Manager;

=head1 NAME

Spread::Queue::Manager - coordinate one-of-many message delivery

=head1 SYNOPSIS

The provided 'sqm' executable does this:

  use Spread::Queue::Manager;
  my $queue_name = shift @ARGV || die "usage: sqm queue-name";
  my $session = new Spread::Queue::Manager($queue_name);
  $session->run;

=head1 DESCRIPTION

The queue manager is responsible for assigning incoming messages
(see Spread::Queue::Sender) to registered workers (see Spread::Queue::Worker).

When a message comes in, it is assigned to the first available worker,
otherwise it is put into a FIFO queue.

When a worker reports availability, it is sent the first pending message,
otherwise it is put into a FIFO queue.

When a message is sent to a worker, the worker should immediately
acknowledge receipt.  If the worker does not acknowledge, the message
will (eventually) be assigned to another worker.

If a queue manager is already running (detected via Spread group membership
messages), the new sqm should terminate.

=head1 METHODS

=cut

require 5.005_03;
use strict;
use vars qw($VERSION);
$VERSION = '0.3';

use Carp;

use Spread::Session;
use Spread;
use Data::Serializer;

use Spread::Queue::ManagedWorker;
use Spread::Queue::FIFO;

use Log::Channel;

BEGIN {
    my $qmlog = new Log::Channel;
    sub qmlog { $qmlog->(@_) }
}

my $DEFAULT_SQM_HEARTBEAT = 3;

my %Worker;

=item B<new>

  my $session = new Spread::Queue::Manager($queue_name);

Initialize Spread messaging environment, and prepare to act
as the queue manager.  If queue_name is omitted, environment
variable SPREAD_QUEUE will be checked.

=cut

sub new {
    my $proto = shift;
    my $class = ref ($proto) || $proto;

    my %config = @_;
    my $self  = \%config;
    bless ($self, $class);

    $self->{QUEUE} = $ENV{SPREAD_QUEUE} unless $self->{QUEUE};
    croak "Queue name is required" unless $self->{QUEUE};

    $self->{WQNAME} = "WQ_$self->{QUEUE}";
    $self->{MQNAME} = "MQ_$self->{QUEUE}";

    $self->{MQ} = new Spread::Queue::FIFO($self->{MQNAME});
    $self->{WQ} = new Spread::Queue::FIFO($self->{WQNAME});

    $self->{SESSION} = new Spread::Session;
    $self->{SESSION}->callbacks(
				message => \&message_callback,
				timeout => \&timeout_callback,
				admin => \&admin_callback,
			       );
    $self->{SESSION}->subscribe($self->{MQNAME});
    $self->{SESSION}->subscribe($self->{WQNAME});

    $self->{SERIALIZER} = new Data::Serializer(serializer => 'Data::Denter');

    $self->{ACTIVE} = 1;

    return $self;
}

=item B<new>

  $session->run;

Run loop for the queue manager.  Does not return unless interrupted.

=cut

sub run {
    my ($self) = shift;

    my $heartbeat = $ENV{SQM_HEARTBEAT} || $DEFAULT_SQM_HEARTBEAT;

    while ($self->{ACTIVE}) {
	$self->{SESSION}->receive($heartbeat, $self);
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

    $self->_check_worker_queue;
    if (my $available_worker = $self->{WQ}->dequeue) {
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

#    qmlog "WORKER ", $worker->private, " status change: $status\n";

    if ($status eq 'ready') {
	$self->worker_ready($worker);
    } elsif ($status eq 'working') {
	$self->worker_working($worker);
    } elsif ($status eq 'terminate') {
	$self->worker_terminated($worker);
    } else {
	qmlog "**** INVALID STATUS '$status' FROM WORKER $sender ***\n";
    }

    $self->_clear_stuck_workers;
}


sub worker_ready {
    my ($self, $worker) = @_;

    delete $worker->{TASK};

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

    if ($worker->is_assigned) {
	qmlog "WORKER ", $worker->private, " ACKNOWLEDGED\n";
	$worker->acknowledged;
    } else {
	qmlog "WHAT THE HECK IS ", $worker->private, " DOING???\n";
    }
}


sub worker_terminated {
    my ($self, $worker) = @_;

    $self->_dispose($worker);
#    $self->_check_worker_queue;
}


sub dispatch {
    my ($self, $worker, $message) = @_;

    qmlog "DISPATCH MESSAGE FROM $message->{originator} TO ", $worker->private, "\n";

    $self->{SESSION}->publish($worker->private,
			      $self->{SERIALIZER}->serialize($message));
    $worker->{TASK} = $message;
    $worker->assigned;
}


sub timeout_callback {
    my ($self) = shift;

    # scrub workers from the front of the queue
    # who haven't signalled readiness lately

    foreach my $worker ($self->{WQ}->all) {
	qmlog "\t...worker $worker->{PRIVATE} is $worker->{STATUS}\n";
    }

    foreach my $worker ($self->{WQ}->all) {
	if ($worker->is_talking) {
	    # leader looks OK
	    last;
	}
	my $worker = $self->{WQ}->dequeue;
	$self->_dispose($worker);
    }

    $self->_clear_stuck_workers;
}

sub _check_worker_queue {
    my ($self) = shift;

    # scrub workers from the front of the queue
    # who haven't signalled readiness lately

    foreach my $worker ($self->{WQ}->all) {
	if ($worker->is_talking) {
	    # this one is fine
	    return;
	}
	my $worker = $self->{WQ}->dequeue;
	$self->_dispose($worker);
    }
}

sub _dispose {
    my ($self, $worker) = @_;

    qmlog "WORKER ", $worker->private, " TERMINATED\n";

    # reassign the task, and retire the worker
    my $task = $worker->{TASK};
    if ($task) {
	qmlog "Reassigning stuck message\n";
	$self->handle_message($task->{originator},
			      $task->{body});
    }
    delete $worker->{TASK};
    $worker->terminated;
}

sub _clear_stuck_workers {
    my $self = shift;

    foreach my $worker (values %Worker) {
	if ($worker->is_stuck) {
	    qmlog "WORKER ", $worker->private, " IS STUCK\n";
	    $self->_dispose($worker);
	}
    }
}

# Called for Spread admin messages - in particular, changes in
# group membership.  There should only be one listener subscribed
# to the MQ_ and WQ_ groups for this queue.

sub admin_callback {
    my ($service_type, $sender, $groups, $message, $self) = @_;

    if ($service_type & REG_MEMB_MESS) {
#	qmlog "New member(s) for $sender: ", join(",", @$groups), "\n";
	foreach my $group (@$groups) {
	    if ($group ne $self->{SESSION}->{PRIVATE_GROUP}) {
		if (!$self->{INCUMBENT}) {
		    carp "Duplicate sqm $group detected for $self->{QUEUE}; aborting";
		    $self->{ACTIVE} = 0;
		} else {
		    carp "Duplicate sqm $group detected for $self->{QUEUE}; other should abort";
		}
	    }
	}
	$self->{INCUMBENT} = 1;
    }
}

1;


=head1 AUTHOR

Jason W. May <jmay@pobox.com>

=head1 COPYRIGHT

Copyright (C) 2002 Jason W. May.  All rights reserved.
This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

The license for the Spread software can be found at 
http://www.spread.org/license

=head1 SEE ALSO

  L<Spread::Session>
  L<Spread::Queue::FIFO>
  L<Spread::Queue::Sender>
  L<Spread::Queue::Worker>
  L<Spread::Queue::ManagedWorker>
  L<Data::Serializer>

=cut
