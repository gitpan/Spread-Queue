package Spread::Queue::Worker;

require 5.005_03;
use strict;
use vars qw($VERSION);
$VERSION = '0.1';

use Spread::Session;
use Data::Serializer;
use Log::Channel;

BEGIN {
    my $sqwlog = new Log::Channel;
    sub sqwlog { $sqwlog->(@_) }
}

sub new {
    my $proto = shift;
    my $class = ref ($proto) || $proto;

    my $queue = shift;

    my $self  = {};
    bless ($self, $class);

    $self->{QUEUE} = $queue;
    $self->{WQNAME} = "WQ_$queue";

    my $session = new Spread::Session;
    $self->{SESSION} = $session;
    $session->callbacks(
			message => \&message_callback,
			timeout => \&timeout_callback,
		       );

    $self->{SERIALIZER} = new Data::Serializer(serializer => 'Data::Denter');

    return $self;
}

sub callbacks {
    my ($self) = shift;

    my (%callbacks) = @_;
    while (my ($name, $coderef) = each %callbacks) {
	sqwlog "Registering $name as $coderef\n";
	$self->{CALLBACKS}->{$name} = $coderef;
    }
}


sub run {
    my ($self) = shift;

    $self->{SESSION}->publish($self->{WQNAME},
			      $self->{SERIALIZER}->serialize({ status => 'ready' }));

    for (;;) {
	$self->{SESSION}->receive(2, $self);

	last if $self->{TERMINATED};
    }
}


sub message_callback {
    my ($sender, $groups, $message, $self) = @_;

    # set status with the queue manager
    $self->{SESSION}->publish($self->{WQNAME},
			      $self->{SERIALIZER}->serialize( { status => 'working' }));

    my $content = $self->{SERIALIZER}->deserialize($message);

    # end-to-end delivery acknowledgement back to the originator
    $self->{SESSION}->publish($content->{originator},
			      $self->{SERIALIZER}->serialize({ status => 'working' }));


    my $body = $self->{SERIALIZER}->deserialize($content->{body});
    my $method = $body->{method};
    my $payload = $body->{payload};

    my $callback = $self->{CALLBACKS}->{$method};

    # error handling if callback not defined

    die "Unknown method $method" if !defined $callback;

    # invocation should be in eval {}

    $callback->($self,
		$content->{originator},
		$payload);
}

sub timeout_callback {
    my ($self) = @_;

    return if $self->{TERMINATED};

    sqwlog "Signal ready on $self->{QUEUE}\n";
    $self->{SESSION}->publish($self->{WQNAME},
			      $self->{SERIALIZER}->serialize({ status => 'ready' }));
}

sub disable {
    my ($self) = @_;

    sqwlog "Disabling $self->{QUEUE}\n";
    $self->{SESSION}->publish($self->{WQNAME},
			      $self->{SERIALIZER}->serialize({ status => 'terminate' }));
    $self->{TERMINATED}++;
}

sub respond {
    my ($self, $originator, $payload) = @_;

    my $response = {
		    status => 'response',
		    body => $payload,
		   };
    $self->{SESSION}->publish($originator,
			      $self->{SERIALIZER}->serialize($response));
    $self->{SESSION}->publish($self->{WQNAME},
			      $self->{SERIALIZER}->serialize({ status => 'ready' }));
}

1;
