package Spread::Queue::Sender;

require 5.005_03;
use strict;
use vars qw($VERSION);
$VERSION = '0.1';

use Spread::Session;
use Data::Serializer;

=head1 NAME

  Spread::Queue::Sender - send data via Spread to a worker queue

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 METHODS

=cut

my $DEFAULT_TIMEOUT = 5;

sub new {
    my $proto = shift;
    my $class = ref ($proto) || $proto;

    my $queue = shift;

    # configuration options: override default timeout

    my $self  = {};
    bless ($self, $class);

    $self->{QUEUE} = $queue;
    $self->{MQNAME} = "MQ_$queue";

    my $session = new Spread::Session;
    $self->{SESSION} = $session;
    $session->callbacks(
			message => \&message_callback,
			timeout => \&timeout_callback,
		       );

    $self->{SERIALIZER} = new Data::Serializer(serializer => 'Data::Denter');

    return $self;
}


sub submit {
    my ($self, $method, $payload) = @_;

    my $request = {
		   method => $method,
		   payload => $payload,
		  };
    my $content = $self->{SERIALIZER}->serialize($request);
    $self->{SESSION}->publish($self->{MQNAME},
			      $content);
}


sub receive {
    my $self = shift;
    my $timeout = shift || $DEFAULT_TIMEOUT;

    my $data;
    for (;;) {
	$data = $self->{SESSION}->receive($timeout, $self);
	last if $data->{status} eq 'response';
    }

    return $data->{body};
}

=item B<rpc>

  my $response = $sender->rpc($remote_method, $data [, $timeout]);

RPC-style invocation of a remote operation.  Waits $timeout seconds
for a response (returns undef if no response arrives).

=cut

sub rpc {
    my ($self, $method, $payload, $timeout) = @_;

    $self->submit($method, $payload);
    return $self->receive($timeout);
}


sub message_callback {
    my ($sender, $groups, $message, $self) = @_;

    my $data = $self->{SERIALIZER}->deserialize($message);
    return $data;
}

sub timeout_callback {
    my ($self) = @_;

    return;
}

1;
