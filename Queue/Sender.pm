package Spread::Queue::Sender;

=head1 NAME

Spread::Queue::Sender - submit messages to Spread::Queue message queues

=head1 SYNOPSIS

  use Spread::Queue::Sender;

  my $sender = new Spread::Queue::Sender(QUEUE => "myqueue");

  $sender->submit({ name => "value" });
  my $response = $sender->receive;

or

  my $response = $sender->rpc({ name => "value" });

=head1 DESCRIPTION

A Spread::Queue::Sender can submit messages for queued delivery to
the first available Spread::Queue::Worker.  The sqm queue manager
must be running to receive and route messages.

Spread::Queue messages are serialized Perl hashes.
Spread::Queue does not enforce structure on message contents.

=head1 METHODS

=cut

require 5.005_03;
use strict;
use vars qw($VERSION);
$VERSION = '0.3';

use Spread::Session;
use Data::Serializer;
use Carp;
use Log::Channel;

my $DEFAULT_TIMEOUT = 5;

BEGIN {
    my $sqslog = new Log::Channel;
    sub sqslog { $sqslog->(@_) }
}

=item B<new>

  my $sender = new Spread::Queue::Sender("myqueue");

=cut

sub new {
    my $proto = shift;
    my $class = ref ($proto) || $proto;

    my %config = @_;
    my $self  = \%config;
    bless ($self, $class);

    # configuration options: override default timeout

    $self->{QUEUE} = $ENV{SPREAD_QUEUE} unless $self->{QUEUE};
    croak "Queue name is required" unless $self->{QUEUE};

    $self->{MQNAME} = "MQ_$self->{QUEUE}";

    my $session = new Spread::Session;
    $self->{SESSION} = $session;
    $session->callbacks(
			message => \&_message_callback,
			timeout => \&_timeout_callback,
		       );

    $self->{SERIALIZER} = new Data::Serializer(serializer => 'Data::Denter');

    sqslog "Message queue submitter initialized on $self->{QUEUE}\n";

    return $self;
}


sub submit {
    my ($self, $payload) = @_;

    my $content = $self->{SERIALIZER}->serialize($payload);
    $self->{SESSION}->publish($self->{MQNAME},
			      $content);
}


sub receive {
    my $self = shift;
    my $timeout = shift || $DEFAULT_TIMEOUT;

#    my $data;
#    for (;;) {
#	$data = $self->{SESSION}->receive($timeout, $self);
#	return if !$data;
#	last if ref $data && $data->{status} eq 'response';
#    }
#    return $data->{body};

    my $data = $self->{SESSION}->receive($timeout, $self);
#    return unless ref $data && $data->{status} eq 'response';
#    return $data->{body};
    return $data;
}

=item B<rpc>

  my $response = $sender->rpc($data [, $timeout]);

RPC-style invocation of a remote operation.  Waits $timeout seconds
for a response (returns undef if no response arrives).

=cut

sub rpc {
    my ($self, $payload, $timeout) = @_;

    $self->submit($payload);
    return $self->receive($timeout);
}


sub _message_callback {
    my ($sender, $groups, $message, $self) = @_;

    my $data = $self->{SERIALIZER}->deserialize($message);
    return $data;
}

sub _timeout_callback {
    my ($self) = @_;

    return;
}

1;
