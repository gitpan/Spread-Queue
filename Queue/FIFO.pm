package Spread::Queue::FIFO;

require 5.005_03;
use strict;
use vars qw($VERSION);
$VERSION = '0.3';

use Log::Channel;

=head1 NAME

Spread::Queue::FIFO - basic FIFO queue

=head1 SYNOPSIS

  use Spread::Queue::FIFO;

  my $q = new Spread::Queue::FIFO ("to-do list");
  enqueue $q, "eat breakfast", "go to work";
  my $task = dequeue $q;

=head1 DESCRIPTION

Basic FIFO queue service.  Not thread-safe.

Logging via Log::Channel.

=head1 METHODS

=cut

BEGIN {
    my $qlog = new Log::Channel;
    sub qlog { $qlog->(@_) }
}

=item B<new>

  my $q = new Spread::Queue::FIFO ("to-do list");

Creates a named FIFO queue.  Name will be included in each log message.

=cut

sub new {
    my $proto = shift;
    my $class = ref ($proto) || $proto;

    my $self  = {};
    bless ($self, $class);

    $self->{NAME} = shift;	# optional
    $self->{QUEUE} = [];

    return $self;
}

=item B<enqueue>

  enqueue $q, "eat breakfast", "go to work";

Append one or more items to the end of a queue.

=cut

sub enqueue {
    my $self = shift;

    qlog "enqueue $self->{NAME}\n";

    push (@{$self->{QUEUE}}, @_);
}

=item B<dequeue>

  my $task = dequeue $q;

Remove the first item from the front of the queue and return it.

=cut

sub dequeue {
    my $self = shift;

    qlog "dequeue $self->{NAME}\n";

    return shift @{$self->{QUEUE}};
}

=item B<pending>

  my $tasks = $q->pending;

Retrieve number of items in the queue.

=cut

sub pending {
    my $self = shift;

    return scalar @{$self->{QUEUE}};
}

=item B<pending>

 foreach my $item ($q->all) { ... }

Return the queue contents as a list, for inspection.

=cut

sub all {
    my $self = shift;

    return @{$self->{QUEUE}};
}

1;


=head1 AUTHOR

Jason W. May <jmay@pobox.com>

=head1 COPYRIGHT

Copyright (C) 2002 Jason W. May.  All rights reserved.
This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

=head1 SEE ALSO

  L<Spread::Queue>

=cut
