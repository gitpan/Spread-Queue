package Spread::Queue::ManagedWorker;

use strict;


sub new {
    my $proto = shift;
    my $class = ref ($proto) || $proto;

    my $self  = {};
    bless ($self, $class);

    $self->{PRIVATE} = shift;

    return $self;
}

sub private {
    my ($self) = shift;

    return $self->{PRIVATE};
}

sub status {
    my ($self) = shift;

    return $self->{STATUS};
}

sub is_ready {
    my ($self) = shift;

    return $self->{STATUS} eq "ready";
}

sub is_working {
    my ($self) = shift;

    return $self->{STATUS} eq "working";
}

sub ready {
    my ($self) = shift;

    $self->{STATUS} = "ready";
    $self->{LAST_PING} = time;
}

sub working {
    my ($self) = shift;

    $self->{STATUS} = "working";
    delete $self->{LAST_PING};
}

sub acknowledged {
    my ($self) = shift;

    $self->{STATUS} = "ack";
    delete $self->{LAST_PING};
}

sub terminated {
    my ($self) = shift;

    $self->{STATUS} = "dead";
    delete $self->{LAST_PING};
}

sub is_talking {
    my ($self) = shift;

    return $self->{LAST_PING} > time-5;
}


1;
