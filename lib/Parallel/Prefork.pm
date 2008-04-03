package Parallel::Prefork;

use strict;
use warnings;

use base qw/Class::Accessor::Fast/;
use Proc::Wait3;

__PACKAGE__->mk_accessors(qw/max_workers fork_delay trap_signals signals_received/);

our $VERSION = '0.01';

sub new {
    my ($klass, $opts) = @_;
    $opts ||= {};
    my $self = bless {
        worker_pids      => {},
        max_workers      => 10,
        fork_delay       => 1,
        trap_signals     => [ qw/TERM/ ],
        signals_received => [],
        %$opts,
    }, $klass;
    $SIG{$_} = sub {
        push @{$self->signals_received}, $_[0];
    } for @{$self->trap_signals};
    $self;
}

sub start {
    my $self = shift;
    
    die 'cannot start another process while you are in child process'
        if $self->{in_child};
    
    # for debugging
    return if $self->{max_workers} == 0;
    
    # main loop
    while (! @{$self->signals_received}) {
        if (keys %{$self->{worker_pids}} < $self->max_workers) {
            my $pid = fork;
            die 'fork error' unless defined $pid;
            unless ($pid) {
                # child process
                $self->{in_child} = 1;
                $SIG{$_} = 'DEFAULT' for @{$self->trap_signals};
                exit 0 if @{$self->signals_received};
                return;
            }
            $self->{worker_pids}{$pid} = 1;
            sleep $self->fork_delay
                if $self->fork_delay;
        } elsif (my ($pid) = wait3(1)) {
            delete $self->{worker_pids}{$pid};
        }
    }
    # send SIGTERM to all workers
    foreach my $pid (sort keys %{$self->{worker_pids}}) {
        kill 'TERM', $pid;
    }
    
    1; # return from parent process
}

sub finish {
    my ($self, $exit_code) = @_;
    return unless $self->{max_workers};
    exit($exit_code || 0);
}

sub wait_all_children {
    my $self = shift;
    while (%{$self->{worker_pids}}) {
        if (my $pid = wait) {
            delete $self->{worker_pids}{$pid};
        }
    }
}

1;

__END__

=head1 NAME
Parallel::Prefork - A simple prefork server framework

=head1 SYNOPSIS

  use Parallel::Prefork;
  
  my $pm = Parallel::Prefork->new({
    max_workers  => 10,
    fork_delay   => 1,
    trap_signals => [ qw/TERM HUP/ ],
  });
  
  while ($pm->signal_received ne 'TERM') {
    load_config();
    $pm->start and next;
    
    ... do some work within the child process ...
    
    $pm->finish;
  }
  
  $pm->wait_all_children();

=head1 DESCRIPTION

C<Parallel::Prefork> is much like C<Parallel::ForkManager>, but supports graceful shutdown and run-time reconfiguration.

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

See http://www.perl.com/perl/misc/Artistic.html

=cut
