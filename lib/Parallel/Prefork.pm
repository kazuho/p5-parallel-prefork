package Parallel::Prefork;

use strict;
use warnings;

use base qw/Class::Accessor::Fast/;
use List::Util qw/first/;
use Proc::Wait3;

__PACKAGE__->mk_accessors(qw/max_workers err_respawn_interval trap_signals signal_received manager_pid/);

our $VERSION = '0.01';

sub new {
    my ($klass, $opts) = @_;
    $opts ||= {};
    my $self = bless {
        worker_pids          => {},
        max_workers          => 10,
        err_respawn_interval => 1,
        trap_signals         => {
            TERM => 'TERM',
        },
        signal_received      => undef,
        manager_pid          => undef,
        %$opts,
    }, $klass;
    $SIG{$_} = sub {
        $self->signal_received($_[0]);
    } for keys %{$self->trap_signals};
    $self;
}

sub start {
    my $self = shift;
    
    $self->manager_pid($$);
    
    die 'cannot start another process while you are in child process'
        if $self->{in_child};
    
    # for debugging
    return if $self->{max_workers} == 0;
    
    # main loop
    while (! $self->signal_received) {
        my $pid;
        if (keys %{$self->{worker_pids}} < $self->max_workers) {
            $pid = fork;
            die 'fork error' unless defined $pid;
            unless ($pid) {
                # child process
                $self->{in_child} = 1;
                $SIG{$_} = 'DEFAULT' for keys %{$self->trap_signals};
                exit 0 if $self->signal_received;
                return;
            }
            $self->{worker_pids}{$pid} = 1;
        }
        if (my ($exit_pid, $status) = wait3(! $pid)) {
            delete $self->{worker_pids}{$exit_pid};
            unless ($status == 0) {
                sleep $self->err_respawn_interval;
            }
        }
    }
    # send signals to workers
    if (my $sig = $self->{trap_signals}{$self->signal_received}) {
        $self->signal_all_children($sig);
    }
    
    1; # return from parent process
}

sub finish {
    my ($self, $exit_code) = @_;
    return unless $self->{max_workers};
    exit($exit_code || 0);
}

sub signal_all_children {
    my ($self, $sig) = @_;
    foreach my $pid (sort keys %{$self->{worker_pids}}) {
        kill $sig, $pid;
    }
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
    trap_signals => {
      TERM => TERM,
      HUP  => TERM,
      USR1 => undef,
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
