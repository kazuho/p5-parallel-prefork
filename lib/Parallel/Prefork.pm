package Parallel::Prefork;

use strict;
use warnings;

use base qw/Class::Accessor::Fast/;
use List::Util qw/first/;
use Proc::Wait3;

__PACKAGE__->mk_accessors(qw/max_workers err_respawn_interval trap_signals signal_received manager_pid on_child_reap/);

our $VERSION = '0.05';

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
        signal_received      => '',
        manager_pid          => undef,
        generation           => 0,
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
    $self->signal_received('');
    $self->{generation}++;
    
    die 'cannot start another process while you are in child process'
        if $self->{in_child};
    
    # for debugging
    return if $self->{max_workers} == 0;
    
    # main loop
    while (! $self->signal_received) {
        my $pid;
        if (keys %{$self->{worker_pids}} < $self->max_workers) {
            $pid = fork;
            unless (defined $pid) {
                warn "fork failed:$!";
                sleep $self->err_respawn_interval;
                next;
            }
            unless ($pid) {
                # child process
                $self->{in_child} = 1;
                $SIG{$_} = 'DEFAULT' for keys %{$self->trap_signals};
                exit 0 if $self->signal_received;
                return;
            }
            $self->{worker_pids}{$pid} = $self->{generation};
        }
        if (my ($exit_pid, $status) = wait3(! $pid)) {
            $self->_run_child_reap_cb( $exit_pid, $status );

            if (delete($self->{worker_pids}{$exit_pid}) == $self->{generation}
                && $status != 0) {
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

sub _run_child_reap_cb {
    my ($self, $exit_pid, $status) = @_;
    my $cb = $self->on_child_reap;
    if ($cb) {
        eval {
            $cb->($self, $exit_pid, $status);
        };
        # XXX - hmph, what to do here?
    }
}

sub wait_all_children {
    my $self = shift;
    while (%{$self->{worker_pids}}) {
        if (my $pid = wait) {
            if (delete $self->{worker_pids}{$pid}) {
                $self->_run_child_reap_cb($pid, $?);
            }
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
    }
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

=head1 METHODS

=head2 new

Instantiation.  Takes a hashref as an argument.  Recognized attributes are as follows.

=head3 max_workers

number of worker processes (default: 10)

=head3 err_respawn_interval

interval until next child process is spawned after a worker exits abnormally (default: 1)

=head3 trap_signals

hashref of signals to be trapped.  Manager process will trap the signals listed in the keys of the hash, and send the signal specified in the associated value (if any) to all worker processes.

=head3 on_child_reap

Coderef that is called when a child is reaped. Receives the instance to
the current Paralle::Prefork, the child's pid, and its exit status.

=head2 start

The main routine.  Returns undef in child processes.  Returns a true value within manager process upon receiving a signal specified in the C<trap_signals> hashref.

=head2 finish

Child processes should call this function for termination.  Takes exit code as an optional argument.  Only usable from child processes.

=head2 signal_all_children

Sends signal to all worker processes.  Only usable from manager process.

=head2 wait_all_children

Blocks until all worker processes exit.  Only usable from manager process.

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

See http://www.perl.com/perl/misc/Artistic.html

=cut
