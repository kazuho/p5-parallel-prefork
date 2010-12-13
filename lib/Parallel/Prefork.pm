package Parallel::Prefork;

use strict;
use warnings;

use base qw/Class::Accessor::Fast/;
use List::Util qw/first/;
use Proc::Wait3 ();
use Time::HiRes ();

__PACKAGE__->mk_accessors(qw/max_workers err_respawn_interval trap_signals signal_received manager_pid on_child_reap/);

our $VERSION = '0.08';

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
    
    # main loop
    while (! $self->signal_received) {
        if (my $delayed_task = $self->{_delayed_task}) {
            $delayed_task->($self);
        }
        my $action = $self->_decide_action;
        if ($action > 0) {
            # start a new worker
            my $pid = fork;
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
        } elsif ($action < 0) {
            # stop an existing worker
            kill(
                $self->_action_for('TERM')->[0],
                (keys %{$self->{worker_pids}})[0],
            );
        }
        $self->{__dbg_callback}->()
            if $self->{__dbg_callback};
        if (my ($exit_pid, $status)
                = $self->_wait(! $self->{__dbg_callback} && $action <= 0)) {
            $self->_on_child_reap($exit_pid, $status);
            if (delete($self->{worker_pids}{$exit_pid}) == $self->{generation}
                && $status != 0) {
                sleep $self->err_respawn_interval;
            }
        }
    }
    # send signals to workers
    if (my $action = $self->_action_for($self->signal_received)) {
        my ($sig, $interval) = @$action;
        if ($interval) {
            my @pids = sort keys %{$self->{worker_pids}};
            $self->{delayed_task} = sub {
                my $self = shift;
                my $pid = shift @pids;
                kill $sig, $pid;
                if (@pids == 0) {
                    delete $self->{delayed_task};
                    delete $self->{delayed_task_at};
                } else {
                    $self->{delayed_task_at} = Time::HiRes::time() + $interval;
                }
            };
            $self->{delayed_task}->();
        } else {
            $self->signal_all_children($sig);
        }
    }
    
    1; # return from parent process
}

sub finish {
    my ($self, $exit_code) = @_;
    exit($exit_code || 0);
}

sub signal_all_children {
    my ($self, $sig) = @_;
    foreach my $pid (sort keys %{$self->{worker_pids}}) {
        kill $sig, $pid;
    }
}

sub num_workers {
    my $self = shift;
    return scalar keys %{$self->{worker_pids}};
}

sub _decide_action {
    my $self = shift;
    return 1 if $self->num_workers < $self->max_workers;
    return 0;
}

sub _on_child_reap {
    my ($self, $exit_pid, $status) = @_;
    my $cb = $self->on_child_reap;
    if ($cb) {
        eval {
            $cb->($self, $exit_pid, $status);
        };
        # XXX - hmph, what to do here?
    }
}

# runs delayed tasks (if any) and returns how many seconds to wait
sub _handle_delayed_task {
    my $self = shift;
    while (1) {
        return undef
            unless $self->{delayed_task};
        my $timeleft = $self->{delayed_task_at} - Time::HiRes::time();
        return $timeleft
            if $timeleft >= 0.002;
        $self->{delayed_task}->($self);
    }
}

# returns [sig_to_send, interval_bet_procs] or undef for given recved signal
sub _action_for {
    my ($self, $sig) = @_;
    my $t = $self->{trap_signals}{$sig}
        or return undef;
    $t = [$t, 0] unless ref $t;
    return $t;
}

sub wait_all_children {
    my $self = shift;
    while (%{$self->{worker_pids}}) {
        if (my ($pid) = $self->_wait(1)) {
            if (delete $self->{worker_pids}{$pid}) {
                $self->_on_child_reap($pid, $?);
            }
        }
    }
}

# wrapper function of Proc::Wait3::wait3 that executes delayed task if any.  assumes wantarray == 1
sub _wait {
    my ($self, $blocking) = @_;
    if (! $blocking) {
        $self->_handle_delayed_task();
        return Proc::Wait3::wait3(0);
    } else {
        my $sleep_secs = $self->_handle_delayed_task();
        if (defined $sleep_secs) {
            # wait max sleep_secs or until signalled
            select(my $rin = '', my $win = '', my $ein = '', $sleep_secs);
            if (my @r = Proc::Wait3::wait3(0)) {
                return @r;
            }
        } else {
            if (my @r = Proc::Wait3::wait3(1)) {
                return @r;
            }
        }
        return +();
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
    trap_signals => {
      TERM => 'TERM',
      HUP  => 'TERM',
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
