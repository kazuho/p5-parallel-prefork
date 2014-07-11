#! /usr/bin/perl

use strict;
use warnings;

use Fcntl qw/:flock/;
use Test::More tests => 4;

use Parallel::Prefork;

my $reaped = 0;
my $pm = Parallel::Prefork->new({
    max_workers   => 3,
    fork_delay    => 0,
    on_child_reap => sub {
        $reaped++;
    }
});

my $sig_retain_cnt = 1;
$pm->after_fork(sub {
    $sig_retain_cnt++;
});

my $manager_pid = $$;

until ($pm->signal_received) {
    $pm->start and next;

    my $rcv = 0;
    local $SIG{TERM} = sub { $rcv++ };

    if ($sig_retain_cnt == $pm->max_workers) {
        kill 'TERM', $manager_pid;
    }

    sleep(100) while $rcv < $sig_retain_cnt;

    $pm->finish;
}
is $pm->wait_all_children(1), 2, 'should reap one worker.';
$pm->signal_all_children('TERM');
is $pm->wait_all_children(1), 1, 'should reap one worker.';
$pm->signal_all_children('TERM');
$pm->wait_all_children();
is $pm->num_workers, 0, 'all workers reaped.';

is($reaped, $pm->max_workers, "properly called on_child_reap callback");
