#! /usr/bin/perl

use strict;
use warnings;

use Fcntl qw/:flock/;
use File::Temp qw/tempfile/;
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

my ($fh, $filename) = tempfile;
syswrite $fh, '0', 1;
close $fh;

my $manager_pid = $$;

until ($pm->signal_received) {
    $pm->start and next;

    open my $fh, '+<', $filename
        or die "failed to open temporary file: $filename: ";
    flock $fh, LOCK_EX;
    sysread $fh, my $c, 10;
    $c++;
    seek $fh, 0, 0;
    syswrite $fh, $c, length($c);
    flock $fh, LOCK_UN;
    close $fh;

    my $rcv = 0;
    local $SIG{TERM} = sub { $rcv++ };

    if ($c == $pm->max_workers) {
        kill 'TERM', $manager_pid;
    }

    sleep(100) while $rcv < $c;

    $pm->finish;
}
is $pm->wait_all_children(1), 2, 'should reap one worker.';
$pm->signal_all_children('TERM');
is $pm->wait_all_children(1), 1, 'should reap one worker.';
$pm->signal_all_children('TERM');
$pm->wait_all_children();
is $pm->num_workers, 0, 'all workers reaped.';

is($reaped, $pm->max_workers, "properly called on_child_reap callback");

unlink $filename;
