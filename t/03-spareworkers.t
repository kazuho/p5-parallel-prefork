use strict;
use warnings;

use File::Temp qw();
use Time::HiRes qw(sleep);
use Test::More tests => 8;

use_ok('Parallel::Prefork::SpareWorkers');

my $tempdir = File::Temp::tempdir(CLEANUP => 1);

my $pm = Parallel::Prefork::SpareWorkers->new({
    min_spare_workers    => 3,
    max_spare_workers    => 5,
    max_workers          => 10,
    err_respawn_interval => 0,
    trap_signals         => {
        TERM => 'TERM',
    },
});
is $pm->num_active_workers, 0, 'no active workers';

my @tests = (
    wait_and_test(
        sub {
            is $pm->num_workers, 3, 'min_spare_workers';
            is $pm->num_active_workers, 0, 'no active workers';
            open my $fh, '>', "$tempdir/active"
                    or die "failed to touch file $tempdir/active:$!";
            close $fh;
        },
    ),
    wait_and_test(
        sub {
            is $pm->num_workers, 10, 'max_workers';
            is $pm->num_active_workers, 10, 'all workers active';
            unlink "$tempdir/active"
                or die "failed to unlink file $tempdir/active:$!";
        },
    ),
    wait_and_test(
        sub {
            is $pm->num_workers, 5, 'max_spare_workers';
            is $pm->num_active_workers, 0, 'no active workers';
        },
    ),
);
next_test();

while ($pm->signal_received ne 'TERM') {
    $pm->start and next;
    while (1) {
        $pm->set_status(
            -e "$tempdir/active"
                ? 'A' : Parallel::Prefork::SpareWorkers::STATUS_IDLE(),
        );
        sleep 1;
    }
}

$pm->wait_all_children;

sub wait_and_test {
    my $check_func = shift;
    my $cnt = 0;
    return sub {
        sleep 0.1;
        $cnt++;
        return if $cnt < 30; # 1 second until all clients update their state, plus 10 invocations to min/max the process, plus 1 second bonus
        $check_func->();
        next_test();
    };
}

sub next_test {
    if (@tests) {
        $pm->{__dbg_callback} = shift @tests;
    } else {
        $pm->{__dbg_callback} = sub {};
        $pm->signal_received('TERM');
    }
}
