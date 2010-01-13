package Parallel::Prefork::SpareWorkers;

use List::MoreUtils qw(uniq);

use constant STATUS_NEXIST => '.';
use constant STATUS_IDLE   => '_';

our %EXPORT_TAGS = (
    status => [ qw(STATUS_NEXIST STATUS_IDLE) ],
);
our @EXPORT_OK = uniq sort map { @$_ } values %EXPORT_TAGS;
$EXPORT_TAGS{all} = \@EXPORT_OK;

use base qw/Parallel::Prefork Exporter/;

__PACKAGE__->mk_accessors(qw/min_spare_workers max_spare_workers scoreboard/);

sub new {
    my $klass = shift;
    my $self = $klass->SUPER::new(@_);
    die "mandatory option min_spare_workers not set"
        unless $self->{min_spare_workers};
    $self->{max_spare_workers} ||= $self->max_workers;
    $self->{scoreboard} ||= do {
        require 'Parallel/Prefork/SpareWorkers/Scoreboard.pm';
        Parallel::Prefork::SpareWorkers::Scoreboard->new(
            $self->{scoreboard_file} || undef,
            $self->max_workers,
        );
    };
    $self;
}

sub start {
    my $self = shift;
    my $ret = $self->SUPER::start();
    unless ($ret) {
        # child process
        $self->scoreboard->child_start();
        return;
    }
    return 1;
}

sub num_active_workers {
    my $self = shift;
    scalar grep {
        $_ ne STATUS_NEXIST && $_ ne STATUS_IDLE
    } $self->scoreboard->get_statuses;
}

sub set_status {
    my ($self, $status) = @_;
    $self->scoreboard->set_status($status);
}

sub _decide_action {
    my $self = shift;
    my $spare_workers = $self->num_workers - $self->num_active_workers;
    return 1
        if $spare_workers < $self->min_spare_workers
            && $self->num_workers < $self->max_workers;
    return -1
        if $spare_workers > $self->max_spare_workers;
    return 0;
}

sub _on_child_reap {
    my ($self, $exit_pid, $status) = @_;
    $self->SUPER::_on_child_reap($exit_pid, $status);
    $self->scoreboard->clear_child($exit_pid);
}

1;
