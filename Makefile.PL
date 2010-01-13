use ExtUtils::MakeMaker;

WriteMakefile(
    NAME          => 'Parallel::Prefork',
    VERSION_FROM  => 'lib/Parallel/Prefork.pm',
    PREREQ_PM     => {
        Class::Accessor::Fast => 0.31,
        List::MoreUtils       => 0,
        Proc::Wait3           => 0.03,
        Scope::Guard          => 0,
        Time::HiRes           => 0,
    },
    ABSTRACT_FROM => 'lib/Parallel/Prefork.pm',
    AUTHOR        => 'Kazuho Oku',
);
