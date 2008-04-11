use ExtUtils::MakeMaker;

WriteMakefile(
    NAME          => 'Parallel::Prefork',
    VERSION_FROM  => 'lib/Parallel/Prefork.pm',
    PREREQ_PM     => {
        Class::Accessor::Fast => 0.31,
        Proc::Wait3           => 0.03,
    },
    ABSTRACT_FROM => 'lib/Parallel/Prefork.pm',
    AUTHOR        => 'Kazuho Oku',
);
