use inc::Module::Install;

all_from 'lib/Parallel/Prefork.pm';
readme_from 'lib/Parallel/Prefork.pm';

requires 'Class::Accessor::Fast' => 0.31;
requires 'List::MoreUtils';
requires 'Proc::Wait3' => 0.03;
requires 'Scope::Guard';
test_requires 'Test::Requires';

WriteAll;
