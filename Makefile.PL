use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'		=> 'Spread::Queue',
    'VERSION_FROM'	=> 'Queue/Manager.pm', # finds $VERSION
    'PREREQ_PM'		=> {
			    Spread::Session => 0.1,
			    Data::Serializer => 0.11,
			    Log::Channel => 0.4,
			   }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM => 'Queue.pod', # retrieve abstract from module
       AUTHOR     => 'Jason W. May <jmay@pobox.com>') : ()),
);
