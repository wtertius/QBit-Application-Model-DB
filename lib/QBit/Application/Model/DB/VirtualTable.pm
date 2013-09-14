package QBit::Application::Model::DB::VirtualTable;

use qbit;

use base qw(QBit::Application::Model::DB::Class);

use Sys::Hostname;

__PACKAGE__->mk_ro_accessors(qw(query name));

my $COUNTER = 0;

sub init {
    my ($self) = @_;

    $self->SUPER::init();

    throw Exception::BadArguments
      unless $self->query
          && blessed($self->query)
          && $self->query->isa('QBit::Application::Model::DB::Query');

    $self->{'name'} ||= join('_', 'vt', hostname, $$, $COUNTER++);
}

sub fields {
    my ($self) = @_;

    my @fields;

    foreach my $qtable (@{$self->query->{'__TABLES__'}}) {
        push(@fields, map {{name => $_}} keys(%{$qtable->{'fields'}}));
    }

    return \@fields;
}

sub get_sql_with_data {
    my ($self, %opts) = @_;

    return $self->query->get_sql_with_data(%opts);
}

sub _fields_hs {
    my ($self) = @_;

    return {map {$_->{'name'} => $_} @{$self->fields}};
}

TRUE;
