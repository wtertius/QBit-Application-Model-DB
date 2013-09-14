package QBit::Application::Model::DB::Field;

use qbit;

use base qw(QBit::Application::Model::DB::Class);

__PACKAGE__->mk_ro_accessors(
    qw(
      name
      type
      table
      )
);

sub init {
    my ($self) = @_;

    $self->SUPER::init();

    weaken($self->{'table'});

    $self->init_check();
}

sub init_check {
    my ($self) = @_;

    my @mis_params = grep {!exists($self->{$_})} qw(name type);

    throw gettext('Need required parameter(s): %s (Table "%s")', join(', ', @mis_params), $self->table->name)
      if @mis_params;
}

sub create_sql {throw 'Abstract method'}

TRUE;
