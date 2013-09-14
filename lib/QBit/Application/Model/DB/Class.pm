package QBit::Application::Model::DB::Class;

use qbit;

use base qw(QBit::Class);

__PACKAGE__->mk_ro_accessors(qw(db));

sub init {
    my ($self) = @_;

    $self->SUPER::init();

    throw gettext('Required opt "db" must be QBit::Application::Model::DB descendant')
      unless $self->db && $self->db->isa('QBit::Application::Model::DB');

    weaken($self->{'db'});
}

sub quote {
    my ($self, $name) = @_;

    return $self->db->quote($name);
}

sub quote_identifier {
    my ($self, $name) = @_;

    return $self->db->quote_identifier($name);
}

sub filter {shift->db->filter(@_)}

TRUE;
