package QBit::Application::Model::DB::Table;

use qbit;

use base qw(QBit::Application::Model::DB::Class);

__PACKAGE__->mk_ro_accessors(
    qw(
      name
      inherits
      primary_key
      indexes
      foreign_keys
      )
);

__PACKAGE__->abstract_methods(
    qw(
      create_sql
      add_multi
      add
      edit
      delete
      _get_field_object
      _convert_fk_auto_type
      )
);

sub init {
    my ($self) = @_;

    $self->SUPER::init();

    throw gettext('Required opt \"fields\" ')
      unless $self->{'fields'};

    foreach my $field (@{$self->{'fields'}}) {    # Если нет типа, ищем тип в foreign_keys
        unless (exists($field->{'type'})) {
          FT: foreach my $fk (@{$self->{'foreign_keys'} || []}) {
                for (0 .. @{$fk->[0]} - 1) {
                    if ($field->{'name'} eq $fk->[0][$_]) {
                        my $fk_table_name = $fk->[1];
                        $self->_convert_fk_auto_type($field,
                            $self->db->$fk_table_name->{'__FIELDS_HS__'}{$fk->[2][$_]});
                        last FT;
                    }
                }
            }
        }
        $field = $self->_get_field_object(%$field, db => $self->db, table => $self);
        $self->{'__FIELDS_HS__'}{$field->{'name'}} = $field;
    }
}

sub fields {
    my ($self) = @_;

    return [(map {@{$self->db->$_->fields()}} @{$self->inherits || []}), @{$self->{'fields'}}];
}

sub field_names {
    my ($self) = @_;

    return map {$_->{'name'}} @{$self->fields};
}

sub get_all {
    my ($self, %opts) = @_;

    my $query = $self->db->query->select(
        table => $self,
        hash_transform(\%opts, [qw(fields filter)]),
    );

    $query->group_by(@{$opts{'group_by'}}) if $opts{'group_by'};

    if ($opts{'having'}) {
        throw 'Have not realized yet';
    }

    $query->order_by(@{$opts{'order_by'}}) if $opts{'order_by'};

    $query->limit($opts{'limit'}) if $opts{'limit'};

    $query->distinct() if $opts{'distinct'};

    $query->for_update() if $opts{'for_update'};

    $query->all_langs(TRUE) if $opts{'all_langs'};

    return $query->get_all();
}

sub get {
    my ($self, $id, %opts) = @_;

    throw gettext("No primary key") unless @{$self->primary_key};

    if (ref($id) eq 'ARRAY') {
        $id = {map {$self->primary_key->[$_] => $id->[$_]} 0 .. @$id - 1};
    } elsif (!ref($id)) {
        $id = {$self->primary_key->[0] => $id};
    }

    throw gettext("Bad fields in id")
      if grep {!exists($id->{$_})} @{$self->primary_key};

    return $self->get_all(%opts, filter => {map {$_ => $id->{$_}} @{$self->primary_key}})->[0];
}

sub truncate {
    my ($self) = @_;

    $self->db->_do('TRUNCATE TABLE ' . $self->quote_identifier($self->name));
}

sub default_fields { }

sub default_primary_key { }

sub default_indexes { }

sub default_foreign_keys { }

sub _fields_hs {
    my ($self) = @_;

    return {map {$_->{'name'} => $_} @{$self->fields}};
}

sub _pkeys_or_filter_to_filter {
    my ($self, $pkeys_or_filter) = @_;

    unless (blessed($pkeys_or_filter) && $pkeys_or_filter->isa('QBit::Application::Model::DB::Filter')) {
        if (ref($pkeys_or_filter) eq 'ARRAY') {
            $pkeys_or_filter = [$pkeys_or_filter] if !ref($pkeys_or_filter->[0]) && @{$self->primary_key} > 1;
        } else {
            $pkeys_or_filter = [$pkeys_or_filter];
        }

        my $filter = $self->db->filter();
        foreach my $pk (@$pkeys_or_filter) {
            if (!ref($pk) && @{$self->primary_key} == 1) {
                $pk = {$self->primary_key->[0] => $pk};
            } elsif (ref($pk) eq 'ARRAY') {
                $pk = {map {$self->primary_key->[$_] => $pk->[$_]} 0 .. @{$self->primary_key} - 1};
            }

            throw gettext('Bad primary key') if ref($pk) ne 'HASH' || grep {!defined($pk->{$_})} @{$self->primary_key};
            $filter->or({map {$_ => $pk->{$_}} @{$self->primary_key}});
        }
        $pkeys_or_filter = $filter;
    }

    return $pkeys_or_filter;
}

sub have_fields {
    my ($self, $fields) = @_;

    $fields = [$fields] if ref($fields) ne 'ARRAY';

    my %field_names_hs = map {$_ => TRUE} $self->field_names;

    return @$fields == grep {$field_names_hs{$_}} @$fields;
}

TRUE;
