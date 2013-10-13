package Exception::DB;
use base qw(Exception);

package Exception::DB::DuplicateEntry;
use base qw(Exception::DB);

package QBit::Application::Model::DB;

use qbit;

use base qw(QBit::Application::Model);

use DBI;

our $DEBUG = FALSE;

__PACKAGE__->abstract_methods(qw(query filter _get_table_object _create_sql_db _connect _is_connection_error));

sub meta {
    my ($package, %meta) = @_;

    throw gettext("First argument must be package name, QBit::Application::Model::DB descendant")
      if !$package
          || ref($package)
          || !$package->isa('QBit::Application::Model::DB');

    my $pkg_stash = package_stash(ref($package) || $package);
    $pkg_stash->{'__META__'} = \%meta;
}

sub get_all_meta {
    my ($self, $package) = @_;

    $package = (ref($self) || $self) unless defined($package);
    my $meta = {};

    foreach my $pkg (eval("\@${package}::ISA")) {
        next unless $pkg->isa(__PACKAGE__);
        $self->_add_meta($meta, $pkg->get_all_meta($pkg));
    }

    $self->_add_meta($meta, package_stash($package)->{'__META__'} || {});

    return $meta;
}

sub init {
    my ($self) = @_;

    $self->SUPER::init();

    my $meta = $self->get_all_meta();

    my %tables;
    foreach my $table_name (keys(%{$meta->{'tables'} || {}})) {
        my %table = %{$meta->{'tables'}{$table_name}};

        $table{'class'} = $self->_get_table_class(type => $table{'type'});
        $table{'fields'}       = [$table{'class'}->default_fields(%table),       @{$table{'fields'}       || []}];
        $table{'indexes'}      = [$table{'class'}->default_indexes(%table),      @{$table{'indexes'}      || []}];
        $table{'foreign_keys'} = [$table{'class'}->default_foreign_keys(%table), @{$table{'foreign_keys'} || []}];
        $table{'primary_key'}  = $table{'class'}->default_primary_key(%table)
          unless exists($table{'primary_key'});

        $tables{$table_name} = \%table;
    }

    $self->{'__TABLE_TREE_LEVEL__'}{$_} = $self->_table_tree_level(\%tables, $_, 0) foreach keys(%tables);
    $self->{'__TABLES__'} = {};

    foreach my $table_name ($self->_sorted_tables(keys(%tables))) {
        throw gettext('Cannot create table object, "%s" is reserved', $table_name)
          if $self->can($table_name);
        {
            no strict 'refs';
            my $db_self = $self;
            *{__PACKAGE__ . "::$table_name"} = sub {
                my ($self) = @_;

                $self->{'__TABLES__'}{$table_name} = $tables{$table_name}->{'class'}->new(
                    %{$tables{$table_name}},
                    name => $table_name,
                    db   => $self,
                ) unless exists($self->{'__TABLES__'}{$table_name});

                return $self->{'__TABLES__'}{$table_name};
            };
        };
        $self->$table_name if $self->get_option('preload_accessors');
    }

    $self->{'__SAVEPOINTS__'} = 0;
}

sub quote {
    my ($self, $name) = @_;

    my ($res) = $self->_sub_with_connected_dbh(
        sub {
            my ($self, $name) = @_;
            return $self->{'__DBH__'}{$$}->quote($name);
        },
        [$self, $name]
    );

    return $res;
}

sub quote_identifier {
    my ($self, $name) = @_;

    my ($res) = $self->_sub_with_connected_dbh(
        sub {
            my ($self, $name) = @_;
            return $self->{'__DBH__'}{$$}->quote_identifier($name);
        },
        [$self, $name]
    );

    return $res;
}

sub begin {
    my ($self) = @_;

    $self->_connect();

    $self->{'__SAVEPOINTS__'} == 0
      ? $self->_do('BEGIN')
      : $self->_do('SAVEPOINT SP' . $self->{'__SAVEPOINTS__'});

    ++$self->{'__SAVEPOINTS__'};
}

sub commit {
    my ($self) = @_;

    $self->_connect();

    --$self->{'__SAVEPOINTS__'}
      ? $self->_do('RELEASE SAVEPOINT SP' . $self->{'__SAVEPOINTS__'})
      : $self->_do('COMMIT');
}

sub rollback {
    my ($self) = @_;

    $self->_connect();

    my $sql =
      --$self->{'__SAVEPOINTS__'}
      ? $self->_do('ROLLBACK TO SAVEPOINT SP' . $self->{'__SAVEPOINTS__'})
      : $self->_do('ROLLBACK');
}

sub transaction {
    my ($self, $sub) = @_;

    $self->begin();
    try {
        $sub->();
    }
    catch {
        my $ex = shift;
        $self->rollback();
        throw $ex;
    };

    $self->commit();
}

sub create_sql {
    my ($self) = @_;

    $self->_connect();

    my $SQL = '';

    my $meta = $self->get_all_meta();

    $SQL .= join("\n\n", map {$self->$_->create_sql()} $self->_sorted_tables(keys(%{$meta->{'tables'}})))
      if exists($meta->{'tables'});

    return "$SQL\n";
}

sub init_db {
    my ($self) = @_;

    $self->_connect();

    my $meta = $self->get_all_meta();

    if (exists($meta->{'tables'})) {
        $self->_do($self->$_->create_sql()) foreach $self->_sorted_tables(keys(%{$meta->{'tables'}}));
    }
}

sub finish {
    my ($self) = @_;

    if ($self->{'__SAVEPOINTS__'}) {
        $self->rollback() while $self->{'__SAVEPOINTS__'};
        throw gettext("Unclosed transaction");
    }
}

sub _do {
    my ($self, $sql, @params) = @_;

    $self->timelog->start($self->_log_sql($sql, \@params));

    my ($res) = $self->_sub_with_connected_dbh(
        sub {
            my ($self, $sql, @params) = @_;

            my $err_code;
            return $self->{'__DBH__'}{$$}->do($sql, undef, @params)
              || ($err_code = $self->{'__DBH__'}{$$}->err())
              && throw Exception::DB $self->{'__DBH__'}{$$}->errstr() . " ($err_code)\n" . $self->_log_sql($sql, \@params),
              errorcode => $err_code;
        },
        \@_
    );

    $self->timelog->finish();

    return $res;
}

sub _get_all {
    my ($self, $sql, @params) = @_;

    $self->timelog->start($self->_log_sql($sql, \@params));

    my ($data) = $self->_sub_with_connected_dbh(
        sub {
            my ($self, $sql, @params) = @_;

            my $err_code;
            $self->timelog->start(gettext('DBH prepare'));
            my $sth = $self->{'__DBH__'}{$$}->prepare($sql)
              || ($err_code = $self->{'__DBH__'}{$$}->err())
              && throw Exception::DB $self->{'__DBH__'}{$$}->errstr() . " ($err_code)\n" . $self->_log_sql($sql, \@params),
              errorcode => $err_code;

            $self->timelog->finish();

            $self->timelog->start(gettext('STH execute'));
            $sth->execute(@params)
              || ($err_code = $self->{'__DBH__'}{$$}->err())
              && throw Exception::DB $sth->errstr() . " ($err_code)\n" . $self->_log_sql($sql, \@params),
              errorcode => $err_code;
            $self->timelog->finish();

            $self->timelog->start(gettext('STH fetch_all'));
            my $data = $sth->fetchall_arrayref({})
              || ($err_code = $self->{'__DBH__'}{$$}->err())
              && throw Exception::DB $sth->errstr() . " ($err_code)\n" . $self->_log_sql($sql, \@params),
              errorcode => $err_code;
            $self->timelog->finish();

            $self->timelog->start(gettext('STH finish'));
            $sth->finish()
              || ($err_code = $self->{'__DBH__'}{$$}->err())
              && throw Exception::DB $sth->errstr() . " ($err_code)\n" . $self->_log_sql($sql, \@params),
              errorcode => $err_code;
            $self->timelog->finish();

            return $data;
        },
        \@_
    );

    $self->timelog->finish();

    return $data;
}

sub _log_sql {
    my ($self, $sql, $params) = @_;

    $sql =~ s/\?/$self->quote($_)/e foreach @{$params || []};

    l $sql if $DEBUG;

    return $sql;
}

sub _add_meta {
    my ($self, $res, $meta) = @_;

    foreach my $obj_type (keys %{$meta}) {
        foreach my $obj (keys %{$meta->{$obj_type}}) {
            warn gettext('Object "%s" (%s) overrided', $obj, $obj_type)
              if exists($res->{$obj_type}{$obj});
            $res->{$obj_type}{$obj} = $meta->{$obj_type}{$obj};
        }
    }
}

sub _table_tree_level {
    my ($self, $tables, $table_name, $level) = @_;

    return $self->{'__TABLE_TREE_LEVEL__'}{$table_name} + $level
      if exists($self->{'__TABLE_TREE_LEVEL__'}{$table_name});

    my @foreign_tables =
      ((map {$_->[1]} @{$tables->{$table_name}{'foreign_keys'}}), @{$tables->{$table_name}{'inherits'} || []});

    return @foreign_tables
      ? array_max(map {$self->_table_tree_level($tables, $_, $level + 1)} @foreign_tables)
      : $level;
}

sub _sorted_tables {
    my ($self, @table_names) = @_;

    return
      sort {($self->{'__TABLE_TREE_LEVEL__'}{$a} || 0) <=> ($self->{'__TABLE_TREE_LEVEL__'}{$b} || 0) || $a cmp $b}
      @table_names;
}

sub _sub_with_connected_dbh {
    my ($self, $sub, $params, $try) = @_;

    $try ||= 1;
    my @res;

    try {
        $self->_connect();
        @res = $sub->(@{$params || []});
    }
    catch {
        my $exception = shift;

        if (
            $try < 3
            && (!exists($self->{'__DBH__'}{$$})
                || $self->_is_connection_error($exception->{'errorcode'} || $self->{'__DBH__'}{$$}->err()))
           )
        {
            delete($self->{'__DBH__'}{$$}) if exists($self->{'__DBH__'}{$$});

            if ($self->{'__SAVEPOINTS__'}) {
                throw $exception;
            } else {
                @res = $self->_sub_with_connected_dbh($sub, $params, $try + 1);
            }
        } else {
            throw $exception;
        }
    };

    return @res;
}

sub DESTROY {
    my ($self) = @_;

    $self->{'__DBH__'}{$$}->disconnect() if exists($self->{'__DBH__'}{$$});
}

TRUE;
