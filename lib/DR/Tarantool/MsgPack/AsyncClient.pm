use utf8;
use strict;
use warnings;

package DR::Tarantool::MsgPack::AsyncClient;

=head1 NAME

DR::Tarantool::MsgPack::AsyncClient - async client for tarantool.

=head1 SYNOPSIS

    use DR::Tarantool::MsgPack::AsyncClient;

    DR::Tarantool::MsgPack::AsyncClient->connect(
        host => '127.0.0.1',
        port => 12345,
        spaces => $spaces,
        sub {
            my ($client) = @_;
        }
    );

    $client->insert('space_name', [1,2,3], sub { ... });


=head1 Class methods

=head2 connect

Connect to <Tarantool:http://tarantool.org>, returns (by callback) an
object which can be used to make requests.

=head3 Arguments

=over

=item host & port & user & password

Address and auth information of remote tarantool.

=item space

A hash with space description or a L<DR::Tarantool::Spaces> reference.

=item reconnect_period

An interval to wait before trying to reconnect after a fatal error
or unsuccessful connect. If the field is defined and is greater than
0, the driver tries to reconnect to the server after this interval.

Important: the driver does not reconnect after the first
unsuccessful connection. It calls callback instead.

=item reconnect_always

Try to reconnect even after the first unsuccessful connection.

=back

=cut


use DR::Tarantool::MsgPack::LLClient;
use DR::Tarantool::Spaces;
use DR::Tarantool::Tuple;
use Carp;
$Carp::Internal{ (__PACKAGE__) }++;
use Scalar::Util ();
use Data::Dumper;

sub connect {
    my $class = shift;
    my ($cb, %opts);
    if ( @_ % 2 ) {
        $cb = pop;
        %opts = @_;
    } else {
        %opts = @_;
        $cb = delete $opts{cb};
    }

    $class->_llc->_check_cb( $cb );

    my $host = $opts{host} || 'localhost';
    my $port = $opts{port} or croak "port isn't defined";

    my $user        = delete $opts{user};
    my $password    = delete $opts{password};
    my $reconnect_period    = $opts{reconnect_period} || 0;
    my $reconnect_always    = $opts{reconnect_always} || 0;

    DR::Tarantool::MsgPack::LLClient->connect(
        host                => $host,
        port                => $port,
        user                => $user,
        password            => $password,
        reconnect_period    => $reconnect_period,
        reconnect_always    => $reconnect_always,
        sub {
            my ($client) = @_;
            my $self;
            if (ref $client) {
                $self = bless {
                    llc         => $client,
                } => ref($class) || $class;
            } else {
                $self = $client;
            }
            $self->_load_schema($cb);
        }
    );

    return;
}

sub _load_schema {
    my ( $self, $cb ) = @_;

    my %spaces = ();
    my ( $get_spaces_cb, $get_indexes_cb );

    # get numbers of existing non-service spaces
    $get_spaces_cb = sub {
        my ( $status, $data ) = @_;
        croak 'cannot call lua "box.space._space:select"' unless $status eq 'ok';
        my $next = $data;
        LOOP: {
            do {{
                last LOOP unless $next;
                my $raw = $next->raw;
                # $raw structure:
                # [space_no, uid, space_name, engine, field_count, {temporary}, [format]]

                next unless $raw->[2];     # no space name
                next if $raw->[2] =~ /^_/; # skip service spaces

                $spaces{$raw->[0]} =
                    {
                        name   => $raw->[2],
                        fields => [
                                    map { $_->{type} = uc($_->{type}); $_ }
                                        @{ ref $raw->[6] eq 'ARRAY' ? $raw->[6] : [$raw->[6]] }
                                  ],
                    }
            }} while ($next = $next->next);
        }

        DR::Tarantool::MsgPack::AsyncClient::call_lua($self,'box.space._vindex:select' => [], $get_indexes_cb);
    };

    # get index structure for each of spaces we got
    $get_indexes_cb = sub {
        my ( $status, $data ) = @_;
        croak 'cannot call lua "box.space._vindex:select"' unless $status eq 'ok';
        my $next = $data;
        LOOP: {
            do {{
                last LOOP unless $next;
                my $raw = $next->raw;
                # $raw structure:
                # [space_no, index_no, index_name, index_type, {params}, [fields] ]

                my $space_no = $raw->[0];
                next unless exists $spaces{$space_no};

                unless ( defined($raw->[1]) and defined($raw->[2]) ) {
                    delete $spaces{$space_no};
                    next;
                }
                $spaces{$space_no}->{indexes}{$raw->[1]} =
                    {
                        name => $raw->[2],
                        fields => [ map { $_->[0] } @{ $raw->[5] } ],
                    };

                # add to fields array ones found in 'indexes'
                # but not present in 'fields'
                my $were_fields_count = scalar @{ $spaces{$space_no}->{fields} };
                push @{ $spaces{$space_no}->{fields} },
                    map { { type => uc($_->[1]) }  } @{ $raw->[5] }[ $were_fields_count .. $#{$raw->[5]} ];

            }} while ($next = $next->next);
        }

        for my $space ( keys %spaces ) {
            unless ( $spaces{$space}{fields} ) {
                delete $spaces{$space};
                next;
            }
            unless ( $spaces{$space}{indexes} ) {
                delete $spaces{$space};
                next;
            }
            for my $index ( values %{$spaces{$space}->{indexes}} ) {
                @{ $index->{fields} } =
                    map { exists $spaces{$space}{fields}[$_]{name} ? $spaces{$space}{fields}[$_]{name} : $_ }
                        @{ $index->{fields} };
            }
        }
        $self->{spaces} = DR::Tarantool::Spaces->new(\%spaces);
        $self->{spaces}->family(2);

        $self->set_schema_id($cb);
    };

    DR::Tarantool::MsgPack::AsyncClient::call_lua($self, 'box.space._space:select' => [], $get_spaces_cb);

    return $self;
}

sub _llc { return $_[0]{llc} if ref $_[0]; 'DR::Tarantool::MsgPack::LLClient' }


sub _cb_default {
    my ($res, $s, $cb, $connect_obj, $caller_sub) = @_;
    if ($res->{status} ne 'ok') {
        if ($res->{CODE} == 32877) { # wrong schema_id, need reload
            $connect_obj->{SCHEMA_ID} = undef;
            $connect_obj->_load_schema($caller_sub);
            return;
        }
        $cb->($res->{status} => $res->{CODE}, $res->{ERROR});
        return;
    }

    if ($s) {
        $cb->(ok => $s->tuple_class->unpack( $res->{DATA}, $s ), $res->{CODE});
        return;
    }

    unless ('ARRAY' eq ref $res->{DATA}) {
        $cb->(ok => $res->{DATA}, $res->{CODE});
        return;
    }

    unless (@{ $res->{DATA} }) {
        $cb->(ok => undef, $res->{CODE});
        return;
    }
    $cb->(ok => DR::Tarantool::Tuple->new($res->{DATA}), $res->{CODE});
    return;
}

=head1 Worker methods

All methods accept callbacks which are invoked with the following
arguments:

=over

=item status

On success, this field has value 'ok'. The value of this parameter
determines the contents of the rest of the callback arguments.

=item a tuple or tuples or an error code

On success, the second argument contains tuple(s) produced by the
request. On error, it contains the server error code.

=item errorstr

Error string in case of an error.

    sub {
        if ($_[0] eq 'ok') {
            my ($status, $tuples) = @_;
            ...
        } else {
            my ($status, $code, $errstr) = @_;
            ...
        }
    }

=back


=head2 ping

Ping the server.

    $client->ping(sub { ... });

=head2 insert, replace


Insert/replace a tuple into a space.

    $client->insert('space', [ 1, 'Vasya', 20 ], sub { ... });
    $client->replace('space', [ 2, 'Petya', 22 ], sub { ... });


=head2 call_lua

Call Lua function.

    $client->call_lua(foo => ['arg1', 'arg2'], sub {  });


=head2 select

Select a tuple (or tuples) from a space by index.

    $client->select('space_name', 'index_name', [ 'key' ], %opts, sub { .. });

Options can be:

=over

=item limit

=item offset

=item iterator

An iterator for index. Can be:

=over

=item ALL

Returns all tuples in space.

=item EQ, GE, LE, GT, LT

=back

=back


=head2 delete

Delete a tuple.

    $client->delete('space_name', [ 'key' ], sub { ... });


=head2 update

Update a tuple.

    $client->update('space', [ 'key' ], \@ops, sub { ... });

C<@ops> is array of operations to update.
Each operation is array of elements:

=over

=item code

Code of operation: C<=>, C<+>, C<->, C<&>, C<|>, etc

=item field

Field number or name.

=item arguments

=back

=cut


sub set_schema_id {
    my $self = shift;
    my $cb = pop;

    $self->_llc->_check_cb( $cb );
    $self->_llc->ping(
        sub {
            my ( $res ) = @_;
            if ($res->{status} ne 'ok') {
                croak 'cannot perform ping in order to get schema_id '
                    . "status=$res->{status}, code=$res->{CODE}, error=$res->{ERROR}";
                return;
            }

            $self->{SCHEMA_ID} = $res->{SCHEMA_ID};
            $cb->($self);
            return;
    });
}

sub ping {
    my $self = shift;
    my $cb = pop;

    $self->_llc->_check_cb( $cb );
    $self->_llc->ping(sub { _cb_default($_[0], undef, $cb, $self) });
}

sub insert {
    my $self = shift;
    my $cb = pop;
    $self->_llc->_check_cb( $cb );
    my $space = shift;
    my $tuple = shift;
    $self->_llc->_check_tuple( $tuple );


    my $sno;
    my $s;

    if (Scalar::Util::looks_like_number $space) {
        $sno = $space;
    } else {
        $s = $self->{spaces}->space($space);
        $sno = $s->number,
        $tuple = $s->pack_tuple( $tuple );
    }

    my $subref = undef;
    $subref = sub {
        my $self = shift;
        $self->_llc->insert(
            $sno,
            $tuple,
            $self->{SCHEMA_ID},
            sub {
                my ($res) = @_;
                _cb_default($res, $s, $cb, $self, $subref);
            }
        );
    };
    $subref->($self);
    return;
}

sub replace {
    my $self = shift;
    my $cb = pop;
    $self->_llc->_check_cb( $cb );
    my $space = shift;
    my $tuple = shift;
    $self->_llc->_check_tuple( $tuple );


    my $sno;
    my $s;

    if (Scalar::Util::looks_like_number $space) {
        $sno = $space;
    } else {
        $s = $self->{spaces}->space($space);
        $sno = $s->number,
        $tuple = $s->pack_tuple( $tuple );
    }

    my $subref = undef;
    $subref = sub {
        my $self = shift;
        $self->_llc->replace(
            $sno,
            $tuple,
            $self->{SCHEMA_ID},
            sub {
                my ($res) = @_;
                _cb_default($res, $s, $cb, $self, $subref);
            }
        );
    };
    $subref->($self);
    return;
}

sub delete :method {
    my $self = shift;
    my $cb = pop;
    $self->_llc->_check_cb( $cb );
    
    my $space = shift;
    my $key = shift;


    my $sno;
    my $s;

    if (Scalar::Util::looks_like_number $space) {
        $sno = $space;
    } else {
        $s = $self->{spaces}->space($space);
        $sno = $s->number;
    }

    my $subref = undef;
    $subref = sub {
        my $self = shift;
        $self->_llc->delete(
            $sno,
            $key,
            $self->{SCHEMA_ID},
            sub {
                my ($res) = @_;
                _cb_default($res, $s, $cb, $self, $subref);
            }
        );
    };
    $subref->($self);
    return;
}

sub select :method {
    my $self = shift;
    my $cb = pop;
    $self->_llc->_check_cb( $cb );
    my $space = shift;
    my $index = shift;
    my $key = shift;
    my %opts = @_;

    my $sno;
    my $ino;
    my $s;
    if (Scalar::Util::looks_like_number $space) {
        $sno = $space;
        croak 'If space is number, index must be number too'
            unless Scalar::Util::looks_like_number $index;
        $ino = $index;
    } else {
        $s = $self->{spaces}->space($space);
        $sno = $s->number;
        $ino = $s->_index( $index )->{no};
    }
    my $subref = undef;
    $subref = sub {
        my $self = shift;
        $self->_llc->select(
            $sno,
            $ino,
            $key,
            $opts{limit},
            $opts{offset},
            $opts{iterator},
            $self->{SCHEMA_ID},
            sub {
                my ($res) = @_;
                _cb_default($res, $s, $cb, $self, $subref);
            }
        );
    };
    $subref->($self);
}

sub update :method {
    my $self = shift;
    my $cb = pop;
    $self->_llc->_check_cb( $cb );
    my $space = shift;
    my $key = shift;
    my $ops = shift;

    my $sno;
    my $s;
    if (Scalar::Util::looks_like_number $space) {
        $sno = $space;
    } else {
        $s = $self->{spaces}->space($space);
        $sno = $s->number;
        $ops = $s->pack_operations($ops);
    }
    my $subref = undef;
    $subref = sub {
        my $self = shift;
        $self->_llc->update(
            $sno,
            $key,
            $ops,
            $self->{SCHEMA_ID},
            sub {
                my ($res) = @_;
                _cb_default($res, $s, $cb, $self, $subref);
            }
        );
    };
    $subref->($self);
}

sub call_lua {
    my $self = shift;
    my $cb = pop;
    $self->_llc->_check_cb( $cb );

    my $proc = shift;
    my $tuple = shift;

    $tuple = [ $tuple ] unless ref $tuple;
    $self->_llc->_check_tuple( $tuple );


    $self->_llc->call_lua(
        $proc,
        $tuple,
        sub {
            my ($res) = @_;
            _cb_default($res, undef, $cb, $self);
        }
    );
    return;
}


sub last_code { $_[0]->_llc->last_code }


sub last_error_string { $_[0]->_llc->last_error_string }

1;
