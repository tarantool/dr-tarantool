use utf8;
use strict;
use warnings;

package DR::Tarantool::AEConnection;
use AnyEvent;
use AnyEvent::Socket ();
use Carp;
use List::MoreUtils ();
use Scalar::Util ();

sub _errno() {
    while (my ($k, $v) = each(%!)) {
        return $k if $v;
    }
    return $!;
}

sub new {
    my ($class, %opts) = @_;

    $opts{state} = 'init';
    $opts{host}  ||= '127.0.0.1';
    croak 'port is undefined' unless $opts{port};


    $opts{on}{connected}    ||= sub {  };
    $opts{on}{connfail}     ||= sub {  };
    $opts{on}{connfail_user}||= sub {  };
    $opts{on}{disconnect}   ||= sub {  };
    $opts{on}{error}        ||= sub {  };
    $opts{on}{reconnecting} ||= sub {  };

    $opts{success_connects} = 0;
    $opts{wbuf} = '';

    $opts{read} = { any => [] };

    bless \%opts => ref($class) || $class;
}


sub on {
    my ($self, $name, $cb) = @_;
    croak "wrong event name: $name" unless exists $self->{on}{$name};
    $self->{on}{$name} = $cb || sub {  };
    $self;
}

sub fh      { $_[0]->{fh} }
sub state   { $_[0]->{state} }
sub host    { $_[0]->{host} }
sub port    { $_[0]->{port} }
sub error   { $_[0]->{error} }
sub errno   { $_[0]->{errno} }

{
    my @methods = qw/
        reconnect_always
        reconnect_period
        request_timeout
        connect_attempts
        connect_tries
        connect_timeout
    /;
    no strict 'refs';

    for my $method (@methods) {
        *$method = sub {
            my ($self) = @_;
            return $self->{$method} if @_ == 1;
            return $self->{$method} = $_[1];

        }
    }
}

sub set_error {
    my ($self, $error, $errno) = @_;
    $errno ||= $error;
    $self->{state} = 'error';
    $self->{error} = $error;
    $self->{errno} = $errno;
    $self->{on}{error}($self);
    $self->{guard} = {};
    $self->{wbuf} = '';

    if ($self->_check_reconnect) {
        $self->reset_requests_timers;
        $self->{_connect_cb}  = sub { my $self = shift; $self->recall_requests };
        $self->{on}{connfail} = sub { my $self = shift; $self->requests_failed };
    }
    else {
        $self->requests_failed;
    }
    
}

sub _check_reconnect {
    Scalar::Util::weaken(my $self = shift);
    return if $self->state eq 'connected';
    return if $self->state eq 'connecting';
    return if $self->{guard}{rc};

    return unless $self->reconnect_period;
    unless ($self->reconnect_always) {
        return unless $self->{success_connects};
    }
    return if $self->connect_tries >= $self->connect_attempts;

    $self->{guard}{rc} = AE::timer $self->reconnect_period, 0, sub {
        return unless $self;
        delete $self->{guard}{rc};
        $self->{on}{reconnecting}($self);
        $self->{_connect_cb} ||= sub { };
        $self->connect;
    };
    return 1;
}

sub connect {
    Scalar::Util::weaken(my $self = shift);

    return if $self->state eq 'connected' or $self->state eq 'connecting';

    $self->{state} = 'connecting';
    $self->{error} = undef;
    $self->{errno} = undef;
    $self->{guard} = {};
    $self->{connect_tries}++;

    $self->{guard}{c} = AnyEvent::Socket::tcp_connect
        $self->host,
        $self->port,
        sub {
            $self->{guard} = {};
            my ($fh) = @_;
            if ($fh) {
                $self->{fh} = $fh;

                $self->{state} = 'connected';
                $self->{success_connects}++;
                $self->{connect_tries} = 0;
                $self->on(connfail => undef);

                $self->push_write('') if length $self->{wbuf};
                $self->{on}{connected}($self);
                return;
            }
    
            $self->{error} = $!;
            $self->{errno} = _errno;
            $self->{state} = 'connfail';
            $self->{guard} = {};
            unless ( $self->_check_reconnect ) {
                $self->{on}{connfail_user}($self);
                $self->{on}{connfail     }($self);
            }
            return unless $self;
        },
        sub {

        }
    ;

    if (defined $self->connect_timeout) {
        AE::now_update;
        $self->{guard}{t} = AE::timer $self->connect_timeout, 0, sub {
            delete $self->{guard}{t};
            return unless $self->state eq 'connecting';

            $self->{error} = 'Connection timeout';
            $self->{errno} = 'ETIMEOUT';
            $self->{state} = 'connfail';
            $self->{guard} = {};
            unless ( $self->_check_reconnect ) {
                $self->{on}{connfail_user}($self);
                $self->{on}{connfail     }($self);
            }
        };
    }
   
    $self;
}

sub disconnect {
    Scalar::Util::weaken(my $self = shift);
    return if $self->state eq 'disconnect' or $self->state eq 'init';

    $self->{guard} = {};
    $self->{error} = 'Disconnected';
    $self->{errno} = 'SUCCESS';
    $self->{state} = 'disconnect';
    $self->{wbuf} = '';
    $self->{connect_tries} = 0;
    close ($self->{fh}) if (exists $self->{fh} and $self->{fh});
    $self->{on}{disconnect}($self);
}


sub push_write {
    Scalar::Util::weaken(my $self = shift);
    my ($str) = @_;

    $self->{wbuf} .= $str;

    return unless $self->state eq 'connected';
    return unless length $self->{wbuf};
    return if $self->{guard}{write};

    $self->{guard}{write} = AE::io $self->fh, 1, sub {
        my $l = syswrite $self->fh, $self->{wbuf};
        unless(defined $l) {
            return if $!{EINTR};
            $self->set_error($!, _errno);
            return;
        }
        substr $self->{wbuf}, 0, $l, '';
        return if length $self->{wbuf};
        delete $self->{guard}{write};
    };
}

sub read_while {
    # has to be strong reference to be closured and not get destroyed while reading
    my $self = shift;
    my ($condition) = @_;

    return unless grep { $condition eq $_ } qw(handshake requests);

    $self->{read_while}{$condition} = 1;

    return unless $self->state eq 'connected';
    return if     $self->{guard}{read};

    $self->{guard}{read} = AE::io $self->fh, 0, sub {
        my $rd = sysread $self->fh, my $buf, 4096;
        unless(defined $rd) {
            return if $!{EINTR};

            delete $self->{read_while};
            $self->_fatal_error("Socket error: $!");
            return;
        }

        unless($rd) {
            delete $self->{read_while};
            $self->_fatal_error("Socket error: Server closed connection");
            return;
        }
        $self->{rbuf} .= $buf;
        $self->_check_rbuf;

        delete $self->{read_while}{handshake} unless $self->{handshake};
        delete $self->{read_while}{requests } unless scalar keys %{ $self->{wait} };

        return if scalar keys %{ $self->{read_while} };
        delete $self->{guard}{read};
    };
}



1;
