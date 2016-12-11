
package DR::Tarantool::MsgPack::AsyncClientInit;

use utf8;
use strict;
use warnings;

use Scalar::Util;
use Carp;
use DR::Tarantool::MsgPack::AsyncClient;

=head1 NAME

DR::Tarantool::MsgPack::AsyncClientInit - wrapper for AsyncClinet for lazy connects and reconnects.

=head1 SYNOPSIS

=cut

our $AUTOLOAD;

sub new {
    my $class = shift;

    my ($cb, %opts);
    if ( @_ % 2 ) {
        $cb = pop;
        %opts = @_;
    } else {
        %opts = @_;
        $cb = delete $opts{cb};
    }

    DR::Tarantool::MsgPack::AsyncClient->_llc->_check_cb( $cb );

    $cb->( bless { opts => \%opts } => $class );

    return;
}

my %supported_methods = map {$_ => 1} qw/
   ping
   insert
   replace
   select
   update
   upsert
   delete
   call_lua

   last_code
   last_error_string

   disconnect
   reconnect_always
   reconnect_period
   request_timeout
   connect_attempts
   connect_tries
   connect_timeout
/;

sub AUTOLOAD {
    Scalar::Util::weaken(my $self = shift);
    my @call_opts = @_;

    my $method = $AUTOLOAD;
    $method =~ s/.*AsyncClientInit:://;

    unless ($supported_methods{$method}) {
        my $cb  = pop;
        my $err = "unsupported method $method";

        if (ref $cb eq "CODE") {
            $cb->($err);
        }
        else {
            carp $err;
        }
        return;
    }

    my $callback = sub {
        no strict 'refs';
        $self->{client} || return;
        $self->{client}->$method(@call_opts);
    };

    unless ($self->{client}) {
        DR::Tarantool::MsgPack::AsyncClient->connect(
            %{ $self->{opts} }, 
            sub {
                $self->{client} = shift;
                $callback->()
            }
        );
        return;
    }

    unless ($self->{client}->_llc->is_connected) {
        $self->{client}->reconnect($callback);
        return;
    }

    return $callback->();
}

sub DESTROY {
    return;
}
