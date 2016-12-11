# tarantool-perl
Perl driver for Tarantool 1.6.  
Fully featured client to new versions (>=1.6) of [tarantool](http://tarantool.org) database, providing methods for all commonly used database query types.

Currently in process of uploading on CPAN.  

#### Features
  - Async queries (event-driven development)
  - Sync queries
  - Ability to use spaces and indexes names (schema loading) in following queries
  - SELECT, INSERT, DELETE, REPLACE, UPDATE, UPSERT, LUA functions call  
   
#### Usage  
Since tarantool database has two completely different versions (1.5 and 1.6), tarantool-perl client is created to be universal, which is able to handle both versions.

##### Tarantool version 1.5
Methods are located in namespace **DR::Tarantool::\***

##### Tarantool version >= 1.6
Methods are located in namespace **DR::Tarantool::MsgPack::\*** .  Some examples are listed below.
First, let's use synchronous client to get connection to database:
```perl
my $connection = DR::Tarantool::MsgPack::SyncClient->connect(
    host => $host,
    port => $port);
```
Using this object one can call various methods like:
```perl
my $data = $connection->select('space_name', 'index_name', [ 22 ]);
```
to fetch data from space *space_name* using index *index_name*.  

To be honest, *sync* client is a simple wrapper for *async* one, so the core of the application is projected to be *async*. The following examples are to demonstrate the way async methods should be called (here [AnyEvent](https://metacpan.org/pod/AnyEvent) library is used for event-loop implementation):
```perl
use AnyEvent;
use Data::Dumper;

my $cv = AnyEvent->condvar();
DR::Tarantool::MsgPack::AsyncClient->connect(
    host => $host,
    port => $port,
    sub {
        shift->select(
            'space_name', 
            'index_name',
            [22],
            sub { print Dumper $_[1]->raw; $cv->send }
        );
    });
# ... your code here
$cv->recv();
```
Here we pass one more argument to `connect()` method, which is callback to be invocked when the connection has actually been established. In this callback we in turn call `select()` method with its own callback, which dumps the structure *select* returned and stops event-loop.

The other methods are to be invoked in a similar way, the functions prototypes and usage examples can be found in perldoc.

### Version
0.57

### Tech

Main dependencies for this package are the following perl modules:

* [AnyEvent] - event-loop implementation
* [Coro] - coroutines

### Installation

Until the module is released on CPAN in order to install tarantool-perl its sources can be downloaded from here and compiled (lowlevel parts are written with XS).

```sh
$ perl Makefile.PL
$ make && make install
```

### Development

Want to contribute? Great! It's github, open issues, send pull requests :)

### TODO

 - Make the way the errors are delivered from all methods clear

### License

Artistic
