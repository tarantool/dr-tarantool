#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

BEGIN {
    use constant PLAN       => 37;
    use Test::More;
    use DR::Tarantool::StartTest;

    unless (DR::Tarantool::StartTest::is_version('1.6', 2)) {

        plan skip_all => 'tarantool 1.6 is not found';
    } else {
        plan tests => PLAN;
    }
}

use File::Spec::Functions 'catfile', 'rel2abs';
use File::Basename 'dirname';
use Encode qw(decode encode);
use lib qw(lib ../lib ../../lib);
use lib qw(blib/lib blib/arch ../blib/lib
    ../blib/arch ../../blib/lib ../../blib/arch);


BEGIN {
    # Подготовка объекта тестирования для работы с utf8
    my $builder = Test::More->builder;
    binmode $builder->output,         ":utf8";
    binmode $builder->failure_output, ":utf8";
    binmode $builder->todo_output,    ":utf8";

    use_ok 'DR::Tarantool::MsgPack::SyncClient';
    use_ok 'AnyEvent';
}

my $cfg = catfile dirname(__FILE__), 'data', 'll.lua';
my $cfgg = catfile dirname(__FILE__), 'data', 'll-grant.lua';

ok -r $cfg, "-r config file ($cfg)";
ok -r $cfgg, "-r config file ($cfgg)";


my $t = DR::Tarantool::StartTest->run(
    family  => 2,
    cfg     => $cfg,


);

ok $t->started, 'tarantool was started';

$t->admin(q[ box.schema.user.create('user1', { password = 'password' }) ]);
$t->admin(q[ box.schema.user.grant('user1', 'read,write,execute', 'universe')]);
$t->admin(q[ box.schema.create_space('name_in_script', { id = 7 }).n]);
$t->admin(q[ box.space.name_in_script:create_index('pk', { type = 'tree' })]);

my $tnt = DR::Tarantool::MsgPack::SyncClient->connect(
    port => $t->primary_port,
    user        => 'user1',
    password    => 'password',
#    spaces      => {
#        7 => {
#            name => 'name_in_script',
#            fields => [ 'id', 'name', 'age' ],
#            indexes => {
#                0  => { name => 'id', fields => [ 'id' ] }
#            }
#        },
#
#    },
#    Without schema specifiation -- automatic load
);

isa_ok $tnt => 'DR::Tarantool::MsgPack::SyncClient', 'client is created';
ok $tnt->ping, 'ping';

is_deeply
    $tnt->insert('name_in_script', [ 1, 'вася', 21 ])->raw,
    [ 1, 'вася', 21 ],
    'insert';

is eval { $tnt->insert('name_in_script', [ 1, 'вася', 21 ]) }, undef, 'repeat';
like $@ => qr{Duplicate key exists}, 'error message';
isnt $tnt->last_code, 0, 'last_code';
like $tnt->last_error_string => qr{Duplicate key}, 'last_error_string';

is_deeply
    $tnt->replace('name_in_script', [ 1, 'вася', 23 ])->raw,
    [ 1, 'вася', 23 ],
    'insert';
is_deeply
    $tnt->replace('name_in_script', [ 2, 'петя', 23 ])->raw,
    [ 2, 'петя', 23 ],
    'insert';

is_deeply
    $tnt->delete('name_in_script', 1)->raw,
    [ 1, 'вася', 23 ],
    'delete';

eval {
    $t->admin(q[ box.schema.create_space('test_temp_space_to_delete', { id = 123 }).n]);
    $t->admin(q[ box.space.test_temp_space_to_delete:drop() ]);
};
ok ( !$@, 'schema_id changed' );


is_deeply
    $tnt->select('name_in_script', 0, 1),
    undef,
    'select';

# Change schema
## insert
eval {
    $t->admin(q[ box.schema.create_space('test_temp_space_to_delete2', { id = 124 }).n]);
    $t->admin(q[ box.space.test_temp_space_to_delete2:create_index('pk', { type = 'tree' })]);
};
ok ( !$@, 'schema_id changed for insert' );

is_deeply
    $tnt->insert('test_temp_space_to_delete2', [ 1, 'вася', 21 ])->raw,
    [ 1, 'вася', 21 ],
    'insert changed schema';

$t->admin(q[ box.space.test_temp_space_to_delete2:truncate()]);
$t->admin(q[ box.space.test_temp_space_to_delete2:drop()]);


## update
eval {
    $t->admin(q[ box.schema.create_space('test_temp_space_to_delete2', { id = 124 }).n]);
    $t->admin(q[ box.space.test_temp_space_to_delete2:create_index('pk', { type = 'tree' })]);
};
ok ( !$@, 'schema_id changed for insert' );

is_deeply
    $tnt->insert('test_temp_space_to_delete2', [ 1, 'вася', 21 ])->raw,
    [ 1, 'вася', 21 ],
    'insert for update changed schema';

is_deeply
    $tnt->update('test_temp_space_to_delete2', 1, [['+',2,22]])->raw,
    [ 1, 'вася', 43 ],
    'update changed schema';


is_deeply
    $tnt->select('test_temp_space_to_delete2', 'pk', 1)->raw,
    [ 1, 'вася', 43 ],
    'select for update changed schema';


$t->admin(q[ box.space.test_temp_space_to_delete2:truncate()]);
$t->admin(q[ box.space.test_temp_space_to_delete2:drop()]);

## upsert
eval {
    $t->admin(q[ box.schema.create_space('test_temp_space_to_delete2', { id = 124 }).n]);
    $t->admin(q[ box.space.test_temp_space_to_delete2:create_index('pk', { type = 'tree' })]);
};
ok ( !$@, 'schema_id changed for insert' );

$tnt->upsert('test_temp_space_to_delete2', [1, 'вася', 43], [['+',2,22]]);

is_deeply
    $tnt->select('test_temp_space_to_delete2', 'pk', 1)->raw,
    [ 1, 'вася', 43 ],
    'select for upsert changed schema';

$tnt->upsert('test_temp_space_to_delete2', [1, 'вася', 43], [['+',2,22]]);

is_deeply
    $tnt->select('test_temp_space_to_delete2', 'pk', 1)->raw,
    [ 1, 'вася', 65 ],
    'select for upsert changed schema';


$t->admin(q[ box.space.test_temp_space_to_delete2:truncate()]);
$t->admin(q[ box.space.test_temp_space_to_delete2:drop()]);



## select
eval {
    $t->admin(q[ box.schema.create_space('test_temp_space_to_delete4', { id = 124 }).n]);
    $t->admin(q[ box.space.test_temp_space_to_delete4:create_index('pk', { type = 'tree' })]);
};
ok ( !$@, 'schema_id changed for select' );

is_deeply
    $tnt->insert('test_temp_space_to_delete4', [ 1, 'вася', 21 ])->raw,
    [ 1, 'вася', 21 ],
    'insert for select';

# some schema changes
$t->admin(q[ box.schema.create_space('test_temp_space_to_delete_now', { id = 222 }).n]);
$t->admin(q[ box.space.test_temp_space_to_delete_now:drop]);

is_deeply
    $tnt->select('test_temp_space_to_delete4', pk => [ 1 ])->raw,
    [ 1, 'вася', 21 ],
    'select changed schema';

$t->admin(q[ box.space.test_temp_space_to_delete4:drop()]);

## replace
eval {
    $t->admin(q[ box.schema.create_space('test_temp_space_to_delete3', { id = 124 }).n]);
    $t->admin(q[ box.space.test_temp_space_to_delete3:create_index('pk', { type = 'tree' })]);
};
ok ( !$@, 'schema_id changed for replace' );

is_deeply
    $tnt->insert('test_temp_space_to_delete3', [ 1, 'вася', 21 ])->raw,
    [ 1, 'вася', 21 ],
    'insert for replace';

# some schema changes
$t->admin(q[ box.schema.create_space('test_temp_space_to_delete_now', { id = 222 }).n]);
$t->admin(q[ box.space.test_temp_space_to_delete_now:drop]);

is_deeply
    $tnt->replace('test_temp_space_to_delete3', [1, 'петя', 33 ])->raw,
    [ 1, 'петя', 33 ],
    'replace';

$t->admin(q[ box.space.test_temp_space_to_delete3:drop()]);

## delete
eval {
    $t->admin(q[ box.schema.create_space('test_temp_space_to_delete5', { id = 124 }).n]);
    $t->admin(q[ box.space.test_temp_space_to_delete5:create_index('pk', { type = 'tree' })]);
};
ok ( !$@, 'schema_id changed for delete' );

is_deeply
    $tnt->insert('test_temp_space_to_delete5', [ 1, 'вася', 21 ])->raw,
    [ 1, 'вася', 21 ],
    'insert for replace';

# some schema changes
$t->admin(q[ box.schema.create_space('test_temp_space_to_delete_now', { id = 222 }).n]);
$t->admin(q[ box.space.test_temp_space_to_delete_now:drop]);

$tnt->delete('test_temp_space_to_delete5', [1]);
is_deeply
    $tnt->select('test_temp_space_to_delete5', pk => [ 1 ]),
    undef,
    'select deleted changed schema';


$t->admin(q[ box.space.test_temp_space_to_delete5:drop()]);


is_deeply
    $tnt->call_lua('box.space.name_in_script.index.pk:select', 2)->raw,
    [2, 'петя', 23],
    'call_lua';

is $tnt->last_code, 0, 'last code';
