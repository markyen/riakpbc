var should = require('should'),
    riak = require('../index').createClient({ bucket: 'test' });

describe('put', function () {
    it('can create an item with a given key', function (done) {
        riak.put({ data: 'test', key: 'test' }, function (err, reply) {
            done(err);
        }); 
    });

    it('can create an item with a riak generated key', function (done) {
        riak.put({ data: 'test' }, function (err, reply) {
            reply.should.have.ownProperty('key');
            riak.del({ key: reply.key }, function (err, reply) {
                done(err);
            });
        });
    });

    it('returns the body when asked', function (done) {
        riak.put({ data: 'test', returnbody: true }, function (err, reply) {
            reply.content[0].value.should.equal('test');
            riak.del({ key: reply.key }, function (err, reply) {
                done(err);
            });
        });
    });

    it('can create an item with an index', function (done) {
        riak.put({ data: 'one index', key: 'test_one', index: { test: 'one_index' } }, function (err, reply) {
            done(err);
        });
    });

    it('can create an item with two indexes', function (done) {
        riak.put({ data: 'two indexes', key: 'test_multi', index: { test: 'two_index', test2: 'two_index' } }, function (err, reply) {
            done(err);
        });
    });
});

describe('getIndexes', function () {
    it('can list indexes', function (done) {
        riak.getIndexes({ key: 'test_multi' }, function (err, reply) {
            reply.should.have.ownProperty('test');
            reply.test.should.equal('two_index');
            reply.should.have.ownProperty('test2');
            reply.test2.should.equal('two_index');
            done(err);
        });
    });
});

describe('get', function () {
    it('can get an item', function (done) {
        riak.get({ key: 'test' }, function (err, reply) {
            reply.content[0].value.should.equal('test');
            done(err);
        });
    });

    it('can get an item by index', function (done) {
        riak.get({ index: { test: 'one_index' } }, function (err, reply) {
            reply.should.be.an.instanceOf(Array);
            reply.length.should.equal(1);
            reply[0].content[0].value.should.equal('one index');
            done(err);
        });
    });

    it('can get an item by two indexes', function (done) {
        riak.get({ index: { test: 'two_index', test2: 'two_index' } }, function (err, reply) {
            reply.should.be.an.instanceOf(Array);
            reply.length.should.equal(1);
            reply[0].content[0].value.should.equal('two indexes');
            done(err);
        });
    });

    it('returns 404 when no item matches', function (done) {
        riak.get({ key: 'asdfasdfasdf' }, function (err, reply) {
            err.errmsg.should.equal('not found');
            done();
        });
    });
});

describe('getBuckets', function () {
    it('can list buckets', function (done) {
        riak.getBuckets(function (err, reply) {
            reply.should.have.ownProperty('buckets');
            reply.buckets.should.be.an.instanceOf(Array);
            reply.buckets.length.should.be.above(1);
            done(err);
        });
    });
});

describe('getBucket', function () {
    it('can get bucket properties', function (done) {
        riak.getBucket({ }, function (err, reply) {
            reply.should.have.ownProperty('props');
            done(err);
        });
    });
});

describe('setBucket', function () {
    it('can set bucket properties', function (done) {
        var allow_multi;
        riak.getBucket(function (err, reply) {
            allow_multi = !reply.props.allow_mult;
            riak.setBucket({ allow_mult: allow_multi }, function (err, reply) {
                riak.getBucket(function (err, reply) {
                    reply.props.allow_mult.should.equal(allow_multi);
                    done(err);
                });
            });
        });
    });
});


describe('getKeys', function () {
    it('can list keys', function (done) {
        riak.getKeys({ }, function (err, reply) {
            reply.should.have.ownProperty('keys');
            reply.keys.should.be.an.instanceOf(Array);
            reply.keys.length.should.be.above(1);
            done(err);
        });
    });

    it('can list keys matching an index', function (done) {
        riak.getKeys({ index: { test: 'one_index' } }, function (err, reply) {
            reply.should.have.ownProperty('keys');
            reply.keys.should.be.an.instanceOf(Array);
            reply.keys.length.should.equal(1);
            done(err);
        });
    });

    it('can list keys matching two indexes', function (done) {
        riak.getKeys({ index: { test: 'two_index', test2: 'two_index' } }, function (err, reply) {
            reply.should.have.ownProperty('keys');
            reply.keys.should.be.an.instanceOf(Array);
            reply.keys.length.should.equal(1);
            done(err);
        });
    });

    it('returns an empty array when no keys are found', function (done) {
        riak.getKeys({ index: { bacon: true } }, function (err, reply) {
            reply.should.have.ownProperty('keys');
            reply.keys.should.be.an.instanceOf(Array);
            reply.keys.length.should.equal(0);
            done(err);
        });
    });
});


describe('modify', function () {
    before(function (done) {
        riak.put({ key: 'test_modify', data: 'test' }, function (err, reply) {
            riak.get({ key: 'test_modify' }, function (err, reply) {
                done(err);
            });
        });
    });

    it('can modify a body', function (done) {
        riak.modify({ key: 'test_modify', transform: function (data) { return data + ' again'; } }, function (err, reply) {
            riak.get({ key: 'test_modify' }, function (err, reply) {
                reply.content[0].value.should.equal('test again');
                done(err);
            });
        });
    });

    it('can add an index', function (done) {
        riak.modify({ key: 'test_modify', index: { sample: 'changethis' } }, function (err, reply) {
            riak.getKeys({ index: { sample: 'changethis' } }, function (err, reply) {
                reply.should.have.ownProperty('keys');
                reply.keys.should.be.an.instanceOf(Array);
                reply.keys.length.should.equal(1);
                done(err);
            });
        });
    });

    it('can remove an index', function (done) {
        riak.modify({ key: 'test_modify', index: { sample: undefined } }, function (err, reply) {
            riak.getKeys({ index: { sample: 'changethis' } }, function (err, reply) {
                reply.should.have.ownProperty('keys');
                reply.keys.should.be.an.instanceOf(Array);
                reply.keys.length.should.equal(0);
                done(err);
            });
        });
    });
});


describe('del', function () {
    it('can delete a key', function (done) {
        riak.del({ key: 'test' }, function (err, reply) {
            done(err);
        });
    });

    it('cannot delete a key by index', function (done) {
        riak.del({ index: { test: 'one_index' } }, function (err, reply) {
            done(err);
        });
    });
});

describe('mapred', function () {
    function map(v) {
        return [v];
    }

    it('can mapreduce an entire bucket', function (done) {
        riak.mapred({ map: 'Riak.mapValues' }, function (err, reply) {
            reply.response.should.be.an.instanceOf(Array);
            done(err);
        });
    });

    it('can mapreduce based on key', function (done) {
        riak.mapred({ key: 'test', map: map }, function (err, reply) {
            reply.response.should.be.an.instanceOf(Array);
            done(err);
        });
    });

    it('can mapreduce based on two keys', function (done) {
        riak.mapred({ key: ['test', 'test_one'], map: map }, function (err, reply) {
            reply.response.should.be.an.instanceOf(Array);
            done(err);
        });
    });

    it('can mapreduce based on index', function (done) {
        riak.mapred({ index: { test: 'one_index' }, map: map }, function (err, reply) {
            reply.response.should.be.an.instanceOf(Array);
            reply.response.length.should.equal(1);
            done(err);
        });
    });

    it('can mapreduce based on two indexes', function (done) {
        riak.mapred({ index: { test: 'two_index', test2: 'two_index' }, map: map }, function (err, reply) {
            reply.response.should.be.an.instanceOf(Array);
            reply.response.length.should.equal(1);
            done(err);
        });
    });
});

describe('ping', function () {
    it('can ping the server', function (done) {
        riak.ping(function (err, reply) {
            done(err);
        });
    });
});
