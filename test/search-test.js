var Lab = require('lab');
var expect = Lab.expect;
var describe = Lab.experiment;
var it = Lab.test;
var before = Lab.before;
var after = Lab.after;

var q = require('q');
var _ = require('lodash-node');

var client = require('../index').createClient({ host: 'localhost', port: 8087 });

var bucket = 'test-search-bucket-' + Date.now();
var index = 'test-search-index-' + Date.now();
var schema = {
    name: 'test-search-schema-' + Date.now(),
    content: require('fs').readFileSync(require('path').resolve(__dirname, './yz-test-schema.xml'))
};

describe('Search', function () {

    describe('prepare', function () {
        it('create schema', function (done) {
            client.yzPutSchema({
                schema: {
                    name: schema.name,
                    content: schema.content
                }
            }, function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('get schema', function (done) {
            client.yzGetSchema({
                name: schema.name,
            }, function (err, reply) {
                expect(err).to.not.exist;
                expect(reply).to.be.an('object');
                expect(reply).to.have.property('schema');
                expect(reply.schema).to.have.property('name', schema.name);
                expect(reply.schema).to.have.property('content', schema.content.toString());
                done();
            });
        });

        it('create index', {timeout: 5000}, function (done) {
            client.yzPutIndex({
                index: {
                    name: index,
                    schema: schema.name
                }
            }, function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('get index', {timeout: 10000}, function (done) {
            setTimeout(function () {
                client.yzGetIndex({
                    name: index,
                }, function (err, reply) {
                    expect(err).to.not.exist;
                    expect(reply).to.be.an('object');
                    expect(reply).to.have.property('index').that.is.an('array').and.have.length(1);
                    expect(reply.index[0]).to.be.an('object');
                    expect(reply.index[0]).to.be.have.property('name', index);
                    expect(reply.index[0]).to.be.have.property('schema', schema.name);
                    done();
                });
            }, 7000); // wait for solr to create its index
        });
    });

    describe('search for docs', function () {
        before(function (done) {
            client.setBucket({
                bucket: bucket,
                props: {
                    search_index: index
                }
            }, function (err) {
                expect(err).to.not.exist;

                q.all(_.map(['abc 123', 'def 456', 'abc def 123 456'], function (text, ind) {
                    return q.ninvoke(client, 'put', {
                        bucket: bucket,
                        key: 'key' + ind,
                        content: {
                            value: JSON.stringify({
                                title: 'test',
                                text: text
                            }),
                            content_type: 'application/json'
                        }
                    });
                }))
                .then(function () { return q.delay(1000); }) // wait for docs to index
                .nodeify(done);
            });
        });

        it('should return docs', function (done) {
            client.search({
                q: 'text:abc AND title:test',
                index: index
            }, function (err, reply) {
                expect(err).to.not.exist;
                expect(reply).to.be.an('object');
                expect(reply).to.have.property('docs').that.is.an('array').and.have.length(2);
                expect(reply).to.have.property('num_found', 2);
                var keys = _.map(reply.docs, function (doc) {
                    return _.find(doc.fields, {key: '_yz_rk'}).value;
                });
                expect(keys).to.contain('key0');
                expect(keys).to.contain('key2');
                done();
            });
        });
    });

});

