var Lab = require('lab');
var expect = Lab.expect;
var describe = Lab.experiment;
var it = Lab.test;
var before = Lab.before;
var after = Lab.after;

var riakpbc = require('../index');
var client = riakpbc.createClient({ host: 'localhost', port: 8087 });

/**
 * make sure to do run `make dt-setup` to create required bucket types
 */

var bucket = 'dt-test-bucket', bucketType = 'dt-test-set';

describe('DataTypes', function () {

    describe('set', function () {

        it('#updateDtype - add value', function (done) {
            client.updateDtype({
                bucket: bucket,
                key: 'test',
                type: bucketType,
                return_body: true,
                op: {
                    set_op: {
                        adds: 'someSetValue'
                    }
                }
            }, function (err, reply) {
                if (err && err.message.indexOf('Error no bucket type') !== -1) {
                    throw new Error('Please run "make dt-setup" before running tests');
                }
                expect(err).to.not.exist;
                expect(reply.set_value).to.exist;
                expect(reply.set_value).to.contain('someSetValue');
                done();
            });
        });

        it('#fetchDtype - get value', function (done) {
            client.fetchDtype({
                bucket: bucket,
                key: 'test',
                type: bucketType
            }, function (err, reply) {
                expect(err).to.not.exist;
                expect(reply.value.set_value).to.exist;
                expect(reply.value.set_value).to.contain('someSetValue');
                done();
            });
        });

        it('#updateDtype - remove value', function (done) {
            client.updateDtype({
                bucket: bucket,
                key: 'test',
                type: bucketType,
                return_body: true,
                op: {
                    set_op: {
                        removes: 'someSetValue'
                    }
                }
            }, function (err, reply) {
                expect(err).to.not.exist;
                expect(reply.set_value).to.not.exist;
                done();
            });
        });

    });

});
