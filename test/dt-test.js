var chai = require('chai');
chai.Assertion.includeStack = true; // defaults to false
var expect = chai.expect;

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
