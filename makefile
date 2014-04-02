dt-setup:
	riak-admin bucket-type create dt-test-set '{"props":{"datatype":"set","allow_mult":"true"}}'
	riak-admin bucket-type activate dt-test-set

test:
	@node node_modules/lab/bin/lab

test-cov:
	@node node_modules/lab/bin/lab -t 100

test-cov-html:
	@node node_modules/lab/bin/lab -r html -o coverage.html

.PHONY: test test-cov test-cov-html
