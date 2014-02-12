MOCHA?=node_modules/.bin/mocha
REPORTER?=spec
GROWL?=--growl
FLAGS=$(GROWL) --reporter $(REPORTER) --colors

dt-setup:
	riak-admin bucket-type create dt-test-set '{"props":{"datatype":"set","allow_mult":"true"}}'
	riak-admin bucket-type activate dt-test-set

test:
	$(MOCHA) $(shell find test -name "*-test.js") $(FLAGS)

one:
	$(MOCHA) $(NAME) $(FLAGS)

unit:
	$(MOCHA) $(shell find test/unit -name "*-test.js") $(FLAGS)

integration:
	$(MOCHA) $(shell find test/integration -name "*-test.js") $(FLAGS)

.PHONY: test
