test:
	@./node_modules/.bin/mocha \
		--reporter spec \
		--bail \
		--timeout 5s \
		--require test/_common.js

.PHONY: test