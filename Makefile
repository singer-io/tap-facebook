.DEFAULT_GOAL := test

test: pylint
	nosetests

pylint:
	pylint tap_facebook -d C,R,W
