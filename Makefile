.PHONY: clean code-style coverage help test static-analysis install-dependencies install-dependencies-lowest update-dependencies pcov-enable pcov-disable infection-testing
.DEFAULT_GOAL := test

PHPUNIT =  ./vendor/bin/phpunit -c ./phpunit.xml
PHPSTAN  = ./vendor/bin/phpstan
PHPCS = ./vendor/bin/phpcs --extensions=php
CONSOLE = ./bin/console
INFECTION = ./vendor/bin/infection

clean:
	rm -rf ./build ./vendor

code-style: pcov-disable
	mkdir -p build/logs/phpcs
	${PHPCS} --report-full --report-gitblame --standard=PSR12 ./src --exclude=Generic.Commenting.Todo --report-junit=build/logs/phpcs/junit.xml

coverage: pcov-enable
	${PHPUNIT} && ./vendor/bin/coverage-check clover.xml 100

test: pcov-disable
	${PHPUNIT}

static-analysis: pcov-disable
	mkdir -p build/logs/phpstan
	${PHPSTAN} analyse --no-progress

update-dependencies:
	composer update

install-dependencies:
	composer install

install-dependencies-lowest:
	composer install --prefer-lowest

infection-testing:
	make coverage
	cp -f build/logs/phpunit/junit.xml build/logs/phpunit/coverage/junit.xml
	sudo php-ext-disable pcov
	${INFECTION} --coverage=build/logs/phpunit/coverage --min-msi=91 --threads=`nproc`
	sudo php-ext-enable pcov

pcov-enable:
	sudo php-ext-enable pcov

pcov-disable:
	sudo php-ext-disable pcov

help:
	# Usage:
	#   make <target> [OPTION=value]
	#
	# Targets:
	#   clean               		Cleans the coverage and the vendor directory
	#   code-style          		Check codestyle using phpcs
	#   coverage            		Generate code coverage (html, clover)
	#   help                		You're looking at it!
	#   test (default)      		Run all the tests with phpunit
	#   static-analysis     		Run static analysis using phpstan
	#   infection-testing   		Run infection/mutation testing
	#   install-dependencies		Run composer install
	#   install-dependencies-lowest	Run composer install with --prefer-lowest
	#   update-dependencies 		Run composer update
	#   pcov-enable         		Enable pcov
	#   pcov-disable        		Disable pcov
