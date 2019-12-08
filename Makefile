.PHONY: clean code-style coverage help test static-analysis update-dependencies xdebug-enable xdebug-disable
.DEFAULT_GOAL := test

PHPUNIT =  ./vendor/bin/phpunit -c ./phpunit.xml
PHPDBG =  phpdbg -qrr ./vendor/bin/phpunit -c ./phpunit.xml
PHPSTAN  = ./vendor/bin/phpstan
PHPCS = ./vendor/bin/phpcs --extensions=php
CONSOLE = ./bin/console

clean:
	rm -rf ./build ./vendor

code-style:
	mkdir -p build/logs/phpcs
	${PHPCS} --report-full --report-gitblame --standard=PSR12 ./src --exclude=Generic.Commenting.Todo --report-junit=build/logs/phpcs/junit.xml

coverage: xdebug-disable
	${PHPDBG} && ./vendor/bin/coverage-check clover.xml 100

test: xdebug-disable
	${PHPUNIT}

static-analysis: xdebug-disable
	mkdir -p build/logs/phpstan
	${PHPSTAN} analyse --no-progress

update-dependencies:
	composer update

install-dependencies:
	composer install

install-dependencies-lowest:
	composer install --prefer-lowest

xdebug-enable:
	sudo php-ext-enable xdebug

xdebug-disable:
	sudo php-ext-disable xdebug

help:
	# Usage:
	#   make <target> [OPTION=value]
	#
	# Targets:
	#   clean               Cleans the coverage and the vendor directory
	#   code-style          Check codestyle using phpcs
	#   coverage            Generate code coverage (html, clover)
	#   help                You're looking at it!
	#   test (default)      Run all the tests with phpunit
	#   static-analysis     Run static analysis using phpstan
	#   install-dependencies Run composer install
	#   update-dependencies Run composer update
	#   xdebug-enable       Enable xdebug
	#   xdebug-disable      Disable xdebug
