.PHONY: up down db-init dev-producer dev-consumer dev-api dev test test-watch

up:
	npm run up

down:
	npm run down

db-init:
	npm run db:init

dev-producer:
	npm run dev:producer

dev-consumer:
	npm run dev:consumer

dev-api:
	npm run dev:api

dev:
	npm run dev

test:
	npm run test

test-watch:
	npm run test:watch
