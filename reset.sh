#!/bin/bash
set -ex
psql "postgres://postgres:postgres@localhost:5432/postgres" < postgres/reset.sql