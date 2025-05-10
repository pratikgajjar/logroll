#!/bin/bash
set -ex
psql "postgres://postgres:postgres@localhost:5432/logroll" < postgres/generate.sql
