#!/bin/bash

mkdir data

cd data

wget http://data.githubarchive.org/2017-01-01-{0..23}.json.gz

gzip -d 2017-01-01-{0..23}.json.gz
