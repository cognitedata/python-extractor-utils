#!/bin/bash

set -e -x

target_schemas=(
    "schema/base_config.schema.json"
    "schema/unstable/connection_config.schema.json"
    "schema/unstable/schedule_config.schema.json"
    "schema/unstable/extractor_config.schema.json"
)

for schema in "${target_schemas[@]}"; do
    echo "Processing $schema"
    publish-extractor schema --schema "$schema" --output bundled.schema.json
    echo "Generating docs for $schema"
    publish-extractor docs --schema bundled.schema.json
    rm bundled.schema.json
done
