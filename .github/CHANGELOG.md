## Unreleased

- **Airbyte schema handling**: Improved `create_yml_schema.py` for both `init_setup_files_v1` and `init_setup_files_v2` to robustly handle the new Airbyte connection schema format.
  - Added recursive flattening for nested `object` fields into dot-notation column names.
  - Safely handle `anyOf` branches and objects that only define `additionalProperties` (no `properties`) to avoid `KeyError: 'properties'`.
  - Kept existing behaviour for flat schemas that use `airbyte_type` (e.g. MySQL-style connections), ensuring backwards compatibility.

