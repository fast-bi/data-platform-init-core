# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.1.1] - 2025-01-19

### Fixed
- **Critical Bug Fix**: Resolved `KeyError: 'properties'` in `create_yml_schema.py`
  - **Issue**: The script was failing when processing JSON schema structures that don't contain nested properties
  - **Location**: Line 250 in `create_yml_schema.py` within the `create_source_yml_dict` function
  - **Root Cause**: Code assumed all object types would have a "properties" key without checking for its existence
  - **Solution**: Added conditional check `if "properties" in col_info:` before accessing properties
  - **Impact**: Script now handles both nested object structures and simple object types gracefully
  - **Error Context**: 
    ```
    KeyError: 'properties'
    File "/usr/src/app/fastbi_resources_bi/create_yml_schema.py", line 250, in create_source_yml_dict
    for sub_col_obj_prop in col_info["properties"]:
    ```

### Technical Details
- **Files Modified**: `init_setup_files_v2/create_yml_schema.py`
- **Function**: `create_source_yml_dict`
- **Change Type**: Defensive programming - added null safety check
- **Backward Compatibility**: ✅ Maintained - no breaking changes
- **Testing**: Handles edge cases where JSON schema objects lack nested properties

### Migration Notes
- No migration required
- Existing functionality preserved
- Enhanced error handling for malformed or incomplete JSON schemas

---

## [v0.1.0] - Initial Release
- Initial version of the data platform initialization core
- Basic YAML schema generation functionality
- Support for multiple data warehouse platforms (Snowflake, Redshift, BigQuery)
