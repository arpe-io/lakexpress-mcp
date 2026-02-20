# Changelog

## [0.1.0] - 2026-02-20

### Added
- Initial release of LakeXpress MCP Server
- 6 MCP tools: preview_command, execute_command, validate_auth_file, list_capabilities, suggest_workflow, get_version
- Support for all 14 LakeXpress subcommands: logdb (init/drop/truncate/locks/release-locks), config (create/delete/list), sync, sync[export], sync[publish], run, status, cleanup
- Version detection and capabilities registry for LakeXpress v0.2.8
- Comprehensive input validation with Pydantic models
- Command builder with proper CLI argument construction
- Execution logging with configurable log directory
- Auth file validation tool
- Workflow suggestion engine
