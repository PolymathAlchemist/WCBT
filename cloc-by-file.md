# cloc by-file report

Generated via:
`cloc --vcs=git --by-file --md --hide-rate`

Note: Reformatted using PyCharm Markdown table formatter.

---

| cloc | github.com/AlDanial/cloc v 2.06 |
|------|---------------------------------|

| File                                                             |    blank |  comment |     code |
|:-----------------------------------------------------------------|---------:|---------:|---------:|
| .\gui\tabs\authoring_tab.py                                      |      129 |       56 |      486 |
| .\backup_engine\verify.py                                        |       98 |      165 |      372 |
| .\gui\tabs\restore_tab.py                                        |       92 |        6 |      349 |
| .\backup_engine\restore\service.py                               |       73 |      134 |      325 |
| .\backup_engine\restore\verify.py                                |       68 |      116 |      252 |
| .\backup_engine\backup\service.py                                |       56 |      112 |      251 |
| .\backup_engine\data_models.py                                   |       88 |      119 |      232 |
| .\wcbt\cli.py                                                    |       43 |       37 |      224 |
| .\backup_engine\manifest_store.py                                |       87 |      197 |      223 |
| .\gui\tabs\run_tab.py                                            |       55 |        5 |      215 |
| .\backup_engine\restore\stage.py                                 |       61 |      100 |      187 |
| .\backup_engine\profile_lock.py                                  |       66 |      114 |      185 |
| .\gui\dialogs\rule_editor_dialog.py                              |       46 |       28 |      177 |
| .\backup_engine\backup\execute.py                                |       54 |      143 |      155 |
| .\README.md                                                      |       70 |        0 |      149 |
| .\backup_engine\profile_store\sqlite_store.py                    |       50 |       93 |      141 |
| .\gui\adapters\profile_store_adapter.py                          |       33 |       25 |      136 |
| .\tests\audit_docstrings.py                                      |       57 |       87 |      128 |
| .\tools\audit_docstrings.py                                      |       57 |       86 |      127 |
| .\tests\test_profile_lock.py                                     |       31 |        0 |      126 |
| .\backup_engine\restore\plan.py                                  |       53 |      115 |      121 |
| .\tests\test_restore_plan_materialize.py                         |       30 |        2 |      115 |
| .\tools\minecraft\wcbt_fabric_probe_and_stage.bat                |       25 |       43 |      115 |
| .\backup_engine\restore\execute.py                               |       52 |      130 |      113 |
| .\backup_engine\paths_and_safety.py                              |       64 |      154 |      110 |
| .\wcbt\README.md                                                 |       48 |        0 |      106 |
| .\backup_engine\backup\plan.py                                   |       60 |      143 |      100 |
| .\tests\test_cli_exit_codes.py                                   |       28 |        1 |       96 |
| .\backup_engine\backup\scan.py                                   |       38 |       70 |       95 |
| .\tests\test_copy_execution.py                                   |       27 |        0 |       84 |
| .\tests\test_restore_conflicts.py                                |       16 |        1 |       83 |
| .\tests\test_end_to_end_backup_restore.py                        |       24 |       10 |       81 |
| .\tests\test_restore_verify_stage.py                             |       22 |        1 |       81 |
| .\UpdateAndRun.bat                                               |       11 |        8 |       69 |
| .\tests\test_manifest_store_list_backup_runs.py                  |       18 |        4 |       67 |
| .\backup_engine\restore\data_models.py                           |       29 |       95 |       65 |
| .\backup_engine\restore\materialize.py                           |       28 |       57 |       65 |
| .\backup_engine\backup\materialize.py                            |       21 |       32 |       62 |
| .\tests\test_restore_cli_dry_run_artifacts.py                    |       21 |       16 |       62 |
| .\pyproject.toml                                                 |       21 |        3 |       61 |
| .\tests\test_verify_run_writes_jsonl_on_failure_hash_mismatch.py |       20 |        2 |       60 |
| .\restore_cli.patch                                              |        6 |       34 |       59 |
| .\tests\test_verify_run_writes_jsonl_on_failure_unreadable.py    |       20 |        2 |       57 |
| .\tests\test_audit_docstrings_tool.py                            |       20 |        0 |       55 |
| .\tests\test_cli_parser_restore.py                               |       13 |        1 |       55 |
| .\tests\test_restore_promotes_stage.py                           |       15 |        2 |       54 |
| .\gui\app.py                                                     |       25 |        5 |       53 |
| .\tests\test_verify_run_writes_jsonl_on_failure.py               |       18 |        2 |       53 |
| .\tests\test_paths_and_safety.py                                 |       20 |        0 |       52 |
| .\tests\test_manifest_roundtrip.py                               |        8 |        0 |       50 |
| .\tests\test_restore_execute_promotion.py                        |       21 |        0 |       49 |
| .\tests\test_restore_service_execute.py                          |       17 |       10 |       45 |
| .\tests\test_verify_run_writes_report_on_failure.py              |       18 |        2 |       45 |
| .\tests\test_verify_run_writes_jsonl_on_success.py               |       16 |        0 |       43 |
| .\backup_engine\restore\verification_results.py                  |       23 |       73 |       41 |
| .\tests\verification_results.py                                  |       16 |        7 |       41 |
| .\backup_engine\restore\execution_results.py                     |       27 |       85 |       40 |
| .\backup_engine\backup\render.py                                 |       22 |       33 |       38 |
| .\backup_engine\restore\journal.py                               |       20 |       53 |       36 |
| .\tests\test_restore_promote_stage_to_destination.py             |       12 |        0 |       36 |
| .\.github\workflows\ci.yml                                       |       12 |        0 |       35 |
| .\tests\test_verify_run_writes_report_on_success.py              |       14 |        0 |       34 |
| .\LICENSE.md                                                     |       12 |        0 |       29 |
| .\tests\test_restore_stage_build.py                              |       14 |        0 |       29 |
| .\.pre-commit-config.yaml                                        |        3 |        0 |       27 |
| .\backup_engine\profile_store\api.py                             |       35 |      106 |       25 |
| .\tests\test_restore_execution_journal.py                        |       12 |        0 |       24 |
| .\backup_engine\init_profile.py                                  |       13 |       29 |       23 |
| .\tests\test_init_profile.py                                     |        6 |        0 |       22 |
| .\tests\test_default_data_root.py                                |        9 |        0 |       20 |
| .\tests\test_cli_smoke.py                                        |       10 |        6 |       19 |
| .\backup_engine\clock.py                                         |       16 |       31 |       18 |
| .\cloc_by_file_md.bat                                            |        5 |        2 |       18 |
| .\tools\minecraft\README.md                                      |        6 |        0 |       16 |
| .\.idea\inspectionProfiles\Project_Default.xml                   |        0 |        0 |       12 |
| .\.idea\wcbt.iml                                                 |        0 |        0 |       10 |
| .\backup_engine\restore\errors.py                                |       24 |       55 |        9 |
| .\tests\test_profile_store_sqlite_roundtrip.py                   |        6 |        3 |        9 |
| .\.idea\modules.xml                                              |        0 |        0 |        8 |
| .\.idea\misc.xml                                                 |        0 |        0 |        7 |
| .\.idea\vcs.xml                                                  |        0 |        0 |        7 |
| .\backup_engine\errors.py                                        |       14 |       13 |        7 |
| .\.idea\inspectionProfiles\profiles_settings.xml                 |        0 |        0 |        6 |
| .\wcbt\__main__.py                                               |       10 |       19 |        6 |
| .\backup_engine\exceptions.py                                    |        9 |        5 |        5 |
| .\cloc-by-file.md                                                |        4 |        0 |        5 |
| .\backup_engine\profile_store\errors.py                          |        7 |        4 |        4 |
| .\sync-dev.bat                                                   |        0 |        0 |        4 |
| .\sync-full.bat                                                  |        0 |        0 |        3 |
| .\backup_engine\profile_store\schema.py                          |        7 |       26 |        2 |
| .\backup_engine\__init__.py                                      |        0 |        1 |        0 |
| .\backup_engine\profile_store\__init__.py                        |        1 |        3 |        0 |
| .\backup_engine\restore\__init__.py                              |        1 |        4 |        0 |
| .\gui\__init__.py                                                |        1 |        2 |        0 |
| .\gui\adapters\__init__.py                                       |        2 |        9 |        0 |
| .\gui\tabs\__init__.py                                           |        0 |        1 |        0 |
| --------                                                         | -------  | -------  | -------  |
| SUM:                                                             |     2560 |     3138 |     7596 |
