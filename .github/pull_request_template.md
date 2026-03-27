# Pull Request

## Summary
<!-- What does this PR do? 1-2 sentences. -->

## Type of change
- [ ] New data source / ingestion
- [ ] Raw layer change (schema, format, partitioning)
- [ ] dbt model (new or modified)
- [ ] Data quality test
- [ ] Orchestration (DAG / flow change)
- [ ] Bug fix
- [ ] Refactor / cleanup
- [ ] Documentation

---

## Changes
<!-- List the specific files or models changed and why. -->

- 
- 

## Data impact
<!-- Does this change affect row counts, field values, or downstream models? -->

- **Models affected:**
- **Estimated row impact:**
- **Fields added / removed / renamed:**

---

## Testing
<!-- What did you do to validate this works correctly? -->

- [ ] Ran `dbt build` locally with no failures
- [ ] Ran `dbt test` — all tests pass
- [ ] Spot-checked output rows against source
- [ ] Verified null handling for nullable fields (e.g. `child_birthday`)
- [ ] Braze sync tested against a sandbox / test user segment (if applicable)

## Sample output
<!-- Paste a few rows or a COUNT(*) result that confirms the change looks right. -->

```
-- paste query + result here
```

---

## Checklist
- [ ] No hardcoded credentials or API keys
- [ ] `profiles.yml` changes are not committed (only `.yml.example` if needed)
- [ ] New models have at least `not_null` and `unique` tests on primary keys
- [ ] `dbt_project.yml` updated if new models or directories were added
- [ ] `README` updated if setup steps changed

## Notes for reviewer
<!-- Anything that needs extra eyes, open questions, or follow-up tickets. -->