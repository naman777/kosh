-- Posted entries are never updated or deleted.
-- Triggers reject the change at write time; this check proves they are installed and enabled,
-- and that every resolved entry carries a resolution time and every unresolved one does not.
SELECT 'trigger_missing_or_disabled' AS problem, expected.tgname AS ref
FROM (VALUES ('postings_append_only'),
             ('postings_no_truncate'),
             ('journal_entries_guard'),
             ('journal_entries_no_truncate')) AS expected (tgname)
LEFT JOIN pg_trigger t ON t.tgname = expected.tgname AND t.tgenabled <> 'D'
WHERE t.oid IS NULL
UNION ALL
SELECT 'hold_resolution_time_mismatch', id::text
FROM journal_entries
WHERE expires_at IS NOT NULL AND (state = 'pending') = (resolved_at IS NOT NULL)
UNION ALL
SELECT 'direct_entry_has_resolution_time', id::text
FROM journal_entries
WHERE expires_at IS NULL AND resolved_at IS NOT NULL;
