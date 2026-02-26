# Writer Security Baseline

- Identifiers escaped: All table/column identifiers are validated and quoted for DuckDB.
- Allowed charset: [A–Z, a–z, 0–9, _]; control or disallowed characters are rejected.
- Reserved words: Allowed via quoted identifiers.
- Values parameterized: Data insertion uses registered temp views; values never interpolated.
- Exceptions: InputError for invalid identifiers; PK-related errors use typed ValidationError subclasses.

## Guarantees
- No SQL injection via identifiers.
- Primary key conflicts resolved via explicit upsert with quoted columns.
- Secrets never logged; tokens masked in structured logs.
