/*============================================================================
 * FILE: TC-21_regex_compress_verify_propcase.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - PRXMATCH(perl_regex_literal, var) → REGEXP_INSTR(var, pattern) > 0
 *   - PRXCHANGE('s/pattern/repl/', -1, var) → REGEXP_REPLACE(var, pattern, repl)
 *   - COMPRESS(str, remove_chars) → REGEXP_REPLACE(str, '[remove_chars]', '')
 *   - COMPRESS(str, , 'dk') keep-digits modifier → REGEXP_REPLACE(str,'[^0-9]','')
 *   - COMPRESS(str, , 'ak') keep alpha+numeric  → REGEXP_REPLACE(str,'[^A-Za-z0-9]','')
 *   - COMPRESS(str, ' -', 'k') mixed remove+keep → no single Oracle native; needs TRANSLATE
 *   - VERIFY(str, allowed) → REGEXP_INSTR(str, '[^allowed_char_class]')
 *   - ANYDIGIT(str) → REGEXP_INSTR(str, '[0-9]')
 *   - NOTALPHA(str) → REGEXP_INSTR(str, '[^A-Za-z]')
 *   - PROPCASE(str) → INITCAP(LOWER(str))
 *   - CHAR(str, pos) → SUBSTR(str, pos, 1)
 *   - PRXCHANGE with nested calls → nested REGEXP_REPLACE
 *   - SAS date literals in WHERE → TO_DATE()
 *   - &macro_var. → ${prop_varname}
 *
 * COMPLEXITY: VERY HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   - COMPRESS keep modifier changes semantics completely from remove modifier;
 *     LLMs typically treat all COMPRESS calls as simple REPLACE and lose the modifier
 *   - VERIFY returns 0 when all chars are valid (success), > 0 for first bad position;
 *     Oracle REGEXP_INSTR returns 0 when NOT found — same polarity, different construction
 *   - PRXCHANGE n=-1 means unlimited substitutions; REGEXP_REPLACE replaces all by default
 *   - Nested PRXCHANGE calls must become nested REGEXP_REPLACE (order matters)
 *   - PROPCASE capitalises after spaces AND hyphens in SAS; INITCAP only after spaces
 *     in older Oracle — a subtle semantic gap LLMs miss
 *   - CHAR(str, pos) is SAS-specific; SUBSTR(str, pos, 1) is the Oracle form
 *============================================================================*/

%LET report_date = 20250531;
%LET client_code = NORTH;
%LET min_iban_len = 15;

PROC SQL;
   CREATE TABLE work.customer_identity_cleansed AS
   SELECT
       t1.customer_id,
       COMPRESS(t1.iban_raw, ' -') AS iban_clean,
       COMPRESS(t1.phone_raw, , 'dk') AS phone_digits_only,
       COMPRESS(t1.name_raw, , 'ak') AS name_alphanumeric,
       PROPCASE(LOWCASE(t1.last_name)) AS last_name_std,
       PROPCASE(LOWCASE(t1.first_name)) AS first_name_std,
       CHAR(t1.gender_code, 1) AS gender_initial,
       VERIFY(
           COMPRESS(t1.iban_raw, ' '),
           'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
       ) AS iban_invalid_char_pos,
       ANYDIGIT(t1.name_raw) AS name_first_digit_pos,
       NOTALPHA(COMPRESS(t1.last_name, ' ')) AS last_name_first_nonalpha_pos
   FROM source_data.customer_master t1
   WHERE t1.client_code = "&client_code."
     AND t1.valid_from le "&report_date."d
     AND t1.valid_to gt "&report_date."d;
QUIT;

PROC SQL;
   CREATE TABLE work.customer_iban_validated AS
   SELECT
       t1.customer_id,
       t1.iban_clean,
       t1.phone_digits_only,
       CASE
           WHEN LENGTH(t1.iban_clean) < &min_iban_len.
               THEN 'TOO_SHORT'
           WHEN t1.iban_invalid_char_pos > 0
               THEN 'INVALID_CHARS'
           WHEN PRXMATCH('/^[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}$/', t1.iban_clean) = 0
               THEN 'BAD_FORMAT'
           ELSE 'VALID'
       END AS iban_status,
       CASE
           WHEN PRXMATCH('/^[0-9]{10,15}$/', t1.phone_digits_only) > 0
               THEN 'VALID'
           WHEN t1.phone_digits_only IS MISSING
               THEN 'MISSING'
           ELSE 'INVALID'
       END AS phone_status,
       PRXCHANGE('s/\s{2,}/ /', -1, t1.last_name_std) AS last_name_dedup_spaces,
       PRXCHANGE('s/[^A-Za-z\s]//g', -1, t1.first_name_std) AS first_name_letters_only
   FROM work.customer_identity_cleansed t1;
QUIT;

PROC SQL;
   CREATE TABLE work.address_standardised AS
   SELECT
       t1.customer_id,
       t1.iban_status,
       t1.phone_status,
       t2.street_raw,
       PRXCHANGE('s/\bStr\.?\b/Strasse/i', -1,
           PRXCHANGE('s/\bSt\.?\b/Strasse/i', -1, t2.street_raw)
       ) AS street_std,
       COMPRESS(t2.postcode_raw, , 'dk') AS postcode_digits,
       PROPCASE(LOWCASE(t2.city_raw)) AS city_std,
       VERIFY(
           COMPRESS(t2.postcode_raw, ' '),
           '0123456789'
       ) AS postcode_nondigit_pos,
       CASE
           WHEN PRXMATCH('/^\d{4,5}$/', COMPRESS(t2.postcode_raw, ' ')) > 0
               THEN 'VALID'
           ELSE 'INVALID'
       END AS postcode_status
   FROM work.customer_iban_validated t1
   INNER JOIN source_data.customer_address t2
       ON t1.customer_id = t2.customer_id
      AND t2.address_type = 'PRIMARY'
      AND t2.valid_from le "&report_date."d
      AND t2.valid_to gt "&report_date."d
   WHERE t1.iban_status NE 'INVALID_CHARS';
QUIT;

PROC SQL;
   CREATE TABLE work.identity_quality_report AS
   SELECT
       t1.customer_id,
       t1.iban_status,
       t1.phone_status,
       t2.postcode_status,
       t2.city_std,
       CASE
           WHEN t1.iban_status = 'VALID'
            AND t1.phone_status = 'VALID'
            AND t2.postcode_status = 'VALID'
               THEN 'PASS'
           WHEN t1.iban_status NE 'VALID'
            AND t1.phone_status NE 'VALID'
               THEN 'FAIL'
           ELSE 'PARTIAL'
       END AS overall_quality_flag,
       PRXCHANGE('s/[[:punct:]]//g', -1,
           CATS(t1.last_name_dedup_spaces, ' ', t1.first_name_letters_only)
       ) AS full_name_normalised
   FROM work.customer_iban_validated t1
   INNER JOIN work.address_standardised t2
       ON t1.customer_id = t2.customer_id
   ORDER BY t1.customer_id;
QUIT;
