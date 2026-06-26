/*******************************************************************************
 * FILE: TC-10_string_functions_cats_tranwrd_index_scan.sas
 * SOURCE CHAT: SAS-to-Pentaho migration chat (anonymized)
 *
 * CONVERSION PATTERNS PRESENT:
 *   - CATS(a, b, c) → TRIM(a)||TRIM(b)||TRIM(c)  (strips trailing blanks then concat)
 *   - CAT(a, b, c)  → a||b||c  (no trimming, raw concat)
 *   - CATX(sep, a, b, c) → TRIM(a)||sep||TRIM(b)||sep||TRIM(c)
 *   - TRANWRD(str, from, to) → REPLACE(str, from, to)
 *   - INDEX(str, substr) → INSTR(str, substr) — returns 0 if not found (SAS) vs 0 (Oracle)
 *   - SCAN(str, n, delim) → complex REGEXP_SUBSTR or SUBSTR+INSTR expansion
 *   - TRIM(LEFT(str)) → LTRIM(str)
 *   - LEFT(str) alone → LTRIM(str)
 *   - RIGHT(str) → RTRIM(str)
 *   - REPEAT(str, n) → Oracle RPAD/LPAD trick or REGEXP trick (n repeats of str)
 *   - REVERSE(str) → Oracle REVERSE(str)  (compatible)
 *   - INPUT(str, format.) → TO_NUMBER() or TO_DATE() depending on format
 *   - PUT(date, YYMMDD10.) → TO_CHAR(date, 'YYYY-MM-DD')
 *   - PUT(num, Z8.)  → LPAD(TO_CHAR(num), 8, '0')
 *   - SUBSTR(str, pos, len) → Oracle SUBSTR (compatible)
 *   - LENGTH(str) → Oracle LENGTH (compatible; note SAS LENGTH excludes trailing spaces)
 *   - LENGTHN(str) → NVL(LENGTH(str), 0)  (SAS LENGTHN returns 0 for missing)
 *   - CHAR(str, pos) → SUBSTR(str, pos, 1)
 *
 * COMPLEXITY: HIGH
 *
 * EDGE CASES / TRICKY PARTS:
 *   1. CATS with 3+ args: must TRIM every argument before ||
 *   2. CATX with NULL args: SAS skips NULLs, Oracle does not — needs CASE guards
 *   3. SCAN(str, 2, ' ') — extracts nth token; Oracle needs nested INSTR+SUBSTR
 *   4. INDEX returns 0 (not found) in both SAS and Oracle — no change needed,
 *      but comparisons like INDEX(...) > 0 must be preserved.
 *   5. INPUT(str, DATE9.) → TO_DATE(str, 'DDMONYYYY') — format mapping required
 *   6. INPUT(str, 12.2)   → TO_NUMBER(str) — straightforward
 *   7. PUT(num, Z8.) is zero-padded integer; LPAD(TO_CHAR(num), 8, '0')
 *   8. PUT(date, YYMMDD10.) produces 'YYYY-MM-DD'; TO_CHAR(date, 'YYYY-MM-DD')
 *   9. LENGTH in SAS strips trailing spaces before measuring — Oracle LENGTH
 *      does not; semantically different on padded CHAR columns
 *   10. REPEAT(str, n) in SAS repeats the string n+1 times total (n extra copies)
 *       → Oracle: need REGEXP or RPAD trick — edge case for tool
 ******************************************************************************/

%LET report_date     = 20250531;
%LET client_id       = 77421;
%LET separator       = '-';
%LET mask_char       = X;

PROC SQL;
   CREATE TABLE WORK.NORMALISED_NAMES AS
   SELECT t1.PARTNER_ID,
          CATS(t1.TITLE_CODE, '. ', t1.FIRST_NAME, ' ', t1.LAST_NAME)
                                                          AS FULL_DISPLAY_NAME,
          CAT(t1.LAST_NAME, ', ', t1.FIRST_NAME)         AS SORT_NAME,
          CATX(' ', t1.FIRST_NAME, t1.MIDDLE_NAME, t1.LAST_NAME)
                                                          AS FULL_LEGAL_NAME,
          TRANWRD(t1.STREET_ADDRESS, 'Street', 'St.')     AS SHORT_ADDRESS,
          TRANWRD(TRANWRD(t1.STREET_ADDRESS, 'Avenue', 'Ave.'), 'Boulevard', 'Blvd.')
                                                          AS ABBREV_ADDRESS,
          UPCASE(STRIP(t1.EMAIL_ADDRESS))                 AS EMAIL_UPPER,
          LOWCASE(COMPRESS(t1.MOBILE_NUMBER, , 'kd'))     AS MOBILE_DIGITS
   FROM SOURCE.PARTNER_MASTER t1
   WHERE t1.STATUS_FLAG NOT = 'D'
     AND t1.CLIENT_ID = &client_id.;
QUIT;

PROC SQL;
   CREATE TABLE WORK.EMAIL_CLASSIFIED AS
   SELECT t1.PARTNER_ID,
          t1.EMAIL_UPPER,
          t1.FULL_DISPLAY_NAME,
          (CASE
              WHEN INDEX(t1.EMAIL_UPPER, '@COMPANY.COM')  > 0 THEN 'INTERNAL'
              WHEN INDEX(t1.EMAIL_UPPER, '@PARTNER.ORG')  > 0 THEN 'PARTNER'
              WHEN INDEX(t1.EMAIL_UPPER, '.GOV')          > 0 THEN 'GOVERNMENT'
              WHEN INDEX(t1.EMAIL_UPPER, '@') = 0             THEN 'INVALID'
              ELSE 'EXTERNAL'
           END)                                            AS EMAIL_CLASS,
          SUBSTR(t1.EMAIL_UPPER, 1, INDEX(t1.EMAIL_UPPER, '@') - 1)
                                                           AS EMAIL_LOCAL_PART,
          SUBSTR(t1.EMAIL_UPPER, INDEX(t1.EMAIL_UPPER, '@') + 1)
                                                           AS EMAIL_DOMAIN,
          LENGTH(t1.FULL_DISPLAY_NAME)                    AS NAME_LENGTH,
          LENGTHN(t1.MOBILE_DIGITS)                       AS MOBILE_LEN
   FROM WORK.NORMALISED_NAMES t1
   WHERE NOT MISSING(t1.EMAIL_UPPER);
QUIT;

PROC SQL;
   CREATE TABLE WORK.TOKENISED_REFERENCE AS
   SELECT t1.PARTNER_ID,
          t1.FULL_LEGAL_NAME,
          SCAN(t1.FULL_LEGAL_NAME, 1, ' ')               AS FIRST_TOKEN,
          SCAN(t1.FULL_LEGAL_NAME, 2, ' ')               AS SECOND_TOKEN,
          SCAN(t1.FULL_LEGAL_NAME, -1, ' ')              AS LAST_TOKEN,
          CHAR(t1.FULL_LEGAL_NAME, 1)                    AS INITIAL_CHAR,
          TRIM(LEFT(t1.ABBREV_ADDRESS))                  AS ADDR_LTRIMMED,
          RIGHT(STRIP(t1.SORT_NAME))                     AS SORT_RTRIMMED,
          REVERSE(t1.FULL_LEGAL_NAME)                    AS NAME_REVERSED,
          REPEAT('*', LENGTH(t1.MOBILE_DIGITS) - 1)      AS MASK_STARS
   FROM WORK.NORMALISED_NAMES t1
   WHERE t1.EMAIL_CLASS IS MISSING
      OR t1.MOBILE_LEN > 0;
QUIT;

PROC SQL;
   CREATE TABLE WORK.CONVERTED_TYPES AS
   SELECT t1.RECORD_ID,
          INPUT(t1.AMOUNT_CHAR, 14.2)                          AS AMOUNT_NUM,
          INPUT(t1.ENTRY_DATE_CHAR, DATE9.)                    AS ENTRY_DATE_SAS,
          INPUT(t1.ISO_DATE_CHAR, YYMMDD10.)                   AS ISO_DATE_SAS,
          PUT(INPUT(t1.ENTRY_DATE_CHAR, DATE9.), YYMMDD10.)    AS ENTRY_DATE_ISO_STR,
          PUT(t1.REFERENCE_NUM, Z8.)                           AS REF_ZERO_PADDED,
          PUT(t1.AMOUNT_NUM, COMMA12.2)                        AS AMOUNT_FORMATTED,
          PUT(INPUT(t1.ENTRY_DATE_CHAR, DATE9.), DDMMYY10.)    AS ENTRY_DATE_EU,
          CATS(PUT(t1.REFERENCE_NUM, Z8.), &separator.,
               PUT(INPUT(t1.ENTRY_DATE_CHAR, DATE9.), YYMMDD10.))
                                                               AS COMPOSITE_REF_KEY
   FROM STAGING.RAW_IMPORTS t1
   WHERE NOT MISSING(t1.RECORD_ID)
     AND NOT MISSING(t1.AMOUNT_CHAR)
     AND NOT MISSING(t1.ENTRY_DATE_CHAR);
QUIT;

PROC SQL;
   CREATE TABLE WORK.FINAL_PARTNER_EXPORT AS
   SELECT n.PARTNER_ID,
          n.FULL_DISPLAY_NAME,
          n.FULL_LEGAL_NAME,
          n.EMAIL_UPPER,
          e.EMAIL_CLASS,
          e.EMAIL_LOCAL_PART,
          e.EMAIL_DOMAIN,
          e.NAME_LENGTH,
          tok.FIRST_TOKEN,
          tok.LAST_TOKEN,
          tok.INITIAL_CHAR,
          c.AMOUNT_NUM,
          c.ENTRY_DATE_SAS,
          c.REF_ZERO_PADDED,
          c.COMPOSITE_REF_KEY,
          CATX(' | ', n.SHORT_ADDRESS, n.ABBREV_ADDRESS)  AS ADDRESS_COMBINED,
          CATS(e.EMAIL_CLASS, '_', PUT(n.PARTNER_ID, Z6.)) AS CLASSIFICATION_KEY
   FROM WORK.NORMALISED_NAMES   n
        LEFT JOIN WORK.EMAIL_CLASSIFIED  e   ON e.PARTNER_ID = n.PARTNER_ID
        LEFT JOIN WORK.TOKENISED_REFERENCE tok ON tok.PARTNER_ID = n.PARTNER_ID
        LEFT JOIN WORK.CONVERTED_TYPES   c   ON c.RECORD_ID  = n.PARTNER_ID
   WHERE n.EMAIL_CLASS IN ('INTERNAL', 'PARTNER')
      OR c.AMOUNT_NUM > &threshold_amount.
   ORDER BY n.FULL_LEGAL_NAME,
            n.PARTNER_ID;
QUIT;
