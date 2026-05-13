REVISION = '2'
# 1 → 2 (2026-05-13): bumped to force a full re-run of process_pdfs on
# all PDF-pipeline contexts. The pre-2026-05-13 OCR post-processor
# (fix_ocr_hebrew_text / fix_ocr_full_content) unconditionally reversed
# every Hebrew word's characters, corrupting modern Tesseract output —
# which already returns logical-order Hebrew. Verified end-to-end:
# 102/548 committee_decisions chunks and a tail across other PDF
# contexts had mojibake titles/dates/summaries. The cache key
# `(url, revision)` keeps revision='1' rows alongside any revision='2'
# rows produced after this bump; ops can compare or purge revision='1'
# rows once the rewritten extractions are verified.
