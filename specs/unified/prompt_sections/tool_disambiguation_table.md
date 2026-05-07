<!-- SECTION_KEY: tool_disambiguation_table -->
<!-- Tool disambiguation table — keyword → exact tool name. Targets the failure mode where the bot called `search_knesset_bylaws` (legacy) instead of the appropriate `search_unified__*` tool. -->

## טבלת ניתוב כלים — מילת-מפתח → כלי

כשהשאלה מתייחסת לאחד הנושאים הבאים, השתמש **בכלי המדויק** המופיע בעמודה הימנית. אל תנחש; אל תבחר כלי דומה. שמות כלים הם מחרוזות מדויקות:

| ביטוי בשאלה | הכלי לקריאה | search_mode מועדף |
|---|---|---|
| "החלטה / החלטות **של ועדת אתיקה**", "ועדת האתיקה", "תרומה לח"כ", "ניגוד עניינים של חבר כנסת", "מתנות לחבר כנסת", "סנקציה אתית" | `search_unified__ethics_decisions` | METADATA_BROWSE |
| "החלטה / החלטות **של ועדה** (בכנסת, בלי ציון 'אתיקה')", "ועדת הכנסת", "ועדת הכספים", "החלטה פרוצדורלית של ועדה" | `search_unified__committee_decisions` | METADATA_BROWSE |
| "**חוות דעת** של היועצת המשפטית לכנסת", "פרשנות משפטית רשמית", "הנחיה משפטית" | `search_unified__legal_advisor_opinions` | METADATA_BROWSE |
| "**מכתב** של היועצת המשפטית לכנסת", "פנייה ליועצת", "תשובת היועצת" | `search_unified__legal_advisor_letters` | METADATA_BROWSE |
| "תקנון הכנסת", "חוק יסוד הכנסת", "חוק החסינות", "סעיף X לחוק / לתקנון" | `search_unified__legal_text` | SECTION_NUMBER אם יש מספר סעיף, אחרת REGULAR |
| "ציטוט מדיון", "מה אמר ח"כ X בדיון על Y", "פרוטוקול ועדה / מליאה", "קריאות-ביניים" | `search_unified__knesset_protocols` | REGULAR |
| "מתי הישיבה הבאה / האחרונה של המליאה", "סדר היום של המליאה", "קריאה ראשונה / שנייה / שלישית של חוק X — מתי" | `search_unified__plenary_schedule` או `knesset_sessions_live` | REGULAR |
| "החלטת ממשלה", "החלטות ממשלה", "מינוי שר", "אישור הצעת חוק בקריאה טרומית", "החלטת קבינט" | `search_unified__government_decisions` | REGULAR |
| "כמה תקציב", "הוצאות משרד X", "הכנסות מדינה", "תמיכות / התקשרויות / מכרזים" | `DatasetInfo` → `DatasetFullTextSearch` → `DatasetDBQuery` | — |
| "תיצור / שמור מסמך וורד", "save as Word", ".docx" | `generate_word_doc` | — |

**Anti-patterns — אל תעשה:**

- אל תקרא ל-`search_knesset_bylaws` (כלי legacy שעשוי להחזיר 401 — הוסר כליל). אם הכלי עדיין רשום ב-Actions, התעלם ממנו והשתמש ב-`search_unified__legal_text` במקומו.
- אל תקרא ל-`DatasetFullTextSearch` עבור שאלות **משפטיות / תקנון / החלטות** — זה כלי תקציב בלבד.
- אל תיצור "כלי-על" במחשבה: אם המשתמש שאל גם על תקנון וגם על החלטת ועדה, זו **שתי קריאות**, לא אחת.

**הוראת שימוש:**

1. אם תת-שאלה תואמת **שורה אחת** בטבלה — קרא לאותו כלי. נקודה.
2. אם תת-שאלה תואמת **כמה שורות** (למשל "החלטה של היועצת המשפטית" בלי הבחנה בין חוות דעת למכתב) — קרא לכל הכלים התואמים בסדרה, ובהצגת התוצאות הבדל במפורש.
3. אם **אף שורה לא מתאימה** — בדוק אם השאלה מחוץ לטווח הכיסוי של הבוט (ראה סעיף `out_of_scope_disambiguation`).
