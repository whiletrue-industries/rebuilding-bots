#!/usr/bin/env python3
"""
Create test PDF files for PDF discovery testing.
"""

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os

def create_test_pdf(filename, title, content):
    """Create a simple test PDF file."""
    c = canvas.Canvas(f"test_data/{filename}", pagesize=letter)
    width, height = letter
    
    # Add title
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 50, title)
    
    # Add content
    c.setFont("Helvetica", 12)
    y_position = height - 100
    
    # Split content into lines
    lines = content.split('\n')
    for line in lines:
        if y_position < 50:  # Start new page if needed
            c.showPage()
            y_position = height - 50
            c.setFont("Helvetica", 12)
        
        c.drawString(50, y_position, line)
        y_position -= 20
    
    c.save()
    print(f"Created: {filename}")

def main():
    """Create test PDF files."""
    print("Creating test PDF files...")
    
    # Ensure test_data directory exists
    os.makedirs("test_data", exist_ok=True)
    
    # Test document 1
    create_test_pdf(
        "test_document_1.pdf",
        "החלטה מספר 1 - ועדת האתיקה",
        """תאריך: 15 בינואר 2024
מספר החלטה: 2024-001

נושא: החלטה בנוגע לנוהלי עבודה

תוכן ההחלטה:
ועדת האתיקה של הכנסת החליטה לאשר את הנוהלים החדשים לעבודת הוועדה.
ההחלטה כוללת הנחיות ברורות לגבי התנהגות חברי הכנסת בדיונים.

סעיפים עיקריים:
1. נוהלי דיון בוועדה
2. כללי התנהגות לחברי הכנסת
3. הליכי אכיפה

החלטה זו נכנסת לתוקף מיידית."""
    )
    
    # Test document 2
    create_test_pdf(
        "test_document_2.pdf",
        "החלטה מספר 2 - ועדת האתיקה",
        """תאריך: 20 בינואר 2024
מספר החלטה: 2024-002

נושא: הנחיות לגבי ניגוד עניינים

תוכן ההחלטה:
הוועדה החליטה על הנחיות מפורטות בנוגע לניגוד עניינים של חברי הכנסת.
ההנחיות כוללות דרישות דיווח וכללי התנהגות.

סעיפים עיקריים:
1. הגדרת ניגוד עניינים
2. דרישות דיווח
3. הליכי בדיקה
4. סנקציות במקרה של הפרה

ההנחיות יחולו על כל חברי הכנסת החל מהכנס הבא."""
    )
    
    # Test document 3
    create_test_pdf(
        "test_document_3.pdf",
        "החלטה מספר 3 - ועדת האתיקה",
        """תאריך: 25 בינואר 2024
מספר החלטה: 2024-003

נושא: כללי שקיפות ופרסום

תוכן ההחלטה:
ועדת האתיקה מאשרת כללים חדשים לשקיפות ופרסום מידע.
הכללים נועדו לשפר את השקיפות בפעילות הכנסת.

סעיפים עיקריים:
1. פרסום החלטות הוועדה
2. שקיפות בדיונים
3. גישה למידע
4. הגנת פרטיות

הכללים ייכנסו לתוקף תוך 30 יום."""
    )
    
    # Report 2024
    create_test_pdf(
        "report_2024.pdf",
        "דוח שנתי 2024 - ועדת האתיקה",
        """תאריך: 1 בפברואר 2024
סוג מסמך: דוח שנתי

תקציר:
דוח זה מסכם את פעילות ועדת האתיקה בשנת 2024.
הדוח כולל סטטיסטיקות, החלטות חשובות ותכניות לעתיד.

תוכן הדוח:
1. סקירה כללית של השנה
2. החלטות עיקריות
3. סטטיסטיקות פעילות
4. אתגרים ופתרונות
5. תכניות לשנה הבאה

הדוח אושר על ידי הוועדה פה אחד."""
    )
    
    # Guidelines
    create_test_pdf(
        "guidelines.pdf",
        "הנחיות אתיקה לחברי הכנסת",
        """תאריך: 10 בפברואר 2024
סוג מסמך: הנחיות

מטרה:
מסמך זה מגדיר את כללי האתיקה הבסיסיים לחברי הכנסת.
ההנחיות מבוססות על עקרונות דמוקרטיים וערכי המדינה.

עקרונות יסוד:
1. יושרה אישית
2. שקיפות
3. אחריות ציבורית
4. כבוד הדדי

הנחיות מעשיות:
- התנהגות בדיונים
- יחסים עם הציבור
- ניגוד עניינים
- פרסום מידע

ההנחיות מחייבות את כל חברי הכנסת."""
    )
    
    print("\n✅ All test PDF files created successfully!")
    print("Files created in test_data/ directory:")
    for filename in ["test_document_1.pdf", "test_document_2.pdf", "test_document_3.pdf", 
                     "report_2024.pdf", "guidelines.pdf"]:
        print(f"  - {filename}")

if __name__ == "__main__":
    main() 