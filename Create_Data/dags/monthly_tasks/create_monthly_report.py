import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime, timedelta

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.platypus import Table, TableStyle, Paragraph
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dateutil.relativedelta import relativedelta

def create_churn_report(df_1, df_2,  output_pdf, execution_date):

    execution_date = datetime.fromisoformat(str(execution_date))
    execution_date = execution_date.replace(tzinfo=None)
    execution_date = pd.to_datetime(execution_date) 
    quatrimestral_limit =  execution_date - relativedelta(days=100)
    quatrimestral_limit = quatrimestral_limit.replace(tzinfo=None)

    df_1['date'] = pd.to_datetime(df_1['date'])
    df_1 = df_1[(df_1['date'] < execution_date) & (df_1['date'] > quatrimestral_limit)]
    df_1 = df_1.sort_values('date', ascending=True)
    unique_dates = df_1['date'].unique()
    ordered_list_months = []
    for i in unique_dates:
        m = i.month
        ordered_list_months.append(m)
    df_1['month'] = pd.DatetimeIndex(df_1['date']).month
    df_1['year'] = pd.DatetimeIndex(df_1['date']).year
    df_graph = df_1[['month', 'churn_id']].groupby('month').count()

    

    # Get page dimensions
    w, h = letter
    
    # Create canvas
    c = canvas.Canvas(output_pdf, pagesize=letter)
    
    
    
    # ====== PAGE 1: Title and Graph ======
    
    # Generate and save plot
    plt.figure(figsize=(8, 5))
    sns.barplot(data=df_graph, x='month', y='churn_id', palette="YlOrBr", order=ordered_list_months)
    plt.title("Churn per Month")
    plt.xlabel("Month")
    plt.ylabel("Amount of churn flags")
    plt.savefig("plot.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    
    # Add title
    c.setFont("Helvetica-Bold", 24)
    title = "Churn Analysis Report"
    
    title_width = c.stringWidth(title)
    c.drawString((w - title_width) / 2, h - 50, title)
    
    # Insert Image - positioned below title with more space
    c.drawImage("plot.png", 50, h - 400, width=500, height=300)
    
    # Show page number
    c.setFont("Helvetica", 10)
    c.drawString(w - 50, 30, "1")
    
    # Move to next page
    c.showPage()
    
    # ====== PAGE 2: Table ======
    
    # Add title to second page
    c.setFont("Helvetica-Bold", 24)
    c.drawString(50, h - 50, "List of top 20 clients with churn profile")
    
    

    # Convert DataFrame to table data
    column_headers = [col for col in df_2.columns if col not in ['marital_status', 'gender']]
    data = [column_headers] + df_2[column_headers].head(20).values.tolist()

    #header_mapping = {
    #'client_id': 'ID',
    #'first_name': 'First Name',
    #'last_name': 'Last Name',
    #'email': 'Email',
    #'educational_level': 'Education',
    #'income_category': 'Income',
    #'phone_number': 'Phone',
    #'date': 'Join Date',
    #'age': 'Age',
    #'number_of_churn_flags': 'Churn Flags'
    #      *}
    ## Apply the mapping to your column headers
    #column_headers = [header_mapping.get(col, col) for col in df_2.columns if col not in ['marital_status', 'gender']]
    

    # Specify relative column widths.
    column_proportions = {
        'client_id': 0.05,          
        'first_name': 0.08,
        'last_name': 0.08,
        'email': 0.18,              
        'educational_level': 0.1,   
        'income_category': 0.1,     
        'phone_number': 0.08,
        'date': 0.08,
        'age': 0.04,                
        'number_of_churn_flags': 0.13  
    }

    # Calculate widths based on proportions and available width
    available_width = w - 100  # 50px margin on each side
    col_widths = [available_width * column_proportions.get(col, 0.1) for col in column_headers]
    
   
    
    # Create table
    table = Table(data, colWidths=col_widths)
    # Modify your TableStyle to optimize header display
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),  # Center header text
        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),   # Left align data
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('TOPPADDING', (0, 0), (-1, 0), 6),    # Add top padding to headers
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 0), (-1, 0), 5),      # Smaller font for headers
        ('FONTSIZE', (0, 1), (-1, -1), 5),     # Smaller font for data
        ('WORDWRAP', (0, 0), (-1, -1), True),  # Enable word wrapping
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE') # Vertically center all content
    ]))

        # Create a mapping for cleaner header names
    
    
    # Position table on second page with more space at top
    table.wrapOn(c, w, h)
    table.drawOn(c, 50, h - 500)  # Positioned higher on the page

    # Add footer with date
    c.setFont("Helvetica", 9)
    c.drawString(50, 30, f"Report generated on: {pd.Timestamp.now().strftime('%Y-%m-%d')}")
    
    # Show page number
    c.setFont("Helvetica", 10)
    c.drawString(w - 50, 30, "2")
    
    c.save()
    print(f"PDF saved at {output_pdf}")

    



def create_monthly_report(**context):

    base_path = context.get('project_path')
    execution_date = context['execution_date']

    # Convert execution_date to a formatted string
    ds = execution_date.strftime('%Y-%m-%d')  # 'YYYY-MM-DD' format

    # Define date boundaries
    start_date = (execution_date.replace(day=1) - pd.DateOffset(months=4)).strftime('%Y/%m/01')
    end_date = execution_date.strftime('%Y/%m/01')

    query_1 = """
            SELECT * FROM credit_card_db.churn_clients
        """

    query_2 = text(
    f"""
    WITH 
        churn AS (
	        SELECT 
                client_id, COUNT(churn_id) AS number_of_churn_flags
	        FROM 
                credit_card_db.churn_clients
            WHERE  
                churn_clients.date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 
                client_id
                    )
    SELECT 
        clients.*, churn.number_of_churn_flags
    FROM 
        credit_card_db.clients
    RIGHT JOIN 
        churn
    ON 
        clients.client_id = churn.client_id
    ORDER BY 
        number_of_churn_flags DESC
        """)

    engine = create_engine('mysql+pymysql://root:admin@db:3306/credit_card_db?connect_timeout=60')
    df_1 = pd.read_sql_query(query_1, engine)
    df_2 = pd.read_sql_query(query_2, engine)
    output_file = os.path.join(
        base_path, 
        'dags', 
        'monthly_tasks', 
        'monthly_reports', 
        f"report_{execution_date.year}_{execution_date.month}.pdf"
    )



    create_churn_report(df_1, df_2,  output_file, execution_date)


