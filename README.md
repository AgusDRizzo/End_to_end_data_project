# End_to_end_data_project
This project consists in an automated workflow to create a synthetic database in MySQL, simulating the database of a credit card company,  extract data on a monthly basis and predict clients with a churn profile.
# 🏦 Credit Card Transaction Simulation & Churn Prediction

## 📌 Project Overview

This project simulates a credit card company's database, automating transactions and payments while predicting potential customer churn. It integrates **Data Engineering, Data Analytics, and Machine Learning** into a single workflow.

🔹 **Database:** MySQL (Relational Model)  
🔹 **Automation:** Apache Airflow (Daily transactions & Monthly payments)  
🔹 **Machine Learning:** Predicting churn using a trained model  
🔹 **Reporting:** Automated PDF reports  

## 👨‍💻 About Me

Hi! I'm **Agustin**, a **biochemist** turned **data enthusiast**. Over the past two years, I've explored the data world, and this is my most ambitious project so far. It brings together my skills in **Data Engineering, Data Science, and Data Analytics** into a single, dynamic system.

---

## 🏗️ Project Structure

### **Step 1: Initialize Database**
🔹 `initiate_db.py` creates a synthetic database in MySQL.  
🔹 Generates **500 clients**, each with up to **6 credit cards**.  
🔹 Clients have a maximum **3-year** relationship with the company.  
🔹 **Initial 5000 synthetic transactions** are recorded for the past 2 years.  
🔹 Two client types:
   - **Debtless Clients (20%)**: Always pay full monthly balance.
   - **Other Clients (80%)**: Carry varying debt month to month.  

### **Step 2: Maintain Database**
🔹 **Daily:** Simulated credit card transactions (via Airflow).  
🔹 **Monthly:**
   - Payments are processed.
   - Customer consumption & payment patterns analyzed.
   - Machine Learning model predicts churn risk.
   - PDF reports are generated automatically.

---

## 🛠️ Tech Stack

| Technology | Purpose |
|------------|---------|
| **Python 3.12.2** | Core scripting |
| **MySQL** | Relational database storage |
| **Apache Airflow** | Task scheduling & automation |
| **Docker** | Containerized environment for MySQL & Airflow |
| **Pandas** | Data manipulation |
| **SQLAlchemy** | Python-MySQL interaction |
| **Sklearn** | Machine Learning (preprocessing & modeling) |
| **Pickle** | Model serialization |
| **ReportLab** | PDF report generation |
| **Seaborn & Matplotlib** | Data visualization |

---

## 🚀 How It Works

1️⃣ **Setup & Run**: Initialize the database using `initiate_db.py`.  
2️⃣ **Automate**: Airflow schedules daily transactions & monthly updates.  
3️⃣ **Predict Churn**: Monthly analysis identifies high-risk clients.  
4️⃣ **Generate Reports**: Insights are compiled into a PDF.  

---

## 📈 Visual Representation

### 🗄 Database Schema
The relational model of the credit card company's database:

![Database Schema](database_schema.png)

### 🔄 Airflow Task Execution
Below are the Airflow execution patterns for daily transactions and monthly churn predictions:

**Daily Transactions Execution:**
![Daily Transactions](create_daily_consumptions.jpg)

**Monthly Churn Prediction Execution:**
![Monthly Churn Prediction](predict_monthly_churn.jpg)

---

## 🤝 Contributing

Interested in improving this project? Feel free to open an issue or submit a pull request!

---

## 📜 License

This project is licensed under the **MIT License**.

---

## 📧 Contact

For any questions or collaborations, reach out to me on **[LinkedIn/GitHub]**!

