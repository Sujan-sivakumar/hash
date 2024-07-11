import re
import json
import requests
import pdfplumber
import pandas as pd
import snowflake.connector
from snowflake.connector import DictCursor
from collections import Counter
from datetime import datetime


def extract_text_from_pdf(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            pdf_text = ''
            for page in pdf.pages:
                pdf_text += page.extract_text()
        return pdf_text
    except Exception as e:
        print(f"Error extracting text from PDF: {e}")
        return ""


def parse_resume(ocr_text):
    lines = ocr_text.split('\n')
    name = ''
    email = ''
    phone = ''
    dob = ''
    experience = ''
    current_company = ''
    college = ''
    skills = []
    in_skills_section = False
    phone_pattern = r'[\+\(]?[1-9][0-9 .\-\(\)]{8,}[0-9]'
    dob_pattern = r'\d{1,2}-\d{1,2}-\d{4}'
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

    for line in lines:
        line = line.strip()

        if not phone:
            match = re.search(phone_pattern, line)
            if match:
                phone = match.group(0)

        if not email:
            email_match = re.search(email_pattern, line)
            if email_match:
                email = email_match.group(0)

        if not dob and ('DOB' in line or 'Date of Birth' in line):
            dob_match = re.search(dob_pattern, line)
            if dob_match:
                dob_value = dob_match.group(0)
                try:
                    dob = datetime.strptime(dob_value, '%d-%m-%Y').strftime('%Y-%m-%d')
                except ValueError:
                    dob = ''

        if 'Experience' in line or 'Years of Experience' in line:
            experience = line

        if 'Current Company' in line or 'Company' in line:
            current_company = line

        if 'College' in line or 'University' in line:
            college = line

        if 'Skills' in line or 'Technical Skills' in line:
            in_skills_section = True
            continue

        if in_skills_section:
            if line:
                skills.append(line.strip())

    return name, email, phone, dob, experience, current_company, college, skills

def count_words(ocr_text):
    words = ocr_text.split()
    return len(words)

def most_common_words(ocr_text, num_common=2):
    words = re.findall(r'\b\w+\b', ocr_text.lower())
    common_words = Counter(words).most_common(num_common)
    return common_words


def send_request_to_gemini(prompt):
    gemini_url = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key=AIzaSyDxf0aOesKZeYwyokPRfav2p7sqqrhtUJk'
    headers = {
        'Content-Type': 'application/json',
    }
    try:
        response = requests.post(gemini_url, headers=headers, json=prompt)
        if response.status_code == 200:
            parsed_data = response.json()
            if 'text' in parsed_data and parsed_data['text']:
                generated_text = parsed_data['text']
            else:
                generated_text = None
            return generated_text
        else:
            print(f"Request failed with status code {response.status_code}")
            print(response.text)
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error with API request: {e}")
        return None

def insert_into_snowflake(df):
    conn_params = {
        'account': 'ajmkdgb-mk15830',
        'user': 'SUJAN',
        'password': 'Sujan@2005',
        'warehouse': 'COMPUTE_WH',
        'database': 'TASK',
        'schema': 'EMPTASK'
    }

    conn = None
    try:
        conn = snowflake.connector.connect(
            user=conn_params['user'],
            password=conn_params['password'],
            account=conn_params['account'],
            warehouse=conn_params['warehouse'],
            database=conn_params['database'],
            schema=conn_params['schema']
        )

        cur = conn.cursor()

        for index, row in df.iterrows():
           
            skills_json = json.dumps(row['Skills']) if row['Skills'] else None

            insert_query = """
                INSERT INTO resumes (name, email, phone, dob, college, skills)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            dob_value = row['DOB'] if row['DOB'] else None
            cur.execute(insert_query, (
                row['Name'],
                row['Email'],
                row['Phone'],
                dob_value,
                row['College'],
                skills_json 
            ))

        conn.commit()
        print("Data inserted successfully into Snowflake")

    except snowflake.connector.Error as e:
        print(f"Error inserting data into Snowflake: {e}")

    finally:
        if conn:
            conn.close()

def main():
    pdf_file_path = 'Resume 2.pdf'  
    ocr_text = extract_text_from_pdf(pdf_file_path)
    name, email, phone, dob, experience, current_company, college, skills = parse_resume(ocr_text)
    total_words = count_words(ocr_text)
    common_words = most_common_words(ocr_text)

    prompt = {
        "contents": [
            {
                "parts": [
                    {
                        "text": f"Given the resume, fetch the name: {name}, email: {email}, phone: {phone}, dob: {dob}, experience: {experience}, current company: {current_company}, college: {college}, top 5 skills: {', '.join(skills)}, vertica as one of Full stack, Data Engineering, Dev Ops, Manual Testing, Automation."
                    }
                ]
            }
        ]
    }

    generated_text = send_request_to_gemini(prompt)

    df = pd.DataFrame({
        'Name': [name],
        'Email': [email],
        'Phone': [phone],
        'DOB': [dob],
        'Experience': [experience],
        'Current Company': [current_company],
        'College': [college],
        'Skills': [skills],
        'Generated Text': [generated_text],
        'Total Words': [total_words],
        'Most Common Words': [common_words]
    })

    print("DataFrame created:")
    print(df)

    insert_into_snowflake(df)

if __name__ == "__main__":
    main()
