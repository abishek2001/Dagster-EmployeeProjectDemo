from cgitb import small
import csv
import os
import boto3
from fpdf import FPDF
from dagster import job, op, get_dagster_logger, repository
from dagster import RunRequest, ScheduleDefinition
from pathlib import Path
import numpy as np
import pandas as pd

@op
def load_s3(_):
    s3 = boto3.client("s3")
    Key="employees.csv"
    s3.download_file(
    Bucket="employeestorage", Key="employees.csv", Filename="D:/abc.csv"
)
    return Key
@op
def load_data(_,key):
   dataset_path = os.path.join(os.path.dirname(__file__), key)
   with open(dataset_path, "r") as fd:
       employee = [row for row in csv.DictReader(fd)]
   return employee

@op
def report_generator(_,key):
   data_file = Path(key)
   df = pd.read_csv(data_file)
   count_row = df.shape[0]
   records = df.to_dict(orient='records')
   for i in range(count_row):
      record = records[i]
      report = {'employeefname': record['FIRST_NAME'], 'employeesname': record['LAST_NAME'], 'salary': record['SALARY']}
      pdf = FPDF()
      pdf.add_page()
      pdf.set_font("Arial", size=15)
      pdf.cell(200, 10, txt="Employee's Individual Offer Letter ", ln=2, align='C')
      
      pdf.cell(200, 10, txt="Welcome to Quicket Solutions", ln=2, align='C') 
      pdf.cell(200, 10, txt="Hello Mr. "+record['FIRST_NAME']+" "+record['LAST_NAME'], ln=2, align='C') 
      pdf.cell(150, 10, txt = "Your Salary would be "+str(record['SALARY']),ln = 1, align = 'C')
      
      pdf.output("./output/{}_OfferLetter.pdf".format(record['FIRST_NAME']))
      s3 = boto3.client("s3")
      s3.upload_file(
      Filename="./output/{}_OfferLetter.pdf".format(record['FIRST_NAME']),
      Bucket="employeedetailsstorage",
      Key="details/output/{}_OfferLetter.pdf".format(record['FIRST_NAME']),
)

@op
def average_salary(_, employee):
   employee_salary = [int((emp["SALARY"])) for emp in employee]
   total_salary = sum(employee_salary)
   nb_of_employee = len(employee)
   return total_salary/nb_of_employee


@op
def team_lead(_, employee):
   team_lead_list = [lead for lead in employee if lead["is_team_lead"] == "TRUE"]
   return len(team_lead_list)

@op
def display_results(context, salary, number_of_team_lead):
   context.log.info(f"Average employee's salary: {salary}")
   context.log.info(f"Number of team lead: {number_of_team_lead}")
   pdf = FPDF()
   pdf.add_page()
   pdf.set_font("Arial", size = 15)
   pdf.cell(200, 10, txt = "Employee's Report Summary ",ln = 1, align = 'C')
   pdf.cell(100, 10, txt = "The Average employee's Salary is "+str(salary),ln = 1, align = 'C')
   pdf.cell(100, 10, txt = "The Number of team leads are "+str(number_of_team_lead),ln = 1, align = 'C')
   pdf.output("summary.pdf")
   s3 = boto3.client("s3")
   s3.upload_file(
    Filename="summary.pdf",
    Bucket="employeestorage",
    Key="summary.pdf",
)
   

@job
def small_pipeline():
    employee = load_data(load_s3())
    report_generator(load_s3())
    display_results(
       salary=average_salary(employee),
       number_of_team_lead=team_lead(employee),
   )

@repository
def repo():
   return [small_pipeline]

basic_schedule = ScheduleDefinition(job=small_pipeline, cron_schedule="0 0 * * *")