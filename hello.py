import csv
import os
import boto3
from fpdf import FPDF
from dagster import job, op, get_dagster_logger, repository

@op
def load_s3(_):
    s3 = boto3.client("s3")
    Key="employees.csv"
    s3.download_file(
    Bucket="employeestorage", Key="employees.csv", Filename="D:/abc.csv"
)
    return Key
@op
def load_p(_,key):
   dataset_path = os.path.join(os.path.dirname(__file__), key)
   with open(dataset_path, "r") as fd:
       employee = [row for row in csv.DictReader(fd)]
   return employee

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
   pdf.cell(150, 10, txt = "The Average employee's Salary is "+str(salary),ln = 1, align = 'C')
   pdf.cell(150, 10, txt = "The Number of team leads are "+str(number_of_team_lead),ln = 2, align = 'C')
   pdf.output("summary.pdf")
   

@job
def small_pipeline():
    employee = load_p(load_s3())
    display_results(
       salary=average_salary(employee),
       number_of_team_lead=team_lead(employee),
   )

@repository
def repo():
   return [small_pipeline]