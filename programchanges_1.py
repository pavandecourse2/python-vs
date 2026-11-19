employees = [
    {"employee_id": 1, "name": "Asha", "department": "Sales"},
    {"employee_id": 2, "name": "Ravi", "department": "IT"},
    {"employee_id": 3, "name": "Mina", "department": "Sales"},
]

grouped ={}
for employee in employees:
    department = employee["department"]
    grouped.setdefault(department, []).append(employee)
#print(grouped)
#####################################################################################
#11. Return the top k values
def top3_values(values):
    
    k=3
    top3_k=[]
    for i in values:
        position = 0
        while position < len(top3_k) and top3_k[position] >= i:
            position +=1
        top3_k.insert(position,i)

        if len(top3_k) >3:
            top3_k.pop()
    print("values of top3_k",top3_k)

values=[10,12,5,30,25,40,33]                
#top3_values(values)

###################################################################
def department_average(employees):
    total={}
    for row in employees:
         
        department = row["department"]
        amount = row["salary"]
        total[amount] = total.get(row,0)+ amount
        total[amount] = total[amount]/department

employees = [
    {"department": "Sales", "salary": 50000},
    {"department": "Engineering", "salary": 80000},
    {"department": "Sales", "salary": 60000},
]

#
# 
# department_average(employees)

#######################################################################
def department_average(employees):
    totals = {}
    counts = {}

    for row in employees:
        department = row["department"]
        salary = row["salary"]

        totals[department] = totals.get(department, 0) + salary
        counts[department] = counts.get(department, 0) + 1

        print(f"After {department}, salary {salary}:")
        print("  totals:", totals)
        print("  counts:", counts)

    averages = {
        department: totals[department] / counts[department]
        for department in totals
    }

    return averages

employees = [
    {"department": "Sales", "salary": 50000},
    {"department": "Engineering", "salary": 80000},
    {"department": "Sales", "salary": 60000},
]

department_average(employees)
#####################################

#13. Filter and select columns
#Question Given employees_df, keep employees with salary at least 80,000 and return employee name and salary

import pandas as pd
from pyspark.sql import function as f

employees_df = pd.DataFrame([
    {"name": "Asha", "department": "Sales", "salary": 75_000},
    {"name": "Ravi", "department": "Engineering", "salary": 80_000},
    {"name": "Mina", "department": "Engineering", "salary": 95_000},
])

least_salary_df = employees_df.filter(f.col("salary") >= 80000).select("name","department","salary")
display(least_salary_df)

select name,department,salary 
from employee_df
where salary >= 80000


##############################################
#14. Aggregate average salary by department
#Question Calculate average salary and headcount for every department. provide sample data
from pyspark.sql import functions as f

employees_df = spark.createDataFrame([
    ("Asha", "Sales", 75000),
    ("Ravi", "Engineering", 80000),
    ("Mina", "Engineering", 95000),
    ("Dev", "Sales", 65000),
    ("Noor", "HR", 60000),
], ["name", "department", "salary"])

#sql
select department ,avg(salary) as average_salary,count(employee) as head_count
from employee_df
group by department
order by department
#pyspark
department_average_salary_df = (employees_df.groupBy("department")
                                .agg(
                                     f.avg("salary").alias("average_salary"),
                                     f.count("employee").alias("head_count")
                                    )
)    
display(department_average_salary_df)

########################################################

#15. Join employees with department names
#Question Join employees_df to departments_df and show names with their department names.
employees_df = spark.createDataFrame([
    (1, "Asha", 10),
    (2, "Ravi", 20),
    (3, "Mina", 10),
], ["employee_id", "name", "department_id"])

departments_df = spark.createDataFrame([
    (10, "Sales"),
    (20, "Engineering"),
    (30, "HR"),
], ["department_id", "department_name"])

#sql
select name,department,e.dept_id
from employee e join department d 
on e.dept_id = d.dept_id

#pyspark
dept_name_df = (employees_df.alias("e").join(departments_df.alias("d"),f.col("e.dept_id") == f.col("d.dept_id") ,"inner")
          .select(f.col("e.name"),
                  f.col("e.department").alias("department"),
                  f.col("e.dept_id")
                  )
)  

display(dept_name_df)
###############################################################
#16. Rank employees within each department
#Question Add a rank column ordered by descending salary, with equal salaries receiving equal ranks

from pyspark.sql import functions as f
from pyspark.sql.window import Window

employees_df = spark.createDataFrame([
    ("Asha", "Sales", 75000),
    ("Ravi", "Sales", 90000),
    ("Mina", "Sales", 90000),
    ("Dev", "Engineering", 80000),
    ("Noor", "Engineering", 95000),
], ["name", "department", "salary"])

window = Window.partitionBy(f.col("department")).orderBy(f.col("salary").desc())

employee_rank_df = (employees_df.withColumn("rrn",f.rank().over(window)).orderby("department",f.col("rrn"))
.select("name","department","salary")
.drop(f.col("rrn"))
)

#sql
with rank_function as(

    select name,department,salary, rank() over(partition by department order by salary desc) as rrn
    from employees_df

)
    select name,department,salary 
    from rank_function
    order by department,rrn;

##########################################################################

#17. Keep the latest record per employee
#Question A history_df has employee_id, updated_at, and status. Keep the most recently updated row per employee

from pyspark.sql import functions as f
from pyspark.sql.window import Window

history_df = spark.createDataFrame([
    (101, "2026-01-10 09:00:00", "Active"),
    (101, "2026-02-15 14:30:00", "On Leave"),
    (102, "2026-01-20 11:00:00", "Active"),
    (102, "2026-03-01 08:15:00", "Inactive"),
], ["employee_id", "updated_at", "status"])

# Convert updated_at from text to timestamp
history_df = history_df.withColumn(
    "updated_at",
    f.to_timestamp("updated_at")
)

latest_window = (
    Window
    .partitionBy("employee_id")
    .orderBy(f.col("updated_at").desc())
)

latest_history_df = (
    history_df
    .withColumn("row_num", f.row_number().over(latest_window))
    .filter(f.col("row_num") == 1)
    .drop("row_num")
)

display(latest_history_df)