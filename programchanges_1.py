employees = [
    {"employee_id": 1, "name": "Asha", "department": "Sales"},
    {"employee_id": 2, "name": "Ravi", "department": "IT"},
    {"employee_id": 3, "name": "Mina", "department": "Sales"},
]

grouped ={}
for employee in employees:
    department = employee["department"]
    grouped.setdefault(department, []).append(employee)

print(grouped)
