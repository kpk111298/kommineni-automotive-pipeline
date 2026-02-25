"""
generate_data.py
Generates synthetic dealership data for local dev/testing.
Run this once to populate data/raw/ with CSVs.
"""

import random
import os
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd

fake = Faker()
random.seed(None)  # different data each run

# ============================================================
# SECTION 1: LOCATIONS
# Kommineni Automotive has 5 branches across the US
# ============================================================

def generate_locations():
    locations = [
        {
            "location_id": "LOC001",
            "city": "Dallas",
            "state": "TX",
            "manager_name": "Marcus Johnson",
            "monthly_target": 850000,
            "opened_date": "2018-03-15"
        },
        {
            "location_id": "LOC002",
            "city": "Chicago",
            "state": "IL",
            "manager_name": "Sarah Mitchell",
            "monthly_target": 920000,
            "opened_date": "2017-06-01"
        },
        {
            "location_id": "LOC003",
            "city": "Atlanta",
            "state": "GA",
            "manager_name": "David Reyes",
            "monthly_target": 780000,
            "opened_date": "2019-01-10"
        },
        {
            "location_id": "LOC004",
            "city": "Phoenix",
            "state": "AZ",
            "manager_name": "Linda Park",
            "monthly_target": 700000,
            "opened_date": "2020-08-22"
        },
        {
            "location_id": "LOC005",
            "city": "Seattle",
            "state": "WA",
            "manager_name": "James Okafor",
            "monthly_target": 810000,
            "opened_date": "2019-11-05"
        }
    ]
    return pd.DataFrame(locations)


# ============================================================
# SECTION 2: EMPLOYEES
# Each location has salespeople and service technicians
# ============================================================

def generate_employees():
    employees = []
    employee_id = 1

    # Each location gets 4 salespeople and 3 service technicians
    for loc_num in range(1, 6):
        location_id = f"LOC00{loc_num}"

        # Generate 4 salespeople per location
        for _ in range(4):
            employees.append({
                "employee_id": f"EMP{str(employee_id).zfill(3)}",
                "full_name": fake.name(),
                "role": "salesperson",
                "location_id": location_id,
                "hire_date": fake.date_between(
                    start_date="-5y",
                    end_date="-6m"
                ).strftime("%Y-%m-%d"),
                "commission_rate": round(random.uniform(0.02, 0.05), 3)
            })
            employee_id += 1

        # Generate 3 service technicians per location
        for _ in range(3):
            employees.append({
                "employee_id": f"EMP{str(employee_id).zfill(3)}",
                "full_name": fake.name(),
                "role": "service_technician",
                "location_id": location_id,
                "hire_date": fake.date_between(
                    start_date="-5y",
                    end_date="-6m"
                ).strftime("%Y-%m-%d"),
                "commission_rate": 0.0
            })
            employee_id += 1

    return pd.DataFrame(employees)


# ============================================================
# SECTION 3: VEHICLES
# Each location has inventory of cars to sell
# ============================================================

def generate_vehicles():
    vehicles = []

    makes_models = [
        ("Toyota", "Camry", 28000),
        ("Toyota", "RAV4", 35000),
        ("Ford", "F-150", 45000),
        ("Ford", "Explorer", 38000),
        ("Honda", "Civic", 25000),
        ("Honda", "CR-V", 33000),
        ("BMW", "3 Series", 55000),
        ("BMW", "X5", 72000),
        ("Chevrolet", "Silverado", 42000),
        ("Chevrolet", "Equinox", 30000)
    ]

    vehicle_id = 1

    for loc_num in range(1, 6):
        location_id = f"LOC00{loc_num}"

        # Each location gets 20 vehicles in inventory
        for _ in range(20):
            make, model, base_price = random.choice(makes_models)
            # Add some price variation around the base price
            price = base_price + random.randint(-2000, 5000)

            vehicles.append({
                "vehicle_id": f"VEH{str(vehicle_id).zfill(4)}",
                "make": make,
                "model": model,
                "year": random.choice([2022, 2023, 2024, 2025]),
                "list_price": price,
                "status": random.choice(
                    ["available", "available", "available", "reserved"]
                ),
                "location_id": location_id
            })
            vehicle_id += 1

    return pd.DataFrame(vehicles)


# ============================================================
# SECTION 4: SALES TRANSACTIONS
# These are the actual car sales happening daily
# ============================================================

def generate_sales(employees_df, vehicles_df, days_back=30):
    sales = []
    transaction_id = 1

    # Get only salespeople (not technicians)
    salespeople = employees_df[
        employees_df["role"] == "salesperson"
    ].copy()

    # Get available vehicles
    available_vehicles = vehicles_df[
        vehicles_df["status"] == "available"
    ].copy()

    # Generate sales for each of the past 30 days
    for day in range(days_back, 0, -1):
        sale_date = datetime.now() - timedelta(days=day)

        # Each day generates 3 to 8 sales across all locations
        num_sales = random.randint(3, 8)

        for _ in range(num_sales):
            # Pick a random salesperson
            salesperson = salespeople.sample(1).iloc[0]

            # Pick a random available vehicle from same location
            location_vehicles = available_vehicles[
                available_vehicles["location_id"] == salesperson["location_id"]
            ]

            if len(location_vehicles) == 0:
                continue

            vehicle = location_vehicles.sample(1).iloc[0]

            # Sale price is usually slightly below list price (negotiation)
            sale_price = vehicle["list_price"] * random.uniform(0.92, 1.02)

            sales.append({
                "transaction_id": f"TXN{str(transaction_id).zfill(5)}",
                "vehicle_id": vehicle["vehicle_id"],
                "employee_id": salesperson["employee_id"],
                "location_id": salesperson["location_id"],
                "sale_price": round(sale_price, 2),
                "sale_date": sale_date.strftime("%Y-%m-%d %H:%M:%S"),
                "financing_approved": random.choice([True, True, False])
            })
            transaction_id += 1

    return pd.DataFrame(sales)


# ============================================================
# SECTION 5: SERVICE JOBS
# Cars coming in for oil changes, repairs etc
# ============================================================

def generate_service_jobs(employees_df, vehicles_df, days_back=30):
    jobs = []
    job_id = 1

    # Get only service technicians
    technicians = employees_df[
        employees_df["role"] == "service_technician"
    ].copy()

    job_types = [
        ("oil_change", 1.0, 89),
        ("tire_rotation", 0.5, 49),
        ("brake_replacement", 3.0, 350),
        ("engine_repair", 8.0, 950),
        ("transmission_service", 5.0, 650),
        ("ac_repair", 4.0, 480),
        ("battery_replacement", 1.0, 220)
    ]

    for day in range(days_back, 0, -1):
        job_date = datetime.now() - timedelta(days=day)

        # Each day generates 5 to 12 service jobs
        num_jobs = random.randint(5, 12)

        for _ in range(num_jobs):
            technician = technicians.sample(1).iloc[0]
            vehicle = vehicles_df.sample(1).iloc[0]
            job_type, est_hours, labor_rate = random.choice(job_types)

            # Sometimes jobs take longer than estimated (realistic!)
            actual_hours = est_hours * random.uniform(0.8, 1.4)

            jobs.append({
                "job_id": f"JOB{str(job_id).zfill(5)}",
                "vehicle_id": vehicle["vehicle_id"],
                "technician_id": technician["employee_id"],
                "location_id": technician["location_id"],
                "job_type": job_type,
                "estimated_hours": est_hours,
                "actual_hours": round(actual_hours, 2),
                "labor_revenue": round(actual_hours * labor_rate, 2),
                "job_date": job_date.strftime("%Y-%m-%d %H:%M:%S")
            })
            job_id += 1

    return pd.DataFrame(jobs)


# ============================================================
# MAIN FUNCTION
# This runs everything and saves CSV files
# ============================================================

def main():
    print("Kommineni Automotive - Generating data...")

    # Create output folder if it does not exist
    output_path = "../data/raw"
    os.makedirs(output_path, exist_ok=True)

    # Generate all datasets
    print("Generating locations...")
    locations = generate_locations()

    print("Generating employees...")
    employees = generate_employees()

    print("Generating vehicles...")
    vehicles = generate_vehicles()

    print("Generating sales transactions...")
    sales = generate_sales(employees, vehicles)

    print("Generating service jobs...")
    service_jobs = generate_service_jobs(employees, vehicles)

    # Save everything as CSV files
    locations.to_csv(f"{output_path}/locations.csv", index=False)
    employees.to_csv(f"{output_path}/employees.csv", index=False)
    vehicles.to_csv(f"{output_path}/vehicles.csv", index=False)
    sales.to_csv(f"{output_path}/sales_transactions.csv", index=False)
    service_jobs.to_csv(f"{output_path}/service_jobs.csv", index=False)

    print("")
    print("Data generation complete!")
    print(f"  Locations:    {len(locations)} rows")
    print(f"  Employees:    {len(employees)} rows")
    print(f"  Vehicles:     {len(vehicles)} rows")
    print(f"  Sales:        {len(sales)} rows")
    print(f"  Service Jobs: {len(service_jobs)} rows")
    print("")
    print(f"Files saved to: {output_path}/")


if __name__ == "__main__":
    main()