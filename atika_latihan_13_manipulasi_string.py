logs = ["ERROR: Disk Full on ServerA", "INFO: User login success", "WARNING: High CPU on ServerB", "ERROR: Database connection lost"]
x = []

for item in logs:
    if item.startswith("ERROR"):
        x = item.split(":")
        print(x[1])
    else:
        continue

