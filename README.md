# work-database
A database for keeping track of computational results

Example:
```python
from work_database import WorkDatabase

# 1. Create and populate the database
with WorkDatabase("demo.db") as db:
    db.begin()
    for i in range(5):
        db.add({"number": i})
    db.commit()

# 2. Remove duplicate input entries (optional)
with WorkDatabase("demo.db") as db:
    db.remove_duplicates()

# 3. Process incomplete rows (compute square of number)
with WorkDatabase("demo.db") as db:
    for record_id, input_obj in db.incomplete():
        result = {"square": input_obj["number"] ** 2}
        db.update(record_id, result)
    db.commit()

# 4. Export completed records to JSONL
with WorkDatabase("demo.db") as db:
    db.write_jsonl("results.jsonl")

print("Done! Check results.jsonl for output.")
```
