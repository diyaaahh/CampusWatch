old_schema= {
    "users":{
        "columns":{
            "id": {"type":"integer", "nullable":False},
            "name":{"type":"varchar", "nullable":False},
            "email":{"type":"varchar", "nullable":False},
            "phone":{"type":"varchar", "nullable":True}
        }
    },
    "incidents":{
        "columns":{
            "id":{"type":"integer", "nullable":False},
            "status":{"type":"varchar", "nullable":False},
            "title":{"type":"varchar", "nullable":False}
        }

    }
}
queries=[
    "ALTER TABLE users ADD COLUMN age integer",
    "ALTER TABLE users RENAME COLUMN name to full_name",
    "ALTER TABLE incidents DROP COLUMN title",
    "ALTER TABLE users ALTER COLUMN phone SET NOT NULL"
]

def check_schema_compatibility(old_schema , queries):
    for query in queries:
        query_lower = query.lower()
        if "rename column" or "drop column" or "set not null" or "alter column" in query_lower:
            return False 
        else:
            return True
        

        
required_dependencies={
    "numpy":"1.18.0",
    "pandas":"1.0.0",
    "requests":"2.22.0"
}

current_dependencies={
    "numpy":"1.19.0",
    "pandas":"1.1.0",
    "requests":"2.21.0"
}
        

def check_dependencies_compatibility(required_dependencies , current_dependencies):
    for package, version in required_dependencies.items():
        current_version = current_dependencies.get(package)
        if current_version is None or current_version < version:
            return False 
        else:
            return True