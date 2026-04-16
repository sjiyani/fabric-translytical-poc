"""
setup_fabric_sql.py
===============================================================================
Creates a Fabric SQL Database in the workspace and seeds the POC schema.

Why Fabric SQL DB instead of Azure SQL?
  - UDF uses fn.FabricSqlConnection  → zero credential config in the UDF
  - Semantic Model uses native Fabric connection → no OAuth2 step in the report
  - Matches the EXACT pattern from the official MS Gist:
    https://gist.github.com/Sujata994/c354ec8d0821e875e45c86f2bd1d5cc8

Run once:
  .venv\Scripts\python.exe setup_fabric_sql.py
===============================================================================
"""
import requests, json, time, struct, pyodbc
from azure.identity import DefaultAzureCredential

WORKSPACE_ID = "4d0c0836-838c-48c0-8b10-5cda66f145c0"
DB_DISPLAY   = "poc_translytical_v3"

cred = DefaultAzureCredential()

def ftok():
    return cred.get_token("https://api.fabric.microsoft.com/.default").token

def fh():
    return {"Authorization": f"Bearer {ftok()}", "Content-Type": "application/json"}

def sqltok():
    """Token for Azure SQL / Fabric SQL DB auth."""
    return cred.get_token("https://database.windows.net/.default").token

def pyodbc_conn(server, database):
    """Connect to Fabric SQL DB using AAD token (no password needed)."""
    token      = sqltok()
    tok_bytes  = token.encode("utf-16-le")
    tok_struct = struct.pack(f"<I{len(tok_bytes)}s", len(tok_bytes), tok_bytes)
    conn_str   = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={server},1433;"
        f"Database={database};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str, attrs_before={1256: tok_struct})

def poll(url, label="", interval=8, limit=40):
    h = {"Authorization": f"Bearer {ftok()}"}
    for _ in range(limit):
        time.sleep(interval)
        r = requests.get(url, headers=h)
        d = r.json()
        s = d.get("status", "")
        print(f"  [{label}] {s}")
        if s == "Succeeded":
            return d
        if s in ("Failed", "Cancelled"):
            raise RuntimeError(f"{label} failed:\n{json.dumps(d, indent=2)[:800]}")
    raise TimeoutError(f"{label} timed out")


# ── STEP 1: Delete old Fabric SQL DB if it exists ───────────────────────────
print("\n=== STEP 1: Check for existing Fabric SQL DB ===")
existing = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/sqlDatabases",
    headers=fh()
)
for db in existing.json().get("value", []):
    if db.get("displayName") == DB_DISPLAY:
        print(f"  Deleting existing: {db['id']}")
        requests.delete(
            f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/sqlDatabases/{db['id']}",
            headers=fh()
        )
        time.sleep(5)

# ── STEP 2: Create Fabric SQL Database ──────────────────────────────────────
print("\n=== STEP 2: Create Fabric SQL Database ===")
r = requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/sqlDatabases",
    headers=fh(),
    json={"displayName": DB_DISPLAY}
)
print(f"  Create DB: {r.status_code}")
if r.status_code == 202:
    result = poll(r.headers.get("Location"), "SQL DB")
    # ID is nested in result.result for Fabric long-running ops
    db_id  = (result.get("result") or {}).get("id") or result.get("id") or result.get("itemId")
elif r.status_code in (200, 201):
    db_id  = r.json().get("id")
else:
    print("  Error:", r.text[:600])
    raise SystemExit(1)

# Fallback: list all SQL DBs and find by name
if not db_id:
    print("  DB ID not in poll response, listing all SQL DBs...")
    time.sleep(5)
    all_dbs = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/sqlDatabases",
        headers=fh()
    ).json()
    print("  All SQL DBs:", json.dumps(all_dbs, indent=2)[:400])
    for db in all_dbs.get("value", []):
        if db.get("displayName") == DB_DISPLAY:
            db_id = db["id"]
            break

if not db_id:
    raise RuntimeError("Could not determine Fabric SQL DB ID. Check the API response above.")

# ── STEP 3: Get connection properties ───────────────────────────────────────
print("\n=== STEP 3: Get connection info ===")
time.sleep(15)   # give Fabric SQL DB time to fully provision
for attempt in range(3):
    r2 = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/sqlDatabases/{db_id}",
        headers=fh()
    )
    db_info = r2.json()
    if "errorCode" not in db_info:
        break
    print(f"  Attempt {attempt+1}: {db_info.get('message')} — retrying in 15s...")
    time.sleep(15)
print(json.dumps(db_info, indent=2))

props       = db_info.get("properties", {})
server_fqdn = props.get("serverFqdn", "")
db_name     = props.get("databaseName", "") or props.get("name", "")

if not server_fqdn:
    server_fqdn = f"{db_id}.datawarehouse.fabric.microsoft.com"
if not db_name:
    db_name = DB_DISPLAY

print(f"\n  DB ID:    {db_id}")
print(f"  Server:   {server_fqdn}")
print(f"  Database: {db_name}")

# Save connection info to file for use by create_dashboard_v3.py
with open("fabric_sql_config.json", "w") as f:
    json.dump({"db_id": db_id, "server_fqdn": server_fqdn, "db_name": db_name}, f, indent=2)
print("  Saved to fabric_sql_config.json")

# ── STEP 4: Create schema + seed data ───────────────────────────────────────
print("\n=== STEP 4: Create tables and seed data ===")
print(f"  Connecting to: {server_fqdn} / {db_name}")
time.sleep(20)   # Fabric SQL DB needs ~30-60s to be queryable after creation

try:
    conn   = pyodbc_conn(server_fqdn, db_name)
    cursor = conn.cursor()

    # ── Products table ──────────────────────────────────────────────────────
    cursor.execute("""
    IF OBJECT_ID('dbo.Products','U') IS NULL
    CREATE TABLE dbo.Products (
        ProductID   INT IDENTITY(1,1) PRIMARY KEY,
        ProductName NVARCHAR(200)  NOT NULL,
        Category    NVARCHAR(100),
        UnitPrice   DECIMAL(10,2) NOT NULL DEFAULT 0,
        Status      NVARCHAR(50)  NOT NULL DEFAULT 'Active',
        LastUpdated DATETIME2     NOT NULL DEFAULT SYSDATETIME()
    )
    """)

    # ── StatusHistory table ─────────────────────────────────────────────────
    cursor.execute("""
    IF OBJECT_ID('dbo.StatusHistory','U') IS NULL
    CREATE TABLE dbo.StatusHistory (
        HistoryID      INT IDENTITY(1,1) PRIMARY KEY,
        ProductID      INT            NOT NULL REFERENCES dbo.Products(ProductID),
        PreviousStatus NVARCHAR(50),
        NewStatus      NVARCHAR(50)   NOT NULL,
        UpdatedBy      NVARCHAR(200),
        Notes          NVARCHAR(500),
        UpdatedAt      DATETIME2      NOT NULL DEFAULT SYSDATETIME()
    )
    """)

    # ── ProductAnnotations table ────────────────────────────────────────────
    cursor.execute("""
    IF OBJECT_ID('dbo.ProductAnnotations','U') IS NULL
    CREATE TABLE dbo.ProductAnnotations (
        AnnotationID INT IDENTITY(1,1) PRIMARY KEY,
        ProductID    INT           NOT NULL REFERENCES dbo.Products(ProductID),
        Annotation   NVARCHAR(MAX) NOT NULL,
        CreatedAt    DATETIME2     NOT NULL DEFAULT SYSDATETIME()
    )
    """)

    # ── PriceHistory table ──────────────────────────────────────────────────
    cursor.execute("""
    IF OBJECT_ID('dbo.PriceHistory','U') IS NULL
    CREATE TABLE dbo.PriceHistory (
        PriceID      INT IDENTITY(1,1) PRIMARY KEY,
        ProductID    INT          NOT NULL REFERENCES dbo.Products(ProductID),
        OldPrice     DECIMAL(10,2),
        NewPrice     DECIMAL(10,2) NOT NULL,
        ChangedAt    DATETIME2     NOT NULL DEFAULT SYSDATETIME()
    )
    """)

    conn.commit()
    print("  Tables created.")

    # ── Seed Products ───────────────────────────────────────────────────────
    products = [
        ("Surface Pro 11",                "Laptop",        1299.99, "Active"),
        ("Surface Laptop 7",              "Laptop",         999.99, "Active"),
        ("Microsoft 365 Business Premium","Software",        22.00, "Active"),
        ("Xbox Series X",                 "Gaming",         499.99, "Low Stock"),
        ("Surface Hub 3",                 "Collaboration", 8999.99, "Active"),
        ("Microsoft Teams Rooms",         "Collaboration", 3499.99, "Active"),
        ("Power BI Premium",              "Analytics",     4995.00, "Active"),
        ("Azure OpenAI Service",          "AI",               0.00, "Active"),
        ("HoloLens 2",                    "Mixed Reality", 3500.00, "Low Stock"),
        ("Surface Headphones 2",          "Accessories",    249.99, "Active"),
    ]

    cursor.execute("SELECT COUNT(*) FROM dbo.Products")
    if cursor.fetchone()[0] == 0:
        cursor.executemany(
            "INSERT INTO dbo.Products (ProductName, Category, UnitPrice, Status) VALUES (?,?,?,?)",
            products
        )
        print(f"  Seeded {len(products)} products.")
    else:
        print("  Products already seeded, skipping.")

    # ── Seed StatusHistory ──────────────────────────────────────────────────
    cursor.execute("SELECT COUNT(*) FROM dbo.StatusHistory")
    if cursor.fetchone()[0] == 0:
        cursor.execute("SELECT ProductID, Status FROM dbo.Products")
        rows = cursor.fetchall()
        history = [(r[0], None, r[1], "system", "Initial status") for r in rows]
        cursor.executemany(
            "INSERT INTO dbo.StatusHistory (ProductID, PreviousStatus, NewStatus, UpdatedBy, Notes) VALUES (?,?,?,?,?)",
            history
        )
        # Sample history entries
        cursor.executemany(
            "INSERT INTO dbo.StatusHistory (ProductID, PreviousStatus, NewStatus, UpdatedBy, Notes) VALUES (?,?,?,?,?)",
            [
                (4, "Active",     "Low Stock",    "admin@atqor.com", "Inventory running low"),
                (9, "Active",     "Low Stock",    "admin@atqor.com", "Supply chain delay"),
                (4, "Low Stock",  "Active",       "ops@atqor.com",   "Restocked"),
            ]
        )
        print(f"  Seeded {len(rows) + 3} status history rows.")
    else:
        print("  StatusHistory already seeded, skipping.")

    conn.commit()
    conn.close()
    print("  Done seeding.")

except Exception as e:
    print(f"\n  ERROR connecting to Fabric SQL DB: {e}")
    print("  The DB may still be provisioning. Wait 2 minutes and re-run.")
    print("  Connection info saved to fabric_sql_config.json for manual use.")
    raise SystemExit(1)

print(f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FABRIC SQL DB READY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DB ID:      {db_id}
Server:     {server_fqdn}
Database:   {db_name}
Config:     fabric_sql_config.json

NEXT STEPS:
1. Register connection in UDF:
   Workspace → poc_writeback UDF → Manage connections
   → Add connection → SQL Database → select '{DB_DISPLAY}' → alias = 'POC_DB'
   → Publish

2. Run create_dashboard_v3.py to deploy the report
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")
