"""
create_dashboard_v3.py
===============================================================================
Builds the v3 Translytical POC dashboard using Fabric SQL Database.

Key difference from v2:
  - Semantic Model connects to Fabric SQL DB (not Azure SQL)
  - No OAuth2 credential step needed — Fabric handles auth natively
  - Reads connection info from fabric_sql_config.json (written by setup_fabric_sql.py)

Run AFTER setup_fabric_sql.py:
  .venv\Scripts\python.exe create_dashboard_v3.py
===============================================================================
"""
import requests, json, base64, time, uuid
from azure.identity import DefaultAzureCredential

WORKSPACE_ID = "4d0c0836-838c-48c0-8b10-5cda66f145c0"
UDF_ID       = "faacd100-1a8a-41e7-ad85-b0bfc8db51b9"
SM_NAME      = "poc_v3_dataset"
REPORT_NAME  = "Translytical_POC_Dashboard_V3"

# Load Fabric SQL DB config written by setup_fabric_sql.py
try:
    with open("fabric_sql_config.json") as f:
        cfg = json.load(f)
    SQL_SERVER = cfg["server_fqdn"].split(",")[0]   # strip port suffix if present
    SQL_DB     = cfg["db_name"]
    SQL_DB_ID  = cfg["db_id"]
    print(f"Using Fabric SQL DB: {SQL_SERVER} / {SQL_DB}")
except FileNotFoundError:
    print("ERROR: fabric_sql_config.json not found.")
    print("Run setup_fabric_sql.py first.")
    raise SystemExit(1)

# ── auth & helpers ─────────────────────────────────────────────────────────
cred = DefaultAzureCredential()

def ftok():
    return cred.get_token("https://api.fabric.microsoft.com/.default").token

def fh():
    return {"Authorization": f"Bearer {ftok()}", "Content-Type": "application/json"}

def b64e(obj):
    s = json.dumps(obj, indent=2) if isinstance(obj, (dict, list)) else str(obj)
    return base64.b64encode(s.encode("utf-8")).decode("utf-8")

def js(obj):
    return json.dumps(obj, separators=(",", ":"))

def uid():
    return str(uuid.uuid4())

def poll(url, label="", interval=5, limit=40):
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
            raise RuntimeError(f"{label} failed:\n{json.dumps(d, indent=2)[:600]}")
    raise TimeoutError(f"{label} timed out")


# ============================================================================
# STEP 1 — SEMANTIC MODEL (Fabric SQL DB — no credential setup needed)
# ============================================================================
print("\n=== STEP 1: Semantic Model (v3 — Fabric SQL DB) ===")

def _tbl(name, line, columns, partitions, measures=None, annotations=None):
    t = {"name": name, "lineageTag": line, "columns": columns, "partitions": partitions}
    if measures:    t["measures"]    = measures
    if annotations: t["annotations"] = annotations
    return t

def _col(name, dtype, src, line, fmt=None, summarize="none"):
    c = {"name": name, "dataType": dtype, "lineageTag": line,
         "sourceColumn": src, "summarizeBy": summarize}
    if fmt: c["formatString"] = fmt
    return c

def _dq(expr):
    return [{"name": "DirectQuery", "mode": "directQuery",
             "source": {"type": "m", "expression": expr}}]

def _mq(schema, item):
    """M expression for Fabric SQL Database DirectQuery table."""
    return (
        'let\n'
        f'    Source = Sql.Database("{SQL_SERVER}", "{SQL_DB}"),\n'
        f'    nav    = Source{{[Schema="{schema}", Item="{item}"]}}[Data]\n'
        'in\n'
        '    nav'
    )

# ── Table 1: Products ────────────────────────────────────────────────────
T_PROD_LINE = uid()
products_table = _tbl(
    "Products", T_PROD_LINE,
    columns=[
        _col("ProductID",   "int64",    "ProductID",   uid()),
        _col("ProductName", "string",   "ProductName", uid()),
        _col("Category",    "string",   "Category",    uid()),
        _col("UnitPrice",   "double",   "UnitPrice",   uid(), fmt="\\$#,##0.00", summarize="sum"),
        _col("Status",      "string",   "Status",      uid()),
        _col("LastUpdated", "dateTime", "LastUpdated", uid(), fmt="m/d/yyyy h:mm AM/PM"),
    ],
    partitions=_dq(_mq("dbo", "Products")),
    annotations=[{"name": "PBI_QueryOrder", "value": '["Products"]'},
                 {"name": "__PBI_TimeIntelligenceEnabled", "value": "0"}]
)

# ── Table 2: StatusHistory ───────────────────────────────────────────────
T_HIST_LINE = uid()
history_table = _tbl(
    "StatusHistory", T_HIST_LINE,
    columns=[
        _col("HistoryID",      "int64",    "HistoryID",      uid()),
        _col("ProductID",      "int64",    "ProductID",      uid()),
        _col("PreviousStatus", "string",   "PreviousStatus", uid()),
        _col("NewStatus",      "string",   "NewStatus",      uid()),
        _col("UpdatedBy",      "string",   "UpdatedBy",      uid()),
        _col("Notes",          "string",   "Notes",          uid()),
        _col("UpdatedAt",      "dateTime", "UpdatedAt",      uid(), fmt="m/d/yyyy h:mm AM/PM"),
    ],
    partitions=_dq(_mq("dbo", "StatusHistory")),
)

# ── Table 3: ProductAnnotations ──────────────────────────────────────────
T_ANN_LINE = uid()
annotations_table = _tbl(
    "Annotations", T_ANN_LINE,
    columns=[
        _col("AnnotationID", "int64",    "AnnotationID", uid()),
        _col("ProductID",    "int64",    "ProductID",    uid()),
        _col("Annotation",   "string",   "Annotation",   uid()),
        _col("CreatedAt",    "dateTime", "CreatedAt",    uid(), fmt="m/d/yyyy h:mm AM/PM"),
    ],
    partitions=_dq(_mq("dbo", "ProductAnnotations")),
)

# ── Table 4: StatusOptions (calculated import) ───────────────────────────
T_OPT_LINE = uid()
status_options_table = _tbl(
    "StatusOptions", T_OPT_LINE,
    columns=[
        _col("Status",      "string", "Status",      uid()),
        _col("SortOrder",   "int64",  "SortOrder",   uid(), summarize="sum"),
        _col("StatusColor", "string", "StatusColor", uid()),
    ],
    partitions=[{
        "name": "StatusOptions", "mode": "import",
        "source": {"type": "calculated", "expression": (
            'DATATABLE(\n'
            '  "Status", STRING,\n'
            '  "SortOrder", INTEGER,\n'
            '  "StatusColor", STRING,\n'
            '  {\n'
            '    { "Active",       1, "#107C10" },\n'
            '    { "Low Stock",    2, "#FFB900" },\n'
            '    { "Discontinued", 3, "#D13438" }\n'
            '  }\n'
            ')'
        )}
    }],
)

# ── Table 5: UserInputs (single blank row — gives Input slicers a column) ─
T_INPUT_LINE = uid()
user_inputs_table = _tbl(
    "UserInputs", T_INPUT_LINE,
    columns=[
        _col("NotesText",      "string", "NotesText",      uid()),
        _col("MessageText",    "string", "MessageText",    uid()),
        _col("AnnotationText", "string", "AnnotationText", uid()),
    ],
    partitions=[{
        "name": "UserInputs", "mode": "import",
        "source": {"type": "calculated", "expression": (
            'DATATABLE(\n'
            '  "NotesText",      STRING,\n'
            '  "MessageText",    STRING,\n'
            '  "AnnotationText", STRING,\n'
            '  {\n'
            '    { "", "", "" }\n'
            '  }\n'
            ')'
        )}
    }],
)

# ── Table 6: Measures ────────────────────────────────────────────────────
T_MEAS_LINE = uid()
measures_table = _tbl(
    "Translytical Measures", T_MEAS_LINE,
    columns=[{"name": "_Value", "dataType": "int64", "lineageTag": uid(),
              "sourceColumn": "[_Value]", "summarizeBy": "sum", "isHidden": True}],
    partitions=[{"name": "Translytical Measures", "mode": "import",
                 "source": {"type": "calculated", "expression": "{1}"}}],
    measures=[
        {"name": "Latest Status", "lineageTag": uid(), "dataType": "string",
         "expression": (
            'IF(\n'
            '  HASONEVALUE( Products[ProductID] ),\n'
            '  CALCULATE(\n'
            '    LASTNONBLANKVALUE( StatusHistory[UpdatedAt], MAX( StatusHistory[NewStatus] ) ),\n'
            '    ALLEXCEPT( StatusHistory, StatusHistory[ProductID] )\n'
            '  ),\n'
            '  BLANK()\n'
            ')'
         )},
        {"name": "Status Change Count", "lineageTag": uid(), "dataType": "int64",
         "formatString": "#,##0",
         "expression": "COUNTROWS( StatusHistory )"},
        {"name": "Updated By", "lineageTag": uid(), "dataType": "string",
         "expression": "USERPRINCIPALNAME()"},
        {"name": "Selected Product ID", "lineageTag": uid(), "dataType": "int64",
         "formatString": "0",
         "expression": "SELECTEDVALUE( Products[ProductID] )"},
        {"name": "Annotation Count", "lineageTag": uid(), "dataType": "int64",
         "formatString": "#,##0",
         "expression": "COUNTROWS( Annotations )"},
    ]
)

RELATIONSHIPS = [
    {"name": uid(), "fromTable": "StatusHistory", "fromColumn": "ProductID",
     "toTable": "Products", "toColumn": "ProductID", "crossFilteringBehavior": "oneDirection"},
    {"name": uid(), "fromTable": "Annotations", "fromColumn": "ProductID",
     "toTable": "Products", "toColumn": "ProductID", "crossFilteringBehavior": "oneDirection"},
]

BIM = {
    "name": SM_NAME,
    "compatibilityLevel": 1567,
    "model": {
        "defaultPowerBIDataSourceVersion": "powerBI_V3",
        "relationships": RELATIONSHIPS,
        "annotations": [
            {"name": "__PBI_TimeIntelligenceEnabled", "value": "0"},
            {"name": "PBI_QueryOrder", "value": '["Products","StatusHistory","Annotations","StatusOptions","UserInputs"]'}
        ],
        "tables": [
            products_table, history_table, annotations_table,
            status_options_table, user_inputs_table, measures_table,
        ]
    }
}

SM_PLATFORM = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {"type": "SemanticModel", "displayName": SM_NAME},
    "config": {"version": "2.0", "logicalId": uid()}
}

# Delete old v3 SM
sms = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/semanticModels",
    headers=fh()
)
for sm in sms.json().get("value", []):
    if sm.get("displayName") == SM_NAME:
        requests.delete(
            f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/semanticModels/{sm['id']}",
            headers=fh()
        )
        print(f"  Deleted existing SM: {sm['id']}")
        time.sleep(2)

r = requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/semanticModels",
    headers=fh(),
    json={"displayName": SM_NAME, "definition": {"parts": [
        {"path": "model.bim",        "payload": b64e(BIM),                                      "payloadType": "InlineBase64"},
        {"path": "definition.pbism", "payload": b64e({"version": "4.0", "settings": {}}),       "payloadType": "InlineBase64"},
        {"path": ".platform",        "payload": b64e(SM_PLATFORM),                               "payloadType": "InlineBase64"},
    ]}}
)
print(f"Create SM: {r.status_code}")
if r.status_code == 202:
    poll(r.headers.get("Location"), "SM")
elif r.status_code not in (200, 201):
    print("Error:", r.text[:600])
    raise SystemExit(1)

time.sleep(3)
SM_OBJ = next((x for x in requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/semanticModels",
    headers=fh()
).json().get("value", []) if x.get("displayName") == SM_NAME), None)
SM_ID = SM_OBJ["id"] if SM_OBJ else None
print(f"  SM ID: {SM_ID}")


# ============================================================================
# STEP 2 — REPORT LAYOUT
# ============================================================================
print("\n=== STEP 2: Report Layout ===")

PAGE_W, PAGE_H = 1280, 820
LEFT_W  = 740
RIGHT_X = 758
RIGHT_W = PAGE_W - RIGHT_X - 16

def pos(x, y, w, h, z=0, tab=0):
    return {"x": x, "y": y, "z": z, "width": w, "height": h, "tabOrder": tab}

def vc(vid, x, y, w, h, cfg_obj, query_obj=None, dtrans_obj=None, z=0):
    c = {"id": vid, "x": x, "y": y, "z": z, "width": w, "height": h,
         "config": js({"name": f"vis{vid}",
                       "layouts": [{"id": 0, "position": pos(x, y, w, h, z, vid * 100)}],
                       "singleVisual": cfg_obj}),
         "filters": "[]"}
    if query_obj:   c["query"]          = js(query_obj)
    if dtrans_obj:  c["dataTransforms"] = js(dtrans_obj)
    return c

def label(vid, x, y, w, text, color="#333333", size="9pt", bold=True):
    return vc(vid, x, y, w, 28,
        {"visualType": "textbox",
         "vcObjects": {"general": [{"properties": {"paragraphs": [
             {"textRuns": [{"value": text,
                            "textRunStyle": {"bold": bold, "fontSize": size,
                                             "fontFace": "Segoe UI", "color": color}}],
              "horizontalTextAlignment": "Left"}
         ]}}]}})

def section_divider(vid, x, y, w):
    return vc(vid, x, y, w, 2,
        {"visualType": "shape",
         "vcObjects": {
             "line":    [{"properties": {"strokeWidth": {"expr": {"Literal": {"Value": "1D"}}},
                                          "strokeColor": {"solid": {"color": "#E0E0E0"}}}}],
             "fill":    [{"properties": {"fillColor":   {"solid": {"color": "#E0E0E0"}},
                                          "transparency": {"expr": {"Literal": {"Value": "0D"}}}}}],
             "general": [{"properties": {"shapeType": {"expr": {"Literal": {"Value": "'rectangle'"}}}}}]
         }})

FROM_PROD  = [{"Name": "p", "Entity": "Products",     "Type": 0}]
FROM_OPT   = [{"Name": "o", "Entity": "StatusOptions", "Type": 0}]
FROM_INPUT = [{"Name": "u", "Entity": "UserInputs",   "Type": 0}]

PROD_COLS = ["ProductID", "ProductName", "Category", "UnitPrice", "Status", "LastUpdated"]

def dq_slicer(vid, x, y, w, h, entity, col, from_clause, mode="Tile"):
    src  = from_clause[0]["Name"]
    sel  = [{"Column": {"Expression": {"SourceRef": {"Source": src}}, "Property": col},
              "Name": f"{entity}.{col}"}]
    q    = {"Version": 2, "From": from_clause, "Select": sel}
    dt   = {"selects": [{"queryName": f"{entity}.{col}", "roles": {"Values": 1}}]}
    return vc(vid, x, y, w, h,
        {"visualType": "slicer",
         "projections": {"Values": [{"queryRef": f"{entity}.{col}"}]},
         "prototypeQuery": q, "drillFilterOtherVisuals": True,
         "vcObjects": {"data": [{"properties": {"mode": {"expr": {"Literal": {"Value": f"'{mode}'"}}}}}]}},
        query_obj=q, dtrans_obj=dt)

def input_slicer(vid, x, y, w, h, col_name, placeholder):
    """Input slicer bound to UserInputs column — renders as a free-text box."""
    sel = [{"Column": {"Expression": {"SourceRef": {"Source": "u"}}, "Property": col_name},
             "Name": f"UserInputs.{col_name}"}]
    q   = {"Version": 2, "From": FROM_INPUT, "Select": sel}
    dt  = {"selects": [{"queryName": f"UserInputs.{col_name}", "roles": {"Values": 1}}]}
    return vc(vid, x, y, w, h,
        {"visualType": "slicer",
         "projections": {"Values": [{"queryRef": f"UserInputs.{col_name}"}]},
         "prototypeQuery": q, "drillFilterOtherVisuals": False,
         "vcObjects": {
             "data": [{"properties": {"mode": {"expr": {"Literal": {"Value": "'Input'"}}}}}],
             "general": [{"properties": {"title": {"expr": {"Literal": {"Value": f"'{placeholder}'"}}}}}]
         }},
        query_obj=q, dtrans_obj=dt)

def fn_button(vid, x, y, w, h, btn_text, fn_name, bg="#0078D4"):
    """Data-function button — wired to UDF. Map parameters in Format pane after deploy."""
    return vc(vid, x, y, w, h,
        {"visualType": "actionButton",
         "vcObjects": {
             "action": [{"properties": {
                 "actionType":              {"expr": {"Literal": {"Value": "'DataFunction'"}}},
                 "dataFunctionItemId":      {"expr": {"Literal": {"Value": f"'{UDF_ID}'"}}},
                 "dataFunctionWorkspaceId": {"expr": {"Literal": {"Value": f"'{WORKSPACE_ID}'"}}},
                 "dataFunctionName":        {"expr": {"Literal": {"Value": f"'{fn_name}'"}}}
             }}],
             "text":   [{"properties": {
                 "text":      {"expr": {"Literal": {"Value": f"'{btn_text}'"}}},
                 "fontColor": {"solid": {"color": "#FFFFFF"}},
                 "bold":      {"expr": {"Literal": {"Value": "true"}}}
             }}],
             "fill":   [{"properties": {
                 "fillColor":    {"solid": {"color": bg}},
                 "transparency": {"expr": {"Literal": {"Value": "0D"}}}
             }}],
             "border": [{"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}],
             "general":[{"properties": {"fontSize": {"expr": {"Literal": {"Value": "10D"}}}}}]
         }})

# ── Build visuals ──────────────────────────────────────────────────────────
vid = 0

def nv():
    global vid; vid += 1; return vid

HEADER_H = 58
BTN_H    = 40
SLICER_H = 68

# Header
header_bg = vc(nv(), 0, 0, PAGE_W, HEADER_H,
    {"visualType": "shape", "vcObjects": {
        "line":    [{"properties": {"strokeWidth": {"expr": {"Literal": {"Value": "0D"}}}}}],
        "fill":    [{"properties": {"fillColor":   {"solid": {"color": "#008272"}},
                                     "transparency":{"expr": {"Literal": {"Value": "0D"}}}}}],
        "general": [{"properties": {"shapeType":   {"expr": {"Literal": {"Value": "'rectangle'"}}}}}]
    }}, z=-1)

title_vis = vc(nv(), 16, 10, 900, 38,
    {"visualType": "textbox",
     "vcObjects": {"general": [{"properties": {"paragraphs": [
         {"textRuns": [{"value": "Translytical Task Flow POC  |  Tutorial 1 (Write-back) + Tutorial 2 (Status Update + Teams)",
                        "textRunStyle": {"bold": True, "fontSize": "12pt",
                                         "fontFace": "Segoe UI", "color": "#FFFFFF"}}],
          "horizontalTextAlignment": "Left"}
     ]}}]}})

# Products table (left panel)
SEL_PROD = [{"Column": {"Expression": {"SourceRef": {"Source": "p"}}, "Property": c},
              "Name": f"Products.{c}"} for c in PROD_COLS]
tbl_q  = {"Version": 2, "From": FROM_PROD, "Select": SEL_PROD}
tbl_dt = {"selects": [{"queryName": f"Products.{c}", "roles": {"Values": 1}} for c in PROD_COLS]}
products_table_vis = vc(nv(), 16, HEADER_H + 12, LEFT_W - 32, PAGE_H - HEADER_H - 20,
    {"visualType": "tableEx",
     "projections": {"Values": [{"queryRef": f"Products.{c}"} for c in PROD_COLS]},
     "prototypeQuery": tbl_q, "drillFilterOtherVisuals": True, "vcObjects": {}},
    query_obj=tbl_q, dtrans_obj=tbl_dt)

# ── Right panel ────────────────────────────────────────────────────────────
Y0 = HEADER_H + 10

# Section A — Tutorial 2: Update Status
v_a_hdr  = label(nv(), RIGHT_X, Y0, RIGHT_W, "① Update Status  (Tutorial 2)", "#008272", "9.5pt")
Y0 += 28
v_a_slbl = label(nv(), RIGHT_X, Y0, RIGHT_W, "1) Select new status:")
Y0 += 22
v_a_sli  = dq_slicer(nv(), RIGHT_X, Y0, RIGHT_W, SLICER_H, "StatusOptions", "Status", FROM_OPT, "Tile")
Y0 += SLICER_H + 4
v_a_nlbl = label(nv(), RIGHT_X, Y0, RIGHT_W, "2) Add notes (optional):")
Y0 += 22
v_a_nsli = input_slicer(nv(), RIGHT_X, Y0, RIGHT_W, 48, "NotesText", "Type notes here...")
Y0 += 52
v_a_btn  = fn_button(nv(), RIGHT_X, Y0, RIGHT_W, BTN_H, "Update Product Status", "update_product_status", "#0078D4")
Y0 += BTN_H + 8

v_div1 = section_divider(nv(), RIGHT_X, Y0, RIGHT_W); Y0 += 12

# Section B — Tutorial 2: Request Status Update (Teams)
v_b_hdr  = label(nv(), RIGHT_X, Y0, RIGHT_W, "② Request Status Update via Teams  (Tutorial 2)", "#008272", "9.5pt")
Y0 += 28
v_b_mlbl = label(nv(), RIGHT_X, Y0, RIGHT_W, "Message to Teams channel (optional):")
Y0 += 22
v_b_msli = input_slicer(nv(), RIGHT_X, Y0, RIGHT_W, 48, "MessageText", "Type message here...")
Y0 += 52
v_b_btn  = fn_button(nv(), RIGHT_X, Y0, RIGHT_W, BTN_H, "Request Status Update (Teams)", "request_status_update", "#5C2D91")
Y0 += BTN_H + 8

v_div2 = section_divider(nv(), RIGHT_X, Y0, RIGHT_W); Y0 += 12

# Section C — Tutorial 1: Price Update + Annotation
v_c_hdr  = label(nv(), RIGHT_X, Y0, RIGHT_W, "③ Price Update & Annotation  (Tutorial 1)", "#008272", "9.5pt")
Y0 += 28
v_c_plbl = label(nv(), RIGHT_X, Y0, RIGHT_W, "Price filter (data source for new price):")
Y0 += 22
v_c_psli = dq_slicer(nv(), RIGHT_X, Y0, RIGHT_W, 52, "Products", "UnitPrice", FROM_PROD, "Between")
Y0 += 56
v_c_albl = label(nv(), RIGHT_X, Y0, RIGHT_W, "Annotation text:")
Y0 += 22
v_c_asli = input_slicer(nv(), RIGHT_X, Y0, RIGHT_W, 48, "AnnotationText", "Type annotation here...")
Y0 += 52

HALF = (RIGHT_W - 8) // 2
v_c_pbtn = fn_button(nv(), RIGHT_X,            Y0, HALF, BTN_H, "Update Price",   "update_price",    "#107C10")
v_c_abtn = fn_button(nv(), RIGHT_X + HALF + 8, Y0, HALF, BTN_H, "Add Annotation", "add_annotation",  "#008272")

visuals = [
    header_bg, title_vis,
    products_table_vis,
    v_a_hdr, v_a_slbl, v_a_sli, v_a_nlbl, v_a_nsli, v_a_btn,
    v_div1,
    v_b_hdr, v_b_mlbl, v_b_msli, v_b_btn,
    v_div2,
    v_c_hdr, v_c_plbl, v_c_psli, v_c_albl, v_c_asli, v_c_pbtn, v_c_abtn,
]

SECTION_NAME = "Section" + str(uuid.uuid4()).replace("-", "")[:8]
REPORT_JSON = {
    "id": 0, "version": "5.47",
    "config": js({"version": "5.47",
                   "themeCollection": {"baseTheme": {"name": "CY24SU10", "version": "5.47", "type": 3}}}),
    "sections": [{
        "id": 1, "name": SECTION_NAME, "displayName": "Products Dashboard",
        "config": js({"relationships": [], "dataBindings": [], "objects": {},
                       "selects": None, "filters": None}),
        "filters": "[]",
        "width": PAGE_W, "height": PAGE_H,
        "visualContainers": visuals
    }]
}

REPORT_DEF = {
    "version": "4.0",
    "datasetReference": {
        "byPath":   None,
        "byConnection": {
            "connectionType": "pbiServiceLive",
            "pbiServiceModelId": None,
            "pbiModelVirtualServerName": "sobe_wowvirtualserver",
            "pbiModelDatabaseName": SM_ID,
            "name": "EntityDataSource",
            "connectionString": None
        }
    }
}
REPORT_PLATFORM = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {"type": "Report", "displayName": REPORT_NAME},
    "config": {"version": "2.0", "logicalId": uid()}
}

# Delete old v3 report
reps = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/reports",
    headers=fh()
)
for rp in reps.json().get("value", []):
    if rp.get("displayName") == REPORT_NAME:
        requests.delete(
            f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/reports/{rp['id']}",
            headers=fh()
        )
        print(f"  Deleted existing Report: {rp['id']}")
        time.sleep(2)

r3 = requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/reports",
    headers=fh(),
    json={"displayName": REPORT_NAME, "definition": {"parts": [
        {"path": "report.json",      "payload": b64e(REPORT_JSON),     "payloadType": "InlineBase64"},
        {"path": "definition.pbir",  "payload": b64e(REPORT_DEF),      "payloadType": "InlineBase64"},
        {"path": ".platform",        "payload": b64e(REPORT_PLATFORM), "payloadType": "InlineBase64"},
    ]}}
)
print(f"Create Report: {r3.status_code}")
if r3.status_code == 202:
    poll(r3.headers.get("Location"), "Report")
elif r3.status_code not in (200, 201):
    print("Error:", r3.text[:600])
    raise SystemExit(1)

time.sleep(3)
REPORT_OBJ = next((x for x in requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/reports",
    headers=fh()
).json().get("value", []) if x.get("displayName") == REPORT_NAME), None)
REPORT_ID  = REPORT_OBJ["id"] if REPORT_OBJ else "unknown"
REPORT_URL = f"https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}/reports/{REPORT_ID}"

print(f"""
=== DONE ===
Workspace:   https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}/list
SM ID:       {SM_ID}
Report URL:  {REPORT_URL}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REMAINING STEPS (one-time, ~10 minutes):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

A. PASTE UDF v3 CODE  (uses fn.FabricSqlConnection — no credentials needed)
   1. Workspace → poc_writeback UDF → Edit (pencil icon)
   2. Replace ALL code with contents of poc_udf_v3.py
   3. Manage connections → Add connection → SQL Database
      → select poc_translytical_db → alias = POC_DB → Save
   4. Libraries → Add → requests 2.31.0
   5. Publish

B. CONFIGURE BUTTON PARAMETERS  (in report Edit mode)
   Click Edit → click each button → Format pane → Action:

   "Update Product Status":
     productId  → [Selected Product ID]   (Measure)
     newStatus  → StatusOptions[Status]    (Slicer)
     updatedBy  → [Updated By]             (Measure)
     notes      → UserInputs[NotesText]    (Slicer)

   "Request Status Update (Teams)":
     productId   → [Selected Product ID]  (Measure)
     requestedBy → [Updated By]           (Measure)
     message     → UserInputs[MessageText] (Slicer)

   "Update Price":
     productId  → [Selected Product ID]   (Measure)
     newPrice   → Products[UnitPrice]     (Slicer)

   "Add Annotation":
     productId  → [Selected Product ID]   (Measure)
     annotation → UserInputs[AnnotationText] (Slicer)

   For ALL buttons: Action → enable "Refresh report after successful outcome"

C. TEAMS WEBHOOK (optional — enables Tutorial 2 notifications)
   1. Teams → channel → ... → Connectors → Incoming Webhooks → Add
   2. Paste URL into _TEAMS_WEBHOOK_URL in poc_udf_v3.py → re-publish UDF
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")
