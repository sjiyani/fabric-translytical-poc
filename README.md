# Translytical Task Flows — Implementation Guide

> **Source**: [MS Learn — Understand translytical task flows](https://learn.microsoft.com/en-us/power-bi/create-reports/translytical-task-flow-overview)

Translytical task flows let end users automate actions — updating records, adding annotations, or triggering workflows in other systems — directly from a Power BI report, without leaving the report experience.

---

## What You Can Build

| Action Type | Example |
|---|---|
| **Add data** | Add a new customer record to a table and see it reflected in the report immediately |
| **Edit data** | Update a status field or annotation on an existing record |
| **Delete data** | Remove a customer record that is no longer needed |
| **Call an external API** | Post a Teams Adaptive Card, trigger a REST endpoint, or create a work item in an approvals pipeline |

---

## How It Works

Translytical task flows use [Fabric User Data Functions](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/user-data-functions-overview) to invoke Python functions from a Power BI report button. The function can read from and write to Fabric data sources, then return a string message displayed back to the user.

```
Power BI Report  →  Data Function Button  →  User Data Function (Python)
                                                      ↓
                                            Fabric SQL Database / Warehouse / Lakehouse
                                                      ↓
                                            (Optional) External API — Teams, REST
                                                      ↓
                                            Return string → Report auto-refreshes
```

### Native Fabric data source connections (no credentials needed)

| Data Source | Support |
|---|---|
| Fabric SQL Database | ✅ Recommended for write-back |
| Fabric Warehouse | ✅ |
| Fabric Lakehouse (files) | ✅ |

> For most write-back scenarios, Microsoft recommends **Fabric SQL Database** — it performs well under the heavy read/write operations typical in reporting scenarios.

---

## Architecture Components

| Component | Purpose |
|---|---|
| **Fabric SQL Database** | Stores data with full history. All writes go here. |
| **Lakehouse** *(optional)* | Provides shortcuts to SQL tables for Direct Lake semantic models |
| **Variable Library** *(optional)* | Stores config values (webhook URLs, report URLs) separately from function code — update without republishing |
| **User Data Functions** | Python functions that handle write-back to SQL and post notifications |
| **Power BI Semantic Model** | Defines the data model, relationships, and measures |
| **Power BI Report** | The user interface — data function buttons trigger the workflow |
| **Microsoft Teams** | Receives Adaptive Card notifications with status changes and deep links back to the report |

---

## Tutorial 1 — Data Write-Back (Annotation Scenario)

> **Full tutorial**: [MS Learn — Tutorial: Create a translytical task flow](https://learn.microsoft.com/en-us/power-bi/create-reports/translytical-task-flow-tutorial)

This tutorial creates a translytical task flow where a user adds a product description directly from a Power BI report. The description is written back to a SQL Database in Fabric.

### Step 1 — Create a Fabric SQL Database

1. Go to your Fabric workspace → **New item** → **SQL database**
2. Choose the **Sample data** option to load the **AdventureWorksLT** dataset
3. Note the database name — you will use it when connecting the UDF

### Step 2 — Create a User Data Function

1. In your workspace → **New item** → **User Data Functions** (under *Develop data*)
2. Name it, for example: `sqlwriteback`
3. Select **New function**

#### Connect to the SQL database

1. Select **Manage connections** → **Add data connection**
2. Select your `AdventureWorksLT` database → **Connect**
3. Note the auto-generated **Alias** — you will reference it in your function code

#### Write the function code

The function takes a product description and product model ID, validates input length, then inserts into the `[SalesLT].[ProductDescription]` table. It must return a `str` — Power BI displays this message to the user after execution.

```python
import fabric.functions as fn
import uuid

udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB", alias="<YOUR_CONNECTION_ALIAS>")
@udf.function()
def write_one_to_sql_db(sqlDB: fn.FabricSqlConnection, productDescription: str, productModelId: int) -> str:

    if len(productDescription) > 200:
        raise fn.UserThrownError("Descriptions have a 200 character limit.", {"Description:": productDescription})

    connection = sqlDB.connect()
    cursor = connection.cursor()

    insert_description_query = "INSERT INTO [SalesLT].[ProductDescription] (Description) OUTPUT INSERTED.ProductDescriptionID VALUES (?)"
    cursor.execute(insert_description_query, productDescription)
    results = cursor.fetchall()

    cultureId = str(uuid.uuid4())

    insert_model_description_query = "INSERT INTO [SalesLT].[ProductModelProductDescription] (ProductModelID, ProductDescriptionID, Culture) VALUES (?, ?, ?);"
    cursor.execute(insert_model_description_query, (productModelId, results[0][0], cultureId[:6]))

    connection.commit()
    cursor.close()
    connection.close()

    return "Product description was added"
```

#### Publish and test

1. Select **Publish**
2. Hover over the function in **Functions explorer** → select **Run**
3. Provide sample values: a string for `productDescription`, an integer (1–127) for `productModelId`
4. Select **Run** and review output

### Step 3 — Build the Power BI Report

1. In Power BI Desktop → **Get data** → **OneLake Catalog** → **SQL database** → select your database
2. Load these tables: `SalesLT.ProductDescription`, `SalesLT.ProductModel`, `SalesLT.ProductModelProductDescription`
3. Choose **DirectQuery** mode (required for live data refresh after write-back)

#### Add report visuals

1. Add a **Table** visual with `ProductModel.Name` and `ProductModelProductDescription.ProductModelID`
2. Add an **Input slicer** visual — title it `Write a new product description`
3. Insert a **Blank button** below the slicer

#### Configure the button Action

1. Select the button → **Format button** → expand **Action** → turn **On**
2. Set **Type** = `Data function`
3. Select your **Workspace** and **Function Set** (`sqlwriteback`)
4. Select your **Data function** (`write_one_to_sql_db`)
5. Map the parameters:

   | Parameter | Value |
   |---|---|
   | `productDescription` | Select the `Write a new product description` input slicer |
   | `productModelId` | Use **Conditional formatting (fx)** → Field value → `SalesLT.ProductModel > ProductModelID` → Summarization: **Maximum** |

6. Label the button `Enter` in **Format button** → **Style** → **Text**
7. Set **Loading state** text to `Submitting` with a **Spinner** icon

### Step 4 — Publish and Run

1. **Publish** the report to your Fabric workspace
2. If prompted with *"data source missing credentials"*: open the semantic model → **Settings** → **Data source credentials** → **Edit credentials** → choose **OAuth2** → **Sign in**
3. Open the report in the Power BI web portal, select a product, write a description, and select **Enter**

---

## Tutorial 2 — Status Update Workflow with Teams Notifications

> **Full tutorial**: [MS Learn — Tutorial: Create a status update workflow](https://learn.microsoft.com/en-us/power-bi/create-reports/translytical-task-flow-tutorial-status-update)

This tutorial extends the pattern with a project tracking solution. Users update project status from a Power BI report. Each update writes to SQL history and sends a Teams Adaptive Card notification.

### User Flow

```
User selects project in report
    → Selects new status + adds notes
    → Selects "Update Status" button
        → UDF writes new row to [Status updates] table
        → UDF posts Adaptive Card to Teams
        → Report auto-refreshes to show updated status
```

The *Request Update* button sends a Teams notification to the project owner without writing to the database — useful for nudging someone to provide an update.

### Step 1 — Create the SQL Database Tables

Run the following SQL in your Fabric SQL Database to create the project tracking schema:

```sql
-- Project table
CREATE TABLE [Project] (
    [Project id] INT NOT NULL,
    [Project name] NVARCHAR(200) NOT NULL,
    [Priority] NVARCHAR(20),
    [Project manager] NVARCHAR(100),
    [Department] NVARCHAR(100),
    [Is active] BIT,
    CONSTRAINT PK_Project PRIMARY KEY NONCLUSTERED ([Project id])
);

-- Status history table (append-only — never update rows)
CREATE TABLE [Status updates] (
    [Update id] INT NOT NULL,
    [Project id] INT NOT NULL,
    [Status] NVARCHAR(50) NOT NULL,
    [Updated date] DATETIME2 NOT NULL,
    [Updated by] NVARCHAR(100) NOT NULL,
    [Notes] NVARCHAR(4000),
    CONSTRAINT PK_StatusUpdates PRIMARY KEY NONCLUSTERED ([Update id])
);

-- View: latest status per project
CREATE VIEW [Project status] AS
SELECT
    p.[Project id],
    p.[Project name],
    COALESCE(ls.[Latest status], 'Not Started') AS [Latest status],
    ls.[Latest notes]
FROM [Project] p
LEFT JOIN (
    SELECT
        [Project id],
        [Status] AS [Latest status],
        [Notes] AS [Latest notes],
        ROW_NUMBER() OVER (PARTITION BY [Project id] ORDER BY [Update id] DESC) AS RowNum
    FROM [Status updates]
) ls ON p.[Project id] = ls.[Project id] AND ls.RowNum = 1;
```

### Step 2 — Set Up a Variable Library

A Variable Library stores config values (like webhook URLs) **outside your function code**. When the URL changes, you update the library — no code republish needed.

1. Workspace → **New item** → **Variable library**
2. Name it `ProjectVariables`
3. Add the following variables:

   | Variable Name | Type | Purpose |
   |---|---|---|
   | `TEAMS_WEBHOOK_URL` | String | Your Teams incoming webhook URL |
   | `POWERBI_REPORT_URL` | String | URL to your published Power BI report |

4. To get a Teams webhook URL, follow: [Create an Incoming Webhook](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook)

> ⚠️ Anyone with the webhook URL can post to your Teams channel. Store it only in Variable Library — never hard-code it.

### Step 3 — Write the User Data Functions

Create a User Data Functions item with **two connections**:

```python
@udf.connection(argName="sqlDb", alias="ProjectTrackingDb")    # → Fabric SQL Database
@udf.connection(argName="varLib", alias="ProjectVariables")    # → Variable Library
```

The two key functions are:

#### `update_project_status` — writes to DB + posts Teams card

Validates the new status value against the allowed list, inserts a new row into `[Status updates]`, reads the previous status for comparison, and posts an Adaptive Card showing the transition (`Previous → New`).

**Parameters**: `projectId`, `newStatus`, `updatedBy`, `notes`, `updatedDate`

**Allowed status values**: `Not Started` · `In Progress` · `On Hold` · `Completed` · `Cancelled`

#### `request_status_update` — Teams notification only, no DB write

Queries project details (name, manager, current status), then posts a Teams card with an **Update Status** deep-link button pointing back to the report.

**Parameters**: `projectId`, `requestedBy`, `message`

### Step 4 — Configure the Power BI Report Buttons

Connect to the SQL database in Power BI Desktop using **DirectQuery**. Add visuals for project list, status slicer, notes input, and date picker. Then configure two data function buttons:

| Button | Function | Key Parameters |
|---|---|---|
| **Update Status** | `update_project_status` | `projectId` → selected project ID · `newStatus` → status slicer · `notes` → notes input · `updatedBy` → fixed text (user name) |
| **Request Update via Teams** | `request_status_update` | `projectId` → selected project ID · `requestedBy` → fixed text · `message` → message input |

For both buttons: enable **"Refresh the report after a successful outcome"** in **Format button → Action**.

### Step 5 — (Optional) Set Up Lakehouse for Direct Lake

Direct Lake mode cannot read SQL views — only tables. To use the `[Project status]` view with Direct Lake:

1. Create a **Lakehouse** with schemas enabled
2. Add **shortcuts** pointing to `[Project]` and `[Status updates]` SQL tables
3. Create a **Materialized Lake View** in the Lakehouse that computes the latest status per project using Spark SQL

---

## Supported Capabilities and Limitations

### What translytical task flows support

- Any write-back pattern: insert, update, upsert, soft-delete
- External API calls from within the UDF (Teams, REST APIs, Azure OpenAI, etc.)
- Full input validation with `fn.UserThrownError` — error details shown to the user in the report
- Auto-refresh of the report after a successful function call
- Loading button state with spinner icon during function execution

### Known limitations

- User data functions must return a `str` type to be used in a report button
- Power BI Embedded is supported only for **secure embed** scenarios
- Direct Lake cannot read SQL views directly — use Lakehouse shortcuts + materialized lake views
- Service limits apply: [User data functions service limits](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/user-data-functions-service-limits)

---

## Granting User Permissions

For users other than the report author to trigger data function buttons:

1. On the User Data Functions page → **Share**
2. Add users or groups
3. Select **Execute Functions and View Functions Logs** from **Additional permissions**
4. Select **Send**

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| *"The data source is missing credentials"* | Semantic model needs OAuth2 sign-in | SM → Settings → Data source credentials → Edit → OAuth2 → Sign in |
| *"Something went wrong"* on button click | Function error, timeout, or unauthorized user | Select **View details** in the error popup for the specific reason |
| Button stays disabled | No row selected in the table | Select a row in the report table to pass context to the button |
| Function not visible in button config | Function does not return `str` | Ensure `-> str` is declared on the function signature |
| Teams notification not sent | Webhook URL is empty or invalid | Set `TEAMS_WEBHOOK_URL` in Variable Library (or `_TEAMS_WEBHOOK_URL` in function code) |
| Direct Lake fails on SQL views | Direct Lake cannot read views | Create Lakehouse shortcuts + materialized lake views |

---

## MS Learn References

| Resource | Link |
|---|---|
| Translytical Task Flow Overview | [learn.microsoft.com](https://learn.microsoft.com/en-us/power-bi/create-reports/translytical-task-flow-overview) |
| Tutorial 1 — Data Write-Back | [learn.microsoft.com](https://learn.microsoft.com/en-us/power-bi/create-reports/translytical-task-flow-tutorial) |
| Tutorial 2 — Status Update + Teams | [learn.microsoft.com](https://learn.microsoft.com/en-us/power-bi/create-reports/translytical-task-flow-tutorial-status-update) |
| Create a data function button in Power BI | [learn.microsoft.com](https://learn.microsoft.com/en-us/power-bi/create-reports/translytical-task-flow-button) |
| User Data Functions Overview | [learn.microsoft.com](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/user-data-functions-overview) |
| User Data Functions Service Limits | [learn.microsoft.com](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/user-data-functions-service-limits) |
| Create a Fabric SQL Database | [learn.microsoft.com](https://learn.microsoft.com/en-us/fabric/database/sql/create) |
| Fabric Variable Library | [learn.microsoft.com](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/user-data-functions-overview) |
| Create an Incoming Webhook (Teams) | [learn.microsoft.com](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook) |
| Official UDF Code Samples (Gist) | [github.com/Sujata994](https://gist.github.com/Sujata994/c354ec8d0821e875e45c86f2bd1d5cc8) |
