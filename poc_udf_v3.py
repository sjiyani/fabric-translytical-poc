"""
poc_udf_v3.py
===============================================================================
User Data Function using the OFFICIAL fn.FabricSqlConnection pattern.

Based on: https://gist.github.com/Sujata994/c354ec8d0821e875e45c86f2bd1d5cc8
Tutorials: Tutorial 1 (write-back) + Tutorial 2 (status update + Teams)

HOW TO USE:
  1. Open Fabric workspace → poc_writeback UDF → Edit
  2. Replace ALL code with this file's contents
  3. Manage connections → Add → SQL Database → select poc_translytical_db
     → set alias = "POC_DB" → Save
  4. Libraries → Add → requests 2.31.0 → Save
  5. Publish

WHY fn.FabricSqlConnection instead of pyodbc?
  - Zero credential configuration — Fabric handles auth automatically
  - Matches official MS Gist pattern exactly
  - Works out-of-the-box with Fabric SQL Database
===============================================================================
"""
import fabric.functions as fn
import logging

# Teams webhook URL (paste after creating incoming webhook in Teams channel)
# Teams → channel → Manage channel → Connectors → Incoming Webhooks → Add
_TEAMS_WEBHOOK_URL = ""   # e.g. "https://outlook.office.com/webhook/..."

udf = fn.UserDataFunctions()


# ── TUTORIAL 2 ─────────────────────────────────────────────────────────────
# Matches: Update Discount + Request Discount patterns from the official Gist
# ───────────────────────────────────────────────────────────────────────────

@udf.connection(argName="sqlDB", alias="pocdb")
@udf.function()
def update_product_status(
    sqlDB: fn.FabricSqlConnection,
    productId: int,
    newStatus: str,
    updatedBy: str,
    notes: str
) -> str:
    """
    Tutorial 2: Write status update to DB + post Teams Adaptive Card.
    Adapted from UpdateDiscount in the official Gist.
    """
    logging.info("update_product_status called for productId=%s", productId)

    if not newStatus:
        raise fn.UserThrownError("New status cannot be empty.")
    if newStatus not in ("Active", "Low Stock", "Discontinued"):
        raise fn.UserThrownError(
            "Invalid status value.",
            {"allowed": "Active, Low Stock, Discontinued", "received": newStatus}
        )

    conn   = sqlDB.connect()
    cursor = conn.cursor()

    # Get current status for history
    cursor.execute("SELECT Status, ProductName FROM dbo.Products WHERE ProductID = ?", productId)
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise fn.UserThrownError("Product not found.", {"productId": productId})

    prev_status, product_name = row[0], row[1]

    if prev_status == newStatus:
        conn.close()
        return f"'{product_name}' is already set to '{newStatus}'. No change made."

    # Update Products table
    cursor.execute(
        "UPDATE dbo.Products SET Status = ?, LastUpdated = SYSDATETIME() WHERE ProductID = ?",
        newStatus, productId
    )

    # Write to StatusHistory (Tutorial 2 pattern)
    cursor.execute(
        """INSERT INTO dbo.StatusHistory
           (ProductID, PreviousStatus, NewStatus, UpdatedBy, Notes)
           VALUES (?, ?, ?, ?, ?)""",
        productId, prev_status, newStatus, updatedBy or "anonymous", notes or ""
    )

    conn.commit()
    conn.close()

    # Post to Teams if webhook is configured
    if _TEAMS_WEBHOOK_URL:
        _post_teams_card(
            title   = f"Status Updated: {product_name}",
            facts   = [
                {"name": "Product",       "value": product_name},
                {"name": "Previous",      "value": prev_status},
                {"name": "New Status",    "value": newStatus},
                {"name": "Updated By",    "value": updatedBy or "anonymous"},
                {"name": "Notes",         "value": notes or "(none)"},
            ]
        )

    return f"'{product_name}' status updated from '{prev_status}' to '{newStatus}'."


@udf.connection(argName="sqlDB", alias="pocdb")
@udf.function()
def request_status_update(
    sqlDB: fn.FabricSqlConnection,
    productId: int,
    requestedBy: str,
    message: str
) -> str:
    """
    Tutorial 2: Request Status Update — sends Teams notification only, no DB write.
    Adapted from RequestDiscount in the official Gist.
    """
    logging.info("request_status_update called for productId=%s", productId)

    conn   = sqlDB.connect()
    cursor = conn.cursor()

    cursor.execute("SELECT ProductName, Status FROM dbo.Products WHERE ProductID = ?", productId)
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise fn.UserThrownError("Product not found.", {"productId": productId})

    product_name, current_status = row[0], row[1]

    if _TEAMS_WEBHOOK_URL:
        _post_teams_card(
            title = f"Status Update Request: {product_name}",
            facts = [
                {"name": "Product",        "value": product_name},
                {"name": "Current Status", "value": current_status},
                {"name": "Requested By",   "value": requestedBy or "anonymous"},
                {"name": "Message",        "value": message or "(none)"},
            ]
        )
        return f"Teams notification sent for '{product_name}' (current: {current_status})."
    else:
        return (
            f"Request noted for '{product_name}' (current: {current_status}). "
            "Teams webhook not configured — set _TEAMS_WEBHOOK_URL to enable notifications."
        )


# ── TUTORIAL 1 ─────────────────────────────────────────────────────────────
# Matches: AddAnnotation + UpdateDiscount patterns from the official Gist
# ───────────────────────────────────────────────────────────────────────────

@udf.connection(argName="sqlDB", alias="pocdb")
@udf.function()
def add_annotation(
    sqlDB: fn.FabricSqlConnection,
    productId: int,
    annotation: str
) -> str:
    """
    Tutorial 1: Add data annotation.
    Adapted from AddAnnotation in the official Gist.
    """
    logging.info("add_annotation called for productId=%s", productId)

    if not annotation or len(annotation.strip()) == 0:
        raise fn.UserThrownError("Annotation text cannot be empty.")

    conn   = sqlDB.connect()
    cursor = conn.cursor()

    cursor.execute("SELECT ProductName FROM dbo.Products WHERE ProductID = ?", productId)
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise fn.UserThrownError("Product not found.", {"productId": productId})

    product_name = row[0]

    cursor.execute(
        "INSERT INTO dbo.ProductAnnotations (ProductID, Annotation) VALUES (?, ?)",
        productId, annotation.strip()
    )

    conn.commit()
    conn.close()

    return f"Annotation added to '{product_name}'."


@udf.connection(argName="sqlDB", alias="pocdb")
@udf.function()
def update_price(
    sqlDB: fn.FabricSqlConnection,
    productId: int,
    newPrice: float
) -> str:
    """
    Tutorial 1: Update price with history log.
    Adapted from UpdateDiscount in the official Gist.
    """
    logging.info("update_price called for productId=%s newPrice=%s", productId, newPrice)

    if newPrice < 0:
        raise fn.UserThrownError("Price cannot be negative.")
    if newPrice > 100000:
        raise fn.UserThrownError("Price exceeds maximum allowed value of $100,000.")

    conn   = sqlDB.connect()
    cursor = conn.cursor()

    cursor.execute("SELECT ProductName, UnitPrice FROM dbo.Products WHERE ProductID = ?", productId)
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise fn.UserThrownError("Product not found.", {"productId": productId})

    product_name, old_price = row[0], row[1]

    # Update price
    cursor.execute(
        "UPDATE dbo.Products SET UnitPrice = ?, LastUpdated = SYSDATETIME() WHERE ProductID = ?",
        newPrice, productId
    )

    # Log price history
    cursor.execute(
        "INSERT INTO dbo.PriceHistory (ProductID, OldPrice, NewPrice) VALUES (?, ?, ?)",
        productId, old_price, newPrice
    )

    conn.commit()
    conn.close()

    return f"'{product_name}' price updated from ${old_price:,.2f} to ${newPrice:,.2f}."


# ── Shared helper ───────────────────────────────────────────────────────────

def _post_teams_card(title: str, facts: list) -> None:
    """Post a simple Teams Adaptive Card via Incoming Webhook."""
    import requests as _req

    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": title,
                        "weight": "Bolder",
                        "size": "Medium",
                        "color": "Accent"
                    },
                    {
                        "type": "FactSet",
                        "facts": [{"title": f["name"], "value": str(f["value"])} for f in facts]
                    }
                ]
            }
        }]
    }

    try:
        resp = _req.post(_TEAMS_WEBHOOK_URL, json=card, timeout=10)
        if resp.status_code not in (200, 202):
            logging.warning("Teams webhook returned %s: %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logging.warning("Teams notification failed: %s", str(e))
