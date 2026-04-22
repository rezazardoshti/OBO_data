# Professional Raw Data Package

Dieses Paket ist für ein realistisches Data-Engineering-Projekt gebaut.

## Struktur
data/raw/
- source=erp/entity=orders/ingest_date=2026-04-20/orders.csv
- source=erp/entity=order_items/ingest_date=2026-04-20/order_items.csv
- source=erp/entity=products/ingest_date=2026-04-20/products.csv
- source=erp/entity=inventory_snapshots/ingest_date=2026-04-20/inventory_snapshots.csv
- source=crm/entity=users/ingest_date=2026-04-20/users.csv
- source=web/entity=sessions/ingest_date=2026-04-20/sessions.csv
- source=web/entity=clickstream_events/event_date=YYYY-MM-DD/clickstream_YYYY-MM-DD.jsonl
- source=app/entity=application_logs/ingest_date=2026-04-20/app.log

## Absichtlich eingebaute Raw-Probleme
- uneinheitliche Schreibweisen wie PayPal / Google / Desktop / NEWSLETTER
- Leerzeichen wie DE 
- Nullwerte
- doppelte Rohzeilen
- fehlende Produktnamen / Device-Werte
- gemischte Event-Typ-Schreibweisen

## Dataset-Größe
- users: 300
- products: 144
- sessions: 2974
- events_raw: 29109
- orders_raw: 705
- order_items_raw: 1224
- inventory_snapshots: 1152
- app_log_lines: 240
