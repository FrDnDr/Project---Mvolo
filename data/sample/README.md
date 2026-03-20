# 📂 data/sample/ — Sample Test Data

> Anonymized, fake data files that mirror real API response schemas — for testing, development, and demos.

---

## 📁 Files

| File | Mimics | Records |
|------|--------|---------|
| `sample_bol_orders.json` | Bol.com `/orders` API response | ~50 fake orders |
| `sample_shopify_orders.json` | Shopify `/orders.json` API response | ~50 fake orders |

---

## 🔧 How to Generate

```bash
# Generate default sample data (50 records per entity)
python scripts/seed_sample_data.py

# Generate more records
python scripts/seed_sample_data.py --records 200
```

---

## 📄 Sample Format

### `sample_bol_orders.json`
```json
{
  "orders": [
    {
      "orderId": "BOL-2026-000001",
      "orderPlacedDateTime": "2026-03-10T10:15:30+01:00",
      "orderItems": [
        {
          "orderItemId": "item-001",
          "ean": "8712345678901",
          "title": "Wireless Bluetooth Headphones",
          "quantity": 1,
          "unitPrice": 34.99,
          "commission": 5.25
        }
      ],
      "shipmentDetails": {
        "method": "FBB",
        "trackingCode": "3STEST0001234567"
      }
    }
  ]
}
```

### `sample_shopify_orders.json`
```json
{
  "orders": [
    {
      "id": 1000000001,
      "created_at": "2026-03-10T10:15:30+01:00",
      "total_price": "49.99",
      "currency": "EUR",
      "financial_status": "paid",
      "fulfillment_status": "fulfilled",
      "line_items": [
        {
          "id": 2000000001,
          "title": "USB-C Charging Cable",
          "quantity": 2,
          "price": "24.99",
          "sku": "USB-C-001"
        }
      ],
      "customer": {
        "id": 3000000001,
        "first_name": "Jan",
        "last_name": "de Vries",
        "email": "jan.devries@example.com"
      }
    }
  ]
}
```

---

## ⚠️ Rules

1. **No real data** — All values must be fictitious
2. **Realistic structure** — JSON structure must exactly match real API responses
3. **Consistent** — Same field names, same nesting, same data types
4. **Committed to Git** — These files are tracked (unlike `data/raw/`)
