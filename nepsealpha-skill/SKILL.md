---
name: nepsealpha-skill
description: Information and selectors for scraping financial data from NepseAlpha.com.
---

# NepseAlpha Skill

This skill provides a knowledge base and resources for automating interactions with [NepseAlpha](https://nepsealpha.com/). It includes identified CSS selectors, URL endpoints, and notes on anti-bot protection.

## Pages & Selectors

### 1. Mutual Fund NAVs
**URL**: `https://nepsealpha.com/mutual-fund-navs`

This page contains multiple tabs with different datasets.

#### Tabs
| Tab Name | Href Selector | Description |
| :--- | :--- | :--- |
| **NAV** | `a[href='#home']` | Net Asset Value history |
| **Stock Holdings** | `a[href='#stkHolding']` | Fund PE Ratio & Holdings |
| **Assets Allocation** | `a[href='#assetsAllocation']` | Asset distribution |
| **Distributable Dividend** | `a[href='#distributableDividend']` | Dividend capacity |

#### Data Tables
All tables use the DataTables library. To see all data, you typically need to select "100" from the length menu.

| Section | Table ID | Length Selector (Name) |
| :--- | :--- | :--- |
| **NAV** | `#DataTables_Table_0` | `DataTables_Table_0_length` |
| **Stock Holdings** | `#DataTables_Table_1` | `DataTables_Table_1_length` |
| **Assets Allocation** | `#DataTables_Table_2` | `DataTables_Table_2_length` |
| **Dividend** | `#DataTables_Table_3` | `DataTables_Table_3_length` |

---

### 2. Debentures
**URL**: `https://nepsealpha.com/debenture`

> [!WARNING]
> **Cloudflare Protection**: This page may challenge automated browsers.
> **Workaround**: Use `cloudscraper` or a persistent browser context with stealth plugins.

- **Table ID**: `#DataTables_Table_0` (Likely, consistent with site pattern)
- **Length Selector**: `select[name='DataTables_Table_0_length']`

---

### 3. Detailed Fund Holdings
**URL Pattern**: `https://nepsealpha.com/mutual-fund-navs/{SYMBOL}?fsk=fs`

- **Data Loading**: These pages often load data dynamically or via server-side rendering.
- **Scraping Strategy**: `pandas.read_html` often works on the raw HTML response if Cloudflare doesn't block the request.

## Automation Tips

1.  **Handling Dropdowns**: The "Show entries" dropdown is a standard `<select>` element. Changing it triggers a table redraw.
2.  **Waiting**: Always wait for specific elements (like the table ID) to be visible before parsing. The site is a SPA (Single Page App) in parts, or uses heavy AJAX.
3.  **Bot Detection**: The site uses Cloudflare. Simple `requests` often fail on protected pages like `/debenture`. Playwright (headed) or `cloudscraper` is recommended.
4.  **Rate Limiting**: Rapid requests to detail pages (e.g., iterating through all funds) triggers **403 Forbidden** blocks.
    - **Solution**: Use random delays (3-8 seconds) between requests.
    - **Retry Logic**: Implement exponential backoff for 403 responses.
    - **Timeouts**: Ensure your scraping job has a sufficient timeout (e.g., 30+ mins) to accommodate these delays.

## Machine Readable Selectors
See [selectors.json](./selectors.json) for a JSON representation of these selectors for use in scripts.
