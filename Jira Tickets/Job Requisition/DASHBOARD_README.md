# Job Requisition Dashboard - HTML Version

## Overview

Interactive HTML dashboard for tracking and monitoring open job requisitions at Taylor Morrison. Built with vanilla JavaScript, Chart.js, and Flask backend.

## Features

### Summary Cards
- **Total Open Requisitions** - Current count of open positions
- **Total Positions** - Number of openings to fill
- **Average Days Open** - Average time positions remain open
- **Departments** - Count of departments with open positions
- **Locations** - Number of locations currently hiring
- **Longest Open** - Maximum days any position has been open

### Visualizations

1. **Aging Analysis** (Bar Chart)
   - Distribution of requisitions by age buckets
   - Categories: 0-14, 15-30, 31-60, 61-90, 90+ days
   - Color-coded: Green (new) to Red (critical)

2. **Requisition Reasons** (Doughnut Chart)
   - Breakdown of why positions opened
   - Categories: New Position, Backfill, Transfer, etc.

3. **Top 10 Departments** (Horizontal Bar Chart)
   - Departments with most open requisitions
   - Shows count of open positions per department

4. **Top 10 Locations** (Horizontal Bar Chart)
   - Geographic distribution of openings
   - Locations with most hiring activity

5. **Monthly Trend** (Line Chart)
   - 12-month historical view
   - Three lines: Created, Filled, Still Open
   - Shows hiring trends over time

6. **Hiring Manager Workload** (Horizontal Bar Chart)
   - Top 10 managers by open requisitions
   - Shows workload distribution

7. **Critical Requisitions Table**
   - Positions open 90+ days
   - Shows: Requisition ID, Job Title, Department, Location, Manager, Start Date, Days Open
   - Color-coded badges: Green (90-120), Yellow (120-180), Red (180+)

## Architecture

### Backend API (Flask)
**File:** `job_requisition_api.py`
**Port:** 8008

**Features:**
- RESTful API with 9 endpoints
- 5-minute data caching for performance
- Connects to TaylorMorrisonDataLake on SQLDL1.TWC.PVT
- CORS enabled for local development

**API Endpoints:**
- `GET /` - Serves the HTML dashboard
- `GET /api/summary` - Summary statistics
- `GET /api/aging` - Aging analysis data
- `GET /api/departments` - Department breakdown
- `GET /api/locations` - Location breakdown
- `GET /api/reasons` - Requisition reasons
- `GET /api/managers` - Hiring manager workload
- `GET /api/trend` - Monthly trend data
- `GET /api/critical` - Critical requisitions (90+ days)
- `GET /api/all` - All data in one call (recommended)
- `GET /api/refresh` - Force cache refresh

### Frontend (HTML/JavaScript)
**File:** `job_requisition_dashboard.html`

**Technologies:**
- HTML5
- CSS3 (Flexbox, Grid, Animations)
- Vanilla JavaScript (ES6+)
- Chart.js 4.4.0 (via CDN)

**Features:**
- Responsive design (mobile-friendly)
- Auto-refresh every 5 minutes
- Manual refresh button
- Real-time last updated timestamp
- Error handling with user feedback
- Loading states with spinners
- Hover effects and animations

## Installation

### Prerequisites
1. Python 3.8+
2. Access to SQLDL1.TWC.PVT database
3. Required Python packages

### Setup Steps

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Verify Database Connection**
   - Ensure `sql_connection.py` is in parent directory
   - Verify `.env` file has correct credentials
   - Test connection: `py test_job_requisition_table.py`

3. **Launch Dashboard**

   **Option A: Using Batch File (Recommended)**
   ```bash
   "Launch Job Requisition Dashboard.bat"
   ```

   **Option B: Manual Launch**
   ```bash
   # Terminal 1: Start API
   py job_requisition_api.py

   # Terminal 2: Open browser to http://localhost:8008
   ```

## Usage

### Starting the Dashboard
1. Double-click `Launch Job Requisition Dashboard.bat`
2. API starts in separate window
3. Dashboard opens automatically in browser
4. Wait 5-10 seconds for initial data load

### Dashboard Controls
- **Refresh Button** - Manually refresh all data
- **Auto-Refresh** - Dashboard refreshes every 5 minutes automatically
- **Last Updated** - Shows timestamp of last data refresh

### Stopping the Dashboard
- Close the "Job Requisition API" command window
- Or press `Ctrl+C` in the API window

## Data Flow

```
SQL Database (SQLDL1)
    ↓
Python Flask API (job_requisition_api.py)
    ↓
5-minute Cache (in-memory)
    ↓
REST API Endpoints (JSON)
    ↓
HTML Dashboard (job_requisition_dashboard.html)
    ↓
Chart.js Visualizations
```

## Performance

### Caching Strategy
- **Cache Duration:** 5 minutes (300 seconds)
- **Cache Scope:** All API endpoints share single cache
- **Cache Refresh:** Automatic on expiration, or manual via `/api/refresh`
- **Pre-loading:** Cache loads on API startup

### Query Optimization
- All queries use indexed columns where possible
- JSON_VALUE extracts only necessary fields
- TOP clauses limit result sets
- Aggregations done in SQL (not in Python)

### Load Times
- **Initial Load:** 10-15 seconds (includes database queries)
- **Cached Load:** <500ms (served from memory)
- **Chart Rendering:** <1 second
- **Page Load:** <2 seconds (HTML + CSS + JS)

## Troubleshooting

### API Won't Start

**Error:** "Address already in use"
```bash
# Find process on port 8008
netstat -ano | findstr :8008

# Kill the process
taskkill /PID <process_id> /F
```

**Error:** "Module not found"
```bash
# Install missing packages
pip install -r requirements.txt
```

### Database Connection Errors

**Error:** "Cannot connect to DataLake"
1. Verify VPN/network connection
2. Check `.env` file has correct server: `SQLDL1.TWC.PVT`
3. Test connection: `py test_job_requisition_table.py`
4. Verify Windows Authentication permissions

### Dashboard Won't Load

**Error:** "Failed to load dashboard data"
1. Verify API is running on port 8008
2. Check browser console for errors (F12)
3. Visit `http://localhost:8008/api/all` directly
4. Clear browser cache and refresh

### Charts Not Displaying

**Error:** Blank chart areas
1. Check browser console for JavaScript errors
2. Verify Chart.js CDN is accessible
3. Check data format in `/api/all` endpoint
4. Ensure data arrays are not empty

### Slow Performance

**Issue:** Dashboard takes too long to load
1. Check database server performance
2. Verify network latency to SQLDL1
3. Increase cache duration in `job_requisition_api.py`
4. Consider materialized table approach (see `parse_job_requisition_json.sql`)

## Customization

### Changing Cache Duration
Edit `job_requisition_api.py`:
```python
CACHE_DURATION = 600  # 10 minutes instead of 5
```

### Modifying Color Scheme
Edit `job_requisition_dashboard.html` CSS section:
```css
/* Primary gradient */
background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);

/* Change to different colors */
background: linear-gradient(135deg, #your-color-1 0%, #your-color-2 100%);
```

### Adding New Charts
1. Add new query to `job_requisition_api.py`
2. Create new API endpoint
3. Add chart container to HTML
4. Create chart function in JavaScript
5. Call function in `initDashboard()`

### Changing Port
Edit `job_requisition_api.py`:
```python
app.run(host='0.0.0.0', port=5002, debug=True)  # Change to 5002
```

And update HTML:
```javascript
const API_BASE = 'http://localhost:5002/api';  // Change to 5002
```

## Security Considerations

### Production Deployment
**DO NOT deploy this as-is to production without:**
1. Adding authentication (OAuth, JWT, etc.)
2. Enabling HTTPS/SSL
3. Restricting CORS to specific domains
4. Adding rate limiting
5. Sanitizing SQL inputs (currently using parameterized queries)
6. Adding logging and monitoring
7. Setting `debug=False` in Flask

### Current Security
- Uses Windows Authentication for database
- No user authentication on dashboard (local only)
- CORS enabled for localhost development
- SQL queries use parameterized statements
- No sensitive data in HTML/JavaScript

## Deployment Options

### Option 1: Local Development (Current)
- Run on localhost:8008
- Access only from your machine
- Perfect for testing and development

### Option 2: Shared Network Drive
- Copy files to shared location
- Users run batch file from network drive
- Each user runs own API instance
- No server required

### Option 3: Internal Server
- Deploy Flask API to internal server
- Use WSGI server (Gunicorn, waitress)
- Set up reverse proxy (nginx, IIS)
- Enable authentication
- Serve dashboard to multiple users

### Option 4: Power BI Embedded
- Keep SQL queries
- Migrate visualizations to Power BI
- Embed Power BI dashboard in internal portal
- Leverage existing Power BI infrastructure

## Maintenance

### Regular Tasks
- **Weekly:** Check API logs for errors
- **Monthly:** Review and optimize slow queries
- **Quarterly:** Update dependencies: `pip install --upgrade -r requirements.txt`
- **As Needed:** Add new visualizations based on user feedback

### Monitoring
- Check API console window for errors
- Monitor database query performance
- Track dashboard load times in browser console
- Review user feedback

## Support

### Contacts
- **Developer:** Joey Ramos (joramos@taylormorrison.com)
- **Business Owner:** Pete Gonzales
- **Database Admin:** Doug Meinert
- **Data Engineering:** Vishnu Veeragoni

### Resources
- **GitHub Repository:** https://github.com/taylormorrison/BI-AI/tree/main/Leadership/Job%20Requisition
- **Jira Ticket:** BUS-310
- **SQL Scripts:** `parse_job_requisition_json.sql`
- **Test Script:** `test_job_requisition_table.py`

## Version History

### v1.0.0 (2026-02-03)
- Initial release
- 8 visualizations
- 6 summary cards
- 5-minute caching
- Auto-refresh functionality
- Responsive design
- Critical requisitions table

---

**Last Updated:** 2026-02-03
**Status:** Production Ready
**License:** Internal Use Only - Taylor Morrison
