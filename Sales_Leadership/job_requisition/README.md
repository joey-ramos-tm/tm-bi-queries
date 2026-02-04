# Job Requisition Dashboard

**Real-time tracking of open job requisitions for Taylor Morrison**

---

## 🚀 Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Launch dashboard
Double-click "Launch Job Requisition Dashboard.bat"

# 3. Open browser to http://localhost:8008
```

---

## 📊 Dashboard Features

### Summary Metrics
- **Total Open Requisitions** - Current open positions
- **Total Positions** - Number of openings to fill
- **Average Days Open** - Average time to fill
- **Departments** - Count with open positions
- **Locations** - Geographic hiring activity
- **Longest Open** - Maximum days any position open

### Interactive Visualizations

1. **Aging Analysis Bar Chart**
   - 0-14 days (New)
   - 15-30 days (Active)
   - 31-60 days (Warning)
   - 61-90 days (High Risk)
   - 90+ days (Critical)

2. **Requisition Reasons Doughnut Chart**
   - New Position
   - Backfill
   - Transfer
   - Other

3. **Top 10 Departments Horizontal Bar Chart**

4. **Top 10 Locations Horizontal Bar Chart**

5. **12-Month Trend Line Chart**
   - Requisitions Created
   - Requisitions Filled
   - Still Open

6. **Critical Requisitions Table** (90+ days open)

---

## 🗄️ Data Source

**Database:** TaylorMorrisonDWH_Bronze (SQLDWH1.TWC.PVT)
**Schema:** WorkDay
**Table:** Get_Job_Requisition
**Records:** ~12,709 job requisitions

### Key Fields
- `Requisition` - Primary key
- `Target_Hire_Date` - Target fill date
- `Recruiting_Start_Date` - When recruiting began
- `Scheduled_Weekly_Hours` - Expected hours
- `Job_Profile_Reference` - Job title (JSON)
- `Job_Requisition_Status_Reference` - Status (JSON)
- `Primary_Location_Reference` - Location (JSON)
- `Supervisory_Organization_Reference` - Department (JSON)
- `RequisitionReason` - Why position opened (JSON)
- `Time_Type_Reference` - Full/Part time (JSON)
- `Worker_Type_Reference` - Employee/Contractor (JSON)

---

## 📁 Project Files

| File | Description |
|------|-------------|
| `job_requisition_dashboard.html` | Interactive web dashboard UI |
| `job_requisition_api.py` | Flask REST API backend |
| `sql_connection.py` | Database connection module |
| `Launch Job Requisition Dashboard.bat` | One-click launcher |
| `requirements.txt` | Python dependencies |
| `parse_job_requisition_json.sql` | Comprehensive JSON parsing script |
| `JobRequisition.sql` | Simplified query for Power BI |
| `test_job_requisition_table.py` | Data validation script |

---

## 🔧 Technical Stack

**Frontend:** HTML5, CSS3, JavaScript ES6+, Chart.js 4.4.0
**Backend:** Python Flask, pyodbc, Flask-CORS
**Database:** SQL Server (Windows Authentication)
**Port:** 8008

---

## 🌐 API Endpoints

**Base URL:** http://localhost:8008/api

| Endpoint | Description |
|----------|-------------|
| `/api/summary` | Summary KPIs |
| `/api/aging` | Aging analysis data |
| `/api/departments` | Top 10 departments |
| `/api/locations` | Top 10 locations |
| `/api/reasons` | Requisition reasons |
| `/api/trend` | 12-month trend |
| `/api/critical` | Critical requisitions (90+ days) |
| `/api/all` | All data in one call |
| `/api/refresh` | Force cache refresh |

---

## ⚙️ Configuration

### Change Port
1. Edit `job_requisition_api.py`: Line ~300
   ```python
   app.run(host='0.0.0.0', port=YOUR_PORT)
   ```

2. Edit `job_requisition_dashboard.html`:
   ```javascript
   const API_BASE = 'http://localhost:YOUR_PORT/api';
   ```

### Adjust Cache Duration
Edit `job_requisition_api.py`:
```python
CACHE_DURATION = 600  # 10 minutes
```

---

## 🐛 Troubleshooting

### Port Already in Use
```bash
netstat -ano | findstr :8008
taskkill /PID <process_id> /F
```

### Database Connection Failed
- Check VPN/network connection
- Verify Windows Authentication
- Test: `py test_job_requisition_table.py`

### Dashboard Won't Load
- Check API console for errors
- Verify: http://localhost:8008/api/all
- Clear browser cache

---

## 👥 Team

**Developer:** Joey Ramos (joramos@taylormorrison.com)
**Business Owner:** Pete Gonzales
**DBA:** Doug Meinert
**Data Engineering:** Vishnu Veeragoni

**Jira Ticket:** BUS-310

---

## 📚 SQL Query Examples

### Get All Open Requisitions
```sql
SELECT
    Requisition,
    JSON_VALUE(Job_Profile_Reference, '$[1]."#text"') AS Job_Title,
    JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') AS Status,
    JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') AS Location,
    JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department,
    Target_Hire_Date,
    Recruiting_Start_Date,
    DATEDIFF(DAY, Recruiting_Start_Date, GETDATE()) AS Days_Open
FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
ORDER BY Days_Open DESC;
```

---

## 🔐 Security Notes

**Current:** Local development only (localhost:8008)
**Production Needs:**
- Authentication (OAuth/LDAP)
- HTTPS/SSL
- CORS restrictions
- Rate limiting
- Logging/monitoring
- `debug=False` in Flask

---

## 📄 License

**Internal Use Only** - Taylor Morrison Companies
**Confidential** - Do not distribute

---

**Last Updated:** 2026-02-04
**Repository:** https://github.com/taylormorrison/BI-AI/tree/main/Sales_Leadership/job_requisition
**Status:** ✅ Production Ready
