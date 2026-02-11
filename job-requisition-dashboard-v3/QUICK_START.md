# Quick Start Guide

Get the Job Requisition Dashboard running in 5 minutes!

## Prerequisites

- ✅ Python 3.11+
- ✅ Node.js 18+
- ✅ SQL Server access (TaylorMorrisonDataLake)
- ✅ `.env` file configured

## Step 1: Setup Environment Variables

Create a `.env` file in the project root:

```env
SQL_SERVER=SQLDL1.TWC.PVT
SQL_DATABASE_DATALAKE=TaylorMorrisonDataLake
SQL_DRIVER=ODBC Driver 17 for SQL Server
```

## Step 2: Start the Backend

Open Terminal 1:

```bash
cd backend
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
uvicorn app:app --reload --port 8080
```

Wait for: `INFO:     Application startup complete.`

Test: Open http://localhost:8080/api/docs

## Step 3: Start the Frontend

Open Terminal 2:

```bash
cd frontend
npm install
npm install tailwindcss-animate
npm run dev
```

Wait for: `Local: http://localhost:3000`

## Step 4: Open the Dashboard

Open your browser to: **http://localhost:3000**

You should see:
- Navigation bar at the top
- 4 summary cards
- 6 charts and visualizations
- Critical requisitions table

## Step 5: Test Features

### Test Dashboard
1. Click **Dashboard** in navbar
2. Verify all charts load
3. Click **Refresh** button to reload data

### Test Requisitions Table
1. Click **Requisitions** in navbar
2. Try filtering by department or location
3. Click column headers to sort
4. Change page size to 50 or 100
5. Click **Export to CSV** button

## Troubleshooting

### Backend won't start
- Check `.env` file exists and has correct values
- Verify SQL Server connection: `python sql_connection.py`
- Check Python version: `python --version` (need 3.11+)

### Frontend won't start
- Delete `node_modules` and run `npm install` again
- Check Node version: `node --version` (need 18+)
- Try `npm install --legacy-peer-deps`

### Charts not loading
- Check browser console (F12) for errors
- Verify backend is running at http://localhost:8080
- Check API response at http://localhost:8080/api/analytics/summary

### Database connection fails
- Verify VPN connection
- Check SQL Server is accessible
- Test with: `python sql_connection.py`

## Next Steps

Once everything is working:

1. **Explore the Dashboard**
   - All 6 visualizations
   - Summary statistics
   - Critical requisitions

2. **Test the Requisitions Table**
   - Filtering
   - Sorting
   - Pagination
   - CSV export

3. **Review the Code**
   - Backend: `backend/routes/`, `backend/services/`
   - Frontend: `frontend/src/components/`, `frontend/src/pages/`

4. **Deploy** (when ready)
   - See `BACKEND_SETUP.md` and `README.md` for deployment options
   - Docker option: `cd docker && docker-compose up`

## Support

- **API Documentation**: http://localhost:8080/api/docs (when backend running)
- **Backend Setup**: See `BACKEND_SETUP.md`
- **Project README**: See `README.md`
- **Full Details**: See `IMPLEMENTATION_COMPLETE.md`

---

**Need Help?**
- Check the browser console (F12) for errors
- Check backend terminal for error messages
- Verify `.env` configuration
- Ensure database connection is working

**Ready?** Open http://localhost:3000 and start exploring! 🚀
