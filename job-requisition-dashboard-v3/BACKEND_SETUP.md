# Backend Setup Guide

## Prerequisites

- Python 3.11 or higher
- SQL Server ODBC Driver 17
- Access to TaylorMorrisonDataLake database
- .env file with database credentials

## Installation Steps

### 1. Create Virtual Environment

```bash
cd backend
python -m venv venv
```

### 2. Activate Virtual Environment

**Windows**:
```bash
venv\Scripts\activate
```

**Linux/Mac**:
```bash
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the example .env file:
```bash
cp .env.example .env
```

Edit `.env` and add your credentials:
```env
SQL_SERVER=your_sql_server_name
SQL_DATABASE_DATALAKE=TaylorMorrisonDataLake
SQL_DRIVER=ODBC Driver 17 for SQL Server
API_HOST=0.0.0.0
API_PORT=8080
CORS_ORIGINS=http://localhost:3000,http://localhost:5173
```

### 5. Verify Database Connection

Test the database connection:
```bash
cd ..
python sql_connection.py
```

You should see:
```
Successfully connected to TaylorMorrisonDataLake on SQLDL1.TWC.PVT
```

### 6. Run the Backend

```bash
cd backend
uvicorn app:app --reload --port 8080
```

Or use the Python script directly:
```bash
python app.py
```

### 7. Verify API is Running

Open your browser and navigate to:
- **Health Check**: http://localhost:8080/api/health
- **API Docs (Swagger)**: http://localhost:8080/api/docs
- **API Docs (ReDoc)**: http://localhost:8080/api/redoc

You should see the API documentation with all available endpoints.

## Project Structure

```
backend/
├── routes/
│   ├── analytics.py         # Analytics endpoints
│   ├── requisitions.py      # Requisition endpoints
│   └── exports.py           # Export endpoints
├── models/
│   ├── analytics.py         # Pydantic models for analytics
│   └── requisition.py       # Pydantic models for requisitions
├── services/
│   ├── database.py          # Database connection
│   ├── analytics_service.py # Analytics business logic
│   └── requisition_service.py # Requisition business logic
├── app.py                   # Main FastAPI application
└── requirements.txt         # Python dependencies
```

## API Endpoints

### Analytics

- `GET /api/analytics/summary` - Summary statistics
- `GET /api/analytics/aging` - Aging analysis
- `GET /api/analytics/departments` - Top 10 departments
- `GET /api/analytics/locations` - Top 10 locations
- `GET /api/analytics/reasons` - Requisition reasons
- `GET /api/analytics/trends` - Monthly trends (12 months)
- `GET /api/analytics/critical` - Critical requisitions (90+ days)

### Requisitions

- `GET /api/requisitions` - List requisitions (with pagination, filtering, sorting)
  - Query params: `status`, `department`, `location`, `page`, `page_size`, `sort_by`, `sort_order`
- `GET /api/requisitions/{id}` - Get single requisition details

### Exports

- `POST /api/exports/csv` - Export requisitions to CSV
  - Body: `{"filters": {"status": "Open", "department": "Sales"}}`

### System

- `GET /api/health` - Health check endpoint

## Testing Endpoints

### Using cURL

**Get Summary**:
```bash
curl http://localhost:8080/api/analytics/summary
```

**Get Requisitions (paginated)**:
```bash
curl "http://localhost:8080/api/requisitions?page=1&page_size=10&sort_by=days_open&sort_order=desc"
```

**Export to CSV**:
```bash
curl -X POST http://localhost:8080/api/exports/csv \
  -H "Content-Type: application/json" \
  -d '{"filters": {"status": "Open"}}' \
  --output requisitions.csv
```

### Using Python Requests

```python
import requests

# Get summary
response = requests.get('http://localhost:8080/api/analytics/summary')
print(response.json())

# Get requisitions with filters
params = {
    'department': 'Sales',
    'page': 1,
    'page_size': 25,
    'sort_by': 'days_open',
    'sort_order': 'desc'
}
response = requests.get('http://localhost:8080/api/requisitions', params=params)
print(response.json())
```

## Development

### Hot Reload

The `--reload` flag enables hot reloading. Changes to Python files will automatically restart the server.

### Debugging

To run with debugging:
```bash
uvicorn app:app --reload --log-level debug --port 8080
```

### Adding New Endpoints

1. **Create Pydantic model** in `models/`
2. **Add service method** in `services/`
3. **Create route** in `routes/`
4. **Include router** in `app.py`

Example:
```python
# models/example.py
from pydantic import BaseModel

class ExampleModel(BaseModel):
    id: int
    name: str

# services/example_service.py
class ExampleService:
    @staticmethod
    def get_data():
        return {"id": 1, "name": "Test"}

# routes/example.py
from fastapi import APIRouter
router = APIRouter()

@router.get("/example")
async def get_example():
    return ExampleService.get_data()

# app.py
from routes import example
app.include_router(example.router, prefix="/api/example", tags=["Example"])
```

## Troubleshooting

### Issue: Module not found

**Solution**: Make sure virtual environment is activated
```bash
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac
```

### Issue: Database connection failed

**Solution**:
1. Verify `.env` file has correct credentials
2. Test connection: `python sql_connection.py`
3. Check VPN connection if accessing remote server

### Issue: Port 8080 already in use

**Solution**: Use a different port
```bash
uvicorn app:app --reload --port 8081
```

### Issue: CORS errors in frontend

**Solution**: Add frontend origin to CORS_ORIGINS in `.env`
```env
CORS_ORIGINS=http://localhost:3000,http://localhost:5173
```

## Production Deployment

For production deployment:

1. **Disable reload**:
   ```bash
   uvicorn app:app --host 0.0.0.0 --port 8080 --workers 4
   ```

2. **Use gunicorn** (recommended):
   ```bash
   pip install gunicorn
   gunicorn app:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8080
   ```

3. **Docker deployment**: See `docker/` directory

## Performance Tips

1. **Database Connection Pooling**: Already implemented in `database.py`
2. **Caching**: Consider adding Redis for query result caching
3. **Query Optimization**: Add indexes to frequently queried columns
4. **Async Operations**: FastAPI endpoints are async-ready

## Security Considerations

1. **Environment Variables**: Never commit `.env` to version control
2. **SQL Injection**: All queries use parameterized statements
3. **CORS**: Configure allowed origins in production
4. **API Keys**: Consider adding API key authentication for production

## Next Steps

1. ✅ Backend is running
2. ➡️ Set up frontend: See `frontend/SETUP.md`
3. ➡️ Test integration: Run both backend and frontend
4. ➡️ Deploy: See `DEPLOYMENT.md`

---

For questions or issues, contact the development team.
