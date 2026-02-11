# Job Requisition Dashboard v3.0

Modern, production-ready job requisition analytics dashboard built with FastAPI + React TypeScript.

## 🎯 Overview

This is a complete rebuild of the Job Requisition Dashboard using modern technologies:
- **Backend**: FastAPI (Python 3.11+)
- **Frontend**: React 18 + TypeScript + Vite
- **Database**: SQL Server (TaylorMorrisonDataLake)
- **Styling**: Tailwind CSS + ShadCN/ui
- **Charts**: Recharts
- **State Management**: React Query (TanStack Query)

## 📁 Project Structure

```
job-requisition-dashboard-v3/
├── backend/                    # FastAPI backend
│   ├── routes/                 # API endpoints
│   │   ├── analytics.py        # Dashboard analytics
│   │   ├── requisitions.py     # Requisition CRUD
│   │   └── exports.py          # CSV export
│   ├── models/                 # Pydantic models
│   │   ├── analytics.py        # Analytics data models
│   │   └── requisition.py      # Requisition models
│   ├── services/               # Business logic
│   │   ├── database.py         # DB connection
│   │   ├── analytics_service.py
│   │   └── requisition_service.py
│   ├── app.py                  # Main FastAPI app
│   └── requirements.txt        # Python dependencies
│
├── frontend/                   # React TypeScript frontend
│   ├── src/
│   │   ├── components/         # React components
│   │   │   ├── ui/             # ShadCN/ui components
│   │   │   ├── dashboard/      # Dashboard-specific
│   │   │   ├── layout/         # Layout components
│   │   │   └── charts/         # Chart components
│   │   ├── pages/              # Page components
│   │   ├── hooks/              # React Query hooks
│   │   ├── lib/                # Utilities and API client
│   │   └── types/              # TypeScript types
│   ├── package.json
│   └── vite.config.ts
│
├── docker/                     # Docker configurations
│   ├── Dockerfile.backend
│   ├── Dockerfile.frontend
│   ├── docker-compose.yml
│   └── docker-compose.prod.yml
│
└── shared/                     # Shared utilities
    ├── generate-types.sh       # Type generation script
    └── generate-types.bat      # Windows version
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Node.js 18+
- SQL Server access (TaylorMorrisonDataLake)
- .env file with database credentials

### Backend Setup

1. **Navigate to backend directory**:
   ```bash
   cd backend
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   # or
   source venv/bin/activate  # Linux/Mac
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Copy .env.example to .env** (in project root):
   ```bash
   cp .env.example .env
   ```

5. **Configure .env with your database credentials**:
   ```
   SQL_SERVER=your_server
   SQL_DATABASE_DATALAKE=TaylorMorrisonDataLake
   SQL_DRIVER=ODBC Driver 17 for SQL Server
   ```

6. **Run the backend**:
   ```bash
   uvicorn app:app --reload --port 8080
   ```

7. **Access API docs**:
   - Swagger UI: http://localhost:8080/api/docs
   - ReDoc: http://localhost:8080/api/redoc

### Frontend Setup

1. **Navigate to frontend directory**:
   ```bash
   cd frontend
   ```

2. **Install dependencies**:
   ```bash
   npm install
   npm install tailwindcss-animate  # Required for animations
   ```

3. **Run development server**:
   ```bash
   npm run dev
   ```

4. **Access frontend**:
   - App: http://localhost:3000

### Full Stack Development

To run both backend and frontend simultaneously:

**Terminal 1 (Backend)**:
```bash
cd backend
venv\Scripts\activate
uvicorn app:app --reload --port 8080
```

**Terminal 2 (Frontend)**:
```bash
cd frontend
npm run dev
```

## 📊 API Endpoints

### Analytics Endpoints

- `GET /api/analytics/summary` - Summary statistics
- `GET /api/analytics/aging` - Aging analysis (0-14, 15-30, 31-60, 61-90, 90+ days)
- `GET /api/analytics/departments` - Top 10 departments
- `GET /api/analytics/locations` - Top 10 locations
- `GET /api/analytics/reasons` - Requisition reasons breakdown
- `GET /api/analytics/trends` - 12-month trends
- `GET /api/analytics/critical` - Critical requisitions (90+ days)

### Requisition Endpoints

- `GET /api/requisitions` - List requisitions (paginated, filterable, sortable)
- `GET /api/requisitions/{id}` - Get requisition details

### Export Endpoints

- `POST /api/exports/csv` - Export requisitions to CSV

### System Endpoints

- `GET /api/health` - Health check

## 🎨 Features

### Dashboard Page
- **Summary Cards**: Total open, departments, locations, avg days open
- **Aging Analysis Chart**: Bar chart showing requisition age distribution
- **Requisition Reasons Chart**: Donut chart showing reason breakdown
- **Top Departments Chart**: Horizontal bar chart
- **Top Locations Chart**: Horizontal bar chart
- **Monthly Trends Chart**: Line chart showing 12-month trends
- **Critical Requisitions Table**: 90+ day requisitions

### Requisitions Table Page
- Paginated table (25, 50, 100 per page)
- Filterable by status, department, location
- Sortable by any column
- Export to CSV functionality

## 🎨 Branding

The dashboard uses Taylor Morrison brand colors:
- **Primary**: #A6192E (burgundy)
- **Secondary**: #3B82F6 (blue)
- **Neutrals**: Gray scale

Colors are configured in `frontend/tailwind.config.js`.

## 🔧 Development

### Type Generation

The project includes a type generation pipeline that creates TypeScript types from Pydantic models:

**Windows**:
```bash
cd shared
generate-types.bat
```

**Linux/Mac**:
```bash
cd shared
bash generate-types.sh
```

This generates `frontend/src/types/generated.ts` with all API types.

### Adding New Endpoints

1. **Define Pydantic model** in `backend/models/`
2. **Create service method** in `backend/services/`
3. **Add route** in `backend/routes/`
4. **Regenerate types**: Run `generate-types.bat`
5. **Use in frontend**: Import types from `@/types/generated`

### Code Quality

**Backend**:
```bash
# Format code
black backend/

# Type checking
mypy backend/
```

**Frontend**:
```bash
# Lint code
npm run lint

# Type check
npm run type-check
```

## 🐳 Docker Deployment

### Development

```bash
cd docker
docker-compose up
```

### Production

```bash
cd docker
docker-compose -f docker-compose.prod.yml up -d
```

## 📝 Environment Variables

### Backend (.env)

```env
# SQL Server Configuration
SQL_SERVER=your_server_name
SQL_DATABASE_DATALAKE=TaylorMorrisonDataLake
SQL_DRIVER=ODBC Driver 17 for SQL Server

# API Configuration
API_HOST=0.0.0.0
API_PORT=8080

# CORS Configuration
CORS_ORIGINS=http://localhost:3000,http://localhost:5173
```

## 🧪 Testing

### Backend Tests

```bash
cd backend
pytest
```

### Frontend Tests

```bash
cd frontend
npm run test
```

### Integration Tests

```bash
# Run both backend and frontend, then:
npm run test:e2e
```

## 📈 Performance

- **API Response Time**: < 500ms (cached)
- **Dashboard Load Time**: < 2 seconds
- **Chart Rendering**: < 500ms per chart
- **Table Filtering**: < 200ms
- **CSV Export**: < 5 seconds for 1000 rows

## 🔒 Security

- CORS configured for specific origins
- SQL injection protection via parameterized queries
- Input validation with Pydantic
- TypeScript strict mode for type safety

## 📖 Documentation

- **API Documentation**: http://localhost:8080/api/docs (when running)
- **Frontend Setup**: See `frontend/SETUP.md`
- **Deployment Guide**: See `DEPLOYMENT.md` (coming soon)

## 🤝 Contributing

1. Create a feature branch
2. Make your changes
3. Test thoroughly
4. Submit a pull request

## 📄 License

Internal use only - Taylor Morrison

## 👥 Authors

- **Original Version**: Flask + HTML/CSS/JS
- **v3.0 Rebuild**: FastAPI + React TypeScript

## 🔄 Migration from v2

This version completely replaces the old Flask-based dashboard. Key improvements:

- ✅ Modern React frontend (vs. vanilla JS)
- ✅ TypeScript for type safety
- ✅ Component-based architecture
- ✅ FastAPI for better performance and auto-documentation
- ✅ Pydantic models for data validation
- ✅ Professional UI with ShadCN/ui
- ✅ Recharts for interactive visualizations
- ✅ React Query for efficient data fetching

## 📞 Support

For issues or questions, please contact the BI/Analytics team.

---

**Version**: 3.0.0
**Last Updated**: 2026-02-05
