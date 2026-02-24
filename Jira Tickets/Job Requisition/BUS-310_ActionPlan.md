# Action Plan: BUS-310 - Open Job Requisitions Dashboard

**Ticket URL**: https://taylormorrison.atlassian.net/browse/BUS-310
**Status**: Backlog (Previously Blocked - NOW UNBLOCKED!)
**Priority**: Medium
**Current Assignee**: Pete Gonzales
**Created**: 2023-02-16 (2+ years old)
**Last Updated**: 2025-10-15

---

## 🎉 IMPORTANT UPDATE: BLOCKERS ARE ACTUALLY RESOLVED!

After investigating the blocker tickets, I discovered that **TWO of THREE blockers are actually complete** - they just were never closed in Jira!

### ✅ DATA-490: COMPLETE (as of June 2023)
- **Status in Jira**: Unscheduled (INCORRECT - should be closed)
- **Reality**: Doug Meinert confirmed completion on 2023-06-23
- **Data Location**: `[TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]`
- **Action Required**: Close this ticket in Jira

### ✅ DATA-7020: COMPLETE (as of September 2025)
- **Status in Jira**: Unscheduled (INCORRECT - should be closed)
- **Reality**: Vishnu confirmed "Changes are implemented" on 2025-09-16
- **Access Granted**: TWC\BI_DEV and TWC\BI_DEV_READ have access
- **Database**: TaylormorrisonDWH_Bronze_Encrypted
- **Action Required**: Close this ticket in Jira

### ⚠️ BUS-597: PARTIALLY BLOCKED
- **Status**: On Hold / Blocked
- **Issue**: Missing "Create Job Requisition Reason" field from Workday
- **Last Update**: September 18, 2023
- **Pete's Note**: "Data Extract is partially completed, still need 'Create Job Requisition Reason' which was being brought in via API call"
- **Decision Needed**: Can we build v1 without this field?

---

## Executive Summary

This ticket has been in backlog for 2+ years due to perceived data blockers. **However, the data is actually available** in the Data Warehouse since 2023! The primary blocker now is determining if we can proceed without the "Job Requisition Reason" field, or if we need to wait for Workday to expose it.

**Recommendation**: Test data access this week and build a v1 dashboard with available fields, then add "Requisition Reason" in v2 if/when it becomes available.

---

## THIS WEEK - IMMEDIATE ACTION PLAN

### Step 1: Verify Data Access (30 minutes)

Run these SQL queries to confirm you can access the Workday data:

```sql
-- Test 1: Check Data Lake access
SELECT TOP 100 *
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
ORDER BY 1 DESC;

-- Test 2: Check Bronze Encrypted access (view names may vary)
USE [TaylormorrisonDWH_Bronze_Encrypted];
GO

-- List all Workday views/tables
SELECT
    SCHEMA_NAME(schema_id) AS SchemaName,
    name AS ObjectName,
    type_desc
FROM sys.objects
WHERE SCHEMA_NAME(schema_id) = 'WorkDay'
ORDER BY name;

-- Test 3: Check row count
SELECT COUNT(*) AS RowCount
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition];
```

**If queries work**: Proceed to Step 2
**If queries fail**: Contact Vishnu Veeragoni to request access to TWC\BI_DEV group

### Step 2: Explore Available Data (1 hour)

```sql
-- Get column list and sample data
SELECT TOP 10 *
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition];

-- Check for key fields mentioned in requirements
-- Look for: Job ID, Position Title, Department, Status, Hiring Manager, etc.

-- Export to CSV for detailed analysis
```

Create a data dictionary documenting:
- All available fields
- Field definitions
- Sample values
- Data quality notes
- Missing fields (especially "Job Requisition Reason")

### Step 3: Review Attachments from Jira

Download these files from https://taylormorrison.atlassian.net/browse/BUS-310:
- `image-20251015-212108.png` - Recent screenshot
- `Original Current Report example.jpg` - Current report they use

Also from DATA-490 (https://taylormorrison.atlassian.net/browse/DATA-490):
- `Open_Job_Requisitions (2).xlsx` - Original report example
- `Open_Req_Candidate_Status (1).xlsx` - Candidate status report

**Review these to understand**:
- What fields they currently see
- Layout and design expectations
- Key metrics and KPIs
- Filters and slicers needed

### Step 4: Contact Pete Gonzales (Email)

**Subject**: BUS-310 - Open Job Requisitions Dashboard - Ready to Start!

```
Hi Pete,

Great news! I've been investigating BUS-310 (Open Job Requisitions Dashboard) and discovered
that the data blockers are actually resolved:

- DATA-490: Data loaded to TaylorMorrisonDataLake.WorkDay.Get_Job_Requisition (June 2023)
- DATA-7020: BI team granted access to Bronze Encrypted views (September 2025)

Both tickets were completed but never closed in Jira.

I'm ready to start building this dashboard. I have a few questions:

1. Is this still a priority for 2026?
2. Can we build version 1 without the "Job Requisition Reason" field?
   (This field is still unavailable from Workday per BUS-597)
3. Do you still have the requirements from the 2023 kickoff meeting?
4. Are David Watkins and Roberto Lee still the stakeholders?
5. Should this ticket be reassigned to me?

I can start data exploration this week and have a mockup ready within 1-2 weeks.

Let me know your thoughts!

Thanks,
Joey Ramos
```

### Step 5: Close Completed Blocker Tickets

Update these tickets in Jira:
- **DATA-490**: Change status to "Done", add comment: "Confirmed complete as of 2023-06-23 per Doug Meinert's comment. Data available at [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]"
- **DATA-7020**: Change status to "Done", add comment: "Confirmed complete as of 2025-09-16 per Vishnu's comment. Access granted to TWC\BI_DEV and TWC\BI_DEV_READ."

---

## FULL PROJECT PLAN

### Phase 1: Data Exploration & Analysis (Week 1)
**Duration**: 1 week
**Status**: Ready to start NOW

**Tasks**:
1. ✅ Verify data access (see Step 1 above)
2. ✅ Explore available data fields
3. ✅ Create data dictionary
4. ✅ Download and review all Jira attachments
5. ✅ Document missing fields
6. ✅ Export sample data for mockup
7. ✅ Contact Pete Gonzales for requirements

**Deliverables**:
- `BUS-310_DataDictionary.md` - Complete field documentation
- `BUS-310_SampleData.xlsx` - Sample data extract
- `BUS-310_Requirements.md` - Updated requirements doc

### Phase 2: Power BI Mockup (Week 2)
**Duration**: 1 week
**Prerequisites**: Pete confirms go-ahead, requirements clarified

**Tasks**:
1. Design dashboard layout (multiple pages)
   - Page 1: Open Job Requisitions Summary
   - Page 2: Requisitions by Department
   - Page 3: Requisitions by Hiring Manager
   - Page 4: Candidate Status (if data available)
2. Create mockup with sample data
3. Add key visuals:
   - Total open requisitions (card)
   - Requisitions by status (bar chart)
   - Requisitions by department (bar chart)
   - Aging analysis (how long open)
   - Trend over time (line chart)
4. Add filters/slicers:
   - Date range
   - Department
   - Status
   - Hiring Manager
5. Share mockup with Pete/stakeholders

**Deliverables**:
- `BUS-310_Mockup_v1.pbix` - Initial mockup

### Phase 3: Silver/Gold Layer Design (Week 2-3)
**Duration**: 3-5 days (parallel with mockup review)
**Prerequisites**: Mockup feedback received

**Tasks**:
1. Design target schema for Gold layer
2. Write SQL view definitions
3. Document business rules:
   - What constitutes "Open" requisition?
   - How to calculate "days open"?
   - Status definitions
   - Hiring manager attribution
4. Create test queries
5. Submit to data team for implementation

**Deliverables**:
- `BUS-310_GoldLayerSchema.sql` - View definitions
- `BUS-310_BusinessRules.md` - Business logic doc

### Phase 4: Production Dashboard Development (Week 3-4)
**Duration**: 1-2 weeks
**Prerequisites**: Gold layer views created, access granted

**Tasks**:
1. Connect to Gold layer views
2. Build production data model
3. Create relationships to other tables:
   - Employee/People data
   - Department reference
   - Division reference
4. Implement DAX measures:
   - Total open requisitions
   - Average days open
   - Requisitions by status %
   - Period over period changes
5. Build dashboard pages (refine mockup)
6. Apply company branding
7. Implement Row-Level Security (RLS):
   - HR can see all
   - Managers see their department only
   - Others based on requirements
8. Add bookmarks/navigation
9. Optimize performance

**Deliverables**:
- `OpenJobRequisitions.pbix` - Production file

### Phase 5: Testing & Deployment (Week 5)
**Duration**: 3-5 days
**Prerequisites**: Development complete

**Tasks**:
1. Internal testing
2. User Acceptance Testing (UAT) with Pete
3. Stakeholder review (David Watkins, Roberto Lee)
4. Incorporate feedback
5. Publish to Power BI Service
6. Configure scheduled refresh
7. Set workspace permissions
8. Test RLS with different users
9. Add to Power BI Launch Page
10. Create user documentation

**Deliverables**:
- Production dashboard deployed
- `BUS-310_UserGuide.md` - End user guide
- Launch Page entry

---

## Data Access Verification Checklist

- [ ] Can query TaylorMorrisonDataLake.WorkDay.Get_Job_Requisition
- [ ] Can query TaylormorrisonDWH_Bronze_Encrypted views
- [ ] Understand all available fields
- [ ] Identify missing "Job Requisition Reason" field
- [ ] Export sample data successfully
- [ ] Document data quality issues
- [ ] Confirm data refresh frequency
- [ ] Have access to create Gold layer views (or know who to contact)

---

## Requirements Checklist

From original ticket description:
- [ ] Staff Fields (need to clarify what these are)
- [ ] Open Job Requisitions count
- [ ] Job Requisition details
- [ ] Candidate Status (separate report mentioned)

From Pete's comments:
- [ ] "Create Job Requisition Reason" field (currently missing!)
- [ ] Data from both reports: Open_Job_Requisitions & Open Req Candidate Status

To clarify with stakeholders:
- [ ] Required filters/slicers
- [ ] Key metrics/KPIs
- [ ] Who needs access?
- [ ] RLS requirements
- [ ] Refresh frequency needed
- [ ] Integration with existing reports

---

## Key Stakeholders & Contacts

| Name | Role | Email | Notes |
|------|------|-------|-------|
| **Pete Gonzales** | Original Owner | pgonzales@taylormorrison.com | Requirements owner, needs to confirm priority |
| **David Watkins** | Business Stakeholder | TBD | Mentioned in 2023 kickoff |
| **Roberto Lee** | Business Stakeholder | TBD | Mentioned in 2023 kickoff |
| **Samantha Tran** | Data Team | TBD | Last commenter on JSON data issue |
| **Vishnu Veeragoni** | DBA/Data Engineer | TBD | Implemented DATA-490 and DATA-7020 |
| **Doug Meinert** | DBA | TBD | Confirmed DATA-490 completion |
| **Andrew Magallanes** | Workday Contact | TBD | Contact for Workday questions |

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Cannot access data after all | Low | High | Test access first; contact Vishnu if needed |
| Missing "Requisition Reason" field | High | Medium | Build v1 without it; add in v2 later |
| Requirements changed since 2023 | High | Medium | Re-engage Pete/stakeholders before building |
| Pete no longer owns this | Medium | Medium | Get reassignment or new owner |
| No business sponsor/priority | Medium | High | Confirm priority before investing time |
| Data quality issues | Medium | Medium | Document issues; work with data team |

---

## Success Criteria

### Phase 1 Success:
✅ Data access confirmed
✅ All fields documented
✅ Sample data exported
✅ Attachments reviewed
✅ Pete confirms priority and requirements

### Phase 2 Success:
✅ Mockup created and shared
✅ Stakeholder feedback received
✅ Design approved to proceed

### Phase 3 Success:
✅ Gold layer views created
✅ Business rules documented
✅ Data team sign-off

### Phase 4 Success:
✅ Production dashboard built
✅ All requirements implemented
✅ Performance optimized
✅ RLS tested

### Phase 5 Success:
✅ UAT passed
✅ Stakeholders approve
✅ Published to production
✅ Scheduled refresh working
✅ Users trained
✅ Documentation complete
✅ Ticket closed

---

## Estimated Timeline

| Phase | Duration | Start | Dependencies |
|-------|----------|-------|--------------|
| **Phase 1: Data Exploration** | 1 week | Week 1 | Data access only |
| **Phase 2: Mockup** | 1 week | Week 2 | Pete approval |
| **Phase 3: Gold Layer** | 3-5 days | Week 2-3 | Data team availability |
| **Phase 4: Development** | 1-2 weeks | Week 3-4 | Gold layer complete |
| **Phase 5: Deployment** | 3-5 days | Week 5 | UAT approval |
| **TOTAL** | **5 weeks** | Start ASAP | Pete confirms priority |

**Best Case**: 4 weeks (if everything goes smoothly)
**Realistic**: 5-6 weeks (with some delays/revisions)
**Worst Case**: 8+ weeks (if blockers or requirements issues)

---

## Files to Create

### This Week:
1. ✅ `BUS-310_ActionPlan.md` (this file)
2. `BUS-310_DataDictionary.md` - Document all available fields
3. `BUS-310_SampleData.xlsx` - Export sample data
4. `BUS-310_FieldMapping.xlsx` - Map available fields to requirements

### Week 2:
5. `BUS-310_Requirements.md` - Updated requirements after Pete meeting
6. `BUS-310_Mockup_v1.pbix` - Power BI mockup

### Week 2-3:
7. `BUS-310_GoldLayerSchema.sql` - SQL view definitions
8. `BUS-310_BusinessRules.md` - Business logic documentation

### Week 3-4:
9. `OpenJobRequisitions.pbix` - Production Power BI file
10. `BUS-310_TestCases.xlsx` - Testing scenarios

### Week 5:
11. `BUS-310_UserGuide.md` - End user documentation
12. `BUS-310_TechnicalDoc.md` - Technical documentation

---

## Questions to Answer Before Starting

### Data Questions:
1. ✅ Is the data in TaylorMorrisonDataLake accessible? (TEST THIS WEEK)
2. ✅ What fields are available? (DOCUMENT THIS WEEK)
3. ❓ Can we work without "Job Requisition Reason"? (ASK PETE)
4. ❓ How often does Workday data refresh?
5. ❓ Are there data quality issues?

### Requirements Questions:
6. ❓ Is this still a priority in 2026? (ASK PETE)
7. ❓ What are "Staff Fields"? (ASK PETE)
8. ❓ Who are the current stakeholders?
9. ❓ What does "Open" requisition mean exactly?
10. ❓ Do we need historical data or just current?

### Project Questions:
11. ❓ Should this be reassigned to Joey? (ASK PETE)
12. ❓ What's the target delivery date?
13. ❓ Is there budget/priority for Gold layer work?
14. ❓ Who will maintain this after delivery?

---

## Decision Log

| Date | Decision | Made By | Rationale |
|------|----------|---------|-----------|
| 2026-02-02 | Proceed with data access verification | Joey Ramos | Blockers are resolved; safe to test |
| TBD | Build v1 without Requisition Reason | TBD | Depends on Pete's input |
| TBD | Reassign to Joey or keep with Pete | TBD | Depends on Pete's availability |

---

## Next Steps Summary

### TODAY:
1. ✅ Create this action plan (DONE)
2. Run SQL queries to test data access
3. Document findings

### THIS WEEK:
1. Verify data access
2. Create data dictionary
3. Download Jira attachments
4. Email Pete Gonzales
5. Close completed blocker tickets (DATA-490, DATA-7020)

### NEXT WEEK:
1. Review requirements with Pete
2. Start mockup (if approved)
3. Begin Gold layer design

---

## Notes

- This ticket has been open since 2023 but blockers were actually resolved in 2023 and 2025
- The missing "Job Requisition Reason" field may not be a blocker if stakeholders approve building without it
- Priority confirmation from Pete is critical before investing time
- If Pete is no longer available, ticket may need a new owner
- Consider whether this ticket should be closed if requirements have changed significantly

---

**Status**: ✅ READY TO START - Data blockers resolved
**Next Action**: Test data access with SQL queries above
**Owner**: Joey Ramos (pending reassignment from Pete)
**Created by**: Claude Code
**Date**: 2026-02-02
**Last Updated**: 2026-02-02

