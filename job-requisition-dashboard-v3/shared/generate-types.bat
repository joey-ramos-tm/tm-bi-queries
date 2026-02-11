@echo off
REM Type Generation Script for Windows
REM Generates TypeScript types from Pydantic models

echo Generating TypeScript types from Pydantic models...

cd /d "%~dp0\.."

REM Create generated types file
(
echo /**
echo  * AUTO-GENERATED FILE - DO NOT EDIT
echo  * Generated from Pydantic models
echo  * Run `generate-types.bat` to regenerate
echo  */
echo.
echo // Placeholder types - will be replaced when Pydantic models are created
echo.
echo export interface RequisitionSummary {
echo   totalOpen: number;
echo   totalDepartments: number;
echo   totalLocations: number;
echo   avgDaysOpen: number;
echo }
echo.
echo export interface AgingBucket {
echo   range: string;
echo   count: number;
echo   percentage: number;
echo }
echo.
echo export interface DepartmentStats {
echo   department: string;
echo   count: number;
echo   avgDaysOpen: number;
echo }
echo.
echo export interface LocationStats {
echo   location: string;
echo   count: number;
echo   avgDaysOpen: number;
echo }
echo.
echo export interface TrendDataPoint {
echo   month: string;
echo   created: number;
echo   filled: number;
echo   stillOpen: number;
echo }
echo.
echo export interface RequisitionDetail {
echo   requisitionId: string;
echo   jobTitle: string;
echo   department: string;
echo   location: string;
echo   status: string;
echo   createdDate: string;
echo   daysOpen: number;
echo   reason: string;
echo   hiringManager: string;
echo }
echo.
echo export interface RequisitionListItem {
echo   requisitionId: string;
echo   jobTitle: string;
echo   department: string;
echo   location: string;
echo   status: string;
echo   daysOpen: number;
echo }
echo.
echo export interface PaginatedResponse^<T^> {
echo   data: T[];
echo   total: number;
echo   page: number;
echo   pageSize: number;
echo   totalPages: number;
echo }
) > frontend\src\types\generated.ts

echo TypeScript types generated successfully!
echo Output: frontend\src\types\generated.ts
