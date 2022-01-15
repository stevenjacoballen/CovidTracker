/*
Covid 19 Data Exploration 
Skills used: Joins, CTE's, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types
Tableau Dashboard: https://public.tableau.com/app/profile/steven.allen/viz/COVID_Tracker/Dashboard2
*/

-- Fields containing "new" in their name (ie - new_cases) refer to daily counts.
-- Fields containing "total" in their name (ie - total cases) refer to total counts.

-- New Cases vs Total Cases
DROP VIEW IF EXISTS new_cases_vs_total_cases 
GO 
CREATE VIEW new_cases_vs_total_cases AS 
SELECT 
  location, 
  date, 
  population, 
  total_cases, 
  new_cases 
FROM 
  [Portfolio Project]..CovidDeaths 
ORDER BY 
  1, 
  2 OFFSET 0 ROWS 
  GO



-- New Vaccines vs Total Vaccines
-- NOTE: new_vaccines does NOT include people who are fully vaccinated, as can be seen in the next query.
DROP VIEW IF EXISTS new_vax_vs_total_vax 
GO 
CREATE VIEW new_vax_vs_total_vax AS 
SELECT 
  location, 
  date, 
  total_vaccinations, 
  new_vaccinations 
FROM 
  [Portfolio Project]..CovidVaccinations 
ORDER BY 
  1, 
  2 OFFSET 0 ROWS 
  GO



-- New Cases vs New Full Vaccinations
-- Using CTE to calculate population %age that is becoming fully vaccinated on a daily basis.
DROP VIEW IF EXISTS new_cases_vs_new_full_vax 
GO 
CREATE VIEW new_cases_vs_new_full_vax AS 
WITH cases_v_full_vax (
    location, population, date, new_cases, 
    people_fully_vaccinated, new_people_fully_vaccinated
  ) AS (
    SELECT 
      deaths.location, 
      deaths.population, 
      deaths.date, 
      deaths.new_cases, 
      vaccines.people_fully_vaccinated, 
      people_fully_vaccinated - lag(people_fully_vaccinated, 1, 0) OVER (
        PARTITION BY deaths.location 
        ORDER BY 
          deaths.location, 
          deaths.date
      ) AS new_people_fully_vaccinated 
    FROM 
      [Portfolio Project]..CovidDeaths deaths 
      JOIN [Portfolio Project]..CovidVaccinations vaccines ON deaths.location = vaccines.location 
      AND deaths.date = vaccines.date
  ) 
SELECT 
  *, 
  (
    new_people_fully_vaccinated / population
  ) * 100 AS daily_percent_pop_new_full_vax 
FROM 
  cases_v_full_vax 
  GO



-- New Deaths vs. Total Deaths
DROP VIEW IF EXISTS new_deaths_vs_total_deaths 
GO 
CREATE VIEW new_deaths_vs_total_deaths AS 
SELECT 
  location, 
  date, 
  total_deaths, 
  new_deaths 
FROM 
  [Portfolio Project]..CovidDeaths 
ORDER BY 
  1, 
  2 OFFSET 0 ROWS
  GO



-- New Deaths vs New Cases
DROP VIEW IF EXISTS new_deaths_vs_new_cases 
GO 
CREATE VIEW new_deaths_vs_new_cases AS 
SELECT 
  location, 
  date, 
  new_deaths, 
  new_cases 
FROM 
  [Portfolio Project]..CovidDeaths 
ORDER BY 
  1, 
  2 OFFSET 0 ROWS 
  GO



-- New Deaths vs New Fully Vaccinated People
DROP VIEW IF EXISTS new_deaths_vs_new_full_vax 
GO 
CREATE VIEW new_deaths_vs_new_full_vax AS 
SELECT 
  deaths.location, 
  deaths.date, 
  deaths.new_deaths, 
  vaccines.people_fully_vaccinated, 
  people_fully_vaccinated - lag(people_fully_vaccinated, 1, 0) OVER (
    PARTITION BY deaths.location 
    ORDER BY 
      deaths.location, 
      deaths.date
  ) AS new_people_fully_vaccinated 
FROM 
  [Portfolio Project]..CovidDeaths deaths 
  JOIN [Portfolio Project]..CovidVaccinations vaccines ON deaths.location = vaccines.location 
  AND deaths.date = vaccines.date 
ORDER BY 
  1 OFFSET 0 ROWS 
  GO



-- KPI's. Various KPIS to be placed in the top area of the Tableau Dashboard. 
DROP VIEW IF EXISTS kpi 
GO 
CREATE VIEW kpi AS 
SELECT 
  deaths.location, 
  MAX(population) AS population, 
  MAX(total_cases) AS total_cases, 
  (
    MAX(total_cases)/ MAX(population)
  )* 100 AS percent_case_pop, 
  (
    MAX(people_vaccinated)/ MAX(population)
  )* 100 AS percent_partial_vax, 
  (
    MAX(people_fully_vaccinated)/ MAX(population)
  )* 100 AS percent_fully_vax, 
  MAX(
    CAST(total_deaths AS bigint)
  ) AS total_deaths, 
  (
    MAX(
      CAST(total_deaths AS bigint)
    )/ MAX(total_cases)
  )* 100 AS percent_deaths_cases 
FROM 
  [Portfolio Project]..CovidDeaths deaths 
  JOIN [Portfolio Project]..CovidVaccinations vaccines ON deaths.location = vaccines.location 
GROUP BY 
  deaths.location 
ORDER BY 
  1 OFFSET 0 ROWS
  GO