SELECT *
FROM owid_covid_data
--ORDER BY 3, 4

/*Looking at Total Cases vs Total Deaths
Shows likelihood of dying if you contrat covid in your country
*/
SELECT continent, location, date, total_cases, total_deaths, (total_deaths/total_cases) * 100 AS DeathPercent
FROM owid_covid_data
WHERE total_cases <> 0 AND location like '%states%'
ORDER BY 1, 2

/*Looking at Total Cases vs Population
Shows the Percentage of population that have contracted covid to that date
*/
SELECT continent, location, date, total_cases, population, ROUND((total_cases/population) * 100, 2) AS CovidPopPercent
FROM owid_covid_data
WHERE location LIKE '%states%'
ORDER BY 1, 2

/*Which countries have the highest infection rate
The immediate code below is a bit of data manipulation; I had to convert the data type into a useable format
*/
ALTER TABLE owid_covid_data
ALTER COLUMN population float

SELECT continent, location, MAX(total_cases) AS HighestInfection, population, MAX((total_cases/population) * 100) AS HighestCovidPercent
FROM owid_covid_data
WHERE population <> 0
GROUP BY location, population
ORDER BY 2 DESC

-- Showing countries with Highest Death Count per Population; created a view to round the max percents for readability
CREATE VIEW RoundedPercents AS
SELECT location, MAX(total_cases) AS HighestInfection, MAX(total_deaths) AS HighestDeaths, population, MAX((total_deaths/population) * 100) AS HighestDeathPercent, MAX((total_deaths/total_cases) * 100) AS DeathCasePercent
FROM owid_covid_data
WHERE population <> 0 and total_cases <> 0
GROUP BY location, population

SELECT location, HighestInfection, HighestDeaths, population, ROUND(HighestDeathPercent, 2) AS HighestDeathPercent, ROUND(DeathCasePercent, 2) AS DeathCasePercent
FROM RoundedPercents
ORDER BY 3 DESC



/*Show continents with the highest death count per population
where condition excludes the extraneous data
*/
SELECT continent, MAX(total_deaths) AS TotalDeathCount
FROM owid_covid_data
WHERE continent <> ' '
GROUP BY continent
ORDER BY 2 DESC

-- Global cases
ALTER TABLE owid_covid_data
ALTER COLUMN new_cases float

ALTER TABLE owid_covid_data
ALTER COLUMN new_deaths float

/*Displays global percent with respect to each day
If total numbers were unavailable:
*/
SELECT date, SUM(new_cases) AS TotalCases, SUM(new_deaths) AS TotalDeaths, SUM(new_deaths)/SUM(new_cases) * 100 AS DeathPercent 
FROM owid_covid_data
WHERE new_cases <> 0 and continent <> ' '
GROUP BY date
ORDER BY 1, 2

-- Displays only the global percentage
SELECT SUM(new_cases) AS TotalCases, SUM(new_deaths) AS TotalDeaths, SUM(new_deaths)/SUM(new_cases) * 100 AS DeathPercent 
FROM owid_covid_data
WHERE new_cases <> 0 and continent <> ' '
ORDER BY 1, 2

--- Looking at Vaccinations
SELECT continent, location, date, population, new_vaccinations
FROM owid_covid_data
WHERE continent <> ' '
ORDER BY 1, 2, 3

/*Total population vs Vaccinations
Casting in this case to avoid directly changing the table.
*/
SELECT continent, location, date, population, new_vaccinations, 
	SUM(CAST(new_vaccinations AS float)) OVER (PARTITION BY location ORDER BY location, date) AS RollingVaccs
FROM owid_covid_data
WHERE continent <> ' '
ORDER BY 1, 2, 3

-- Joining the above query with the View created earlier to compare vaccinations with death counts
SELECT continent, cov.location, date, cov.population, cov.total_deaths, cov.total_cases, new_vaccinations, 
	SUM(CAST(new_vaccinations AS float)) OVER (PARTITION BY cov.location ORDER BY cov.location, date) AS RollingVaccs,
	SUM(HighestDeathPercent) OVER (PARTITION BY rou.location ORDER BY rou.location, cov.date) AS RollingDeathPercent,
	SUM(DeathCasePercent) OVER (PARTITION BY rou.location ORDER BY rou.location, cov.date) AS RollingDeathCasePercent,
	rou.HighestInfection, rou.HighestDeaths, ROUND(rou.HighestDeathPercent, 3) AS HighestDeathPercent,
	ROUND(rou.DeathCasePercent, 3) AS DeathCasePercent
FROM owid_covid_data cov
FULL JOIN RoundedPercents rou
	ON cov.location = rou.location 
WHERE continent <> ' ' AND rou.population <> 0 and total_cases <> 0
ORDER BY 2, 3


-- Two methods of creating a table; CTE and Creating a temporary table

/* CTE Method
Rounding functions for readability
*/
WITH PopVacJoin
AS
(
SELECT continent, cov.location, date, cov.population, cov.total_deaths, cov.total_cases, new_vaccinations, 
	SUM(CAST(new_vaccinations AS float)) OVER (PARTITION BY cov.location ORDER BY cov.location, date) AS RollingVaccs,
	SUM(HighestDeathPercent) OVER (PARTITION BY rou.location ORDER BY rou.location, cov.date) AS RollingDeathPercent,
	SUM(DeathCasePercent) OVER (PARTITION BY rou.location ORDER BY rou.location, cov.date) AS RollingDeathCasePercent,
	rou.HighestInfection, rou.HighestDeaths, ROUND(rou.HighestDeathPercent, 3) AS HighestDeathPercent,
	ROUND(rou.DeathCasePercent, 3) AS DeathCasePercent
FROM owid_covid_data cov
FULL JOIN RoundedPercents rou
	ON cov.location = rou.location 
WHERE continent <> ' ' AND rou.population <> 0 and total_cases <> 0
)

SELECT continent, location, date, population, total_deaths, total_cases, new_vaccinations, ROUND(RollingVaccs, 2) AS RollingVaccs,
ROUND(RollingDeathCasePercent, 2) AS RollingDeathCasePercent, HighestDeathPercent, DeathCasePercent
FROM PopVacJoin
ORDER BY 2, 3


-- Creating a temp table 
-- Drop Table query is used to delete table if any changes are needed; useful if the original dataset musn't be altered
DROP TABLE IF EXISTS PercentPopVac
CREATE TABLE PercentPopVac
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population float,
total_deaths float,
total_cases float,
new_vaccinations float,
RollingVaccs float,
RollingDeathPercent float,
RollingDeathCasePercent float,
HighestInfection float,
HighestDeaths float,
HighestDeathPercent float,
DeathCasePercent float
)

INSERT INTO PercentPopVac
SELECT continent, cov.location, date, cov.population, cov.total_deaths, cov.total_cases, new_vaccinations, 
	SUM(CAST(new_vaccinations AS float)) OVER (PARTITION BY cov.location ORDER BY cov.location, date) AS RollingVaccs,
	SUM(HighestDeathPercent) OVER (PARTITION BY rou.location ORDER BY rou.location, cov.date) AS RollingDeathPercent,
	SUM(DeathCasePercent) OVER (PARTITION BY rou.location ORDER BY rou.location, cov.date) AS RollingDeathCasePercent,
	rou.HighestInfection, rou.HighestDeaths, ROUND(rou.HighestDeathPercent, 3) AS HighestDeathPercent,
	ROUND(rou.DeathCasePercent, 3) AS DeathCasePercent
FROM owid_covid_data cov
FULL JOIN RoundedPercents rou
	ON cov.location = rou.location 
WHERE continent <> ' ' AND rou.population <> 0 and total_cases <> 0


SELECT continent, location, date, population, total_deaths, total_cases, new_vaccinations, ROUND(RollingVaccs, 2) AS RollingVaccs,
ROUND(RollingDeathCasePercent, 2) AS RollingDeathCasePercent, HighestDeathPercent, DeathCasePercent
FROM PercentPopVac
ORDER BY 2, 3


