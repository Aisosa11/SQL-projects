
SELECT * FROM "Covid_death"
WHERE Continent is NOT NULL
ORDER BY 3,4;


---Total Deaths by locations
SELECT Location,MAX(cast(total_deaths as int)) AS Totaldeathcount
FROM "Covid_death"
--WHERE Location like '%States%'
WHERE Continent is NOT NULL
GROUP BY location
ORDER BY Totaldeathcount DESC;

---Total Deaths by continent
SELECT continent,MAX(cast(total_deaths as int)) AS Totaldeathcount
FROM "Covid_death"
--WHERE Location like '%States%'
WHERE Continent is NOT NULL
GROUP BY continent
ORDER BY Totaldeathcount DESC;

--Highest Death Count per location
SELECT Location,MAX(cast(total_deaths as int)) AS Totaldeathcount
FROM "Covid_death"
--WHERE Location like '%States%'
WHERE Continent is NOT NULL
GROUP BY location
ORDER BY Totaldeathcount DESC;

--Highest Death Count per location
SELECT Continent,MAX(cast(total_deaths as int)) AS Totaldeathcount
FROM "Covid_death"
--WHERE Location like '%States%'
WHERE Continent is NOT NULL
GROUP BY Continent
ORDER BY Totaldeathcount DESC;

--Total Cases Per Day
SELECT CAST(date AS DATE),SUM(new_cases)
FROM  "Covid_death"
GROUP BY CAST(date AS DATE)
ORDER BY 1,2

--DAILY TOTAL CASES VS POPULATION RATE PER LOCATION
SELECT Location,CAST(date AS DATE),Population,total_cases,(total_cases/Population)*100 AS cases_rate
FROM "Covid_death"
-- WHERE Location like '%States%'
WHERE Continent is NOT NULL
ORDER BY CAST(date AS DATE);

--TOTAL POPULATION VS VACCINATIONS
WITH POPVSVACC(continent,location,Date,Population,Vaccinations,ROLL_Vaccinated) 
AS
(
SELECT dea.continent,dea.location,CAST(dea.date AS DATE),dea.population,vac.new_vaccinations,
SUM(vac.new_vaccinations)OVER(PARTITION BY dea.location ORDER BY dea.location,CAST(dea.date AS DATE)) AS ROLL_Vaccinated
FROM "Covid_death" dea
JOIN "Covid_vaccinations" vac
on vac.location =dea.location
AND CAST(vac.date AS DATE) = CAST(dea.date AS DATE)
WHERE dea.Continent is NOT NULL
--ORDER BY 2,3
)
SELECT *,(ROLL_Vaccinated/population)*100 AS VaccinatedRate 
FROM POPVSVACC






