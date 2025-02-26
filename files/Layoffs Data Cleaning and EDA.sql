/*
Exploratory Analysis of 2020 - 2023 Layoffs Data

Data obtained from: https://www.kaggle.com/datasets/swaptr/layoffs-2022/code

Goal: 
To gain an understanding of global layoff patterns during 2020 - 2023, analyzing
the market impacts of COVID-19, identifying the most affected industries, and 
tracking the accumulated impact over time.
      
Steps to take: 
1. Clean the data in preparation for analysis.
2. Perform exploratory data analysis on cleansed data.
       
Limitations: 
The dataset has missing values for the number of employees laid off and layoff 
percentages for some companies. Additionally, it contains a disproportionately 
higher volume of US company data compared to other countries, which would skew 
the results, making it appear as though other countries were significantly less 
impacted than the US.
This discrepancy may stem from data collection being primarily conducted in the 
US, making it easier to gather US-based data than data from other countries. 
To improve accuracy, we could expand the project's scope and incorporate a more 
diverse set of data collection tools, ensuring better representation of companies 
from different regions.
*/

/* First we create a working table from the raw table so that raw data is untouched */
DROP TABLE IF EXISTS layoffs;

CREATE TABLE layoffs
LIKE layoffs_raw;

INSERT INTO layoffs
SELECT * 
FROM layoffs_raw; 

/* ==================================
  Data Cleaning Planning
  1. Remove duplicates
  2. Standardize data and formats
  3. Check for NULL and empty values and try to populate
  4. Remove any unnecessary columns
 ===================================*/

/* 1. Remove duplicates */
/* Check for duplicate rows and remove the duplicate */
WITH duplicate_cte AS (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY company, location, industry, total_laid_off, 
                         percentage_laid_off, `date`, stage, country, 
                         funds_raised_millions
            ORDER BY company
        ) AS row_num
    FROM layoffs
)

DELETE FROM layoffs 
WHERE company 
IN (
	SELECT company 
    FROM duplicate_cte 
    WHERE row_num > 1
);

/* 2. Standardize data and formats */
/* Checking each column manually to see format issues
SELECT DISTINCT country
FROM layoffs
ORDER BY 1;
*/

/* Detected some extra white spaces on company names */
UPDATE layoffs
SET company = TRIM(company);

/* Detected duplicate Crypto industries */
UPDATE layoffs
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

/* Standardize date format to date instead of text */
UPDATE layoffs
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs
MODIFY COLUMN `date` DATE;

/* Detected trailing . for country United States */
UPDATE layoffs
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE ('United States%');

/* 3. Check for NULL and empty values and try to populate */
/* Checking each column manually to see format issues
SELECT DISTINCT *
FROM layoffs
WHERE funds_raised_millions IS NULL OR funds_raised_millions = ''
ORDER BY 1;
*/

/* 
Populate `industry` if there are matching entries(same company and location)
that has industry populated
*/
UPDATE layoffs AS t1
JOIN layoffs AS t2
	ON t1.company = t2.company
	AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL AND t2.industry != '');

/* Only 1 company still have NULL industry */
UPDATE layoffs
SET industry = 'Other'
WHERE company = 'Bally''s Interactive';

/* 
Unfortunately since there is no num_of_employees column so 
cannot populate total_laid_off or percentage_laid_off even
with one of these columns populated
*/

/*
There are also some NULL values in `stage` and `funds_raised_millions`
columns but these are acceptable
*/

/* 4. Remove any unnecessary columns */
/* Remove columns where both total_laid_off and percentage_laid_off are NULL */
DELETE
FROM layoffs
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

/* =========================
  Exploratory Data Analysis 
 ========================= */

/* Total number of layoffs by country */
SELECT country, SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs
GROUP BY country
ORDER BY sum_total_laid_off DESC;

/* Check companies that laid off all employees(bankrupt) */
SELECT DISTINCT *
FROM layoffs
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

/* Total number of layoffs over the 2020 - 2023 period by company */
SELECT company, SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs
GROUP BY company
ORDER BY 2 DESC;

/* Total percentage of layoffs over the 2020 - 2023 period by industry */
SELECT industry, ROUND(SUM(percentage_laid_off), 2) AS sum_percentage_laid_off
FROM layoffs
GROUP BY industry
ORDER BY 2 DESC;

/* Total number of laid offs by year per industry */
SELECT YEAR(`date`) AS year_laid_off, industry, 
	SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs
GROUP BY year_laid_off, industry
ORDER BY 2 DESC;

/* Rolling total of layoffs by month */
WITH rolling_total_cte AS (
	SELECT SUBSTRING(`date`, 1, 7) AS month_laid_off, 
		SUM(total_laid_off) AS monthly_laid_off
	FROM layoffs
	WHERE `date` IS NOT NULL
	GROUP BY month_laid_off
	ORDER BY 1 ASC
)
SELECT month_laid_off, monthly_laid_off, SUM(monthly_laid_off) 
	OVER(ORDER BY month_laid_off) as rolling_total
FROM rolling_total_cte;

/* Companies with the highest number of employees laid off by year */
WITH company_year_cte (company, year_laid_off, total_laid_off) AS (
	SELECT company, YEAR(`date`) AS year_laid_off, SUM(total_laid_off)
    FROM layoffs
    GROUP BY company, year_laid_off
),
company_year_rank AS (
	SELECT *, DENSE_RANK() OVER (PARTITION BY year_laid_off 
		ORDER BY total_laid_off DESC) AS ranking
	FROM company_year_cte
	WHERE year_laid_off IS NOT NULL
	ORDER BY year_laid_off ASC
)
SELECT *
FROM company_year_rank
WHERE ranking <= 5; -- Check the top 5 companies each year