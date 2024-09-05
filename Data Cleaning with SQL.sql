-- DATA CLEANING IN SQL 

-- STEPS IN CLEANING THIS DATA 

-- 1. REMOVE DUPLICATES
-- 2. STANDARDIZE THE DATA
-- 3. REMOVE AND POPULATE NULLS AND BLANKS 
-- 4. REMOVE ANY COLUMN OR ROWS 


SELECT *
FROM layoffs;

-- CREATE A NEW TABLE FROM DATA AS WORK SHEET 

CREATE TABLE Layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging
;

INSERT Layoffs_staging
SELECT *
FROM layoffs;

-- REMOVE DUPLICATES 

SELECT *
FROM layoffs_staging
;

SELECT *, 
ROW_NUMBER () OVER(
PARTITION BY COMPANY, location, industry, total_laid_off, 
percentage_laid_off, `DATE`, stage, country, funds_raised_millions ) AS ROW_NUM
FROM layoffs_staging
;

WITH DUPLICATE_CTE AS
( SELECT *, 
ROW_NUMBER () OVER(
PARTITION BY COMPANY, location, industry, total_laid_off, 
percentage_laid_off, `DATE`, stage, country, funds_raised_millions ) AS ROW_NUM
FROM layoffs_staging
)

SELECT *
FROM DUPLICATE_CTE
WHERE ROW_NUM > 1 ;

-- DOUBLE CHECK TO CONFIRM YOUR DUPLICATES BEFORE REMOVING ANY 

SELECT *
FROM layoffs_staging
WHERE company = 'casper';

-- THE CODE BELOW IS WE ADDING A NEW COLUMN (ROW_NUM) TO THE TABLE SO WE DO BE ABLE TO FORMAT IT, WHEREBY WE CREATED A NEW TABLE FOR IT. --

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER () OVER(
PARTITION BY COMPANY, location, industry, total_laid_off, percentage_laid_off, `DATE`, stage, country, funds_raised_millions ) AS ROW_NUM
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1
;

-- DELETE DUPLICATES FROM TABLE 

DELETE 
FROM layoffs_staging2
WHERE row_num > 1
;

SELECT *
FROM layoffs_staging2
;

-- STANDARDIZING DATA 

-- TRIM WHITE SPACE 

SELECT COMPANY, TRIM(COMPANY)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET COMPANY = TRIM(COMPANY);

SELECT DISTINCT INDUSTRY
FROM layoffs_staging2
ORDER BY 1 ;

SELECT *
FROM layoffs_staging2
WHERE INDUSTRY LIKE 'crypto%';

UPDATE layoffs_staging2
SET INDUSTRY = 'Crypto'
WHERE INDUSTRY LIKE 'crypto%';

SELECT DISTINCT country
FROM layoffs_staging2
order by 1
;

UPDATE layoffs_staging2
SET COUNTRY = 'United States'
WHERE COUNTRY LIKE 'United States%';

-- FIX DATE 
-- FIXING THE DATE BY CHANGING THE DATA TYPE AND FORMAT 

SELECT *
FROM layoffs_staging2;

SELECT `date`
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

-- CHANGE DATE DATA TYPE

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- REMOVE NULL AND BLANK VALUES 

-- POPULATING DATA 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT * 
FROM layoffs_staging2
WHERE INDUSTRY IS NULL 
OR INDUSTRY = '' ;

UPDATE layoffs_staging2
SET INDUSTRY = NULL 
WHERE INDUSTRY = '';

SELECT T1.INDUSTRY, T2.INDUSTRY
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.COMPANY = T2.COMPANY
WHERE T1.INDUSTRY IS NULL AND T2.INDUSTRY IS NOT NULL;

UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
SET T1.industry = T2.industry
WHERE T1.industry is null and T2.industry is not null ;

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

-- DELETE BLANKS OF UN-RELEVANT INFORMATION TO DATA

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

-- DROP ROW_NUM COLUMN WE ADDED EARLIER

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;



