-- Active: 1731956490102@@127.0.0.1@3306@world_layoffs
# Create a dummy table
CREATE TABlE layoffs_staging LIKE layoffs;

# Insert data into dummy table from original
INSERT layoffs_staging SELECT * FROM layoffs;

# Perform partition to find duplicate row, if row_num is more than 1
WITH
    duplicate_cte AS (
        SELECT *, ROW_NUMBER() OVER (
                PARTITION BY
                    company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
            ) AS row_num
        FROM layoffs_staging
    )
SELECT *
FROM duplicate_cte;

CREATE TABLE `layoffs_staging2` (
    `company` varchar(512) DEFAULT NULL,
    `location` varchar(512) DEFAULT NULL,
    `industry` varchar(512) DEFAULT NULL,
    `total_laid_off` int DEFAULT NULL,
    `percentage_laid_off` varchar(512) DEFAULT NULL,
    `date` varchar(512) DEFAULT NULL,
    `stage` varchar(512) DEFAULT NULL,
    `country` varchar(512) DEFAULT NULL,
    `funds_raised_millions` int DEFAULT NULL,
    `row_num` int DEFAULT NULL
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

INSERT INTO
    layoffs_staging2
SELECT *, ROW_NUMBER() OVER (
        PARTITION BY
            company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
FROM layoffs_staging;

DELETE FROM layoffs_staging2 WHERE row_num > 1;

# Remoove empty space from table
ALTER TABLE layoffs_staging2  DROP COLUMN row_num;
UPDATE layoffs_staging2 SET company = TRIM(company);

# Check for industry, we will notice there are "Crypto", "Crypto Currency", "CryptoCurrency" which indicate the same thing. We should clear them off
SELECT DISTINCT industry from layoffs_staging2;

UPDATE layoffs_staging2
SET
    industry = "Crypto"
WHERE
    industry LIKE "Crypto%";

SELECT * FROM layoffs_staging2 WHERE industry LIKE 'Crypto%';

# Check for country
SELECT DISTINCT
    country,
    TRIM(
        TRAILING '.'
        FROM country
    )
from layoffs_staging2;

UPDATE layoffs_staging2
SET
    country = TRIM(
        TRAILING '.'
        FROM country
    );

SELECT * FROM layoffs_staging2 WHERE industry LIKE 'Crypto%';

# Check for date
SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y') from layoffs_staging2;

UPDATE layoffs_staging2 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
# Edot table to fit new data type
ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;
# Check for NULL
SELECT *
FROM layoffs_staging2
WHERE
    industry IS NULL
    or industry = '';

SELECT *
FROM
    layoffs_staging2 t1
    JOIN layoffs_staging2 t2 ON t1.company = t2.company
WHERE (
        t1.industry IS NULL
        OR t1.industry = ''
    )
    AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
SET
    t1.industry = t2.industry
WHERE (
        t1.industry IS NULL
        OR t1.industry = ''
    )
    AND t2.industry IS NOT NULL;

# Clean null percentage_laid_off and total_laid_off
SELECT *
FROM layoffs_staging2
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;

DELETE FROM layoffs_staging2 WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;


