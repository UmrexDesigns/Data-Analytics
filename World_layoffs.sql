SELECT * 
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT * 
FROM layoffs;

SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS 
(
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging
WHERE company = 'cazoo';

DELETE FROM layoffs_staging
WHERE id IN (
  SELECT id
  FROM (
    SELECT id,
           ROW_NUMBER() OVER (
             PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions
             ORDER BY id
           ) AS row_num
    FROM layoffs_staging
  ) t
  WHERE row_num > 1
);

SET SQL_SAFE_UPDATES = 0;
select * from layoffs_staging;


ALTER TABLE layoffs_staging
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;

select company, trim(company) 
from layoffs_staging;

update layoffs_staging
set company = trim(company); 

select distinct industry 
from layoffs_staging
order by 1;


select *
from layoffs_staging
Where industry like 'Crypto%';

update layoffs_staging
set industry = 'Crypto'
Where industry like 'Crypto%';

update layoffs_staging
set country = 'United States'
Where country like '%United State%';

select distinct (country) 
from layoffs_staging 
order by 1 ASC;


select `date`, 
str_to_date(`date`, '%m/%d/%Y') AS `date`
from layoffs_staging;

update layoffs_staging
set `date` = str_to_date(`date`, '%m/%d/%Y');

select distinct (company)
from layoffs_staging
order by 1 asc;

UPDATE layoffs_staging t1
JOIN (
    SELECT company, MAX(industry) AS industry
    FROM layoffs_staging
    WHERE industry IS NOT NULL AND industry <> ''
    GROUP BY company
) t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL OR t1.industry = '';



select *
from layoffs;

alter table layoffs_staging
modify column `date` date;

select *
from layoffs_staging;

delete 
from layoffs_staging
where total_laid_off is null
and percentage_laid_off is null;


DELETE FROM layoffs_staging
WHERE company IS NULL
  AND location IS NULL
  AND industry IS NULL
  AND total_laid_off IS NULL
  AND `date` IS NULL
  AND stage IS NULL
  AND country IS NULL
  AND funds_raised_millions IS NULL
  and id is null;
  
  alter table layoffs_staging
  drop column id;
  
  create table layoffs_staging_cleaned
  like layoffs_staging;
  
  select *
  from layoffs_staging_cleaned;
  
  insert into layoffs_staging_cleaned
  select *
  from layoffs_staging;
  
  select year(`date`), sum(total_laid_off)
  from layoffs_staging_cleaned
  group by year(`date`)
  order by 1 desc;